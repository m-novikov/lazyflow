###############################################################################
#   lazyflow: data flow based lazy parallel computation framework
#
#       Copyright (C) 2011-2014, the ilastik developers
#                                <team@ilastik.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the Lesser GNU General Public License
# as published by the Free Software Foundation; either version 2.1
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# See the files LICENSE.lgpl2 and LICENSE.lgpl3 for full text of the
# GNU Lesser General Public License version 2.1 and 3 respectively.
# This information is also available on the ilastik web site at:
# 		   http://ilastik.org/license/
###############################################################################
from __future__ import annotations

import atexit
import logging
import queue
import enum
import threading
from typing import Callable, List, Optional, Any, TypeVar

logger = logging.getLogger(__name__)
T = TypeVar("T")


class StopException(Exception):
    pass


class QueueObject:
    __slots__ = ("_obj", "_exc", "_priority")

    def __init__(self, *, obj: T = None, exc: Exception = None, priority: Optional[List[int]] = None) -> None:
        if (obj is None) == (exc is None):
            raise ValueError("Either obj or exc should be set")

        self._obj = obj
        self._exc = exc

        self._priority = priority or [0]

    def unwrap(self) -> T:
        if self._obj is not None:
            return self._obj

        raise self._exc  # type: ignore

    def __lt__(self, other: QueueObject):
        return self._priority < other._priority


class ThreadPool:
    """Manages a set of worker threads and dispatches tasks to them.

    Attributes:
        num_workers: The number of worker threads.
    """

    workers: List["_Worker"]

    _unassigned_tasks: queue.Queue[QueueObject]
    _ready_workers: queue.Queue[QueueObject]
    _scheduler: threading.Thread

    def __init__(self, num_workers: int) -> None:
        """Start all workers."""
        self._unassigned_tasks = queue.PriorityQueue()
        self._ready_workers = queue.Queue()

        self.workers = [_Worker(self, i) for i in range(num_workers)]
        for w in self.workers:
            w.start()

        atexit.register(self.stop)

        self._scheduler = threading.Thread(target=self._distribute_work, name="LazyflowSchedulerThread")
        self._scheduler.daemon = True
        self._scheduler.start()

    @property
    def queue_size(self) -> int:
        return self._unassigned_tasks.qsize()

    def _distribute_work(self) -> None:
        task = None

        while True:
            try:
                task = self._unassigned_tasks.get().unwrap()

                worker = getattr(task, "assigned_worker", None)

                if not worker:
                    worker = self._ready_workers.get().unwrap()

                worker.job_queue.put_nowait(QueueObject(obj=task))
            except StopException:
                logger.info("Stopping scheduler")
                break

            except Exception:
                logger.exception("Unhandled exception in the scheduler thread")

            finally:
                task = None

    @property
    def num_workers(self) -> int:
        return len(self.workers)

    def enqueue(self, task: Callable[[], None], priority: Optional[List[int]] = None) -> None:
        """
        Schedule the given task on the worker that is assigned to it.
        If it has no assigned worker yet, assign it to the first worker that becomes available.
        """
        self._unassigned_tasks.put_nowait(QueueObject(obj=task, priority=priority))

    def stop(self) -> None:
        """Stop all threads in the pool, and block for them to complete.

        Postcondition: All worker threads have stopped, unfinished tasks are simply dropped.
        """
        stop = QueueObject(exc=StopException(), priority=[-1])
        for w in self.workers:
            w.job_queue.put(stop)

        for w in self.workers:
            w.join()

        self._ready_workers.put(stop)
        self._unassigned_tasks.put(stop)
        self._scheduler.join()

    def get_states(self) -> List[str]:
        return [w.state for w in self.workers]

    def notify_ready(self, worker: "_Worker") -> None:
        self._ready_workers.put(QueueObject(obj=worker))


class _Worker(threading.Thread):
    """Run in a loop until stopped.

    The loop pops one task from the threadpool and executes it.
    """

    thread_pool: ThreadPool
    job_queue: queue.Queue[QueueObject]
    state: str

    def __init__(self, thread_pool: ThreadPool, index: int) -> None:
        super().__init__(name=f"Worker #{index}", daemon=True)
        self.thread_pool = thread_pool
        self.job_queue = queue.Queue()
        self.state = "initialized"

    def get_next_task(self) -> Callable[[], None]:
        self.thread_pool.notify_ready(self)
        return self.job_queue.get().unwrap()

    def run(self) -> None:
        """Keep executing available tasks until we're stopped."""
        # Try to get some work.

        logger.info("Started worker %s", self)

        while True:
            self.state = "waiting"

            next_task: Optional[Callable[[], None]] = None

            try:
                next_task = self.get_next_task()
                self.state = "running task"

                try:
                    next_task.assigned_worker = self
                except Exception:
                    logger.exception("Failed to assign worker to the task %s", next_task)
                    continue

                try:
                    next_task()
                except Exception:
                    logger.exception("Exception during processing %s", next_task)

            except StopException:
                logger.info("Stopping worker %s", self)
                break

            finally:
                self.job_queue.task_done()

        logger.info("Stopped worker %s", self)
