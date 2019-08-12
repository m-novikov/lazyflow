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

import atexit
import logging
import queue
import threading
from typing import Callable, List

logger = logging.getLogger(__name__)
STOP = object()


class ThreadPool:
    """Manages a set of worker threads and dispatches tasks to them.

    Attributes:
        num_workers: The number of worker threads.
    """

    def __init__(self, num_workers: int):
        """Start all workers."""
        self.unassigned_tasks = queue.PriorityQueue()
        self.ready_workers = queue.Queue()

        self.workers = [_Worker(self, i) for i in range(num_workers)]
        for w in self.workers:
            w.start()

        atexit.register(self.stop)

        self._scheduler = threading.Thread(target=self._distribute_work)
        self._scheduler.daemon = True
        self._scheduler.start()

    def _distribute_work(self):
        while True:
            try:
                task = self.unassigned_tasks.get()
                worker = getattr(task, "assigned_worker", None)

                if not worker:
                    worker = self.ready_workers.get()

                worker.job_queue.put_nowait(task)
                task = None
            except Exception as e:
                logger.exception("Exception in scheduler")

    @property
    def num_workers(self):
        return len(self.workers)

    def wake_up(self, task: Callable[[], None]) -> None:
        """Schedule the given task on the worker that is assigned to it.

        If it has no assigned worker yet, assign it to the first worker that becomes available.
        """
        self.unassigned_tasks.put_nowait(task)

    def stop(self) -> None:
        """Stop all threads in the pool, and block for them to complete.

        Postcondition: All worker threads have stopped, unfinished tasks are simply dropped.
        """
        for w in self.workers:
            w.job_queue.put(STOP)

        for w in self.workers:
            w.join()

    def get_states(self) -> List[str]:
        return [w.state for w in self.workers]


class _Worker(threading.Thread):
    """Run in a loop until stopped.

    The loop pops one task from the threadpool and executes it.
    """

    def __init__(self, thread_pool, index):
        super().__init__(name=f"Worker #{index}", daemon=True)
        self.thread_pool = thread_pool
        self.job_queue = queue.Queue()
        self.state = "initialized"

    def get_next_task(self):
        self.thread_pool.ready_workers.put(self)
        return self.job_queue.get()

    def run(self):
        """Keep executing available tasks until we're stopped."""
        # Try to get some work.

        logger.info("Started worker %s", self)

        while True:
            self.state = "waiting"
            next_task = self.get_next_task()

            self.state = "running task"

            if next_task is STOP:
                self.job_queue.task_done()
                break

            try:
                next_task.assigned_worker = self
            except Exception:
                logger.exception("Failed to assign worker to the task %s", task)

            try:
                next_task()
            except Exception:
                logger.exception("Exception during processing %s", next_task)
            finally:
                self.state = "freeing task"
                self.job_queue.task_done()
                # We're done with this request.
                # Free it immediately for garbage collection.
                next_task = None

        logger.info("Stopped worker %s", self)
