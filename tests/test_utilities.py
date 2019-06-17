from __future__ import print_function
import signal
import functools
import threading
import platform


class TimeoutError(Exception):
    def __init__(self, seconds):
        super(TimeoutError, self).__init__(seconds)


def fail_after_timeout(seconds):
    """
    Returns a decorator that causes the wrapped function to asynchronously
    fail with a TimeoutError exception if the function takes too long to execute.

    TODO: Right now, timeout must be an int.  If we used signal.setitimer() instead
          of signal.alarm, we could probably support float, but I don't care right now.

    CAVEATS: Apparently signal.alarm() can only interrupt Python bytecode, so it can't
             be used to detect timeouts during C-functions, such as threading.Lock.acquire(), for example.
             Also, the function you're wrapping must only be called from the main thread.

    EXAMPLE: See example usage at the bottom of this file.
    """
    assert isinstance(seconds, int), "signal.alarm() requires an int"

    def decorator(func):
        if platform.system() == "Windows":
            # Windows doesn't support SIGALRM
            # Therefore, on Windows, we let this decorator be a no-op.
            return func

        def raise_timeout(signum, frame):
            raise TimeoutError(seconds)

        @functools.wraps(func)
        def fail_after_timeout_wrapper(*args, **kwargs):
            assert threading.current_thread().name == "MainThread", (
                "Non-Main-Thread detected: This decorator relies on the signal"
                " module, which may only be used from the MainThread"
            )

            old_handler = signal.getsignal(signal.SIGALRM)
            signal.signal(signal.SIGALRM, raise_timeout)

            # Start the timer, check for conflicts with any previous timer.
            old_alarm_time_remaining = signal.alarm(seconds)
            if old_alarm_time_remaining != 0:
                raise Exception("Can't use fail_after_timeout if you're already using signal.alarm for something else.")

            try:
                return func(*args, **kwargs)
            finally:
                # Restore old handler, clear alarm
                signal.signal(signal.SIGALRM, old_handler)
                signal.alarm(0)

        fail_after_timeout_wrapper.__wrapped__ = func  # Emulate python 3 behavior of @functools.wraps
        return fail_after_timeout_wrapper

    return decorator

import threading


class AtomicCounter:
    __slots__ = ('_lock', '_value')

    def __init__(self, lock = None):
        self._lock = lock or threading.Lock()
        self._value = 0

    @property
    def value(self):
        return self._value

    def increment(self):
        with self._lock:
            self._value += 1

    def decrement(self):
        with self._lock:
            self._value -= 1

    def __eq__(self, other):
        assert isinstance(other,  int)
        return self._value == other

from contextlib import contextmanager

class RWLock:
    def __init__(self, lock_factory=threading.Lock):
        self._wlock = lock_factory()
        self._rlock = lock_factory()
        self._readers_count = 0

    @property
    @contextmanager
    def reader(self):
        with self._rlock:
            self._readers_count += 1

            if self._readers_count == 1:
                self._wlock.acquire()

        yield self

        with self._rlock:
            self._readers_count -= 1

            if self._readers_count == 0:
                self._wlock.release()

    @property
    @contextmanager
    def writer(self):
        with self._wlock:
            yield self


from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import time

def test_thpoool():
    lock = RWLock()

    def work(n):
        with lock.reader:
            print('read some', flush=True)
            time.sleep(0.01)
            print('read done', flush=True)

    def work2():
        print("write enter", flush=True)
        with lock.writer:
            print('write some', flush=True)
            time.sleep(0.01)
            print('write done', flush=True)

    with ThreadPoolExecutor(max_workers=4) as ex:
        t = Thread(target=work2)
        res = ex.map(work, range(10))
        t.start()

    with ThreadPoolExecutor(max_workers=4) as ex:
        res = ex.map(work, range(10))


if __name__ == "__main__":
    import time

    @fail_after_timeout(2)
    def long_running_function():
        time.sleep(3)

    try:
        long_running_function()
    except TimeoutError as ex:
        print("Got TimeoutError, as expected")
    else:
        assert False, "Expected to catch a TimeoutError! Why didn't that happen?"
