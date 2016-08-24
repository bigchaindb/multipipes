import signal
from contextlib import contextmanager


@contextmanager
def deadline(timeout=30):
    """Execute a block of code"""

    def _raise_timeout(signum, frame):
        raise TimeoutError

    if timeout:
        signal.signal(signal.SIGALRM, _raise_timeout)
        signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        yield
    finally:
        if timeout:
            signal.setitimer(signal.ITIMER_REAL, 0)
