import signal
from uuid import uuid4
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


class PoisonPill:
    def __init__(self, uuid=None):
        if not uuid:
            uuid = uuid4()
        self.uuid = uuid

    def __eq__(self, other):
        return self.uuid == other.uuid

