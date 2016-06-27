import time

from pipes import Pipeline


def emit():
    i = 0

    def _emit():
        time.sleep(1)
        nonlocal i
        i += 1
        return i
    return _emit


def test_mp():

    result = []

    def append(val):
        result.append(val)

    p = Pipeline([
        emit(),
        lambda x: x + 1,
        append
    ])

    p.start()

