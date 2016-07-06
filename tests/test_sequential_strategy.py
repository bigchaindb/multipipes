from itertools import count
from pipes import Pipeline


def emit():
    c = count()

    def _emit():
        return next(c)

    return _emit


def test_step():

    result = []

    def append(val):
        result.append(val)

    p = Pipeline([
        emit(),
        lambda x: x**2,
        append
    ])

    p.step()
    assert result == [0]

    p.step()
    assert result == [0, 1]

    p.step()
    assert result == [0, 1, 4]

    p.step()
    p.step()
    p.step()
    assert result == [0, 1, 4, 9, 16, 25]
