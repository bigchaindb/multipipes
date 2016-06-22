import time

from pipes import Pipeline, MultiprocessPipeline


def emit():
    i = 0

    def _emit():
        time.sleep(1)
        nonlocal i
        i += 1
        return i
    return _emit


def test_simple_sequence():
    return

    result = []

    def append(val):
        result.append(val)

    p = Pipeline(
        mapping={
            'emit': emit(),
            'pow': lambda x: x**2,
            'inc': lambda x: x + 1,
            'sum': lambda x, y: x + y,
            'append': append,
        },
        dag=(
            ('emit', 'pow', 'inc'),
            ('pow', 'sum'),
            ('inc', 'sum'),
            ('sum', 'append'),
        )
    )

    p.step()
    assert result.pop() == 3

    p.step()
    assert result.pop() == 7

    p.step()
    assert result.pop() == 13

    p.step()
    assert result.pop() == 21


def test_mp():

    result = []

    def append(val):
        result.append(val)

    p = MultiprocessPipeline(
        mapping={
            'emit': emit(),
            'pow': lambda x: x**2,
            'inc': lambda x: x + 1,
            'sum': lambda x, y: x + y,
            'append': append,
        },
        dag=(
            ('emit', 'pow', 'inc'),
            ('pow', 'sum'),
            ('inc', 'sum'),
            ('sum', 'append'),
        )
    )

    p.start()

