from pipes.pipeline import Collector
from pipes import Pipeline


def test_simple_sequence():

    def emit(amount):
        i = 0
        def _emit():
            nonlocal i
            if i < amount:
                i += 1
                return i
            raise StopIteration()
        return _emit

    def mul(factor):
        def _mul(number):
            return factor * number
        return _mul

    collector = Collector()

    p = Pipeline(
        mapping={
            'emit': emit(4),
            'mul': mul(2),
            'inc': lambda x: x + 1,
            'sum': lambda x, y: x + y,
            'collect': collector,
        },
        dag=(
            ('emit', 'mul', 'inc'),
            ('mul', 'sum'),
            ('inc', 'sum'),
            ('sum', 'collect'),
        )
    )

    p.run()

    assert collector.results == [4, 7, 10, 13]

