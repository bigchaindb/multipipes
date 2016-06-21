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

    result = []
    def append(val):
        result.append(val)

    p = Pipeline(
        mapping={
            'emit': emit(4),
            'mul': mul(2),
            'inc': lambda x: x + 1,
            'sum': lambda x, y: x + y,
            'append': append,
        },
        dag=(
            ('emit', 'mul', 'inc'),
            ('mul', 'sum'),
            ('inc', 'sum'),
            ('sum', 'append'),
        )
    )

    p.step()
    assert result.pop() == 4

    p.step()
    assert result.pop() == 7

    p.step()
    assert result.pop() == 10

    p.step()
    assert result.pop() == 13

