import multiprocessing as mp

from pipes import Node


def test_target_callable_func_is_called():
    inqueue = mp.Queue()
    outqueue = mp.Queue()
    node = Node(lambda x: x + 1, 'test', inqueue, outqueue)
    inqueue.put(0)
    node.run()
    assert outqueue.get() == 1


def test_target_iterator_func_is_called():
    inqueue = mp.Queue()
    outqueue = mp.Queue()

    def gen(n):
        def _gen():
            yield n
            yield n + 1
            yield n + 2
        return _gen()

    node = Node(gen, 'test', inqueue, outqueue)
    inqueue.put(10)
    node.run()
    assert outqueue.get() == 10
    assert outqueue.get() == 11
    assert outqueue.get() == 12

