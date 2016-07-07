import pytest

import queue
import multiprocessing as mp

from pipes import Node


def test_target_callable_func_is_called():
    inqueue = mp.Queue()
    outqueue = mp.Queue()
    node = Node(lambda x: x + 1, inqueue, outqueue)
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

    node = Node(gen, inqueue, outqueue)
    inqueue.put(10)
    node.run()
    assert outqueue.get() == 10
    assert outqueue.get() == 11
    assert outqueue.get() == 12


def test_join():
    inqueue = mp.Queue()
    outqueue = mp.Queue()

    def double(val):
        return val * 2

    node = Node(double, inqueue, outqueue)
    node.start()

    with pytest.raises(queue.Empty):
        outqueue.get(block=False)

    inqueue.put(2)
    assert outqueue.get() == 4

    node.terminate()


def test_poison_pill():
    inqueue = mp.Queue()
    outqueue = mp.Queue()

    def double(val):
        return val * 2

    node = Node(double, inqueue, outqueue)
    node.start()

    with pytest.raises(queue.Empty):
        outqueue.get(block=False)

    inqueue.put(2)

    assert outqueue.get() == 4

    node.poison_pill()
    node.join()
    assert node.is_alive() is False

