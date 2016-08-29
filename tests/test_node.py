import pytest

import queue
import multiprocessing as mp

from multipipes import Node


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


def test_stop():
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

    node.stop()
    node.join()
    assert node.is_alive() is False


def test_stop_triggers_a_timeout():
    inqueue = mp.Queue()
    outqueue = mp.Queue()

    def double(val, timeout=None):
        if timeout:
            return 'TIMEOUT'
        return val * 2

    node = Node(double, inqueue, outqueue, timeout=1)
    node.start()

    inqueue.put(2)

    assert outqueue.get() == 4

    node.stop()
    node.join()

    assert node.is_alive() is False
    assert outqueue.get() == 'TIMEOUT'


def test_restart_change_pid():
    inqueue = mp.Queue()
    outqueue = mp.Queue()

    def double(val):
        return val * 2

    node = Node(double, inqueue, outqueue)
    node.start()
    pid = node.processes[0].pid
    assert pid

    node.restart()
    assert pid != node.processes[0].pid

    node.stop()
    node.join()


def test_node_counts_requests():
    events_queue = mp.Queue()
    inqueue = mp.Queue()
    outqueue = mp.Queue()

    def double(val):
        return val * 2

    node = Node(double, inqueue, outqueue)
    node.max_requests = 10
    node.start(events_queue=events_queue)

    for _ in range(10):
        inqueue.put(2)

    event = events_queue.get()
    assert event['type'] == 'max_requests'
    assert event['context'] == node.processes[0].pid

    node.restart()

    for _ in range(10):
        inqueue.put(2)
        outqueue.get()

    event = events_queue.get()
    assert event['type'] == 'max_requests'
    assert event['context'] == node.processes[0].pid

    node.stop()
