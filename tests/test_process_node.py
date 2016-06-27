import multiprocessing as mp

from pipes.pipeline import ProcessNode


def test_target_callable_func_is_called():
    queue_in = mp.Queue()
    queue_out = mp.Queue()

    pn = ProcessNode('test', lambda x: x + 1, [queue_in], [queue_out])
    queue_in.put(0)
    pn.run()

    assert queue_out.get() == 1


def test_target_iterator_func_is_called():
    queue_in = mp.Queue()
    queue_out = mp.Queue()

    def gen(n):
        def _gen():
            yield n
            yield n + 1
            yield n + 2
        return _gen()

    pn = ProcessNode('test', gen, [queue_in], [queue_out])

    queue_in.put(10)

    pn.run()

    assert queue_out.get() == 10
    assert queue_out.get() == 11


def test_process_with_pool():
    pass
