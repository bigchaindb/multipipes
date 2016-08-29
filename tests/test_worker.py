import pytest


def test_worker_runs_target_function():
    from multipipes import Worker, Pipe

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    worker = Worker(double, indata, outdata)

    indata.put(2)
    worker.run()
    assert outdata.get() == 4


def test_worker_returns_multiple_elements_when_iterator():
    from multipipes import Worker, Pipe

    indata = Pipe()
    outdata = Pipe()

    def unpack(x):
        for i in x:
            yield i

    worker = Worker(unpack, indata, outdata)

    indata.put([1, 2, 3])
    worker.run()
    assert outdata.get() == 1
    assert outdata.get() == 2
    assert outdata.get() == 3


def test_worker_triggers_a_timeout_and_sets_kwarg_to_true():
    from multipipes import Worker, Pipe

    indata = Pipe()
    outdata = Pipe()

    def add(x, y):
        if not x and not y:
            return 'TIMEOUT'
        else:
            return x + y

    worker = Worker(add, indata, outdata,
                    timeout=0.1)

    indata.put((1, 2))
    worker.run()
    assert outdata.get() == 3

    worker.run()
    assert outdata.get() == 'TIMEOUT'


def test_worker_triggers_deadline_when_slow():
    from multipipes import Worker, Pipe
    import time

    indata = Pipe()
    outdata = Pipe()

    def pow(x, y):
        time.sleep(1)
        return x ** y

    worker = Worker(pow, indata, outdata,
                    max_execution_time=0.1)

    indata.put((1, 2))
    with pytest.raises(TimeoutError):
        worker.run()


def test_worker_keyboard_interrupt_triggers_poison_pill():
    from multipipes import exceptions, Worker, Pipe

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        if x == 'keyboard interrupt':
            raise KeyboardInterrupt()
        return x * 2

    worker = Worker(double, indata, outdata)

    indata.put(1)
    indata.put('keyboard interrupt')
    with pytest.raises(exceptions.PoisonPillException):
        worker.run_forever()
    assert outdata.get(2)

