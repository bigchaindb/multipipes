import pytest


def test_task_runs_target_function():
    from multipipes import Task, Pipe

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    task = Task(double, indata, outdata)

    indata.put(2)
    task.step()
    assert outdata.get() == 4


def test_task_runs_target_function_and_count_requests():
    from multipipes import Task, Pipe

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    task = Task(double, indata, outdata)

    indata.put(2)
    indata.put(2)
    indata.put(2)

    task.step()
    task.step()
    task.step()

    assert outdata.get() == 4
    assert outdata.get() == 4
    assert outdata.get() == 4

    assert task.requests_count == 3


def test_task_returns_multiple_elements_when_iterator():
    from multipipes import Task, Pipe

    indata = Pipe()
    outdata = Pipe()

    def unpack(x):
        for i in x:
            yield i

    task = Task(unpack, indata, outdata)

    indata.put([1, 2, 3])
    task.step()
    assert outdata.get() == 1
    assert outdata.get() == 2
    assert outdata.get() == 3


def test_task_triggers_a_timeout_and_sets_kwarg_to_true():
    from multipipes import Task, Pipe

    indata = Pipe()
    outdata = Pipe()

    def add(x=None, y=None):
        if not x and not y:
            return 'TIMEOUT'
        else:
            return x + y

    task = Task(add, indata, outdata, read_timeout=0.1)

    indata.put((1, 2))
    task.step()
    assert outdata.get() == 3

    task.step()
    assert outdata.get() == 'TIMEOUT'


def test_task_triggers_deadline_when_slow():
    from multipipes import Task, Pipe
    import time

    indata = Pipe()
    outdata = Pipe()

    def pow(x, y):
        time.sleep(1)
        return x ** y

    task = Task(pow, indata, outdata, max_execution_time=0.1)

    indata.put((1, 2))
    with pytest.raises(TimeoutError):
        task.step()


def test_task_keyboard_interrupt_triggers_poison_pill():
    from multipipes import Task, Pipe

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        if x == 'keyboard interrupt':
            raise KeyboardInterrupt()
        return x * 2

    task = Task(double, indata, outdata)

    indata.put(1)
    indata.put('keyboard interrupt')
    task.run_forever()
    assert outdata.get(2)


def test_task_handles_max_request_count(monkeypatch):
    from multipipes import exceptions, Task, Pipe

    monkeypatch.setattr('multipipes.worker._randomize_max_requests',
                        lambda x: x)

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    task = Task(double, indata, outdata, max_requests=3)

    indata.put(1)
    indata.put(2)
    indata.put(3)

    with pytest.raises(exceptions.MaxRequestsException):
        task.run_forever()

    assert outdata.get() == 2
    assert outdata.get() == 4
    assert outdata.get() == 6


def test_worker_spawn_process():
    from multipipes import Pipe, Task, Worker

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    task = Task(double, indata, outdata)
    worker = Worker(task)
    worker.start()

    indata.put(1)
    assert outdata.get() == 2

    worker.stop()
    worker.join()

    assert not worker.is_alive()


def test_worker_restart_spawns_a_new_process():
    from multipipes import Pipe, Task, Worker

    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    task = Task(double, indata, outdata)
    worker = Worker(task)
    worker.start()

    original_pid = worker.pid

    indata.put(1)
    assert outdata.get() == 2

    worker.restart()

    assert worker.pid != original_pid

    indata.put(2)
    assert outdata.get() == 4

    worker.stop()
    worker.join()


# @pytest.mark.skipif(reason='Need to refactor how queue are accessed')
def test_worker_restarts_when_task_reaches_max_requests():
    from multipipes import Pipe, Task, Worker
    from multipipes.manager import Manager

    manager = Manager()
    indata = Pipe()
    outdata = Pipe()

    def double(x):
        return x * 2

    task = Task(double, indata, outdata,
                max_requests=3)
    worker = Worker(task, manager=manager)
    worker.start()

    original_pid = worker.pid

    indata.put(1)
    outdata.get()
    assert worker.pid == original_pid

    indata.put(1)
    outdata.get()
    assert worker.pid == original_pid

    indata.put(1)
    outdata.get()
    assert worker.pid == original_pid

    indata.put(2)
    outdata.get()
    new_pid = worker.pid
    assert new_pid != original_pid

    indata.put(2)
    outdata.get()
    assert worker.pid == new_pid

    indata.put(2)
    outdata.get()
    assert worker.pid == new_pid

    worker.stop()
    worker.join()
