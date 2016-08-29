import pytest


@pytest.fixture
def indata_pipeline_outdata():
    from multipipes import Pipeline, Pipe, Node

    indata = Pipe()
    outdata = Pipe()

    def divide(a, b):
        return a / b

    def inc(n):
        return n + 1

    p = Pipeline([
        Node(divide),
        Node(inc, fraction_of_cores=1),
    ])

    p.setup(indata=indata, outdata=outdata)

    return (indata, p, outdata)


def test_pipeline_propagates_exception(indata_pipeline_outdata):
    indata, pipeline, outdata = indata_pipeline_outdata

    pipeline.start()
    indata.put((4, 0))
    pipeline.stop()
    assert len(pipeline.manager.errors) == 1


def test_pipeline_restarts(indata_pipeline_outdata):
    indata, pipeline, outdata = indata_pipeline_outdata
    pipeline.start()
    indata.put((4, 2))

    processes = [process for node in pipeline.nodes
                 for process in node.processes]

    pipeline.restart()

    # old processes should be gone
    assert all(not process.is_alive() for process in processes)

    # new processes should be all running
    assert all(process.is_alive() for node in pipeline.nodes
               for process in node.processes)

    pipeline.stop()


def test_pipeline_restarts_on_error(indata_pipeline_outdata):
    import time

    indata, pipeline, outdata = indata_pipeline_outdata

    pipeline.manager.restart_on_error = True
    pipeline.start()
    processes = [process for node in pipeline.nodes
                 for process in node.processes]
    indata.put((4, 0))
    time.sleep(0.1)

    assert len(pipeline.manager.errors) == 1
    assert isinstance(pipeline.manager.errors[0], ZeroDivisionError)

    # old processes should be gone
    assert all(not process.is_alive() for process in processes)

    alive = [(process.pid, process.is_alive()) for node in pipeline.nodes
             for process in node.processes]
    print(alive)

    # new processes should be all running
    assert all(process.is_alive() for node in pipeline.nodes
               for process in node.processes)

    indata.put((4, 2))
    assert outdata.get() == 3

    indata.put((4, 0))
    time.sleep(1)

    assert len(pipeline.manager.errors) == 2
    assert isinstance(pipeline.manager.errors[1], ZeroDivisionError)

    # old processes should be gone
    assert all(not process.is_alive() for process in processes)

    alive = [(process.pid, process.is_alive()) for node in pipeline.nodes
             for process in node.processes]
    print(alive)

    # new processes should be all running
    assert all(process.is_alive() for node in pipeline.nodes
               for process in node.processes)

    pipeline.stop()


def test_pipeline_kills_and_restarts_exhausted_workers(monkeypatch):
    monkeypatch.setattr('random.randint', lambda: 0)

    from multipipes import Pipeline, Pipe, Node

    indata = Pipe()
    outdata = Pipe()

    def divide(a, b):
        return a / b

    def inc(n):
        return n + 1

    pipeline = Pipeline([
        Node(divide, max_requests=10),
        Node(inc, max_requests=10, fraction_of_cores=1),
    ])

    pipeline.setup(indata=indata, outdata=outdata)

    pipeline.start()
    processes = [process for node in pipeline.nodes
                 for process in node.processes]

    for _ in range(100):
        indata.put((4, 2))
        outdata.get()

    assert any(not process.is_alive() for process in processes)

    # new processes should be all running
    assert all(process.is_alive() for node in pipeline.nodes
               for process in node.processes)

    pipeline.stop()


@pytest.mark.skipif(reason='Feature not yet ready')
def test_pipeline_restart_when_a_process_is_killed(indata_pipeline_outdata):
    import os
    import time
    import signal

    indata, pipeline, outdata = indata_pipeline_outdata

    pipeline.restart_on_error = True
    pipeline.start()
    processes = [process for node in pipeline.nodes
                 for process in node.processes]

    os.kill(processes[0].pid, signal.SIGKILL)
    time.sleep(1.1)

    # old processes should be gone
    assert any(not process.is_alive() for process in processes)

    # new processes should be all running
    assert all(process.is_alive() for node in pipeline.nodes
               for process in node.processes)

    indata.put((4, 2))
    assert outdata.get() == 3

    pipeline.stop()
