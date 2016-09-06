def test_stress_workers_replacement(monkeypatch):
    monkeypatch.setattr('multipipes.node._randomize_max_requests',
                        lambda x: x)

    from multipipes import Manager, Pipeline, Pipe, Node

    indata = Pipe()
    outdata = Pipe()
    manager = Manager()

    def squared(n):
        return n * n

    def inc(n):
        return n + 1

    pipeline = Pipeline([
        Node(squared),
        Node(inc, number_of_processes=10)
    ], manager=manager, max_requests=10)

    pipeline.setup(indata=indata, outdata=outdata)
    pipeline.start()

    for i in range(1000):
        indata.put(i)
        assert outdata.get() == i * i + 1

    import time
    time.sleep(0.1)

    pipeline.stop()
    pipeline.join()
