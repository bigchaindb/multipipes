def double(x):
    return x * 2


def test_node_execute_target_function():
    from multipipes import Node, Pipe

    indata, outdata = Pipe(), Pipe()

    node = Node(double, indata, outdata)
    node.start()

    indata.put(1)
    assert outdata.get() == 2

    node.stop()
    node.join()


def test_node_execute_on_multiple_workers():
    import os
    import time
    from multipipes import Node, Pipe

    indata, outdata = Pipe(), Pipe()

    def getpid(_):
        # And we'll just put a little happy sleep riiight over here.
        # In this way when a process is processing an element from the
        # queue it will stop, giving to the other processes the
        # opportunity to run.
        time.sleep(0.1)
        return os.getpid()

    node = Node(getpid, indata, outdata,
                number_of_processes=4)
    node.start()

    indata.put(1)
    indata.put(2)
    indata.put(3)
    indata.put(4)

    pids = [outdata.get(),
            outdata.get(),
            outdata.get(),
            outdata.get()]

    assert len(set(pids)) == 4
    assert len(node.workers) == 4

    assert all(worker.is_alive() for worker in node.workers)

    node.stop()
    node.join()
