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

    return indata, p, outdata


def test_simple_pipeline_starts_the_node():
    from multipipes import Pipeline, Pipe, Node

    indata = Pipe()
    outdata = Pipe()

    def divide(a, b):
        return a / b

    pipeline = Pipeline([
        Node(divide),
    ])

    pipeline.setup(indata=indata, outdata=outdata)
    pipeline.start()

    indata.put((4, 2))
    assert outdata.get() == 2

    pipeline.stop()


def test_simple_pipeline_goes_through_nodes(indata_pipeline_outdata):
    indata, pipeline, outdata = indata_pipeline_outdata

    pipeline.start()

    indata.put((4, 1))
    assert outdata.get() == 5

    indata.put((4, 2))
    assert outdata.get() == 3

    indata.put((4, 4))
    assert outdata.get() == 2

    indata.put((4, 8))
    assert outdata.get() == 1.5

    pipeline.stop()


def test_pipeline_propagates_exception(indata_pipeline_outdata):
    indata, pipeline, outdata = indata_pipeline_outdata

    pipeline.start()
    indata.put((4, 1))
    outdata.get()
    pipeline.stop()
