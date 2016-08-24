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
    assert len(pipeline.errors) == 1

