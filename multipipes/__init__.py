from .pipeline import Pipeline, Node, Pipe
from .worker import Worker, Task

def set_debug(enabled=True):
    from multipipes import pipeline
    pipeline.DEBUG = enabled

