from .task import Task
from .worker import Worker
from .manager import Manager
from .pipeline import Pipeline, Node, Pipe

def set_debug(enabled=True):
    from multipipes import pipeline
    pipeline.DEBUG = enabled

