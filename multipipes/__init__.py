from .pipeline import Pipeline, Node, Pipe

def set_debug(enabled=True):
    from multipipes import pipeline
    pipeline.DEBUG = enabled

