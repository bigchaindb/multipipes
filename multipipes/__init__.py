from .pipeline import Pipeline, Node, Pipe, POISON_PILL

def set_debug(enabled=True):
    from multipipes import pipeline
    pipeline.DEBUG = enabled

