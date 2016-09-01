"""This module groups all the custom exceptions raised and handles
in MultiPipes."""


class PoisonPillException(Exception):
    """Raised when a node is requested to do a clean exit."""


class MaxRequestsException(Exception):
    """Raised when a node has reached the maximum number of requests
    to process."""


class TimeoutNotSupportedError(Exception):
    """Raised when a Task is called without any arguments because
    a timeout happened, but the Task doesn't have any default
    values."""
