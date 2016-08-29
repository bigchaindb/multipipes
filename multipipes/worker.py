"""Wrapper around a target function."""

import types
import inspect
from multiprocessing import queues

from multipipes import exceptions, utils


def _inspect_target(target):
    params = inspect.signature(target).parameters
    arguments = [name for name, param in params.items()
                 if param.default is inspect.Signature.empty]
    return len(arguments)


class Worker:
    def __init__(self, target, indata=None, outdata=None, *,
                 block=True, timeout=None, max_execution_time=None,
                 max_requests=None):
        self.target = target
        self.indata = indata
        self.outdata = outdata

        self.block = block
        self.timeout = timeout
        # Check if the target function accepts a timeout parameter
        self.arguments = _inspect_target(target)
        self.max_execution_time = max_execution_time
        self.max_requests = max_requests

    def run_forever(self):
        while True:
            try:
                self.run()
            except KeyboardInterrupt:
                if self.indata:
                    self.indata.put(utils.PoisonPill())

    def run(self):
        args, poisoned = self.pull()

        if poisoned:
            if self.timeout:
                result = self.run_target(args)
            else:
                result = None
        else:
            result = self.run_target(args)

        self.push(result)

        if poisoned:
            raise exceptions.PoisonPillException()

    def run_target(self, args):
        with utils.deadline(self.max_execution_time):
            return self.target(*args)

    def pull(self):
        args = ()
        poisoned = False

        if self.indata:
            try:
                args = self.indata.get(block=self.block, timeout=self.timeout)
                if isinstance(args, utils.PoisonPill):
                    poisoned = True
                    raise queues.Empty()
            except queues.Empty:
                args = (None, ) * self.arguments

        if not isinstance(args, tuple):
            args = (args, )

        return args, poisoned

    def push(self, result):
        if result is not None and self.outdata:
            if isinstance(result, types.GeneratorType):
                for item in result:
                    self.outdata.put(item)
            else:
                self.outdata.put(result)

