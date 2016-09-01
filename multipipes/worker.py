"""Wrapper around a target function."""

import types
import inspect
from random import randint
from multiprocessing import Process, queues

from multipipes import exceptions, utils


def _func_accept_default_params(target):
    params = inspect.signature(target).parameters
    return all(param.default is not inspect.Signature.empty
               for param in params.values())


def _randomize_max_requests(value, variance=0.05):
    # Add variance to prevent killing all the workers at the same time
    delta = round(value * variance)
    return value + randint(-delta, delta)


class Task:
    def __init__(self, target, *, max_execution_time=None, max_requests=None):
        self.target = target
        self.accept_timeout = _func_accept_default_params(target)
        self.requests_count = 0

        self.max_execution_time = max_execution_time
        if max_requests:
            self.max_requests = _randomize_max_requests(max_requests)
        else:
            self.max_requests = None

    def __call__(self, *args):
        if not args and not self.accept_timeout:
            raise exceptions.TimeoutNotSupportedError()

        if self.requests_count == self.max_requests:
            raise exceptions.MaxRequestsException()

        self.requests_count += 1

        with utils.deadline(self.max_execution_time):
            return self.target(*args)


class Worker:
    def __init__(self, task=None, indata=None, outdata=None, *, daemon=None):
        self.task = task
        self.indata = indata
        self.outdata = outdata
        self.daemon = daemon

    def pull(self):
        args = ()
        poisoned = False

        if self.indata:
            try:
                args = self.indata.get(block=self.block, timeout=self.timeout)
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

    def run(self):
        try:
            self.task.run_forever()
        except exceptions.MaxRequestsException:
            self.restart()

    def start(self):
        self.process = Process(target=self.run)
        self.process.start()
        self.pid = self.process.pid

    def stop(self):
        pass

    def restart(self, timeout=None):
        self.stop()
        try:
            self.join(timeout=timeout)
        except TimeoutError:
            self.terminate()
            self.join()
        self.start()

    def join(self, timeout=None):
        self.process.join(timeout)

    def terminate(self):
        self.process.terminate()

    def is_alive(self):
        self.process.is_alive()
