"""Wrapper around a target function."""

import types
import inspect
from random import randint
from multiprocessing import Process, queues

from multipipes import exceptions, utils


def _inspect_target(target):
    params = inspect.signature(target).parameters
    arguments = [name for name, param in params.items()
                 if param.default is inspect.Signature.empty]
    return len(arguments)


def _randomize_max_requests(value, variance=0.05):
    # Add variance to prevent killing all the workers at the same time
    delta = round(value * variance)
    return value + randint(-delta, delta)


class Task:
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
        if max_requests:
            self.max_requests = _randomize_max_requests(max_requests)
        else:
            self.max_requests = None
        self.requests_count = 0

    def run_forever(self):
        while True:
            try:
                self.run()
            except exceptions.PoisonPillException:
                break
            except KeyboardInterrupt:
                if self.indata:
                    self.indata.put(utils.PoisonPill())

    def run(self):
        self.run_handle_exceptions()

    def run_handle_exceptions(self):
        try:
            self.run_handle_requests_count()
        except TimeoutError:
            raise
        except Exception:
            raise

    def run_handle_requests_count(self):
        if self.requests_count == self.max_requests:
            raise exceptions.MaxRequestsException()
        self.requests_count += 1
        self.run_handle_args()

    def run_handle_args(self):
        args, poisoned = self.pull()

        if poisoned and not self.timeout:
            result = None
        else:
            result = self.run_handle_target(args)

        self.push(result)

        if poisoned:
            raise exceptions.PoisonPillException()

    def run_handle_target(self, args):
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


class Worker:
    def __init__(self, task=None, *, daemon=None):
        self.daemon = daemon
        self.task = task

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
        self.task.indata.put(utils.PoisonPill())

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
