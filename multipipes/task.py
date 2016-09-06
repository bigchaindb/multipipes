"""Wrapper around a target function."""

import types
import inspect
from multiprocessing import queues

from .utils import deadline
from .exceptions import (MaxRequestsException,
                         TimeoutNotSupportedError,
                         ExitTaskException)


def _inspect_func(target):
    params = inspect.signature(target).parameters
    accept_timeout = all(param.default is not inspect.Signature.empty
                         for param in params.values())
    params_count = len(params)
    return params_count, accept_timeout


class Task:
    def __init__(self, target=None, indata=None, outdata=None, *,
                 max_execution_time=None, max_requests=None,
                 timeout=None, polling_timeout=0.5):
        self.target = target
        if target:
            self.params_count, self.accept_timeout = _inspect_func(target)
            self.name = target.__name__
        else:
            self.params_count, self.accept_timeout = 0, False
            self.name = 'unknown'

        self.requests_count = 0

        self.indata = indata
        self.outdata = outdata
        self.exit_signal = False
        self.running = True

        self.timeout = timeout
        self.polling_timeout = polling_timeout

        if self.timeout and not self.accept_timeout:
            raise TimeoutNotSupportedError()

        self.max_execution_time = max_execution_time
        self.max_requests = max_requests

    def run_forever(self):
        while self.running:
            try:
                self.step()
            except KeyboardInterrupt:
                self.exit_signal = True
            except ExitTaskException:
                self.running = False

    def step(self):
        args = self.pull()

        if not args and not self.timeout:
            raise ExitTaskException()

        result = self(*args)
        self.push(result)
        self.inc()

    def inc(self):
        self.requests_count += 1
        if self.requests_count == self.max_requests:
            raise MaxRequestsException()

    def __call__(self, *args):
        with deadline(self.max_execution_time):
            if self.target:
                return self.target(*args)

    def _read_from_indata(self):
        if self.timeout:
            if self.timeout <= self.polling_timeout:
                try:
                    return self.indata.get(timeout=self.timeout)
                except queues.Empty:
                    return
            else:
                # polling_timeout as much time then delta
                times = int(self.timeout // self.polling_timeout)
                delta = self.timeout - self.polling_timeout
                for _ in range(times):
                    try:
                        return self.indata.get(timeout=self.polling_timeout)
                    except queues.Empty:
                        if self.exit_signal:
                            self.running = False
                            return
                try:
                    return self.indata.get(timeout=delta)
                except queues.Empty:
                    return
        else:
            # while true: polling_timeout
            while True:
                try:
                    return self.indata.get(timeout=self.polling_timeout)
                except queues.Empty:
                    if self.exit_signal:
                        self.running = False
                        return

    def pull(self):
        if self.indata:
            args = self._read_from_indata()
        else:
            args = ()

        if args is None:
            args = ()

        if not isinstance(args, tuple):
            args = (args, )

        return args

    def push(self, result):
        if result is not None and self.outdata:
            if isinstance(result, types.GeneratorType):
                for item in result:
                    self.outdata.put(item)
            else:
                self.outdata.put(result)

    def stop(self):
        self.exit_signal = True
