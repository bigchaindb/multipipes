"""Wrapper around a target function."""

import os
import signal
import types
import inspect
from random import randint
from multiprocessing import Process, queues

from multipipes import exceptions, utils


def _inspect_func(target):
    params = inspect.signature(target).parameters
    accept_timeout = all(param.default is not inspect.Signature.empty
                         for param in params.values())
    params_count = len(params)
    return params_count, accept_timeout


def _randomize_max_requests(value, variance=0.05):
    # Add variance to prevent killing all the workers at the same time
    delta = round(value * variance)
    return value + randint(-delta, delta)


class Task:
    def __init__(self, target, indata=None, outdata=None, *,
                 max_execution_time=None, max_requests=None,
                 read_timeout=None, polling_timeout=0.5):
        self.target = target
        self.params_count, self.accept_timeout = _inspect_func(target)
        self.requests_count = 0

        self.indata = indata
        self.outdata = outdata
        self.exit_signal = False
        self.running = True

        self.read_timeout = read_timeout
        self.polling_timeout = polling_timeout

        if self.read_timeout and not self.accept_timeout:
            raise exceptions.TimeoutNotSupportedError()

        self.max_execution_time = max_execution_time
        if max_requests:
            self.max_requests = _randomize_max_requests(max_requests)
        else:
            self.max_requests = None

    def step(self):
        args = self.pull()
        result = self(*args)
        self.push(result)

        if self.requests_count == self.max_requests:
            raise exceptions.MaxRequestsException()

    def run_forever(self):
        while self.running:
            try:
                self.step()
            except KeyboardInterrupt:
                self.exit_signal = True

    def __call__(self, *args):
        if len(args) != self.params_count:
            if not self.read_timeout:
                return

        with utils.deadline(self.max_execution_time):
            result = self.target(*args)

        self.requests_count += 1
        return result

    def _read_from_indata(self):
        if self.read_timeout:
            if self.read_timeout <= self.polling_timeout:
                try:
                    return self.indata.get(timeout=self.read_timeout)
                except queues.Empty:
                    return
            else:
                # polling_timeout as much time then delta
                times = int(self.read_timeout // self.polling_timeout)
                delta = self.read_timeout - self.polling_timeout
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


class Worker:
    def __init__(self, task=None, *,
                 manager=None, daemon=None):
        self.task = task
        self.manager = manager
        self.daemon = daemon
        self.exit_signal = True

    def run(self):
        signal.signal(signal.SIGINT, self._stop)

        try:
            self.task.run_forever()
        except exceptions.MaxRequestsException:
            self.send_event({'type': 'max_requests'})

    def _stop(self, signum, frame):
        self.task.exit_signal = True

    def send_event(self, event):
        if self.manager:
            event['pid'] = os.getpid()
            self.manager.send_event(event)

    def start(self):
        self.process = Process(target=self.run)
        self.process.start()
        self.pid = self.process.pid

        if self.manager:
            self.manager.register_worker(self.pid, self)

        self.exit_signal = False

    def stop(self):
        # import time
        # time.sleep(1)
        if self.exit_signal:
            return
        os.kill(self.pid, signal.SIGINT)

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
