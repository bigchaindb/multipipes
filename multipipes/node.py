import math
import logging
from random import randint
import multiprocessing as mp

from .worker import Worker
from .task import Task


logger = logging.getLogger(__name__)


def pass_through(val):
    return val


def _randomize_max_requests(value, variance=0.05):
    # Add variance to prevent killing all the workers at the same time
    delta = round(value * variance)
    return value + randint(-delta, delta)


class Node:

    def __init__(self, target=None, indata=None, outdata=None, *,
                 name=None, number_of_processes=None, fraction_of_cores=None,
                 timeout=None, polling_timeout=0.5,
                 max_execution_time=None, max_requests=None,
                 manager=None):

        self.target = target
        self.indata = indata
        self.outdata = outdata
        self.name = name if name else target.__name__
        self.timeout = timeout
        self.polling_timeout = polling_timeout

        if (number_of_processes and number_of_processes <= 0) or\
           (fraction_of_cores and fraction_of_cores <= 0):
            raise ValueError('Cannot assign zero or less cores')

        if number_of_processes is not None and fraction_of_cores is not None:
            raise ValueError('number_of_processes and fraction_of_cores '
                             'are exclusive parameters and cannot be '
                             'used together')

        if fraction_of_cores:
            # math.ceil makes sure we have at least one process running
            self.number_of_processes = math.ceil(mp.cpu_count() *
                                                 fraction_of_cores)
        elif number_of_processes:
            self.number_of_processes = number_of_processes
        else:
            self.number_of_processes = 1

        self.max_execution_time = max_execution_time
        self.set_max_requests(max_requests)
        self.manager = manager

        self.workers = []
        self.process_namespace = 'pipeline'

    def set_max_requests(self, max_requests=None):
        if max_requests:
            # Add variance to prevent killing all the workers at the same time
            delta = int(max_requests * 0.05)
            self.max_requests = max_requests + randint(-delta, delta)
        else:
            self.max_requests = None

    def start(self):
        task = Task(target=self.target if self.target else pass_through,
                    indata=self.indata, outdata=self.outdata,
                    max_execution_time=self.max_execution_time,
                    max_requests=self.max_requests,
                    timeout=self.timeout,
                    polling_timeout=self.polling_timeout)

        self.workers = [Worker(task=task, manager=self.manager)
                        for _ in range(self.number_of_processes)]

        for worker in self.workers:
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.stop()

    def join(self, timeout=None):
        for worker in self.workers:
            worker.join(timeout=timeout)
