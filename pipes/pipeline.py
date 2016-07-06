import os
import types
import math
import queue
from inspect import signature
import multiprocessing as mp


def pass_through(val):
    return val


class Node:

    def __init__(self, target=None, name=None, inqueue=None, outqueue=None,
                 timeout=None, number_of_processes=None,
                 fraction_of_cores=None):

        self.target = target if target else pass_through
        self.timeout = timeout
        self.accept_timeout = 'timeout' in signature(self.target).parameters
        self.name = name if name else target.__name__
        self.inqueue = inqueue
        self.outqueue = outqueue

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

        self.processes = [mp.Process(target=self.safe_run_forever)
                          for i in range(self.number_of_processes)]

    def log(self, *args):
        print('[{}] {}> '.format(os.getpid(), self.name), *args)

    def start(self):
        for process in self.processes:
            process.start()

    def safe_run_forever(self):
        try:
            self.run_forever()
        except KeyboardInterrupt:
            pass

    def run_forever(self):
        while True:
            self.run()

    def run(self):
        kwargs = {'timeout': False}

        if self.inqueue:
            try:
                args = self.inqueue.get(timeout=self.timeout)
            except queue.Empty:
                args = None
                kwargs['timeout'] = True
        else:
            args = ()

        if not isinstance(args, tuple):
            args = (args, )

        if not self.accept_timeout:
            del kwargs['timeout']

        self.log('recv')
        # self.log('recv', args)

        result = self.target(*args, **kwargs)

        # self.log('send', result)
        # self.log('----\n\n')

        if result is not None and self.outqueue:
            if isinstance(result, types.GeneratorType):
                for item in result:
                    self.outqueue.put(item)
            else:
                self.outqueue.put(result)


class Pipeline:

    def __init__(self, nodes, inqueue=None):
        self.inqueue = inqueue
        self.nodes = []

        for node in nodes:
            if not isinstance(node, Node):
                node = Node(target=node)
            self.nodes.append(node)

        self.connect()

    def create_queue(self):
        return mp.Queue()

    def connect(self):
        inqueue = self.inqueue

        for i, node in enumerate(self.nodes):
            if i == len(self.nodes):
                outqueue = None
            else:
                outqueue = self.create_queue()

            node.inqueue = inqueue
            node.outqueue = outqueue

            inqueue = outqueue

    def step(self):
        for node in self.nodes:
            node.run()

    def start(self):
        for node in self.nodes:
            node.start()

