import os
import types
import math
import queue
from inspect import signature
import multiprocessing as mp


class Node:
    def __init__(self, target, name=None,
                 timeout=None,
                 number_of_processes=None,
                 fraction_of_cores=None):
        self.target = target
        self.timeout = timeout
        self.name = name if name else self.target.__name__

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


class ProcessNode:

    def __init__(self, target, timeout=None,
                 name=None, inqueue=None, outqueue=None):
        self.target = target
        self.timeout = timeout
        self.accept_timeout = 'timeout' in signature(target).parameters
        self.name = name
        self.inqueue = inqueue
        self.outqueue = outqueue
        self.process = mp.Process(target=self.run_forever)

    def log(self, *args):
        print('[{}] {}> '.format(os.getpid(), self.name), *args)

    def start(self):
        self.process.start()

    def run_forever(self):
        try:
            while True:
                self.run()
        except KeyboardInterrupt:
            pass

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
                node = Node(node)
            self.nodes.append(node)

        self.procs = []

        self.setup()

    def create_node(self, node, inqueue, outqueue):
        processes = []
        for i in range(node.number_of_processes):
            process_node = ProcessNode(target=node.target,
                                       name=node.name,
                                       timeout=node.timeout,
                                       inqueue=inqueue,
                                       outqueue=outqueue)
            processes.append(process_node)
        return processes

    def create_queue(self):
        return mp.Queue()

    def setup(self):
        inqueue = self.inqueue

        for i, node in enumerate(self.nodes):
            if i == len(self.nodes):
                outqueue = None
            else:
                outqueue = self.create_queue()

            self.procs.extend(self.create_node(node, inqueue, outqueue))

            inqueue = outqueue

    def step(self):
        for process in self.procs:
            process.run()

    def start(self):
        for process in self.procs:
            process.start()
