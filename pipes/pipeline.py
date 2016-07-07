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


class Pipe:
    def __init__(self, maxsize=None):
        self.maxsize = maxsize


class Pipeline:

    def __init__(self, items, inqueue=None):
        self.inqueue = inqueue
        self.items = items
        self.nodes = [item for item in items if isinstance(item, Node)]
        self.connect(self.items, pipe=inqueue if inqueue else False)

    def create_pipe(self, pipe=None):
        if not pipe:
            queue = mp.Queue()
        else:
            queue = mp.Queue(maxsize=pipe.maxsize)
        return queue

    def connect(self, rest, pipe=None):
        if not rest:
            return

        head, *tail = rest

        if isinstance(head, Pipe):
            if pipe is not None:
                raise ValueError('Cannot have two or more pipes next'
                                 ' to each other.')
            return self.connect(tail, pipe=head)

        elif isinstance(head, Node):
            if pipe is not False:
                pipe = self.create_pipe(pipe)
            head.inqueue = pipe
            head.outqueue = self.connect(tail)
            return head.inqueue

    def step(self):
        for node in self.nodes:
            node.run()

    def start(self):
        for node in self.nodes:
            node.start()

