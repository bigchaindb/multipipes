import os
import types
import math
from inspect import signature
import multiprocessing as mp
from multiprocessing import queues


POISON_PILL = 'POISON_PILL'


class PoisonPillException(Exception):
    pass


def Pipe(maxsize=0):
    return queues.Queue(maxsize, ctx=mp.get_context())


def pass_through(val):
    return val


class Node:

    def __init__(self, target=None, inqueue=None, outqueue=None,
                 name=None, timeout=None, number_of_processes=None,
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
            try:
                self.run()
            except PoisonPillException:
                return

    def run(self):
        kwargs = {'timeout': False}

        if self.inqueue:
            try:
                args = self.inqueue.get(timeout=self.timeout)
                if args == POISON_PILL:
                    raise PoisonPillException()
            except queues.Empty:
                args = None
                kwargs['timeout'] = True
        else:
            args = ()

        if not isinstance(args, tuple):
            args = (args, )

        if not self.accept_timeout:
            del kwargs['timeout']

        self.log('recv')
        self.log('recv', args)

        result = self.target(*args, **kwargs)

        self.log('send', result)
        self.log('----\n\n')

        if result is not None and self.outqueue:
            if isinstance(result, types.GeneratorType):
                for item in result:
                    self.outqueue.put(item)
            else:
                self.outqueue.put(result)

    def join(self):
        for process in self.processes:
            process.join()

    def terminate(self):
        for process in self.processes:
            process.terminate()

    def poison_pill(self):
        for i in range(self.number_of_processes):
            self.inqueue.put(POISON_PILL)

    def is_alive(self):
        return any(process.is_alive() for process in self.processes)


class Pipeline:

    def __init__(self, items, pipe=None):
        self.pipe = pipe
        self.items = items
        self.nodes = [item for item in items if isinstance(item, Node)]
        self.connect(self.items, pipe=pipe if pipe else False)

    def connect(self, rest, pipe=None):
        if not rest:
            return

        head, *tail = rest

        if isinstance(head, queues.Queue):
            if pipe is not None:
                raise ValueError('Cannot have two or more pipes next'
                                 ' to each other.')
            return self.connect(tail, pipe=head)

        elif isinstance(head, Node):
            if pipe is None:
                pipe = Pipe()
            head.inqueue = pipe
            head.outqueue = self.connect(tail)
            return head.inqueue

    def step(self):
        for node in self.nodes:
            node.run()

    def start(self):
        for node in self.nodes:
            node.start()

    def join(self):
        for node in self.nodes:
            node.join()

    def poison_pill(self):
        for node in self.nodes:
            node.poison_pill()

    def is_alive(self):
        return any(node.is_alive() for node in self.nodes)

