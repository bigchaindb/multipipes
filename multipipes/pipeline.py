import os
import math
import types
import logging
from uuid import uuid4
from random import randint
from inspect import signature
import multiprocessing as mp
from multiprocessing import queues

from setproctitle import setproctitle

from multipipes import manager, utils


logger = logging.getLogger(__name__)


class PoisonPill:
    def __init__(self, uuid=None):
        if not uuid:
            uuid = uuid4()
        self.uuid = uuid

    def __eq__(self, other):
        return self.uuid == other.uuid


class PoisonPillException(Exception):
    pass


def Pipe(maxsize=0):
    return queues.Queue(maxsize, ctx=mp.get_context())


def pass_through(val):
    return val


class Node:

    def __init__(self, target=None, inqueue=None, outqueue=None,
                 name=None, timeout=None,
                 number_of_processes=None, fraction_of_cores=None,
                 max_execution_time=None, max_requests=None):

        self.target = target if target else pass_through
        self.timeout = timeout
        self.accept_timeout = 'timeout' in signature(self.target).parameters
        self.name = name if name else target.__name__
        self.max_execution_time = max_execution_time

        if max_requests:
            # Add variance to prevent killing all the workers at the same time
            delta = int(max_requests * 0.05)
            self.max_requests = max_requests + randint(-delta, delta)
        else:
            self.max_requests = None

        self.inqueue = inqueue
        self.outqueue = outqueue

        self.processes = []
        self.process_namespace = 'pipeline'

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

    @property
    def alive_processes(self):
        return [process for process in self.processes
                if process.is_alive()]

    def log(self, *args):
        print('[{}] {}> '.format(os.getpid(), self.name), *args)

    def start(self, events_queue=None):
        self.max_requests_count = 0
        self.session_poison_pill = PoisonPill()
        self.processes = [mp.Process(target=self.safe_run_forever)
                          for _ in range(self.number_of_processes)]

        self.events_queue = events_queue

        for process in self.processes:
            process.start()

    def start_one(self):
        self.max_requests_count = 0
        process = mp.Process(target=self.safe_run_forever)
        self.processes.append(process)
        process.start()

    def safe_run_forever(self):
        setproctitle(':'.join([self.process_namespace, self.name]))
        logger.info('Starting %s:%s %s',
                    self.process_namespace, self.name, os.getpid())
        try:
            self.run_forever()
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            if self.events_queue:
                self.events_queue.put({'type': 'exception', 'context': exc})
            raise

    def run_forever(self):
        while True:
            if self.max_requests:
                self.max_requests_count += 1
                # we need to send just one message here:
                if self.max_requests_count == self.max_requests:
                    self.events_queue.put({'type': 'max_requests',
                                           'context': os.getpid()})
                    return
            try:
                with utils.deadline(self.max_execution_time):
                    self.run()
            except PoisonPillException:
                return

    def run(self):
        args = ()
        poisoned = False
        timeout = False

        if self.inqueue:
            try:
                args = self.inqueue.get(timeout=self.timeout)
                if isinstance(args, PoisonPill):
                    if args == self.session_poison_pill:
                        poisoned = True
                        raise queues.Empty()
                    else:
                        # discard the poison pill
                        return
            except queues.Empty:
                timeout = True

        # self.log('recv', args)

        if not isinstance(args, tuple):
            args = (args, )

        if timeout:
            if self.accept_timeout:
                # FIXME: the number of arguments depends on the
                #        function signature.
                args = (None, )
                result = self.target(*args, timeout=timeout)
            else:
                result = None
        else:
            result = self.target(*args)

        # self.log('send', result)

        if result is not None and self.outqueue:
            if isinstance(result, types.GeneratorType):
                for item in result:
                    self.outqueue.put(item)
            else:
                self.outqueue.put(result)

        if poisoned:
            raise PoisonPillException()

    def join(self, timeout=None):
        for process in self.alive_processes:
            process.join(timeout=timeout)

    def terminate(self):
        for process in self.alive_processes:
            process.terminate()

    def stop(self):
        if self.inqueue:
            for _ in self.alive_processes:
                self.inqueue.put(self.session_poison_pill)

    def restart(self):
        self.stop()
        self.join()
        self.start(events_queue=self.events_queue)

    def is_alive(self):
        return all(process.is_alive() for process in self.processes)

    def __str__(self):
        return '{} {}'.format([p.pid for p in self.processes], self.name)

    def __repr__(self):
        return str(self)


class Pipeline:
    def __init__(self, items, *,
                 process_namespace='pipeline',
                 restart_on_error=False):

        self.items = items
        self.events_queue = Pipe()
        self.process_namespace = process_namespace

        self.manager = manager.Manager(self, self.events_queue,
                                       restart_on_error=restart_on_error)
        self.setup()

    def setup(self, indata=None, outdata=None):
        self._last_indata = indata
        self._last_outdata = outdata
        items_copy = self.items[:]

        if indata:
            items_copy.insert(0, indata)
        if outdata:
            items_copy.append(outdata)

        for item in items_copy:
            item.process_namespace = self.process_namespace

        self.nodes = [item for item in items_copy if isinstance(item, Node)]
        self.connect(items_copy, False)

    def connect(self, rest, pipe=None):
        if not rest:
            return pipe

        head, *tail = rest

        if isinstance(head, queues.Queue):
            if pipe:
                raise ValueError('Cannot have two or more pipes next'
                                 ' to each other.')
            return self.connect(tail, pipe=head)

        elif isinstance(head, Node):
            if pipe is None:
                pipe = Pipe()
            if pipe is not False:
                head.inqueue = pipe
            head.outqueue = self.connect(tail)
            return head.inqueue

    def restart(self, hard=False):
        if hard:
            self.terminate()
        else:
            self.stop()
        self.setup(indata=self._last_indata, outdata=self._last_outdata)
        self.start()

    def step(self):
        for node in self.nodes:
            node.run()

    def start(self):
        for node in self.nodes:
            node.start(events_queue=self.events_queue)

    def join(self):
        for node in self.nodes:
            node.join()

    def terminate(self):
        for node in self.nodes:
            node.terminate()

    def stop(self, timeout=30):
        for node in self.nodes:
            node.stop()
            try:
                node.join(timeout=30)
            except TimeoutError:
                node.terminate()

    def is_alive(self):
        return all(node.is_alive() for node in self.nodes)

