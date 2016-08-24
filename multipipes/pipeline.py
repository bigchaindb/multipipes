import os
import sys
import traceback
import types
import math
import signal
from inspect import signature
import multiprocessing as mp
import threading
from multiprocessing import queues

from setproctitle import setproctitle

from multipipes import utils


POISON_PILL = 'POISON_PILL'
DEBUG = bool(int(os.environ.get('PYTHONMULTIPIPESDEBUG', 0)))


class PoisonPillException(Exception):
    pass


def Pipe(maxsize=0):
    return queues.Queue(maxsize, ctx=mp.get_context())


def pass_through(val):
    return val


class Node:

    def __init__(self, target=None, inqueue=None, outqueue=None,
                 name=None, timeout=None, number_of_processes=None,
                 max_execution_time=None, fraction_of_cores=None):

        self.target = target if target else pass_through
        self.timeout = timeout
        self.max_execution_time = max_execution_time
        self.accept_timeout = 'timeout' in signature(self.target).parameters
        self.name = name if name else target.__name__
        self.inqueue = inqueue
        self.outqueue = outqueue
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

        self.processes = [mp.Process(target=self.safe_run_forever)
                          for i in range(self.number_of_processes)]

    def log(self, *args):
        print('[{}] {}> '.format(os.getpid(), self.name), *args)

    def start(self, error_channel=None):
        self.error_channel = error_channel

        for process in self.processes:
            process.start()

    def safe_run_forever(self):
        try:
            self.run_forever()
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            self.error_channel.put(exc)
            raise

    def run_forever(self):
        setproctitle(':'.join([self.process_namespace, self.name]))
        while True:
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
                if args == POISON_PILL:
                    poisoned = True
                    raise queues.Empty()
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
        for process in self.processes:
            process.join(timeout=timeout)

    def terminate(self):
        for process in self.processes:
            process.terminate()

    def stop(self):
        if self.inqueue:
            for i in range(self.number_of_processes):
                self.inqueue.put(POISON_PILL)

    def is_alive(self):
        return any(process.is_alive() for process in self.processes)


class Pipeline:
    def __init__(self, items, *,
                 restart_on_error=False,
                 process_namespace='pipeline'):

        self.items = items
        self.errors = []
        self._error_channel = Pipe()
        self.restart_on_error = restart_on_error
        self.process_namespace = process_namespace

        threading.Thread(target=self.handle_error, daemon=True).start()

        self.setup()

    def setup(self, indata=None, outdata=None):
        items_copy = self.items[:]

        for item in items_copy:
            item.process_namespace = self.process_namespace

        if indata:
            items_copy.insert(0, indata)
        if outdata:
            items_copy.append(outdata)

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

    def handle_error(self):
        exc = self._error_channel.get()
        self.errors.append(exc)

        if DEBUG:
            global LAST_ERROR
            LAST_ERROR = exc
            os.kill(os.getpid(), signal.SIGUSR1)

    def restart(self):
        self.stop()
        self.errors = []
        self.start()

    def step(self):
        for node in self.nodes:
            node.run()

    def start(self):
        for node in self.nodes:
            node.start(error_channel=self._error_channel)

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
        return any(node.is_alive() for node in self.nodes)


LAST_ERROR = None
def exception_handler(signum, frame):
    try:
        raise LAST_ERROR
    except:
        print(traceback.format_exc())
    sys.exit(1)
signal.signal(signal.SIGUSR1, exception_handler)
