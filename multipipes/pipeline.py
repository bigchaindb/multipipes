import logging
import multiprocessing as mp
from multiprocessing import queues

from .node import Node


logger = logging.getLogger(__name__)


def Pipe(maxsize=0):
    return queues.Queue(maxsize, ctx=mp.get_context())


class Pipeline:
    def __init__(self, items, *,
                 manager=None, process_namespace='pipeline'):

        self.items = items
        self.events_queue = Pipe()
        self.process_namespace = process_namespace
        self.manager = manager
        self.setup()

    def setup(self, indata=None, outdata=None):
        self._last_indata = indata
        self._last_outdata = outdata
        items_copy = self.items[:]

        if indata:
            items_copy.insert(0, indata)
        if outdata:
            items_copy.append(outdata)

        self.nodes = [item for item in items_copy
                      if isinstance(item, Node)]

        for node in self.nodes:
            node.process_namespace = self.process_namespace
            node.manager = self.manager

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
                head.indata = pipe
            head.outdata = self.connect(tail)
            return head.indata

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
            node.start()

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

