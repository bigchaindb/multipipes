import types
import multiprocessing as mp
import inspect
from collections import defaultdict, OrderedDict


NO_VAL = object()
STOP = object()


class Pipeline:

    def __init__(self, mapping, dag):
        self.mapping = mapping
        self.argsnum = {name: len(inspect.signature(func).parameters)
                        for name, func in self.mapping.items()}
        self.entry_node = dag[0][0]
        self.dag = OrderedDict((head, tail) for head, *tail in dag)
        self.partials = defaultdict(list)

    def step(self, arg=NO_VAL):
        head, tail = self.entry_node, self.dag[self.entry_node]
        return self._step(head, tail, arg)

    def _step(self, head, tail, arg=NO_VAL):
        args = self.partials[head]
        argsnum = self.argsnum[head]

        if arg is not NO_VAL:
            args.append(arg)

        if len(args) < argsnum:
            return

        result = self.mapping[head](*args)
        args.clear()

        for node in tail:
            self._step(node, self.dag.get(node, []), result)


class ProcessNode:

    def __init__(self, name, target, queues_in=None, queues_out=None):
        self.name = name
        self.target = target
        self.queues_in = queues_in if queues_in else []
        self.queues_out = queues_out if queues_out else []
        self.process = mp.Process(target=self.run_forever)

    def log(self, *args):
        print('{}> '.format(self.name), *args)

    def start(self):
        self.process.start()

    def run_forever(self):
        try:
            while True:
                self.run()
        except KeyboardInterrupt:
            pass

    def run(self):
        args = [queue_in.get() for queue_in in self.queues_in]
        self.log('recv', args)

        result = self.target(*args)

        self.log('send', result)
        if isinstance(result, types.GeneratorType):
            for item in result:
                for queue_out in self.queues_out:
                    queue_out.put(item)
        else:
            for queue_out in self.queues_out:
                queue_out.put(result)


class MultiprocessPipeline(Pipeline):

    def __init__(self, mapping, dag):
        super().__init__(mapping, dag)
        self.procs = {}
        self.queues_in = defaultdict(list)
        self.queues_out = defaultdict(list)

    def start(self):
        self.create_edges(self.entry_node, visited=set())
        self.create_nodes()
        for node, process in self.procs.items():
            process.start()

    def create_nodes(self):
        for node, func in self.mapping.items():
            self.procs[node] = ProcessNode(name=node,
                                           target=func,
                                           queues_in=self.queues_in[node],
                                           queues_out=self.queues_out[node])

    def create_edges(self, node, visited, queue_in=None):
        queues_in = self.queues_in[node]
        queues_out = self.queues_out[node]

        if queue_in:
            queues_in.append(queue_in)

        for next_node in self.dag.get(node, []):
            if (node, next_node) not in visited:
                visited.add((node, next_node))
                queue_out = mp.Queue()
                self.create_edges(next_node, visited, queue_out)
                queues_out.append(queue_out)

