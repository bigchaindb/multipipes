import inspect
from collections import Iterable, defaultdict, OrderedDict


NO_VAL = object()


class Pipeline:

    def __init__(self, mapping, dag):
        self.mapping = mapping
        self.argsnum = {name: len(inspect.signature(func).parameters)
                        for name, func in self.mapping.items()}
        self.start = dag[0][0]
        self.dag = OrderedDict((head, tail) for head, *tail in dag)
        self.partials = defaultdict(list)

    def step(self, arg=NO_VAL):
        head, tail = self.start, self.dag[self.start]
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

