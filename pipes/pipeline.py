import inspect
from collections import Iterable, defaultdict


NO_VAL = object()


class Pipeline:

    def __init__(self, mapping, dag):
        self.mapping = mapping
        self.argsnum = {name: len(inspect.signature(func).parameters)
                        for name, func in self.mapping.items()}
        self.dag = dag
        self.partials = defaultdict(list)

    def step(self, arg=NO_VAL):
        return self._step(self.dag[0], arg)

    def _step(self, entry, arg=NO_VAL):
        head, *tail = entry
        args = self.partials[head]
        argsnum = self.argsnum[head]

        if arg is not NO_VAL:
            args.append(arg)

        if len(args) < argsnum:
            return

        result = self.mapping[head](*args)
        args.clear()

        for element in tail:
            for row in self.dag:
                if row[0] == element:
                    self._step(row, result)
                    break
            else:
                self._step([element], result)

