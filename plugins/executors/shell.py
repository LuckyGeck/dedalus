from util.symver import SymVer
from worker.executor import Executor


class ShellExecutor(Executor):
    name = 'shell'
    version = SymVer(0, 0, 1)

    def execute(self):
        pass
