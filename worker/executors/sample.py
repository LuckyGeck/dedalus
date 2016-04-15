from worker.executor import Executor, SymVer


class SampleExecutor(Executor):
    name = 'sample_plugin'
    version = SymVer(0, 0, 1)

    def execute(self):
        pass
