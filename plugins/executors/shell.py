from util.symver import SymVer
from worker.executor import Executor
from util.config import Config, ConfigField


class ShellExecutorConfig(Config):
    work_dir = ConfigField(type=str, required=True, default='/tmp')


class ShellExecutor(Executor):
    name = 'shell'
    version = SymVer(0, 0, 1)
    config = ShellExecutorConfig()

    def execute(self):
        pass
