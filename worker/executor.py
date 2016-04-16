import abc
from util.plugins import PluginBase, PluginsMaster


class Executor(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, work_dir: str):
        assert work_dir, 'Working dir for an executor should be set'
        self.work_dir = work_dir

    @abc.abstractmethod
    def execute(self):
        pass


class Executors(PluginsMaster):
    plugin_base_class = Executor
