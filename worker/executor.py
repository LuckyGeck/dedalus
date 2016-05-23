import abc
from os import path
from typing import Iterable, Tuple, Optional

from common.models.executor import ExecutorInfo
from util.config import Config
from util.plugins import PluginBase, PluginsMaster


class ExecutionEnded(Exception):
    def __init__(self, retcode: int) -> None:
        self.retcode = retcode

    def __str__(self):
        return 'Execution ended with return code {}'.format(self.retcode)


class Executor(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, execution_id: str, execution_data_root: str,
                 execution_config: dict = None, **kwargs) -> None:
        assert execution_id, 'Session should be non-empty'
        assert execution_data_root, 'Execution data root dir should be non-empty'
        self.execution_id = execution_id
        self.execution_data_root = execution_data_root
        assert execution_config is None or not kwargs, 'Only one of config and kwargs should be set'
        kwargs.update(execution_config)
        self.config = self.config_class()
        self.config.from_json(kwargs)
        self.config.verify()

    @property
    def default_working_dir(self):
        return path.join(self.execution_data_root, self.execution_id)

    @classmethod
    @abc.abstractmethod
    def config_class(cls) -> Config:
        return Config()

    @abc.abstractmethod
    def start(self) -> Iterable[Tuple[Optional[str], Optional[str]]]:
        return ()

    @abc.abstractmethod
    def ping(self):
        pass

    @abc.abstractmethod
    def kill(self, sig: int):
        pass


class Executors(PluginsMaster):
    plugin_base_class = Executor

    def __init__(self, execution_data_root: str, plugins_folder: str = None):
        super().__init__(plugins_folder)
        self.execution_data_root = execution_data_root

    def construct_executor(self, execution_id: str, executor_info: ExecutorInfo) -> Executor:
        return self.find_plugin(executor_info.name, executor_info.min_version)(
            execution_id=execution_id,
            execution_data_root=self.execution_data_root,
            execution_config=executor_info.config)
