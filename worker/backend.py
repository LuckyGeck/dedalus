import abc
from typing import Iterator, Tuple, Optional

from common.models.task import TaskInfo, TaskState
from util.config import Config
from util.plugins import PluginBase, PluginsMaster
from util.symver import SymVer


class TaskNotFound(Exception):
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id

    def __str__(self):
        return 'Task "{}" not found in backend'.format(self.task_id)


class WorkerBackend(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, backend_config: dict) -> None:
        self.config = self.config_class()
        self.config.from_json(backend_config)
        self.config.verify()

    @classmethod
    @abc.abstractmethod
    def config_class(cls) -> Config:
        return Config()

    @abc.abstractmethod
    def read_task_info(self, task_id: str) -> TaskInfo:
        """Read task info by task_id
        :raises TaskNotFound: if task has not been found
        """
        pass

    @abc.abstractmethod
    def save_task_info(self, task_id: str, task_info: TaskInfo):
        """Create or replace task by task_id"""
        pass

    @abc.abstractmethod
    def list_tasks(self, with_info: bool = False) -> Iterator[Tuple[str, Optional[TaskInfo]]]:
        """List all known tasks.
        :returns iterator over pairs of task_id and task_info. If with_info is not set, all task_info's are None
        """
        pass

    def get_task_state(self, task_id: str) -> TaskState:
        """
        Receive task state from backend.
        :raises if task is not found
        :param task_id: Task's id
        :return: Current task state
        """
        return self.read_task_info(task_id).exec_stats.state

    def set_task_state(self, task_id: str, state: str) -> TaskState:
        """
        Changes task state and save it to backend.
        :raises if task is not found
        :param task_id: Task's id to change state for
        :param state: New state for a task
        :return: Previous task state
        """
        task_info = self.read_task_info(task_id)
        old_state = task_info.exec_stats.name.change_state(state, force=False)
        self.save_task_info(task_id, task_info)
        return old_state


class WorkerBackends(PluginsMaster):
    plugin_base_class = WorkerBackend

    def construct_backend(self, backend_type: str, backend_config: dict,
                          backend_min_version: SymVer = SymVer()) -> WorkerBackend:
        return self.find_plugin(backend_type, backend_min_version)(backend_config)
