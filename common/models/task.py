from typing import Optional

from common.models.executor import ExecutorInfo
from common.models.resource import ResourceInfoList
from common.models.state import TaskState
from util.config import Config, ConfigField, BaseConfig, IncorrectFieldType, DateTimeField


class TaskReturnCode(BaseConfig):
    def __init__(self, retcode: int = None):
        self._retcode = retcode

    @property
    def retcode(self) -> 'Optional[int]':
        return self._retcode

    def set_retcode(self, retcode: int):
        self._retcode = retcode

    def to_json(self):
        return self.retcode

    def from_json(self, json_doc: int, skip_unknown_fields=False, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        if not isinstance(json_doc, int) and json_doc is not None:
            raise IncorrectFieldType(
                '{}: TaskReturnCode can be constructed only from int - {} passed.'.format(path_to_node,
                                                                                          json_doc.__class__.__name__))
        self._retcode = json_doc
        return self

    def verify(self, path_to_node: str = ''):
        assert isinstance(self._retcode, int) or self._retcode is None


class TaskExecutionInfo(Config):
    state = TaskState(TaskState.idle)
    retcode = TaskReturnCode()

    prep_start_time = DateTimeField()
    prep_finish_time = DateTimeField()
    prep_msg = ConfigField(type=str, required=False, default=None)

    start_time = DateTimeField()
    finish_time = DateTimeField()

    def start_preparation(self):
        self.state.change_state(TaskState.preparing)
        self.prep_start_time.set_to_now()

    def finish_preparation(self, success: bool, prep_msg: str = 'OK', is_initiated_by_user: bool = False):
        new_status = TaskState.prepared if success else TaskState.prepfailed
        if is_initiated_by_user:
            new_status = TaskState.stopped
        self.state.change_state(new_status)
        self.prep_msg = prep_msg
        self.prep_finish_time.set_to_now()

    def start_execution(self):
        self.state.change_state(TaskState.running)
        self.start_time.set_to_now()

    def finish_execution(self, retcode: int, is_initiated_by_user: bool = False):
        self.finish_time.set_to_now()
        self.retcode.set_retcode(retcode)
        self.state.change_state(TaskState.stopped if is_initiated_by_user else TaskState.finished)


class TaskStruct(Config):
    resources = ResourceInfoList()
    executor = ExecutorInfo()


class TaskInfo(Config):
    task_id = ConfigField(type=str, required=False, default=None)
    structure = TaskStruct()
    exec_stats = TaskExecutionInfo()
