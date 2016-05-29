from typing import Optional

from common.models.executor import ExecutorInfo
from common.models.resource import ResourceInfo
from util.config import Config, ConfigField, BaseConfig, IncorrectFieldType, ListConfigField, DateTimeField


class ForbiddenStateChange(Exception):
    def __init__(self, from_state, to_state):
        self.from_state = from_state
        self.to_state = to_state

    def __str__(self):
        return 'TaskExecution state change from \'{}\' is only allowed to {} (tried to \'{}\')'.format(
            self.from_state, TaskState.links[self.from_state], self.to_state)


class TaskState(BaseConfig):
    idle = 'idle'
    preparing = 'preparing'
    prepared = 'prepared'
    running = 'running'
    finished = 'finished'
    stopped = 'stopped'
    prepfailed = 'prepfailed'

    links = {
        idle: {preparing, stopped},
        preparing: {prepfailed, prepared, stopped},
        prepared: {running, stopped},
        running: {finished, stopped},
        finished: {},
        stopped: {},
        prepfailed: {},
    }

    def __init__(self, state: str = idle):
        assert self.is_status(state), 'Unknown state: {}'.format(state)
        self._state = state

    def to_json(self):
        return self.name

    def from_json(self, json_doc: str, skip_unknown_fields=False):
        if not isinstance(json_doc, str) and json_doc is not None:
            raise IncorrectFieldType(
                'TaskState can be constructed only from str - {} passed.'.format(json_doc.__class__.__name__))
        self.change_state(json_doc, force=True)
        return self

    def verify(self, path_to_node: str = ''):
        assert self.is_status(self._state)

    @property
    def name(self) -> str:
        return self._state

    @classmethod
    def is_status(cls, state) -> bool:
        attr = getattr(cls, state)
        return attr == state and isinstance(attr, str)

    def change_state(self, new_state: str, force: bool = False) -> 'TaskState':
        assert self.is_status(new_state), 'Unknown state: {}'.format(new_state)
        old_state = self._state
        viable_transition = new_state in self.links[old_state]
        if force or old_state == new_state or viable_transition:
            self._state = new_state
            return TaskState(old_state)
        raise ForbiddenStateChange(old_state, new_state)


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

    def from_json(self, json_doc: int, skip_unknown_fields=False):
        if not isinstance(json_doc, int) and json_doc is not None:
            raise IncorrectFieldType(
                'TaskReturnCode can be constructed only from int - {} passed.'.format(json_doc.__class__.__name__))
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
    resources = ListConfigField(ResourceInfo)
    executor = ExecutorInfo()


class TaskInfo(Config):
    task_id = ConfigField(type=str, required=False, default=None)
    structure = TaskStruct()
    exec_stats = TaskExecutionInfo()
