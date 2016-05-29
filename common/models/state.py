import abc
from typing import Mapping, Set
from util.config import BaseConfig, IncorrectFieldType


class ForbiddenStateChange(Exception):
    def __init__(self, from_state_obj: 'StateMachine', to_state_name: str):
        self.from_state_obj = from_state_obj
        self.to_state = to_state_name

    def __str__(self):
        return 'TaskExecution state change from \'{}\' is only allowed to {} (tried to \'{}\')'.format(
            self.from_state_obj.name, self.from_state_obj.links[self.from_state_obj.name], self.to_state)


class StateMachine(BaseConfig):
    idle = 'idle'

    @property
    @abc.abstractmethod
    def links(self) -> 'Mapping[str, Set[str]]':
        pass

    def __init__(self, state: str = idle):
        assert self.is_status(state), 'Unknown state: {}'.format(state)
        self._state = state

    def to_json(self):
        return self.name

    def from_json(self, json_doc: str, skip_unknown_fields=False, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        if not isinstance(json_doc, str) and json_doc is not None:
            raise IncorrectFieldType(
                '{}: {} can be constructed only from str - {} passed.'.format(path_to_node, self.__class__.__name__,
                                                                              json_doc.__class__.__name__))
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

    def change_state(self, new_state: str, force: bool = False) -> 'StateMachine':
        assert self.is_status(new_state), 'Unknown state: {}'.format(new_state)
        old_state = self._state
        viable_transition = new_state in self.links[old_state]
        if force or old_state == new_state or viable_transition:
            self._state = new_state
            return self.__class__(old_state)
        raise ForbiddenStateChange(self, new_state)


class TaskState(StateMachine):
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


class GraphInstanceState(StateMachine):
    idle = 'idle'
    running = 'running'
    finished = 'finished'
    stopped = 'stopped'
    links = {
        idle: {running, stopped},
        running: {finished, stopped},
        finished: {},
        stopped: {},
    }
