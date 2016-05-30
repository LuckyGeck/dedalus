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
    states_aggregation_ordering = [idle]

    @property
    @abc.abstractmethod
    def links(self) -> 'Mapping[str, Set[str]]':
        return dict()

    @property
    @abc.abstractmethod
    def failed_states(self) -> 'Set[str]':
        return {}

    def __init__(self, state: str = idle, **kwargs):
        super().__init__(**kwargs)
        assert self.is_status(state), 'Unknown state: {}'.format(state)
        self._state = state

    def to_json(self):
        return self.name

    def from_json(self, json_doc: str, skip_unknown_fields=False):
        if not isinstance(json_doc, str) and json_doc is not None:
            raise IncorrectFieldType(
                '{}: {} can be constructed only from str - {} passed.'.format(self.path_to_node,
                                                                              self.__class__.__name__,
                                                                              json_doc.__class__.__name__))
        self.change_state(json_doc, force=True)
        return self

    def verify(self):
        assert self.is_status(self._state), \
            '{}: {} should have valid state, but is set to \'{}\''.format(self.path_to_node, self.__class__.__name__,
                                                                          self._state)

    @property
    def name(self) -> str:
        return self._state

    @classmethod
    def is_status(cls, state) -> bool:
        attr = getattr(cls, state)
        return attr == state and isinstance(attr, str)

    @property
    def is_terminal(self) -> bool:
        return not self.links[self._state]

    @property
    def is_failed(self) -> bool:
        return self.name in self.failed_states

    @classmethod
    def aggregate_states(cls, states: 'Set[str]'):
        for state in cls.states_aggregation_ordering:
            if state in states:
                return cls(state)
        return cls()

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
    failed = 'failed'
    stopped = 'stopped'
    prepfailed = 'prepfailed'

    links = {
        idle: {preparing, stopped},
        preparing: {prepfailed, prepared, stopped},
        prepared: {running, stopped},
        running: {finished, failed, stopped},
        finished: {},
        failed: {},
        stopped: {},
        prepfailed: {},
    }

    states_aggregation_ordering = (
        stopped, prepfailed, failed, running, prepared, preparing, idle, finished
    )

    failed_states = {stopped, prepfailed, failed}


class GraphInstanceState(StateMachine):
    idle = 'idle'
    running = 'running'
    finished = 'finished'
    failed = 'failed'
    stopped = 'stopped'
    links = {
        idle: {running, stopped},
        running: {finished, stopped},
        finished: {},
        stopped: {},
    }

    states_aggregation_ordering = (
        stopped, failed, running, idle, finished
    )

    failed_states = {stopped, failed}
