from itertools import chain
from collections import Counter, defaultdict
from typing import Iterable, Tuple, Dict, List, Optional

from common.models.task import TaskStruct
from common.models.state import GraphInstanceState, TaskState
from util.config import Config, ConfigField, create_dict_field_type, create_list_field_type, StrListConfigField, \
    DateTimeField
from util.dependency_loops import detect_loop


class UnknownTasksInDeps(Exception):
    def __init__(self, tasks: 'Iterable[str]'):
        self.tasks = list(tasks)

    def __str__(self):
        return 'Unknown tasks found in deps graph: {}'.format(', '.join(self.tasks))


class DuplicateTasksFound(Exception):
    def __init__(self, task_dups: 'Iterable[Tuple[str, int]]'):
        self.task_dups = list(task_dups)

    def __str__(self):
        return 'Duplicate tasks found in graph: {}'.format(', '.join(k for k, v in self.task_dups))


class UnknownClusters(Exception):
    def __init__(self, clusters: 'Iterable[str]'):
        self.clusters = list(clusters)

    def __str__(self):
        return 'Unknown clusters mentioned: {}'.format(', '.join(self.clusters))


class DependencyLoopFound(Exception):
    def __init__(self, loop: 'Iterable[str]'):
        self.loop = list(loop)

    def __str__(self):
        return 'Loop in task dependencies found: {}'.format('->'.join(self.loop))


class ExtendedTaskStruct(Config):
    task_name = ConfigField(type=str, required=True, default=None)
    task_struct = TaskStruct()
    hosts = StrListConfigField()


class ExtendedTaskList(create_list_field_type(ExtendedTaskStruct)):
    def __iter__(self) -> 'Iterable[ExtendedTaskStruct]':
        return super().__iter__()

    def verify(self):
        super().verify()
        assert all(isinstance(_, ExtendedTaskStruct) for _ in self)
        dups = [(k, v) for k, v in Counter(_.task_name for _ in self).items() if v > 1]
        if dups:
            raise DuplicateTasksFound(dups)
        needed_clusters = set(chain.from_iterable(_.hosts for _ in self))
        not_found_clusters = needed_clusters - set(self._parent.clusters.keys())
        if not_found_clusters:
            raise UnknownClusters(not_found_clusters)


class TaskDependencies(create_dict_field_type(StrListConfigField)):
    def __iter__(self) -> 'Iterable[Tuple[str, List[str]]]':
        return super().__iter__()

    def verify(self):
        super().verify()
        value_tasks = set(chain.from_iterable(self.values()))
        key_tasks = set(self.keys())
        all_tasks = set(_.task_name for _ in self._parent.tasks)
        not_found_tasks = key_tasks - all_tasks
        not_found_tasks.update(value_tasks - all_tasks)
        if not_found_tasks:
            raise UnknownTasksInDeps(not_found_tasks)
        # Checking for cycles
        loop = detect_loop(self)
        if loop:
            raise DependencyLoopFound(loop)


class GraphStruct(Config):
    graph_name = ConfigField(type=str, required=True, default='graph00')
    revision = ConfigField(type=int, required=True, default=0)

    clusters = create_dict_field_type(StrListConfigField)()
    tasks = ExtendedTaskList()
    deps = TaskDependencies()


class TaskOnHostExecutionInfo(Config):
    task_id = ConfigField(type=str, required=False, default=None)
    state = TaskState()


HostToExecutionInfo = create_dict_field_type(TaskOnHostExecutionInfo)


class TaskExecutionInfo(Config):
    per_host_info = HostToExecutionInfo()  # type: Dict[str, TaskOnHostExecutionInfo]
    dependents = StrListConfigField()

    @property
    def aggregated_state(self) -> TaskState:
        states = {_.state.name for _ in self.per_host_info.values() if _.task_id}
        return TaskState.aggregate_states(states)


TaskNameToExecutionInfo = create_dict_field_type(TaskExecutionInfo)


class GraphInstanceExecutionInfo(Config):
    state = GraphInstanceState()

    start_time = DateTimeField()
    finish_time = DateTimeField()

    fail_msg = ConfigField(type=str, required=False, default=None)

    per_task_execution_info = TaskNameToExecutionInfo()  # type: Dict[str, TaskExecutionInfo]

    def start_execution(self):
        self.state.change_state(GraphInstanceState.running)
        self.start_time.set_to_now()

    def init_per_task_execution_info(self):
        assert isinstance(self._parent, GraphInstanceInfo)
        structure = self._parent.structure
        task2dependents = defaultdict(set)
        for task_from, deps in structure.deps.items():
            for task_to in deps:
                task2dependents[task_to].add(task_from)
        for task in structure.tasks:
            # FIXME: upyachka with adding elements to config fields
            task_execution_info = \
                self.per_task_execution_info.setdefault(task.task_name,
                                                        TaskExecutionInfo(parent_object=self.per_task_execution_info,
                                                                          parent_key=task.task_name))
            task_execution_info.dependents.extend(task2dependents[task.task_name])
            for cluster in task.hosts:
                for host in structure.clusters[cluster]:
                    task_execution_info.per_host_info.setdefault(host,
                                                                 TaskOnHostExecutionInfo(
                                                                     parent_object=task_execution_info.per_host_info,
                                                                     parent_key=host
                                                                 ))

    def finish_execution(self, is_failed: bool = False, is_initiated_by_user: bool = False, fail_msg: str = None):
        self.finish_time.set_to_now()
        self.fail_msg = fail_msg
        if not is_failed and not is_initiated_by_user:
            self.state.change_state(GraphInstanceState.finished)
        else:
            self.state.change_state(GraphInstanceState.stopped if is_initiated_by_user else GraphInstanceState.failed)


class GraphInstanceInfo(Config):
    instance_id = ConfigField(type=str, required=True, default=None)
    structure = GraphStruct()
    exec_stats = GraphInstanceExecutionInfo()
