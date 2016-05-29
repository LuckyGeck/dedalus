from itertools import chain
from collections import Counter
from typing import Iterable, Tuple

from common.models.task import TaskStruct
from util.config import Config, ConfigField, create_dict_field_type, create_list_field_type, StrListConfigField


class UnknownTasksInDeps(Exception):
    def __init__(self, tasks: 'Iterable[str]'):
        self.tasks = tasks

    def __str__(self):
        return 'Unknown tasks found in deps graph: {}'.format(', '.join(self.tasks))


class DuplicateTasksFound(Exception):
    def __init__(self, task_dups: 'Iterable[Tuple[str, int]]'):
        self.task_dups = task_dups

    def __str__(self):
        return 'Duplicate tasks found in graph: {}'.format(', '.join(k for k, v in self.task_dups))


class UnknownClusters(Exception):
    def __init__(self, clusters: 'Iterable[str]'):
        self.clusters = clusters

    def __str__(self):
        return 'Unknown clusters mentioned: {}'.format(', '.join(self.clusters))


class ExtendedTaskStruct(Config):
    task_name = ConfigField(type=str, required=True, default=None)
    task = TaskStruct()
    hosts = StrListConfigField()


class ExtendedTaskList(create_list_field_type(ExtendedTaskStruct)):
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
    def verify(self):
        super().verify()
        value_tasks = set(chain.from_iterable(self.values()))
        key_tasks = set(self.keys())
        all_tasks = set(_.task_name for _ in self._parent.tasks)
        not_found_tasks = key_tasks - all_tasks
        not_found_tasks.update(value_tasks - all_tasks)
        if not_found_tasks:
            raise UnknownTasksInDeps(not_found_tasks)


class GraphStruct(Config):
    graph_name = ConfigField(type=str, required=True, default='graph00')
    revision = ConfigField(type=int, required=True, default=0)

    clusters = create_dict_field_type(StrListConfigField)()
    tasks = ExtendedTaskList()
    deps = TaskDependencies()


class GraphInstanceInfo(Config):
    instance_id = ConfigField(type=str, required=True, default=None)
    structure = GraphStruct()
