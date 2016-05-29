from itertools import count
from typing import Iterator, Tuple, Optional
from common.models.task import TaskInfo
from common.models.graph import GraphStruct, GraphInstanceInfo
from common.models.schedule import ScheduledGraph
from util.config import Config, ConfigField
from util.symver import SymVer
from util.tuned_leveldb import LevelDB
from worker.backend import WorkerBackend
from master.backend import MasterBackend, GraphStructureNotFound


class WorkerLevelDBConfig(Config):
    db_path = ConfigField(type=str, required=True, default='/tmp/dedalus-worker-db')


class MasterLevelDBConfig(Config):
    db_path = ConfigField(type=str, required=True, default='/tmp/dedalus-master-db')


class WorkerLevelDBBackend(WorkerBackend):
    name = 'leveldb'
    version = SymVer(0, 0, 1)
    config_class = WorkerLevelDBConfig

    def __init__(self, backend_config: dict) -> None:
        super().__init__(backend_config)
        self.tasks_db = LevelDB(self.config.db_path)

    def read_task_info(self, task_id: str) -> TaskInfo:
        return TaskInfo.create(self.tasks_db.get(task_id))

    def write_task_info(self, task_id: str, task_info: TaskInfo):
        return self.tasks_db.put(task_id, task_info.to_json())

    def list_tasks(self, with_info: bool = False) -> 'Iterator[Tuple[str, Optional[TaskInfo]]]':
        for task_id, task_info in self.tasks_db.iterate_all(include_value=with_info):
            yield task_id, TaskInfo.create(task_info) if task_info else None


class MasterLevelDBBackend(MasterBackend):
    name = 'leveldb'
    version = SymVer(0, 0, 1)
    config_class = MasterLevelDBConfig

    def __init__(self, backend_config: dict) -> None:
        super().__init__(backend_config)
        self.db = LevelDB(self.config.db_path)
        self.graphs = self.db.collection_view('graphs')
        self.schedule = self.db.collection_view('schedule')
        self.instances = self.db.collection_view('instances')

    def read_graph_instance_info(self, instance_id: str) -> GraphInstanceInfo:
        return GraphInstanceInfo.create(self.instances.get(instance_id))

    def write_graph_instance_info(self, instance_id: str, instance_info: GraphInstanceInfo):
        return self.instances.put(instance_id, instance_info.to_json())

    def list_graph_instance_info(self, with_info: bool = False) -> Iterator[Tuple[str, Optional[GraphInstanceInfo]]]:
        for instance_id, instance_info in self.instances.iterate_all(include_value=with_info):
            yield instance_id, GraphInstanceInfo.create(instance_info) if instance_info else None

    def read_graph_struct(self, graph_name: str, revision: int = -1) -> GraphStruct:
        graph_view = self.graphs.collection_view(graph_name)
        if revision == -1:
            last_revision_struct = max(
                map(lambda _: GraphStruct.create(_[1]), graph_view.iterate_all(include_value=True)),
                key=lambda x: x.revision,
                default=None
            )
            if last_revision_struct is None:
                raise GraphStructureNotFound(graph_name)
            return last_revision_struct
        return GraphStruct.create(graph_view.get(str(revision)))

    def add_graph_struct(self, graph_name: str, graph_struct: GraphStruct) -> int:
        graph_view = self.graphs.collection_view(graph_name)
        graph_struct.graph_name = graph_name
        try:
            new_revision = self.read_graph_struct(graph_name).revision + 1
        except:
            new_revision = 0
        graph_struct.revision = new_revision
        # FIXME(luckygeck): possible race condition
        graph_view.put(str(new_revision), graph_struct.to_json())
        return new_revision

    def list_graph_struct(self, graph_name: Optional[str] = None, with_info: bool = False) -> Iterator[
            Tuple[str, int, Optional[GraphStruct]]]:
        db = self.graphs.collection_view(graph_name) if graph_name else self.graphs
        for key, graph_struct in db.iterate_all(include_value=with_info):
            name, revision = (graph_name, key) if graph_name else key.split('=', 1)
            yield name, revision, GraphStruct.create(graph_struct) if graph_struct else None

    def read_schedule(self, graph_name: str) -> ScheduledGraph:
        return ScheduledGraph.create(self.schedule.get(graph_name))

    def write_schedule(self, graph_name: str, schedule: str):
        schedule_json = ScheduledGraph().init(graph_name, schedule).to_json()
        graph_versions = self.list_graph_struct(graph_name, with_info=False)
        if next(graph_versions, None) is None:
            raise GraphStructureNotFound(graph_name)
        return self.schedule.put(graph_name, schedule_json)

    def list_schedules(self) -> Iterator[ScheduledGraph]:
        return (schedule for graph_name, schedule in self.schedule.iterate_all(include_value=True))
