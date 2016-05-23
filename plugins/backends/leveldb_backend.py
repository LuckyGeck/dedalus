from common.models.task import TaskInfo
from util.config import Config, ConfigField
from util.symver import SymVer
from util.tuned_leveldb import LevelDB
from worker.backend import WorkerBackend


class WorkerLevelDBConfig(Config):
    db_path = ConfigField(type=str, required=True, default='/tmp/dedalus-worker-db')


class WorkerLevelDBBackend(WorkerBackend):
    name = 'leveldb'
    version = SymVer(0, 0, 1)
    config_class = WorkerLevelDBConfig

    def __init__(self, backend_config: dict) -> None:
        super().__init__(backend_config)
        self.tasks_db = LevelDB(self.config.db_path)

    def read_task_info(self, task_id: str) -> TaskInfo:
        return TaskInfo.create(self.tasks_db.get(task_id))

    def save_task_info(self, task_id: str, task_info: TaskInfo):
        return self.tasks_db.put(task_id, task_info.to_json())

    def list_tasks(self, with_info: bool = False) -> 'Iterator[Tuple[str, Optional[TaskInfo]]]':
        for task_id, task_info in self.tasks_db.iterate_all(include_value=with_info):
            yield task_id, TaskInfo.create(task_info) if task_info else None
