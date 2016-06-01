import os
from threading import Thread, Event

from common.models.task import TaskInfo
from common.models.state import TaskState
from worker.backend import WorkerBackend
from worker.executor import ExecutionEnded, Executors
from worker.resource import Resources


class TaskExecution(Thread):
    def __init__(self, task_id: str, backend: WorkerBackend,
                 resources: Resources, executors: Executors) -> None:
        super().__init__()
        self.task_id = task_id
        self.backend = backend
        task_info = self.backend.read_task_info(task_id)
        self.resources = [resources.construct_resource(_) for _ in task_info.structure.resources]
        self.executor = executors.construct_executor(self.task_id, task_info.structure.executor)
        self.user_stop = Event()

    def get_task_state(self):
        return self.backend.read_task_state(self.task_id)

    def set_task_state(self, state: str):
        self.backend.write_task_state(self.task_id, state)

    def run(self):
        if self.prepare() and not self.user_stop.is_set():
            self.execute_task()

    def prepare(self):
        task_info = self.backend.read_task_info(self.task_id)
        task_info.exec_stats.start_preparation()
        self.backend.write_task_info(self.task_id, task_info)
        prep_error = None
        for resource in self.resources:
            if self.user_stop.is_set():
                break
            try:
                resource.ensure()
            except Exception as ex:
                print(ex)
                prep_error = ex
                break
        task_info = self.backend.read_task_info(self.task_id)
        task_info.exec_stats.finish_preparation(
            success=prep_error is None,
            prep_msg=prep_error,
            is_initiated_by_user=self.user_stop.is_set()
        )
        self.backend.write_task_info(self.task_id, task_info)
        return prep_error is None

    def execute_task(self):
        it = self.executor.start()
        task_info = self.backend.read_task_info(self.task_id)
        task_info.exec_stats.start_execution()
        self.backend.write_task_info(self.task_id, task_info)
        return_code = None
        try:
            with open(os.path.join(self.executor.work_dir, 'stdout.log'), 'a') as out_file:
                with open(os.path.join(self.executor.work_dir, 'stderr.log'), 'a') as err_file:
                    for stdout, stderr in it:
                        if stdout is not None:
                            print(stdout, file=out_file)
                        if stderr is not None:
                            print(stderr, file=err_file)
        except ExecutionEnded as ex:
            print('Execution ended! RetCode:', ex.retcode)
            return_code = ex.retcode
        task_info.exec_stats.finish_execution(retcode=return_code,
                                              is_initiated_by_user=self.user_stop.is_set())
        self.backend.write_task_info(self.task_id, task_info)

    def set_state(self, target_state: str) -> TaskState:
        state = self.backend.read_task_state(self.task_id)
        old_state_name = state.name
        old_state = state.change_state(new_state=target_state, force=False)  # check for validness of state change
        if old_state_name != target_state:
            if old_state_name == TaskState.idle and target_state == TaskState.preparing:
                self.start()
            elif target_state == TaskState.stopped:
                self.user_stop.set()
                self.executor.kill()
        return old_state


class Engine:
    def __init__(self, backend: WorkerBackend, resources: Resources, executors: Executors) -> None:
        self.tasks = dict()
        self.backend = backend
        self.resources = resources
        self.executors = executors

    def create_idle_task(self, task_id: str, task_struct: dict):
        return self.backend.write_task_info(task_id, TaskInfo.create({
            'task_id': task_id,
            'structure': task_struct
        }))

    def set_task_state(self, task_id: str, state: str) -> str:
        if task_id not in self.tasks:
            task_info = self.backend.read_task_info(task_id)
            old_state = task_info.exec_stats.state.change_state(state)  # check state transition validity
            if state != TaskState.preparing:  # check if we need to create run a task
                self.backend.write_task_info(task_id, task_info)
                return old_state.name
            # TODO: remove non-running tasks from self.tasks
            self.tasks[task_id] = TaskExecution(task_id, self.backend, self.resources, self.executors)
        return self.tasks[task_id].set_state(state).name
