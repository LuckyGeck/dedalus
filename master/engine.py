import time
from typing import Dict, List
from common.models.graph import GraphStruct, GraphInstanceInfo, TaskExecutionInfo, TaskOnHostExecutionInfo, \
    ExtendedTaskStruct
from master.backend import MasterBackend
from threading import Lock, Thread, Event
from common.models.state import GraphInstanceState, TaskState
from worker.api_client import WorkerApiClient
import logging


class TaskMentor:
    def __init__(self, task: ExtendedTaskStruct, graph_mentor: 'GraphMentor'):
        self.task = task
        self.task_name = task.task_name
        self.graph_mentor = graph_mentor
        self.backend = graph_mentor.backend
        self.instance_info = graph_mentor.instance_info
        self._task_execution_info = self.instance_info.exec_stats.per_task_execution_info[self.task_name]  # type: TaskExecutionInfo
        self._per_host_info = self._task_execution_info.per_host_info
        self._dependencies = self.instance_info.structure.deps.get(self.task_name, set())
        self._dependents = self._task_execution_info.dependents

    def create_direct_refs(self):
        self._dependencies = {
            self.graph_mentor.task_mentors[_] for _ in self._dependencies
        }
        self._dependents = {
            self.graph_mentor.task_mentors[_] for _ in self._dependents
        }

    def get_ready_dependents(self) -> 'List[TaskMentor]':
        return [_ for _ in self._dependents if _.all_deps_ready]

    def tick(self):
        # TODO: use asyncio \ aiohttp instead of sequential requests
        # TODO: handle errors
        for host, per_host_info in self._per_host_info.items():
            assert isinstance(per_host_info, TaskOnHostExecutionInfo)
            client = WorkerApiClient(worker_host=host)
            if per_host_info.task_id is None:
                per_host_info.task_id = client.create_task(self.task.task_struct.to_json())
                per_host_info.state.change_state('idle', force=True)
                self._save_to_backend()
            if per_host_info.state.name == TaskState.idle:
                per_host_info.state.change_state(client.start_task(per_host_info.task_id).name, force=True)
                self._save_to_backend()
            if not per_host_info.state.is_terminal:
                new_state_name = client.get_task_state(per_host_info.task_id).name
                if new_state_name != per_host_info.state.name:
                    per_host_info.state.change_state(new_state_name, force=True)
                    self._save_to_backend()
                if per_host_info.state.is_failed:
                    break

    def _save_to_backend(self):
        self.backend.write_graph_instance_info(self.instance_info.instance_id, self.instance_info)

    @property
    def all_deps_ready(self) -> bool:
        return all(_.is_done for _ in self._dependencies)

    @property
    def is_done(self) -> bool:
        return self._task_execution_info.aggregated_state.is_terminal

    @property
    def is_failed(self) -> bool:
        return self._task_execution_info.aggregated_state.is_failed


class GraphMentor:
    def __init__(self, instance_info: GraphInstanceInfo, backend: MasterBackend, shutdown: Event, user_stop: Event):
        self.backend = backend
        self._shutdown = shutdown
        self._user_stop = user_stop
        self.instance_info = instance_info
        self.task_mentors = {
            task.task_name: TaskMentor(task, self)
            for task in instance_info.structure.tasks
        }  # type: Dict[str, TaskMentor]
        for mentor in self.task_mentors.values():
            mentor.create_direct_refs()
        self.working_mentors = {
            task_name: mentor
            for task_name, mentor in self.task_mentors.items()
            if mentor.all_deps_ready and not mentor.is_done
        }  # type: Dict[str, TaskMentor]
        assert self.working_mentors or not self.task_mentors, 'Graph has no tasks without dependencies'

    def tick(self):
        ready_mentors = []
        new_mentors = dict()
        for task_name, mentor in self.working_mentors.items():
            if self._shutdown.is_set() or self._user_stop.is_set():
                self._stop_execution()
                return
            mentor.tick()
            if mentor.is_done:
                ready_mentors.append(task_name)
                if mentor.is_failed:
                    self._stop_execution()
                    return
                new_mentors.update({_.task_name: _ for _ in mentor.get_ready_dependents()})
        for task_name, mentor in new_mentors.items():
            self.working_mentors[task_name] = mentor
        for task_name in ready_mentors:
            del self.working_mentors[task_name]
        if self.is_done:
            self.instance_info.exec_stats.finish_execution(is_failed=False, is_initiated_by_user=False)
            self._save_to_backend()

    def _stop_execution(self):
        if not self._shutdown.is_set():
            self.instance_info.exec_stats.finish_execution(is_failed=self.is_failed,
                                                           is_initiated_by_user=self._user_stop.is_set())
            self._save_to_backend()
        self.working_mentors = {}

    def _save_to_backend(self):
        self.backend.write_graph_instance_info(self.instance_info.instance_id, self.instance_info)

    @property
    def is_failed(self) -> bool:
        return any(_.is_failed for _ in self.task_mentors.values())

    @property
    def is_done(self) -> bool:
        return not self.working_mentors


class GraphExecutor(Thread):
    def __init__(self, instance_id: str, engine: 'Engine'):
        super().__init__(name='dedalus-exec-{}'.format(instance_id))
        self.instance_id = instance_id
        self.engine = engine
        self._user_stop = Event()
        self._shutdown = Event()
        self.start()

    def run(self):
        logging.debug('Start executing %s', self.instance_id)
        try:
            instance_info = self.engine.backend.read_graph_instance_info(self.instance_id)
            if instance_info.exec_stats.state.idle:
                instance_info.exec_stats.start_execution()
                instance_info.exec_stats.init_per_task_execution_info()
                self.engine.backend.write_graph_instance_info(self.instance_id, instance_info)
                instance_info = self.engine.backend.read_graph_instance_info(self.instance_id)
            graph_mentor = GraphMentor(instance_info, self.engine.backend, self._shutdown, self._user_stop)
            while not graph_mentor.is_done:
                time.sleep(1)
                graph_mentor.tick()  # it will switch graph_mentor to is_done to exit on _shutdown and _user_stop Events
        except Exception as ex:
            instance_info = self.engine.backend.read_graph_instance_info(self.instance_id)
            instance_info.exec_stats.finish_execution(is_failed=True, is_initiated_by_user=False, fail_msg=str(ex))
            self.engine.backend.write_graph_instance_info(self.instance_id, instance_info)
        finally:
            with self.engine.instances_lock:
                del self.engine.running_graphs[self.instance_id]
            logging.debug('Stop executing %s', self.instance_id)

    def set_state(self, target_state: str) -> GraphInstanceState:
        state = self.engine.backend.read_instance_state(self.instance_id)
        old_state_name = state.name
        old_state = state.change_state(new_state=target_state, force=False)  # check for validness of state change
        if old_state_name != target_state:
            if target_state == TaskState.stopped:
                self._user_stop.set()
            else:
                self.engine.backend.write_instance_state(self.instance_id, state.name)
        return old_state

    def shutdown(self):
        self._shutdown.set()


class Engine:
    def __init__(self, backend: MasterBackend):
        self.backend = backend
        self.instances_lock = Lock()
        self.running_graphs = dict()  # type: Dict[str, GraphExecutor]
        self._spawn_running_graphs()

    def _spawn_running_graphs(self):
        with self.instances_lock:
            for instance_id, instance_info in self.backend.list_graph_instance_info(with_info=True):
                if instance_info.exec_stats.state.name == GraphInstanceState.running:
                    self.running_graphs[instance_id] = GraphExecutor(instance_id, self)

    def add_graph_struct(self, graph_name: str, graph_struct: dict) -> int:
        return self.backend.add_graph_struct(graph_name, GraphStruct.create(graph_struct))

    def add_graph_instance(self, instance_id: str, graph_struct: GraphStruct) -> GraphInstanceInfo:
        instance = GraphInstanceInfo()
        instance.instance_id = instance_id
        instance.structure = graph_struct
        self.backend.write_graph_instance_info(instance_id, instance)
        return instance

    def set_graph_instance_state(self, instance_id: str, state: str) -> str:
        with self.instances_lock:
            if instance_id in self.running_graphs:
                return self.running_graphs[instance_id].set_state(state).name
            instance_info = self.backend.read_graph_instance_info(instance_id)
            old_state = instance_info.exec_stats.state.change_state(state)  # check state transition validity
            if state != GraphInstanceState.running:  # check if we need to create run a task
                self.backend.write_graph_instance_info(instance_id, instance_info)
            else:
                assert state == GraphInstanceState.running
                self.running_graphs[instance_id] = GraphExecutor(instance_id, self)
            return old_state.name

    def shutdown(self):
        with self.instances_lock:
            for runner in self.running_graphs.values():
                runner.shutdown()
