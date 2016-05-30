import abc
from typing import Iterator, Tuple, Optional

from common.models.graph import GraphInstanceInfo, GraphStruct
from common.models.schedule import ScheduledGraph
from common.models.state import GraphInstanceState
from util.config import Config
from util.plugins import PluginBase, PluginsMaster
from util.symver import SymVer


class GraphInstanceInfoNotFound(Exception):
    def __init__(self, graph_id: str) -> None:
        self.graph_id = graph_id

    def __str__(self):
        return 'GraphInstanceInfo "{}" not found in backend'.format(self.graph_id)


class GraphStructureNotFound(Exception):
    def __init__(self, graph_name: str) -> None:
        self.graph_name = graph_name

    def __str__(self):
        return 'GraphStruct "{}" not found in backend'.format(self.graph_name)


class MasterBackend(PluginBase, metaclass=abc.ABCMeta):
    def __init__(self, backend_config: dict) -> None:
        self.config = self.config_class()
        self.config.from_json(backend_config)
        self.config.verify()

    @classmethod
    @abc.abstractmethod
    def config_class(cls) -> Config:
        return Config()

    @abc.abstractmethod
    def read_graph_instance_info(self, instance_id: str) -> GraphInstanceInfo:
        """Read graph instance info by instance_id
        :raises GraphInstanceInfoNotFound: if graph instance info has not been found
        """
        pass

    @abc.abstractmethod
    def write_graph_instance_info(self, instance_id: str, instance_info: GraphInstanceInfo):
        """Create or replace graph instance info by instance_id"""
        pass

    @abc.abstractmethod
    def list_graph_instance_info(self, with_info: bool = False) -> Iterator[Tuple[str, Optional[GraphInstanceInfo]]]:
        """List all known graph infos.
        :returns iterator over pairs of instance_id and instance_info.
                 If with_info is not set, all instance_infos will be None
        """
        pass

    @abc.abstractmethod
    def read_graph_struct(self, graph_name: str, revision: int = -1) -> GraphStruct:
        """Read graph struct by graph_name and revision. If revision == -1, last revision is selected
        :raises GraphStructureNotFound: if graph struct has not been found
        """
        pass

    @abc.abstractmethod
    def add_graph_struct(self, graph_name: str, graph_struct: GraphStruct) -> int:
        """Create new graph struct revision for graph_name
        :returns int: New revision number
        """
        pass

    @abc.abstractmethod
    def list_graph_struct(self, graph_name: Optional[str] = None, with_info: bool = False) -> Iterator[
            Tuple[str, int, Optional[GraphStruct]]]:
        """List all known graph structs. If graph_name is set, only versions of this graph are shown
        :returns iterator over triplets of graph_name, revision and graph_struct. If with_info is not set, all graph_structs are None
        """
        pass

    @abc.abstractmethod
    def write_schedule(self, graph_name: str, schedule: str):
        """Create or replace existing schedule for graph struct by graph_name. Schedule is in cron format."""
        pass

    @abc.abstractmethod
    def list_schedules(self) -> Iterator[ScheduledGraph]:
        """List all scheduled graphs"""
        pass

    def read_instance_state(self, instance_id: str) -> GraphInstanceState:
        """
        Receives graph instance state from backend.
        :raises if instance is not found
        :param instance_id: GraphInstanceInfo's id
        :return: Current graph instance state
        """
        return self.read_graph_instance_info(instance_id).exec_stats.state

    def write_instance_state(self, instance_id: str, state: str) -> GraphInstanceState:
        """
        Changes graph instance state and saves it to backend.
        :raises if instance is not found
        :param instance_id: GraphInstanceInfo's id to change state for
        :param state: New state for a graph instance
        :return: Previous graph instance state
        """
        assert isinstance(state, GraphInstanceState)
        instance_info = self.read_graph_instance_info(instance_id)
        old_state = instance_info.exec_stats.name.change_state(state, force=False)
        self.write_graph_instance_info(instance_id, instance_info)
        return old_state


class MasterBackends(PluginsMaster):
    plugin_base_class = MasterBackend

    def construct_backend(self, backend_type: str, backend_config: dict,
                          backend_min_version: SymVer = SymVer()) -> MasterBackend:
        return self.find_plugin(backend_type, backend_min_version)(backend_config)
