from common.models.graph import GraphStruct, GraphInstanceInfo
from master.backend import MasterBackend


class Engine:
    def __init__(self, backend: MasterBackend):
        self.backend = backend

    def add_graph_struct(self, graph_name: str, graph_struct: dict) -> int:
        return self.backend.add_graph_struct(graph_name, GraphStruct.create(graph_struct))

    def add_graph_instance(self, instance_id: str, graph_struct: GraphStruct) -> GraphInstanceInfo:
        instance = GraphInstanceInfo(instance_id, graph_struct)
        self.backend.write_graph_instance_info(instance_id, instance_id)
        return instance