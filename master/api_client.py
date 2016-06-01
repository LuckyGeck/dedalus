import requests
from typing import Optional, Tuple, List
from common.models.state import GraphInstanceState
from common.models.graph import GraphStruct, GraphInstanceInfo
import logging
import json


class MasterApiException(Exception):
    def __init__(self, response: dict):
        self.response = response

    @property
    def msg(self) -> str:
        return 'Api Exception happened.'

    def __str__(self):
        return '{}\nResponse:\n{}'.format(self.msg, json.dumps(self.response, indent=2))


class GraphCreationFailed(MasterApiException):
    msg = 'Graph creation failed.'


class InstanceStateChangeFailed(MasterApiException):
    msg = 'Failed to change instance state.'


class MasterApiClient:
    # TODO: Make error handling in client

    def __init__(self, master_host: str = 'localhost', master_port: int = 8080,
                 ssl: bool = False, api_version: str = 'v1.0'):
        self._url_prefix = 'http{}://{}:{}/{}/'.format('s' if ssl else '', master_host, master_port, api_version)

    def _get_graph_url(self, graph_name: Optional[str] = None, graph_revision: Optional[int] = None) -> str:
        url = self._url_prefix + 'graph'
        if graph_name:
            url += '/' + graph_name
            if graph_revision:
                url += '/' + str(graph_revision)
        else:
            assert graph_revision is None
        return url

    def _get_instance_url(self, instance_id: str) -> str:
        return self._url_prefix + 'instance/' + instance_id

    def create_graph(self, graph_struct: dict, graph_name: 'Optional[str]' = None) -> Tuple[str, str]:
        """Creates graph by graph_struct. Optionally, you can set graph_name outside of graph_struct.
        :returns Tuple[str, str]: tuple (name, revision) of created task
        """
        data = requests.post(self._get_graph_url(graph_name), json=graph_struct).json()
        logging.debug(data)
        payload = data.get('payload')
        if payload and payload.get('graph_name') and payload.get('revision') is not None:
            return payload['graph_name'], payload['revision']
        raise GraphCreationFailed(data)

    def read_graph(self, graph_name: str, graph_revision: Optional[int] = None) -> GraphStruct:
        """Read info about a specified graph's version. If graph_revision is not set, last revision is used.
        :returns GraphStruct: info about a graph
        """
        return GraphStruct.create(requests.get(self._get_graph_url(graph_name, graph_revision)).json()['payload'])

    def list_graphs(self, graph_name: Optional[str] = None, offset: int = 0, limit: Optional[int] = None,
                    with_info: bool = False) -> List[GraphStruct]:
        """List all known graphs. If graph_name is set, then include only all versions of this graph.
           If with_info is false, only graph_name and revision are received.
        :returns List[GraphStruct]: List of received graph structs
        """
        data = {offset: offset, with_info: '1' if with_info else '0'}
        if graph_name:
            data['graph_name'] = graph_name
        if limit is not None:
            data['limit'] = limit
        return [GraphStruct.create(_) for _ in requests.get(self._url_prefix + 'graphs', params=data).json()['payload']]

    def launch_graph(self, graph_name: str, graph_revision: Optional[int] = None) -> str:
        """Creates a new instance for specified graph's version. If graph_revision is not set, last revision is used.
        :returns str: created graph instance id
        """
        data = requests.post(self._get_graph_url(graph_name, graph_revision) + '/launch')
        return data.json()['payload']['instance_id']

    def list_instances(self, offset: int = 0, limit: Optional[int] = None,
                       with_info: bool = False) -> List[GraphInstanceInfo]:
        """List all known graph instances.
           If with_info is false, only instance_id will be received.
        :returns List[GraphInstanceInfo]: List of received graph instances
        """
        data = {offset: offset, with_info: '1' if with_info else '0'}
        if limit is not None:
            data['limit'] = limit
        return [GraphInstanceInfo.create(_)
                for _ in requests.get(self._url_prefix + 'instances', params=data).json()['payload']]

    def read_instance(self, instance_id: str) -> GraphInstanceInfo:
        """Read info about a specified graph's instance.
        :returns GraphInstanceInfo: info about a graph's instance
        """
        return GraphInstanceInfo.create(requests.get(self._get_instance_url(instance_id)).json()['payload'])

    def start_instance(self, instance_id: str) -> Tuple[GraphInstanceState, GraphInstanceState]:
        """Tries to set graph instance state to running
        :returns Tuple[GraphInstanceState, GraphInstanceState]: pair of old state and new state
        """
        return self._set_instance_state(instance_id, action='start')

    def stop_instance(self, instance_id: str) -> Tuple[GraphInstanceState, GraphInstanceState]:
        """Tries to set graph instance state to stopped
        :returns Tuple[GraphInstanceState, GraphInstanceState]: pair of old state and new state
        """
        return self._set_instance_state(instance_id, action='stop')

    def _set_instance_state(self, instance_id: str, action: str = 'start') -> Tuple[GraphInstanceState,
                                                                                    GraphInstanceState]:
        assert action in ('start', 'stop')
        data = requests.post(self._get_instance_url(instance_id) + '/' + action).json()
        payload = data.get('payload')
        if payload and payload.get('prev_state') and payload.get('new_state'):
            return (GraphInstanceState().from_json(payload['prev_state']),
                    GraphInstanceState().from_json(payload['new_state']))
        raise InstanceStateChangeFailed(data)

# ('GET', '/ping', 'ping'),
# ('GET', '/v1.0/graphs', 'list_graphs'),
# ('POST', '/v1.0/graph', 'create_graph'),
# ('POST', '/v1.0/graph/{graph_name}', 'create_graph'),
# ('GET', '/v1.0/graph/{graph_name}', 'read_graph'),
# ('GET', '/v1.0/graph/{graph_name}/{revision}', 'read_graph'),
# ('POST', '/v1.0/graph/{graph_name}/{revision}/launch', 'launch_graph'),
# ('POST', '/v1.0/graph/{graph_name}/launch', 'launch_graph'),

# ('GET', '/v1.0/instances', 'list_instances'),
# ('GET', '/v1.0/instance/{instance_id}', 'read_instance'),
# ('POST', '/v1.0/instance/{instance_id}/start', 'start_instance'),
# ('POST', '/v1.0/instance/{instance_id}/stop', 'stop_instance'),
