from typing import Optional
import requests
from common.models.state import TaskState


class WorkerApiClient:
    # TODO: Make error handling in client

    def __init__(self, worker_host: str = 'localhost', worker_port: int = 8081,
                 ssl: bool = False, api_version: str = 'v1.0'):
        self._url_prefix = 'http{}://{}:{}/{}/'.format('s' if ssl else '', worker_host, worker_port, api_version)

    def create_task(self, task_struct: dict) -> str:
        """Creates task by task_struct
        :returns str: task_id of created task
        """
        return requests.post(self._url_prefix + 'task/', json=task_struct).json()['payload']['task_id']

    def start_task(self, task_id: str) -> TaskState:
        """Starts task execution by task_id
        :returns TaskState: new state
        """
        return TaskState(requests.post('{}task/{}/start'.format(self._url_prefix,
                                                                task_id)).json()['payload']['new_state'])

    def get_task_state(self, task_id: str) -> TaskState:
        """Returns task state by task_id
        :returns TaskState: task state
        """
        return TaskState(requests.get('{}task/{}/state'.format(self._url_prefix, task_id)).json()['payload']['state'])

    def get_task_log(self, task_id: str, log_type: str = 'out') -> Optional[str]:
        """Returns task log by task_id and log type
        :returns Optional[str]: Contents of the log. None, if log is not found
        """
        assert log_type in ('out', 'err'), 'Log type should be one of (out, err)'
        result = requests.get('{}task/{}/log/{}'.format(self._url_prefix, task_id, log_type))
        if result.ok:
            return result.json()['payload']['data']
