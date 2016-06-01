#!/usr/bin/env python
from time import sleep
from common.models.state import TaskState
from worker.api_client import WorkerApiClient
import json


def main():
    # http POST :8080/v1.0/task/
    cli = WorkerApiClient(worker_host='localhost', worker_port=8081)
    task_id = cli.create_task(task_struct=json.load(open('worker/tests/sample_task.json')))
    print(task_id)
    task_state = cli.start_task(task_id)
    print(task_state.name)
    while True:
        sleep(1)
        task_state = cli.get_task_state(task_id)
        print(task_state.name)
        if task_state.is_terminal:
            print('Out:')
            print(cli.get_task_log(task_id, 'out'), end='')
            print('Err:')
            print(cli.get_task_log(task_id, 'err'), end='')
            break

if __name__ == '__main__':
    main()

    # http POST :8080/v1.0/task/ < worker/tests/sample_task.json
