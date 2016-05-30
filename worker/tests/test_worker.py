#!/usr/bin/env python
import requests
from time import sleep
from common.models.state import TaskState


def main():
    # http POST :8080/v1.0/task/
    data = requests.post('http://localhost:8080/v1.0/task/', data=open('worker/tests/sample_task.json').read())
    task_id = data.json()['payload']['task_id']
    print(task_id)
    start_result = requests.post('http://localhost:8080/v1.0/task/{}/start'.format(task_id))
    print(start_result.json())
    while True:
        sleep(1)
        state_json = requests.get('http://localhost:8080/v1.0/task/{}/state'.format(task_id)).json()
        print(state_json)
        if TaskState(state_json['payload']['state']).is_terminal:
            break


if __name__ == '__main__':
    main()

    # http POST :8080/v1.0/task/ < worker/tests/sample_task.json
