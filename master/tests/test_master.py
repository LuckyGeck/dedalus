#!/usr/bin/env python
from time import sleep
from common.models.state import GraphInstanceState
from master.api_client import MasterApiClient
import json


def main():
    cli = MasterApiClient(master_host='localhost', master_port=8080)
    graph_name, graph_rev = cli.create_graph(graph_struct=json.load(open('master/tests/simple_graph.json')))
    print(graph_rev)
    task_state = cli.launch_graph(graph_rev)
    print(task_state.name)
    while True:
        sleep(1)
        task_state = cli.get_task_state(task_id)
        print(task_state.name)
        if task_state.is_terminal:
            break


if __name__ == '__main__':
    main()

    # http POST :8080/v1.0/task/ < worker/tests/sample_task.json
