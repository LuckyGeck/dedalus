#!/usr/bin/env python
import argparse
from time import sleep
from master.api_client import MasterApiClient
import json


def main(args):
    cli = MasterApiClient(master_host='localhost', master_port=8080)
    graph_name, graph_rev = cli.create_graph(graph_struct=json.load(open(args.path)),
                                             graph_name='test_graph')
    print('Created graph: {}, rev: {}'.format(graph_name, graph_rev))
    instance_id = cli.launch_graph(graph_name, int(graph_rev))
    print('Created instance: {}'.format(instance_id))
    old_state, new_state = cli.start_instance(instance_id)
    print('State: {} -> {}'.format(old_state.name, new_state.name))
    while True:
        sleep(1)
        data = cli.read_instance(instance_id)
        state = data.exec_stats.state
        print('#'*10)
        print('State:', state.name)
        for task_name, task_info in data.exec_stats.per_task_execution_info.items():
            print('Task', task_name)
            print('\n'.join('\tHost {}: {}'.format(host, info.state.name)
                            for host, info in task_info.per_host_info.items()))
        if state.is_terminal:
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', help='Path to graph file', default='master/tests/simple_graph.json')
    args = parser.parse_args()
    main(args)
