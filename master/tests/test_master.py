#!/usr/bin/env python
import argparse
from time import sleep
from master.api_client import MasterApiClient
from common.models.state import GraphInstanceState
import json


def main(args):
    cli = MasterApiClient(master_host=args.server, master_port=args.port)
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
            print('*' * 20)
            if state.name == GraphInstanceState.failed:
                print('Failed!')
                if data.exec_stats.fail_msg:
                    print('Fail msg: {}'.format(data.exec_stats.fail_msg))
            elif state.name == GraphInstanceState.finished:
                print('Success!')
            print('LOGS:')
            for task_name, task_info in data.exec_stats.per_task_execution_info.items():
                print(' Task', task_name)
                for host in task_info.per_host_info.keys():
                    out = cli.instance_logs(instance_id, task_name, host, 'out')
                    err = cli.instance_logs(instance_id, task_name, host, 'err')
                    print('  Host', host)
                    if out:
                        print('   Out:')
                        print(''.join('+++>{}'.format(_) for _ in out.strip().splitlines(keepends=True)))
                    if err:
                        print('   Err:')
                        print(''.join('--->{}'.format(_) for _ in err.strip().splitlines(keepends=True)))
                print('-' * 10)
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', help='Master host', default='localhost')
    parser.add_argument('--port',  help='Master port', default=8080, type=int)
    parser.add_argument('--path', help='Path to graph file', default='master/tests/simple_graph.json')
    args = parser.parse_args()
    main(args)
