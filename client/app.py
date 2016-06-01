import argparse
import logging
import sys
import json
from typing import TextIO, List, Optional
from master.api_client import MasterApiClient
from common.models.graph import GraphStruct, ExtendedTaskStruct
from plugins.executors.shell import ShellExecutorConfig


SUPPORTED_FORMATS = {
    'script', 'makefile', 'raw'
}


def _prepare_graph_struct(name: Optional[str], graph: TextIO, hosts: List[str], graph_format: str) -> dict:
    if graph_format == 'raw':
        return json.load(graph)
    assert name and hosts, 'Only raw graph format can not set hosts and name'
    result = GraphStruct()
    result.graph_name = name
    result.clusters.from_json({'I': hosts})
    if graph_format == 'script':
        task = ExtendedTaskStruct()
        task.task_name = 'main'
        task.hosts.append('I')
        task.task_struct.executor.name = 'shell'
        executor_cfg = ShellExecutorConfig()
        executor_cfg.shell_script = graph.read()
        task.task_struct.executor.config = executor_cfg.to_json()
        result.tasks.from_json([task.to_json()])
    elif graph_format == 'makefile':
        raise NotImplementedError()
    return result.to_json()


def graph_mode(client: MasterApiClient, args):
    if args.action == 'create':
        name, rev = client.create_graph(
            graph_struct=_prepare_graph_struct(args.name, args.graph, args.hosts, args.format),
            graph_name=args.name
        )
        print('Created graph "{}", revision {}.'.format(name, rev))
    elif args.action == 'info':
        if args.list_all:
            print(json.dumps([_.to_json() for _ in client.list_graphs(args.name, args.revision)],
                             indent=2, ensure_ascii=False))
        else:
            assert args.name, 'Graph name should be set'
            print(json.dumps(client.read_graph(args.name, args.revision).to_json(), indent=2, ensure_ascii=False))
    elif args.action == 'launch':
        print('Created new graph instance: {}'.format(client.launch_graph(args.name, args.revision)))
    else:
        logging.error('Not supported action for graph mode: %s', args.action)
        exit(1)


def instance_mode(client: MasterApiClient, args):
    if args.action == 'info':
        print(json.dumps(client.read_instance(args.id).to_json(), indent=2, ensure_ascii=False))
    elif args.action == 'ctrl':
        answer = None
        if args.target_state == 'start':
            answer = client.start_instance(args.id)
        elif args.target_state == 'stop':
            answer = client.stop_instance(args.id)
        else:
            logging.exception('Unsupported target state: {}'.format(args.target_state))
            exit(1)
        print('State for instance {} changed from {} to {}.'.format(args.id, answer[0].name, answer[1].name))
    elif args.action == 'logs':
        print(client.instance_logs(args.id, args.task_name, args.host, args.log_type), end='')
    else:
        logging.error('Not supported action for instance mode: %s', args.action)
        exit(1)


def main(args):
    client = MasterApiClient(master_host=args.server, master_port=args.port, ssl=args.use_ssl,
                             api_version=args.api_version)
    if args.mode == 'graph':
        graph_mode(client, args)
    elif args.mode == 'instance':
        instance_mode(client, args)
    else:
        logging.error('Not supported mode: %s', args.mode)
        exit(1)

if __name__ == '__main__':
    fmt = formatter_class=argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(description='Dedalus commandline client',
                                     formatter_class=fmt)
    parser.add_argument('--verbose', action='store_true', default=False)
    parser.add_argument('-s', '--server', help='Dedalus master server', default='localhost')
    parser.add_argument('-p', '--port', help='Dedalus master post', type=int, default='8080')
    parser.add_argument('--use-ssl', help='Use SSL while connecting to Dedalus master',
                        default=False, action='store_const', const=True)
    parser.add_argument('--api-version', help='What api version to use', default='v1.0')

    mode = parser.add_subparsers(title='type of objects to manipulate', dest='mode')

    modes = dict()
    modes['graph'] = mode.add_parser('graph', help='Commands to work with graphs')
    graph_sub = modes['graph'].add_subparsers(dest='action')
    create_action = graph_sub.add_parser('create', help='Create new graph', formatter_class=fmt)
    create_action.add_argument('-n', '--name', required=True, help='Graph name to create')
    create_action.add_argument('--graph', help='Path to graph', nargs='?', type=argparse.FileType('r'),
                               default=sys.stdin)
    create_action.add_argument('--hosts', required=False, default=['localhost'], nargs='*',
                               help='List of hosts to run script on (skipped in raw format)')
    create_action.add_argument('-f', '--format', choices=SUPPORTED_FORMATS, default='script',
                               help='Format of graph')

    graph_info_action = graph_sub.add_parser('info', help='Get info about a graph', formatter_class=fmt)
    graph_info_action.add_argument('-n', '--name', help='Graph name to get info about')
    graph_info_action.add_argument('-r', '--revision', default=None, type=int,
                                   help='Sets graph revision to gather info about. If not set, last revision used')
    graph_info_action.add_argument('--list-all', default=False, action='store_const', const=True,
                                   help='If set, then will gather info about all graph revisions')

    graph_launch_action = graph_sub.add_parser('launch', help='Launch graph', formatter_class=fmt)
    graph_launch_action.add_argument('-n', '--name', required=True, help='Graph name to get info about')
    graph_launch_action.add_argument('-r', '--revision', default=None, type=int,
                                     help='Sets graph revision to gather info about. If not set, last revision used')

    modes['instance'] = mode.add_parser('instance', help='Commands to work with instances')
    instance_sub = modes['instance'].add_subparsers(dest='action')

    instance_info_action = instance_sub.add_parser('info', help='Get info about a graph', formatter_class=fmt)
    instance_info_action.add_argument('-i', '--id', required=True, help='Graph instance id to get info about')
    instance_info_action.add_argument('--list-all', default=False, action='store_const', const=True,
                                      help='If set, then will gather info about all graph instances')

    instance_ctrl_action = instance_sub.add_parser('ctrl', help='Switch graph instance to state', formatter_class=fmt)
    instance_ctrl_action.add_argument('-i', '--id', required=True, help='Graph instance id to control')
    instance_ctrl_action.add_argument('-t', '--target-state', default='start', choices=('start', 'stop'),
                                      help='Target state for an instance')

    instance_ctrl_action = instance_sub.add_parser('logs', help='Get graph instance logs', formatter_class=fmt)
    instance_ctrl_action.add_argument('-i', '--id', required=True, help='Graph instance id to control')
    instance_ctrl_action.add_argument('--task-name', required=True, help='Task name to get logs for')
    instance_ctrl_action.add_argument('--host', required=True, help='Host to get logs from')
    instance_ctrl_action.add_argument('--log-type', default='out', choices=('out', 'err'), help='Log type')
    args = parser.parse_args()
    if args.mode is None:
        parser.print_help()
        exit(1)
    elif args.action is None:
        modes[args.mode].print_help()
        exit(1)
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(message)s', level=level)

    main(args)
