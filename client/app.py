import argparse
import logging
import sys


SUPPORTED_FORMATS = {
    'script', 'makefile', 'raw'
}


def main(args):
    print(args)


if __name__ == '__main__':
    fmt = formatter_class=argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(description='Dedalus commandline client',
                                     formatter_class=fmt)
    parser.add_argument('--verbose', action='store_true', default=False)
    parser.add_argument('-s', '--server', help='Dedalus master server', default='localhost')
    parser.add_argument('-p', '--port', help='Dedalus master post', type=int, default='8080')

    mode = parser.add_subparsers(title='type of objects to manipulate', dest='mode')

    modes = dict()
    modes['graph'] = mode.add_parser('graph', help='Commands to work with graphs')
    graph_sub = modes['graph'].add_subparsers(dest='action')
    create_action = graph_sub.add_parser('create', help='Create new graph', formatter_class=fmt)
    create_action.add_argument('-n', '--name', required=True, help='Graph name to create')
    create_action.add_argument('--graph', help='Path to graph', nargs='?', type=argparse.FileType('r'),
                               default=sys.stdin)
    create_action.add_argument('--hosts', required=True, nargs='+',
                               help='List of hosts to run script on (skipped in raw format)')
    create_action.add_argument('-f', '--format', choices=SUPPORTED_FORMATS, default='script',
                               help='Format of graph')

    graph_info_action = graph_sub.add_parser('info', help='Get info about a graph', formatter_class=fmt)
    graph_info_action.add_argument('-n', '--name', required=True, help='Graph name to get info about')
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
    instance_info_action.add_argument('-i', '--instance', required=True, help='Graph instance id to get info about')
    instance_info_action.add_argument('--list-all', default=False, action='store_const', const=True,
                                      help='If set, then will gather info about all graph instances')

    instance_ctrl_action = instance_sub.add_parser('ctrl', help='Switch graph instance to state', formatter_class=fmt)
    instance_ctrl_action.add_argument('-i', '--instance', required=True, help='Graph instance id to control')
    instance_ctrl_action.add_argument('-t', '--target-state', default='start', choices=('start', 'stop'),
                                      help='Target state for an instance')
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
