import argparse
from .job_clients import ActiveClient, PassiveClient


def get_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, choices=["active", "passive"])
    parser.add_argument('--param_a', type=str, default="abc")
    parser.add_argument('--param_b', type=str, default="abc")
    parser.add_argument('--param1', type=str, default="abc")
    parser.add_argument('--param2', type=str, default="abc")
    parser.add_argument('--param3', type=str, default="abc")
    args = parser.parse_args(argv or [])
    return args


def get_run_kwargs(args):
    # return {'id_list': [...], 'label': [...], 'features': tensor} for Active
    # return {'id_list': [...], 'features': tensor} for Passive
    return {}


if __name__ == '__main__':
    args = get_args()
    if args.role == 'active':
        client = ActiveClient(args)
    else:
        client = PassiveClient(args)

    from fedprototype.envs import TCPEnv

    TCPEnv() \
        .add_client(role_name='active', ip="xxx.xxx.xxx.xxx", port=5650) \
        .add_client(role_name='passive', ip="yyy.yyy.yyy.yyy", port=5651) \
        .run(client=client, **get_run_kwargs(args))
