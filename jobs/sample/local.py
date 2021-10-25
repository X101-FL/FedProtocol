import argparse
from .job_clients import ActiveClient, PassiveClient


def get_active_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--param_a', type=str, default="abc")
    parser.add_argument('--param_b', type=str, default="abc")
    parser.add_argument('--param1', type=str, default="abc")
    parser.add_argument('--param2', type=str, default="abc")
    parser.add_argument('--param3', type=str, default="abc")
    args = parser.parse_args(argv or [])
    return args


def get_passive_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--param_a', type=str, default="abc")
    parser.add_argument('--param_b', type=str, default="abc")
    parser.add_argument('--param1', type=str, default="abc")
    parser.add_argument('--param2', type=str, default="abc")
    parser.add_argument('--param3', type=str, default="abc")
    args = parser.parse_args(argv or [])
    return args


def get_active_run_kwargs():
    return {'id_list': None, 'label': None, 'features': None}


def get_passive_run_kwargs():
    return {'id_list': None, 'features': None}


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    LocalEnv() \
        .add_client(client=ActiveClient(get_active_args()), **get_active_run_kwargs()) \
        .add_client(client=PassiveClient(get_passive_args()), **get_passive_run_kwargs()) \
        .run()
