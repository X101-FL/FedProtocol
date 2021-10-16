import argparse
from components.algorithms.vertical.train_clients import Active, Passive


def get_active_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch_size', type=int, default=64)
    parser.add_argument('--lr', type=float, default=0.06)
    parser.add_argument('--data_dir', type=str,
                        default="/root/test/kunkun/dataset/MNIST")
    args = parser.parse_args(argv or [])
    return args


def get_passive_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch_size', type=int, default=64)
    parser.add_argument('--lr', type=float, default=0.06)
    parser.add_argument('--data_dir', type=str,
                        default="/root/test/kunkun/dataset/MNIST")
    args = parser.parse_args(argv or [])
    return args


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    LocalEnv() \
        .add_client(role_name='active', client_class=Active, client_param=get_active_args()) \
        .add_client(role_name='passive', client_class=Passive, client_param=get_passive_args()) \
        .run()
