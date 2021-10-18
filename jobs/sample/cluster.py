import argparse
from components.algorithms.vertical.train_clients import Active, Passive


def get_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, choices=["active", "passive"])

    parser.add_argument('--batch_size', type=int, default=64)
    parser.add_argument('--lr', type=float, default=0.06)
    parser.add_argument('--data_dir', type=str,
                        default="/root/test/kunkun/dataset/MNIST")
    args = parser.parse_args(argv or [])
    return args


def get_run_kwargs():
    # return {'id_list': [...], 'label': [...], 'features': tensor} for Active
    # return {'id_list': [...], 'features': tensor} for Passive
    return {}


if __name__ == '__main__':
    args = get_args()
    client_class = {'active': Active, 'passive': Passive}[args.role]
    from fedprototype.envs import SocketEnv

    SocketEnv() \
        .add_client(role_name='active', ip="xxx.xxx.xxx.xxx", port=5650) \
        .add_client(role_name='passive', ip="yyy.yyy.yyy.yyy", port=5651) \
        .run(client_class=client_class, client_params=args, role_name=args.role, **get_run_kwargs())
