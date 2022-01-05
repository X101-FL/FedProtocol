import argparse

from fedprototype import BaseClient


class ActiveClient(BaseClient):
    def __init__(self, role_name):
        super(ActiveClient, self).__init__(role_name)

    def init(self) -> None:
        pass

    def close(self) -> None:
        pass

    def run(self):
        # 调用TCPComm的_send方法
        self.comm.send('passive', 'label_in_active', [1, 0, 0, 1, 1, 1, 0])
        print("FF")
        self.comm.send('passive', 'feature', [1, 0, 0, 1, 1, 1, 0])


class PassiveClient(BaseClient):
    def __init__(self, role_name):
        super(PassiveClient, self).__init__(role_name)

    def init(self) -> None:
        pass

    def close(self) -> None:
        pass

    def run(self):
        print("--- passive client run ---")
        data = self.comm.receive('active', message_name='label_in_active')
        print("receive:", data)
        data = self.comm.receive('active', message_name='feature')


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, choices=["active", "passive"])
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    # TODO: active运行时需要等待passive接受信息
    if args.role == 'active':
        client = ActiveClient('active')
    else:
        client = PassiveClient('passive')

    from fedprototype.envs.cluster.tcp import TCPEnv

    TCPEnv() \
        .add_client(role_name='active', ip="127.0.0.1", port=6060) \
        .add_client(role_name='passive', ip="127.0.0.1", port=7070) \
        .run(client=client)

    print("Finish TCP task")

# activate pytorch
# cd C:\PyProject\FedPrototype\jobs\sample
# set PYTHONPATH=C:/PyProject/FedPrototype
# python -u cluster.py --role active
# python -u cluster.py --role passive
