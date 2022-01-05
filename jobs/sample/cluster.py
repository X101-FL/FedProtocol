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
        self.comm.send('passive', 'test_message_name', {'haha': "hehe"})


class PassiveClient(BaseClient):
    def __init__(self, role_name):
        super(PassiveClient, self).__init__(role_name)

    def init(self) -> None:
        pass

    def close(self) -> None:
        pass

    def run(self):
        print("GOOD")
        data = self.comm.receive('active', message_name='test_message_name')
        print(data)
        import time
        time.sleep(30)




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
        .add_client(role_name='active', ip="127.0.0.1", port=5650) \
        .add_client(role_name='passive', ip="127.0.0.1", port=5651) \
        .run(client=client)

# activate pytorch
# cd C:\PyProject\FedPrototype\jobs\sample
# set PYTHONPATH=C:/PyProject/FedPrototype
# python -u cluster.py --role active
# python -u cluster.py --role passive
