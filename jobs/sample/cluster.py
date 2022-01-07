import argparse
import pickle
import time

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
        print("--- active client run ---")
        self.comm.send('passive', 'label_in_active', [1, 1, 0, 1, 1, 1, 0])
        time.sleep(5)
        print("send -------------")
        self.comm.send('passive', 'label_in_active', [0, 0, 0, 0, 1, 1])
        self.comm.send('passive', 'feature', [1,2,3,4,5])
        time.sleep(50)


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
        time.sleep(10)
        print("PassiveClient receive label_in_active:", pickle.loads(data))
        data = self.comm.receive('active', message_name='feature')
        print("PassiveClient receive feature:", pickle.loads(data))
        data = self.comm.receive('active', message_name='label_in_active')
        print("PassiveClient receive label_in_active:", pickle.loads(data))

        # 如果message_hub空了，会一直进行receive，除非另一边服务挂了
        data = self.comm.receive('active', message_name='label_in_active')


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
# python -u C:\PyProject\FedPrototype\jobs\sample\cluster.py --role active
# python -u C:\PyProject\FedPrototype\jobs\sample\cluster.py --role passive
