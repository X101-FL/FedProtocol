import argparse
import pickle

from fedprototype import BaseClient

MESSAGE_SPACE1 = 'sub_space_1'  # 两个消息空间名字如果为None或相同
MESSAGE_SPACE2 = 'sub_space_2'  # 则都会卡住，但卡住的位置不同


class Level2ClientA(BaseClient):
    def __init__(self):
        super().__init__('Level2A')

    def run(self):
        self.comm.send('Level2B', 'whoami', f'{self.track_path}')


class Level1ClientA(BaseClient):
    def __init__(self):
        super().__init__("Level1A")
        self.l2_client1 = Level2ClientA()
        self.l2_client2 = Level2ClientA()

    def init(self):
        self.set_sub_client(self.l2_client1,
                            message_space=MESSAGE_SPACE1,
                            role_rename_dict={"Level2A": "Level1A", "Level2B": "Level1B"})
        self.set_sub_client(self.l2_client2,
                            message_space=MESSAGE_SPACE2,
                            role_rename_dict={"Level2A": "Level1A", "Level2B": "Level1B"})
        return self

    def run(self):
        with self.l2_client1.init():
            self.l2_client1.run()
            self.l2_client1.run()  # 发送了两条相同的消息

        # 如果不做消息空间隔离，那么子客户端的消息和父客户端的消息可能会互相干扰
        self.comm.send('Level1B', 'whoami', f'{self.track_path}')

        with self.l2_client2.init():
            self.l2_client2.run()


class Level2ClientB(BaseClient):
    def __init__(self):
        super().__init__('Level2B')

    def run(self):
        whoareyou = self.comm.receive('Level2A', 'whoami')
        self.logger.info(f"get whoareyou : {whoareyou}")

    def close(self) -> None:
        self.logger.info("clear comm")
        self.comm.clear()


class Level1ClientB(BaseClient):
    def __init__(self):
        super().__init__("Level1B")
        self.l2_client1 = Level2ClientB()
        self.l2_client2 = Level2ClientB()

    def init(self):
        self.set_sub_client(self.l2_client1,
                            message_space=MESSAGE_SPACE1,  # 互相通讯的子客户端的消息空间名字应该相同
                            role_rename_dict={"Level2A": "Level1A", "Level2B": "Level1B"})
        self.set_sub_client(self.l2_client2,
                            message_space=MESSAGE_SPACE2,  # 同一消息空间的客户端可以互相发送消息
                            role_rename_dict={"Level2A": "Level1A", "Level2B": "Level1B"})
        return self

    def run(self):
        with self.l2_client1.init():
            # Level2A向Level2B发送了两条消息在'sub_space_1'里面
            # Level2B只接收了一条消息
            # Level2B在close时清空了它所在的消息总线，所以另外一条消息被丢弃了
            self.l2_client1.run()

        # Level2B在close时清空了它所在的消息总线
        # 如果不做消息空间隔离，Level2B会同时清空父级的消息，下面这段就会卡住，因为消息总线里面没有消息了
        whoareyou = self.comm.receive('Level1A', 'whoami')
        print(f"get whoareyou : {pickle.loads(whoareyou)}")
        self.logger.info(f"get whoareyou : {pickle.loads(whoareyou)}")

        with self.l2_client2.init():
            self.l2_client2.run()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, choices=["Level1A", "Level1B"])
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    from fedprototype.envs.cluster.tcp import TCPEnv

    args = get_args()
    if args.role == 'Level1A':
        client = Level1ClientA()
    else:
        client = Level1ClientB()

    TCPEnv() \
        .add_client(role_name='Level1A', ip="127.0.0.1", port=5601) \
        .add_client(role_name='Level1B', ip="127.0.0.1", port=5602) \
        .run(client=client)

    print("Finish TCP task")