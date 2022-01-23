import argparse
import pickle
import time

from fedprototype import BaseClient

# ===========================
# === tcp message space test
# ===========================

MESSAGE_SPACE1 = 'sub_space_1'  # 两个消息空间名字如果为None或相同
MESSAGE_SPACE2 = 'sub_space_2'  # 则都会卡住，但卡住的位置不同


class Level2ClientA(BaseClient):
    """2A客户端"""

    def __init__(self):
        super().__init__('Level2A')

    def run(self):
        # Level2A给Level2B发送消息
        self.comm.send(receiver='Level2B',
                       message_name='who am i',
                       message_obj=f'{self.track_path}',
                       flush=True)


class Level1ClientA(BaseClient):
    """1A客户端"""

    def __init__(self):
        super().__init__("Level1A")
        self.l2_client1 = Level2ClientA()
        self.l2_client2 = Level2ClientA()

    def init(self):
        # l2_client1和l2_client2使用不同的消息空间
        self.set_sub_client(self.l2_client1,
                            message_space=MESSAGE_SPACE1,
                            role_rename_dict={"Level2A": "Level1A", "Level2B": "Level1B"})
        self.set_sub_client(self.l2_client2,
                            message_space=MESSAGE_SPACE2,
                            role_rename_dict={"Level2A": "Level1A", "Level2B": "Level1B"})
        return self

    def run(self):
        with self.l2_client1.init():  # 什么都没做
            # MESSAGE_SPACE1下的Level2A发送给Level2B两条相同的消息
            self.l2_client1.run()
            self.l2_client1.run()

        # 如果不做消息空间隔离，那么子客户端的消息和父客户端的消息可能会互相干扰
        # 父协议发送消息
        self.comm.send(receiver='Level1B',
                       message_name='who am i',
                       message_obj=f'{self.track_path}',
                       flush=True)

        # 1A接收1B消息
        message = self.comm.receive(sender='Level1B', message_name='lock_id')
        print("1A can also receive message from 1B:", pickle.loads(message))

        with self.l2_client2.init():
            # MESSAGE_SPACE2下的Level2A发送Level2B一条消息
            self.l2_client2.run()

        seconds = 1
        print(f"Wait {seconds} seconds to finish Level1A run")
        time.sleep(seconds)


class Level2ClientB(BaseClient):
    """2B客户端"""

    def __init__(self):
        super().__init__('Level2B')

    def run(self):
        # 子协议Level2B去接收Level2A的消息
        message = self.comm.receive(sender='Level2A', message_name='who am i')
        print(f"Level2B get message : {pickle.loads(message)}")
        # self.logger.info(f"get message : {message}")

    def close(self) -> None:
        # self.logger.info("clear comm")
        print("clear comm message.......")
        self.comm.clear('Level2A', 'who am i')


class Level1ClientB(BaseClient):
    """1B客户端"""

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
            # self.l2_client1.run()

        # Level2B在close时清空了它所在的消息总线
        # 如果不做消息空间隔离，Level2B会同时清空父级的消息，下面这段就会卡住，因为消息总线里面没有消息了
        message = self.comm.receive(sender='Level1A', message_name='who am i')
        print(f"++++++ Level1B get message : {pickle.loads(message)}")
        # self.logger.info(f"Level1B get message : {pickle.loads(message)}")

        # 1B发送给1A消息
        self.comm.send(receiver='Level1A',
                       message_name='lock_id',
                       message_obj='123456',
                       flush=True)

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
    elif args.role == 'Level1B':
        client = Level1ClientB()

    TCPEnv() \
        .add_client(role_name='Level1A', host="127.0.0.1", port=5601) \
        .add_client(role_name='Level1B', host="127.0.0.1", port=5602) \
        .run(client=client)

    print("-----> Finish TCP task.")

# cd test/tcp
# python message_space.py --role Level1A
# python message_space.py --role Level1B