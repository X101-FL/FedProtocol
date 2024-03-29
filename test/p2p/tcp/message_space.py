import fedprotocol as fp
from fedprotocol import BaseWorker


class Level2ClientA(BaseWorker):

    def __init__(self):
        super().__init__("Level2", '2A')

    def run(self):
        self.comm.send(receiver='2B',
                       message_name='who am i',
                       message_obj=f'{self.track_path}',
                       flush=True)


class Level1ClientA(BaseWorker):
    def __init__(self):
        super().__init__("Level1", '1A')
        self.l2_client1 = Level2ClientA().rename_protocol("Level2#1")
        self.l2_client2 = Level2ClientA().rename_protocol("Level2#2")

    def init(self):
        self.set_sub_worker(self.l2_client1,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        self.set_sub_worker(self.l2_client2,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        return self

    def run(self):
        with self.l2_client1.init():
            self.l2_client1.run()
            self.l2_client1.run()
            self.l2_client1.run()
            self.l2_client1.run()

        self.comm.send(receiver='1B',
                       message_name='who am i',
                       message_obj=f'{self.track_path}',
                       flush=True)

        with self.l2_client2.init():
            self.l2_client2.run()


class Level2ClientB(BaseWorker):

    def __init__(self):
        super().__init__("Level2", '2B')

    def run(self):
        message = self.comm.receive(sender='2A', message_name='who am i')
        self.logger.info(f"get message : {message}")
        return message

    def close(self) -> None:
        self.comm.clear()


class Level1ClientB(BaseWorker):
    def __init__(self):
        super().__init__("Level1", '1B')
        self.l2_client1 = Level2ClientB().rename_protocol("Level2#1")
        self.l2_client2 = Level2ClientB().rename_protocol("Level2#2")

    def init(self):
        self.set_sub_worker(self.l2_client1,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        self.set_sub_worker(self.l2_client2,
                            role_bind_mapping={"2A": "1A", "2B": "1B"})
        return self

    def run(self):
        with self.l2_client2.init():
            assert self.l2_client2.run() == "Level1.1A/Level2#2.2A"

        with self.l2_client1.init():
            assert self.l2_client1.run() == "Level1.1A/Level2#1.2A"

        message = self.comm.receive(sender='1A', message_name='who am i')
        self.logger.info(f"Level1B get message : {message}")
        assert message == "Level1.1A"


if __name__ == '__main__':

    import sys
    role = sys.argv[1]
    client = eval(f"{role}()")

    fp.set_env(name='TCP') \
        .add_worker(role_name='1A', host="127.0.0.1", port=5601) \
        .add_worker(role_name='1B', host="127.0.0.1", port=5602) \
        .run(worker=client)

# PYTHONPATH=. python test/p2p/tcp/message_space.py Level1ClientA
# PYTHONPATH=. python test/p2p/tcp/message_space.py Level1ClientB
