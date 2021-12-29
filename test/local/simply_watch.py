from fedprototype import BaseClient


class ClientA(BaseClient):

    def __init__(self):
        super().__init__(role_name='PartA')

    def run(self):
        for _receiver in self.comm.get_role_name_list('PartB.'):
            self.logger.info(f"send to {_receiver}")
            self.comm.send(_receiver, 'test_a_to_b', f'hello {_receiver}')

        for sender, message_name, message_obj in self.comm.watch('PartB.', 'test_b_to_a'):
            self.logger.info(f"get a message from {sender}:{message_name} = {message_obj}")
            assert message_obj == f"hello PartA I'm {sender}"


class ClientB(BaseClient):
    def __init__(self, index):
        super().__init__(role_name=f'PartB.{index}')

    def run(self):
        message_obj = self.comm.receive('PartA', 'test_a_to_b')
        self.logger.info(f"receive message : {message_obj}")
        assert message_obj == f'hello {self.role_name}'

        import time
        import random
        self.logger.info("pretend busy ...")
        time.sleep(random.randint(0, 5))
        self.logger.info("sleep done ...")

        self.comm.send('PartA', 'test_b_to_a', f"hello PartA I'm {self.role_name}")


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    env = LocalEnv()
    for index in range(5):
        env.add_client(ClientB(index))
    env.add_client(ClientA())
    env.run()
