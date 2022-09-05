import fedprotocol as fp
from fedprotocol import BaseClient


class ClientA(BaseClient):
    def __init__(self):
        super().__init__("SimplyWatch", 'PartA')

    def run(self):
        for sender, message_name, message_obj in self.comm.watch(
            'PartB.', 'test_b_to_a'
        ):
            self.logger.info(
                f"get a message from {sender}:{message_name} = {message_obj}"
            )
            assert message_obj == f"hello PartA I'm {sender}"


class ClientB(BaseClient):
    def __init__(self, index):
        super().__init__("SimplyWatch", f'PartB.{index}')

    def run(self):
        self.comm.send('PartA', 'test_b_to_a', f"hello PartA I'm {self.role_name}")


if __name__ == '__main__':
    env = fp.set_env(name='Local')
    for index in range(5):
        env.add_client(ClientB(index))
    env.add_client(ClientA())
    env.run()

# PYTHONPATH=. python test/local/simply_watch.py
