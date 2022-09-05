import fedprotocol as fp
from fedprotocol import BaseClient


class ClientA(BaseClient):
    def __init__(self):
        super().__init__('SimplyTest', 'PartA')

    def run(self):
        self.logger.info("send to part B")
        self.comm.send('PartB', 'test_a_to_b', 'BiuBiuBiu')

        message_obj = self.comm.receive('PartB', 'test_b_to_a')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'YouYouYou'


class ClientB(BaseClient):
    def __init__(self):
        super().__init__('SimplyTest', 'PartB')

    def run(self):
        self.logger.info("send to part A")
        self.comm.send('PartA', 'test_b_to_a', 'YouYouYou')

        message_obj = self.comm.receive('PartA', 'test_a_to_b')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'BiuBiuBiu'


if __name__ == '__main__':
    fp.set_env(name='Local').add_client(ClientA()).add_client(ClientB()).run()

# PYTHONPATH=. python test/local/simply_send_receive.py
