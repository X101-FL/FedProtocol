import fedprotocol as fp
from fedprotocol import BaseWorker


class ClientA(BaseWorker):

    def __init__(self):
        super().__init__('SimplyTest', 'PartA')

    def run(self):
        self.logger.info("send to part B")
        self.comm.send('PartB', 'test_a_to_b', 'BiuBiuBiu')

        message_obj = self.comm.receive('PartB', 'test_b_to_a')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'YouYouYou'


class ClientB(BaseWorker):
    def __init__(self):
        super().__init__('SimplyTest', 'PartB')

    def run(self):
        self.logger.info("send to part A")
        self.comm.send('PartA', 'test_b_to_a', 'YouYouYou')

        message_obj = self.comm.receive('PartA', 'test_a_to_b')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'BiuBiuBiu'


if __name__ == '__main__':
    import sys

    role = sys.argv[1]
    client = eval(f"{role}()")

    fp.set_env(name='TCP') \
        .add_worker(role_name='PartA', host="127.0.0.1", port=5601) \
        .add_worker(role_name='PartB', host="127.0.0.1", port=5602) \
        .run(client=client)

# cd test/p2p/tcp
# python simply_send_receive.py ClientA
# python simply_send_receive.py ClientB
