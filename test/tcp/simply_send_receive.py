from fedprototype import BaseClient


class ClientA(BaseClient):

    def __init__(self):
        super().__init__(role_name='PartA')

    def run(self):
        self.logger.info("send to part B")
        self.comm.send('PartB', 'test_a_to_b', 'BiuBiuBiu')

        message_obj = self.comm.receive('PartB', 'test_b_to_a')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'YouYouYou'


class ClientB(BaseClient):
    def __init__(self):
        super().__init__(role_name='PartB')

    def run(self):
        self.logger.info("send to part A")
        self.comm.send('PartA', 'test_b_to_a', 'YouYouYou')

        message_obj = self.comm.receive('PartA', 'test_a_to_b')
        self.logger.info(f"receive message : {message_obj}")

        assert message_obj == 'BiuBiuBiu'


if __name__ == '__main__':
    from fedprototype.envs import TCPEnv
    import sys

    role = sys.argv[1]
    client = eval(f"{role}()")

    TCPEnv() \
        .add_client(role_name='PartA', host="127.0.0.1", port=5601) \
        .add_client(role_name='PartB', host="127.0.0.1", port=5602) \
        .run(client=client)

# cd test/tcp
# python simply_send_receive.py ClientA
# python simply_send_receive.py ClientB
