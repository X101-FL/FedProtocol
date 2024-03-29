import fedprotocol as fp
from fedprotocol import BaseWorker


class ClientA(BaseWorker):

    def __init__(self):
        super().__init__("SimplyWatch", 'PartA')

    def run(self):
        for sender, message_name, message_obj in self.comm.watch('PartB.', 'test_b_to_a'):
            self.logger.info(f"get a message from {sender}:{message_name} = {message_obj}")
            assert message_obj == f"hello PartA I'm {sender}"


class ClientB(BaseWorker):
    def __init__(self, index):
        super().__init__("SimplyWatch", f'PartB.{index}')

    def run(self):
        self.comm.send('PartA', 'test_b_to_a', f"hello PartA I'm {self.role_name}")


def get_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, choices=[ClientA.__name__, ClientB.__name__])
    parser.add_argument('--part_b_index', type=int, default=0)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    if args.role == ClientA.__name__:
        client = ClientA()
    else:
        client = ClientB(index=args.part_b_index)

    fp.set_env(name='TCP') \
        .add_worker(role_name='PartA', host="127.0.0.1", port=5601) \
        .add_worker(role_name='PartB.1', host="127.0.0.1", port=5602) \
        .add_worker(role_name='PartB.2', host="127.0.0.1", port=5603) \
        .add_worker(role_name='PartB.3', host="127.0.0.1", port=5604) \
        .run(worker=client)

# PYTHONPATH=. python test/p2p/tcp/simply_watch.py --role ClientA
# PYTHONPATH=. python test/p2p/tcp/simply_watch.py --role ClientB --part_b_index 1
# PYTHONPATH=. python test/p2p/tcp/simply_watch.py --role ClientB --part_b_index 2
# PYTHONPATH=. python test/p2p/tcp/simply_watch.py --role ClientB --part_b_index 3
