import argparse
import time

import numpy as np

from fedprototype import BaseClient


class ActiveClient(BaseClient):
    def __init__(self, role_name):
        super(ActiveClient, self).__init__(role_name)

    def init(self):
        pass

    def run(self, id_list, label_list, feature_list):
        for passive in self.comm.get_role_name_list("Passive"):
            time.sleep(np.random.randint(0, 5))
            self.comm.send(f"{passive}", "id_list", id_list, flush=False)
            self.logger.info(
                f"don't send message with id_list to {passive} at right, just put it into message buffer"
            )
            self.comm.send(f"{passive}", "label_list", label_list)
            self.logger.info(
                f"send message with label_list to {passive}, and flush message buffer"
            )
            self.comm.send(f"{passive}", "feature_list", feature_list, flush=False)
            self.logger.info(
                f"don't send message with feature_list to {passive} at right, just put it into message buffer"
            )

        time.sleep(np.random.randint(0, 5))
        self.comm.flush(receiver="Passive.0")
        self.logger.info(f"flush message_buffer for Passive.0")

        time.sleep(np.random.randint(0, 5))
        self.comm.flush()
        self.logger.info(f"flush all the message_buffer")

        for sender, _, feature in self.comm.watch("Passive", "feature"):
            self.logger.debug(
                f"Successfully receive feature from {sender}, its value is {feature}."
            )
        self.logger.info(f"Successfully receive feature from all Passives!")

    def close(self):
        pass


class PassiveClient(BaseClient):
    def __init__(self, role_name):
        super(PassiveClient, self).__init__(role_name)

    def init(self):
        pass

    def run(self, feature):
        time.sleep(np.random.randint(0, 5))
        id_list = self.comm.receive("Active", "id_list")
        self.logger.info(
            f"Successfully receive id_list from Active, the value is {id_list}"
        )

        time.sleep(np.random.randint(0, 5))
        self.comm.send("Active", "feature", feature)
        self.logger.info("Successfully send feature to Active!")

        time.sleep(np.random.randint(0, 5))
        label_list = self.comm.receive("Active", "label_list")
        self.logger.info(
            f"Successfully receive label_list from Active, the value is {label_list}"
        )

        time.sleep(np.random.randint(0, 5))
        feature_list = self.comm.receive("Active", "feature_list")
        self.logger.info(
            f"Successfully receive feature_list from Active, the value is {feature_list}"
        )

    def close(self):
        pass

def get_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', type=str, choices=["active", "passive"])
    parser.add_argument('--param_a', type=str, default="abc")
    parser.add_argument('--param_b', type=str, default="abc")
    parser.add_argument('--param1', type=str, default="abc")
    parser.add_argument('--param2', type=str, default="abc")
    parser.add_argument('--param3', type=str, default="abc")
    args = parser.parse_args(argv or [])
    return args


def get_run_kwargs(args):
    # return {'id_list': [...], 'label': [...], 'features': tensor} for Active
    # return {'id_list': [...], 'features': tensor} for Passive
    return {}


if __name__ == '__main__':
    args = get_args()
    if args.role == 'active':
        client = ActiveClient('active')
    else:
        client = PassiveClient('passive')

    from fedprototype.envs.cluster.tcp import TCPEnv

    TCPEnv() \
        .add_client(role_name='active', ip="xxx.xxx.xxx.xxx", port=5650) \
        .add_client(role_name='passive', ip="yyy.yyy.yyy.yyy", port=5651) \
        .run(client=client, **get_run_kwargs(args))
