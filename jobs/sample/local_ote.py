import random
import time

import numpy as np

from components.smc.psi.ote import OTESender, OTEReceiver
from fedprototype.base_client import BaseClient


class ActiveClient(BaseClient):

    def __init__(self):
        super().__init__("Active")
        self.ote_sender = OTESender()

    def init(self):
        self.set_sub_client(self.ote_sender, role_rename_dict={"OTESender": "Active", "OTEReceiver": "Passive"})

    def run(self):
        self.ote_sender.init()
        self.logger.debug(f"OTESender prepare complete")

        for i in range(self.ote_sender.max_count):
            m0 = (str(i) + '0').encode("utf-8")
            m1 = (str(i) + '1').encode("utf-8")

            self.ote_sender.run(m0, m1)

    def close(self):
        pass


class PassiveClient(BaseClient):

    def __init__(self):
        super().__init__("Passive")
        self.r = [random.randint(0, 1) for _ in range(1000)]
        self.ote_receiver = OTEReceiver(self.r)

    def init(self):
        self.set_sub_client(self.ote_receiver, role_rename_dict={"OTESender": "Active", "OTEReceiver": "Passive"})

    def run(self):
        self.ote_receiver.init()
        self.logger.debug(f"OTEReceiver prepare complete")

        for i, b in enumerate(self.r):
            msg = self.ote_receiver.run().decode("utf-8")
            self.logger.debug(f"msg is: {msg[:-1]} {msg[-1]}")
            assert msg == str(i) + str(b)

    def close(self):
        pass


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    env = LocalEnv()
    env.add_client(client=ActiveClient())
    env.add_client(client=PassiveClient())
    env.run()
