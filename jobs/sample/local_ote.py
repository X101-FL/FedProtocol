import random
import time

import numpy as np

from components.smc.psi.oprf import OPRFServer, OPRFClient
from components.smc.psi.ote import OTESender, OTEReceiver
from fedprototype.base_client import BaseClient


class ActiveClient(BaseClient):

    def __init__(self):
        super().__init__("Active")
        # self.ote_sender = OTESender(256)
        self.oprf_server = OPRFServer(512)

    def init(self):
        # self.set_sub_client(self.ote_sender, role_rename_dict={"OTESender": "Active", "OTEReceiver": "Passive"})
        self.set_sub_client(self.oprf_server, role_rename_dict={"OPRFServer": "Active", "OPRFClient": "Passive"})

    def run(self):
        self.oprf_server.init()
        self.logger.debug(f"OPRFServer prepare complete")

        for i in range(1000):
            word = i.to_bytes(4, "big")
            res = self.oprf_server.run(i, word)
            self.comm.send(receiver="Passive", message_name="item", obj=res)
            self.logger.debug(f"{i} {res.hex()}")
        # self.ote_sender.init()
        # self.logger.debug(f"OTESender prepare complete")
        #
        # for i in range(self.ote_sender.max_count):
        #     m0 = (str(i) + '0').encode("utf-8")
        #     m1 = (str(i) + '1').encode("utf-8")
        #
        #     self.ote_sender.run(m0, m1)

    def close(self):
        pass


class PassiveClient(BaseClient):

    def __init__(self):
        super().__init__("Passive")
        # self.r = [random.randint(0, 1) for _ in range(1000)]
        # self.ote_receiver = OTEReceiver(self.r, 256)

        words = [i.to_bytes(4, "big") for i in range(1000)]
        self.oprf_client = OPRFClient(words, 512)

    def init(self):
        # self.set_sub_client(self.ote_receiver, role_rename_dict={"OTESender": "Active", "OTEReceiver": "Passive"})
        self.set_sub_client(self.oprf_client, role_rename_dict={"OPRFServer": "Active", "OPRFClient": "Passive"})

    def run(self):
        self.oprf_client.init()
        self.logger.debug(f"OPRFClient prepare complete")

        for i in range(1000):
            peer_val = self.comm.receive(sender="Active", message_name="item").hex()
            local_val = self.oprf_client.run(i).hex()
            self.logger.debug(f"{i} {local_val} {peer_val}")
            assert local_val == peer_val, f"{i}th oprf is not correct"
        # self.ote_receiver.init()
        # self.logger.debug(f"OTEReceiver prepare complete")
        #
        # for i, b in enumerate(self.r):
        #     msg = self.ote_receiver.run().decode("utf-8")
        #     self.logger.debug(f"msg is: {msg[:-1]} {msg[-1]}")
        #     assert msg == str(i) + str(b)

    def close(self):
        pass


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    env = LocalEnv()
    env.add_client(client=ActiveClient())
    env.add_client(client=PassiveClient())
    env.run()
