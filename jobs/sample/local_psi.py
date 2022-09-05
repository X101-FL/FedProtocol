import logging
import random

from components.smc.psi.oprf import OPRFClient, OPRFServer
from components.smc.psi.ote import OTEReceiver, OTESender
from components.smc.psi.psi import PSIClient, PSIServer
from fedprotocol import BaseClient
from tools.log import LoggerFactory


class ActiveClient(BaseClient):
    def __init__(self, status="PSI"):
        super().__init__("Active")
        assert status in [
            "OT",
            "OTE",
            "OPRF",
            "PSI",
        ], f"{status} is not valid, input right status"

        if status == "OT":
            self.ote_sender = OTESender(128)
        elif status == "OTE":
            self.ote_sender = OTESender(256)
        elif status == "OPRF":
            self.oprf_server = OPRFServer(512)
        elif status == "PSI":
            self.psi_server = PSIServer()
        self.status = status
        self.intersection_result = None

    def init(self):
        if self.status in ["OT", "OTE"]:
            self.set_sub_client(
                self.ote_sender,
                role_rename_dict={"OTESender": "Active", "OTEReceiver": "Passive"},
            )
        elif self.status == "OPRF":
            self.set_sub_client(
                self.oprf_server,
                role_rename_dict={"OPRFServer": "Active", "OPRFClient": "Passive"},
            )
        elif self.status == "PSI":
            self.set_sub_client(
                self.psi_server,
                role_rename_dict={"PSIServer": "Active", "PSIClient": "Passive"},
            )
            self.psi_server.init()

    def run(self, words=None):
        if self.status in ["OT", "OTE"]:
            self.ot_run()
        elif self.status == "OPRF":
            self.oprf_run()
        elif self.status == "PSI":
            self.psi_run(words)

    def ot_run(self):
        self.ote_sender.init()
        self.logger.debug(f"OTESender prepare complete")
        for i in range(self.ote_sender.max_count):
            m0 = (str(i) + "0").encode("utf-8")
            m1 = (str(i) + "1").encode("utf-8")
            self.ote_sender.run(m0, m1)

    def oprf_run(self):
        self.oprf_server.init()
        self.logger.debug(f"OPRFServer prepare complete")
        for i in range(1000):
            word = i.to_bytes(4, "big")
            res = self.oprf_server.run(i, word)
            self.comm.send(receiver="Passive", message_name="item", obj=res)
            self.logger.debug(f"{i} {res.hex()}")

    def psi_run(self, words):
        self.logger.debug(f"Start intersection")
        self.intersection_result = self.psi_server.run(
            words
        )  # intersect stage, res is the intersection
        self.logger.debug(f"Finish intersection")
        self.logger.info(f"result is {self.intersection_result}")

    def close(self):
        pass

    def get_intersection_result(self):
        return self.intersection_result


class PassiveClient(BaseClient):
    def __init__(self, status="PSI"):
        super().__init__("Passive")
        assert status in [
            "OT",
            "OTE",
            "OPRF",
            "PSI",
        ], f"{status} is not valid, input right status"

        self.r = [random.randint(0, 1) for _ in range(1000)]
        if status == "OT":
            self.ote_receiver = OTEReceiver(self.r, 128)

        elif status == "OTE":
            self.ote_receiver = OTEReceiver(self.r, 256)
        elif status == "OPRF":
            self.oprf_client = OPRFClient(512)
        elif status == "PSI":
            self.psi_client = PSIClient()
        self.status = status
        self.intersection_result = None

    def init(self):
        if self.status in ["OT", "OTE"]:
            self.set_sub_client(
                self.ote_receiver,
                role_rename_dict={"OTESender": "Active", "OTEReceiver": "Passive"},
            )
        elif self.status == "OPRF":
            self.set_sub_client(
                self.oprf_client,
                role_rename_dict={"OPRFServer": "Active", "OPRFClient": "Passive"},
            )
        elif self.status == "PSI":
            self.set_sub_client(
                self.psi_client,
                role_rename_dict={"PSIServer": "Active", "PSIClient": "Passive"},
            )
            self.psi_client.init()

    def run(self, words=None):
        if self.status in ["OT", "OTE"]:
            self.ot_run()
        elif self.status == "OPRF":
            self.oprf_run()
        elif self.status == "PSI":
            self.psi_run(words)
        return self.get_intersection_result()

    def ot_run(self):
        self.ote_receiver.init()
        self.logger.debug(f"OTEReceiver prepare complete")

        for i, b in enumerate(self.r):
            msg = self.ote_receiver.run().decode("utf-8")
            self.logger.debug(f"msg is: {msg[:-1]} {msg[-1]}")
            assert msg == str(i) + str(b)

    def oprf_run(self):
        words = [i.to_bytes(4, "big") for i in range(1000)]
        self.oprf_client.complete_init(words)
        self.logger.debug(f"OPRFClient prepare complete")

        for i in range(1000):
            peer_val = self.comm.receive(sender="Active", message_name="item").hex()
            local_val = self.oprf_client.run(i).hex()
            self.logger.debug(f"{i} {local_val} {peer_val}")
            assert local_val == peer_val, f"{i}th oprf is not correct"

    def psi_run(self, words):
        self.logger.debug(f"Start intersection")
        self.intersection_result = self.psi_client.run(
            words
        )  # intersect stage, res is the intersection
        self.logger.debug(f"Finish intersection")
        self.logger.info(f"Results is {self.intersection_result}")

    def close(self):
        pass

    def get_intersection_result(self):
        return self.intersection_result


def get_active_data():
    return {"words": random.sample(range(1, 100), 50)}


def get_passive_data():
    return {"words": random.sample(range(1, 1000), 100)}


if __name__ == "__main__":
    from fedprotocol.envs import LocalEnv

    # OT, OTE, OPRF, PSI
    status = "PSI"
    if status == "PSI":
        LoggerFactory.LEVEL = logging.INFO

    env = LocalEnv()
    env.add_client(client=ActiveClient(status=status), **get_active_data())
    env.add_client(client=PassiveClient(status=status), **get_passive_data())
    env.run()
