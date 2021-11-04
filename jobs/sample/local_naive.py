import time
import numpy as np

from fedprototype import BaseClient


class ActiveClient(BaseClient):

    def __init__(self, role_name):
        super(ActiveClient, self).__init__(role_name)

    def init(self):
        pass

    def run(self, id_list):
        for passive in self.comm.get_role_name_list("Passive"):
            self.comm.send(f"{passive}", "id_list", id_list)
            self.logger.debug(f"Successfully send id_list to {passive}!")
        self.logger.debug(f"Successfully send id_list to all Passives!")

        for sender, message_name, feature in self.comm.watch("Passive", "feature"):
            self.logger.debug(f"Successfully receive feature from {sender}, its value is {feature}.")
        self.logger.debug(f"Successfully receive feature from all Passives!")

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
        self.logger.debug(f"Successfully receive id_list from Active, the value is {id_list}")

        time.sleep(np.random.randint(0, 5))
        self.comm.send("Active", "feature", feature)
        self.logger.debug("Successfully send feature to Active!")

    def close(self):
        pass


def get_active_run_kwargs():
    id_list = [i for i in range(5)]
    return {'id_list': id_list}


def get_passive_run_kwargs(passive_id):
    features = [passive_id]
    return {'feature': features}


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    PASSIVE_NUM = 5

    env = LocalEnv()
    for pid in range(PASSIVE_NUM):
        env.add_client(client=PassiveClient(role_name=f"Passive.{pid}"), **get_passive_run_kwargs(pid))
    env.add_client(client=ActiveClient(role_name="Active"), **get_active_run_kwargs())
    env.run()
