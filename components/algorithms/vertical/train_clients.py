from fedprototype import BaseClient


class ActiveTrainClient(BaseClient):
    def __init__(self, encrypt_key_size, batch_size):
        super().__init__("active")
        self.encrypt_key_size = encrypt_key_size
        self.batch_size = batch_size

    def init(self):
        self._init_encrypt_keys()

    def run(self, id_list, label, features):
        # ...
        # ...
        # return {"model": self.model,
        #         "predicted_label": self.predicted_label,
        #         "encrypt_keys": (self.public_key, self.private_key)
        #         ...
        #         }
        pass

    def close(self):
        pass

    def _init_encrypt_keys(self):
        # ...

        # self.comm.send("passive", public_key, "public_keys")
        # self.passive_public_key = self.comm.get("passive", "public_keys")
        pass


class PassiveTrainClient(BaseClient):
    def __init__(self, encrypt_key_size):
        super().__init__("passive")
        self.encrypt_key_size = encrypt_key_size

    def init(self):
        pass

    def run(self, id_list, features):
        # ...
        # return {"model": self.model,
        #         "predicted_label": self.predicted_label,
        #         "encrypt_keys": (self.public_key, self.private_key)
        #         ...
        #         }
        pass

    def close(self):
        pass
