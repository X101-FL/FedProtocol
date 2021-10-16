from fedprototype import BaseClient


# noinspection PyAttributeOutsideInit
class Active(BaseClient):

    def init_client(self, params):
        self.encrypt_key_size = params.encrypt_key_size
        # ...

    def run(self, id_list, label, features):
        # ...
        self._init_encrypt_keys()
        # ...
        # return {"model": self.model,
        #         "predicted_label": self.predicted_label,
        #         "encrypt_keys": (self.public_key, self.private_key)
        #         ...
        #         }
        pass

    def _init_encrypt_keys(self):
        # ...

        # self.comm.send("passive", public_key, "public_keys")
        # self.passive_public_key = self.comm.get("passive", "public_keys")
        pass


# noinspection PyAttributeOutsideInit
class Passive(BaseClient):

    def init_client(self, params):
        self.encrypt_key_size = params.encrypt_key_size
        # ...
        pass

    def run(self, id_list, features):
        # ...
        # return {"model": self.model,
        #         "predicted_label": self.predicted_label,
        #         "encrypt_keys": (self.public_key, self.private_key)
        #         ...
        #         }
        pass
