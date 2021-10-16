from fedprototype import BaseClient


class Active(BaseClient):

    def init_client(self, params):
        pass

    def run(self, id_list, features, model, encrypt_keys):
        # ...
        # return {"predicted_labels": self.predicted_labels,
        #         }
        pass


class Passive(BaseClient):

    def init_client(self, params):
        pass

    def run(self, id_list, features, model, encrypt_keys):
        # ...
        # return None
        pass
