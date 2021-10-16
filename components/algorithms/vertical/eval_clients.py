from fedprototype import BaseClient


# noinspection PyAttributeOutsideInit
class Active(BaseClient):

    def init_client(self, params):
        self.auc_bucket = params.auc_bucket
        # ...

    def run(self, id_list, label, features, model, encrypt_keys):
        # ...
        # return {"AUC": self.auc,
        #         "precision": self.predicted_label,
        #         }
        pass


class Passive(BaseClient):

    def init_client(self, params):
        pass

    def run(self, id_list, features, model, encrypt_keys):
        # ...
        # return None
        pass
