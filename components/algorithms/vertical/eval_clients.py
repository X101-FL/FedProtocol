from fedprotocol import BaseClient


class ActiveEvalClient(BaseClient):
    def __init__(self, batch_size):
        super().__init__("active")
        self.batch_size = batch_size

    def init(self):
        pass

    def run(self, id_list, label, features, model, encrypt_keys):
        # ...
        # return {"AUC": self.auc,
        #         "precision": self.predicted_label,
        #         "loss": loss
        #         }
        pass

    def close(self):
        pass


class PassiveEvalClient(BaseClient):
    def __init__(self):
        super().__init__("passive")

    def init(self):
        pass

    def run(self, id_list, features, model, encrypt_keys):
        # ...
        # return None
        pass

    def close(self):
        pass
