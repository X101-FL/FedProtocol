from fedprotocol import BaseClient
from components.smc.psi.two_party_clients import Party1, Party2
from components.algorithms.vertical.train_clients import ActiveTrainClient, PassiveTrainClient
from components.algorithms.vertical.eval_clients import ActiveEvalClient, PassiveEvalClient
from sklearn.model_selection import train_test_split


class ActiveClient(BaseClient):
    def __init__(self, args):
        super().__init__("active")
        self.param_a = args.param_a
        self.param_b = args.param_b
        self.psi_client = Party1(args.param1, args.param2, args.param3)
        self.train_client = ActiveTrainClient(encrypt_key_size=1024, batch_size=128)
        self.eval_client = ActiveEvalClient(batch_size=256)
        self.tol = args.tol

    def init(self):
        self.set_sub_client(self.psi_client, role_rename_dict={"party1": "active", "party2": "passive"})
        self.set_sub_client(self.train_client)
        self.set_sub_client(self.eval_client)

    def run(self, id_list, label, features):
        self.psi_client.init()
        intersect_id = self.psi_client.run(id_list)
        self.psi_client.close()

        train_id, eval_id = train_test_split(intersect_id, test_size=0.2)

        self.train_client.init()
        self.eval_client.init()
        loss1, loss2 = -10000, 10000
        train_ans, eval_ans = None, None
        while abs(loss2 - loss1) > self.tol:
            self.comm.send("passive", "new_epoch", True)
            train_ans = self.train_client.run(train_id, label, features)
            eval_ans = self.eval_client.run(eval_id, label, features,
                                            train_ans['model'], train_ans['encrypt_keys'])
            loss1, loss2 = loss2, eval_ans['loss']
        self.comm.send("passive", "new_epoch", False)
        return train_ans, eval_ans

    def close(self):
        self.train_client.close()
        self.eval_client.close()


class PassiveClient(BaseClient):
    def __init__(self, args):
        super().__init__("passive")
        self.param_a = args.param_a
        self.param_b = args.param_b
        self.psi_client = Party2(args.param1, args.param2, args.param3)
        self.train_client = PassiveTrainClient(encrypt_key_size=1024)
        self.eval_client = PassiveEvalClient()

    def init(self):
        self.set_sub_client(self.psi_client, role_rename_dict={"party1": "active", "party2": "passive"})
        self.set_sub_client(self.train_client)
        self.set_sub_client(self.eval_client)

    def run(self, id_list, features):
        self.psi_client.init()
        self.psi_client.run(id_list)
        self.psi_client.close()

        self.train_client.init()
        self.eval_client.init()
        train_ans, eval_ans = None, None
        while self.comm.receive("active", "new_epoch"):
            train_ans = self.train_client.run(id_list, features)
            eval_ans = self.eval_client.run(id_list, features,
                                            train_ans['model'], train_ans['encrypt_keys'])
        return train_ans, eval_ans

    def close(self):
        self.train_client.close()
        self.eval_client.close()
