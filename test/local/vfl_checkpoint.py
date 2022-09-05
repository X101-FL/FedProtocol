from typing import Any, Dict, Optional

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

import fedprotocol as fp
from fedprotocol import BaseClient
from fedprotocol.typing import Client, StateDict


class PsiA(BaseClient):
    def __init__(self):
        super().__init__('PSI', 'PsiA')

    def intersect(self, data_id):
        b_data_id = self.comm.receive('PsiB', 'data_id')
        result = np.array(list(set(b_data_id) & set(data_id)))
        self.comm.send('PsiB', 'result', result)
        self.comm.clear()
        return result


class PsiB(BaseClient):
    def __init__(self):
        super().__init__('PSI', 'PsiB')

    def intersect(self, data_id):
        self.comm.send('PsiA', 'data_id', data_id)
        result = self.comm.receive('PsiA', 'result')
        self.comm.clear()
        return result


class ModelA(BaseClient):
    def __init__(self):
        super().__init__('FedLR', 'ModelA')
        self.sk_model = None

    def init(self) -> Client:
        self.sk_model = LogisticRegression(max_iter=10, warm_start=True)
        return self

    def train(self, X_train, Y_train):
        B_X_train = self.comm.receive('ModelB', 'B_X_train')
        X_train = np.concatenate([X_train, B_X_train], axis=1)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.sk_model.fit(X_train, Y_train)

    def predict(self, X_test):
        B_X_test = self.comm.receive('ModelB', 'B_X_test')
        X_test = np.concatenate([X_test, B_X_test], axis=1)
        return self.sk_model.predict(X_test)

    def state_dict(self) -> Dict[str, Any]:
        return {'sk_model': self.sk_model}

    def load_state_dict(self, state_dict: Dict[str, Any]) -> None:
        self.sk_model = state_dict['sk_model']


class ModelB(BaseClient):
    def __init__(self):
        super().__init__('FedLR', 'ModelB')
        self.sk_transform = None

    def init(self) -> Client:
        self.sk_transform = StandardScaler()
        return self

    def train(self, X_train):
        X_train = self.sk_transform.fit_transform(X_train)
        self.comm.send('ModelA', 'B_X_train', X_train)

    def predict(self, X_test):
        X_test = self.sk_transform.transform(X_test)
        self.comm.send('ModelA', 'B_X_test', X_test)

    def state_dict(self) -> Dict[str, Any]:
        return {'sk_transform': self.sk_transform}

    def load_state_dict(self, state_dict: Dict[str, Any]) -> None:
        self.sk_transform = state_dict['sk_transform']


class VFLA(BaseClient):
    def __init__(self):
        super().__init__("VFL", "VFLA")
        self.psi_a = PsiA()
        self.model_a = ModelA()

    def init(self):
        self.set_sub_client(self.psi_a,
                            role_bind_mapping={"PsiA": "VFLA", "PsiB": "VFLB"})
        self.set_sub_client(self.model_a,
                            role_bind_mapping={"ModelA": "VFLA", "ModelB": "VFLB"})
        return self

    def train(self, ID_train, X_train, Y_train):
        with self.psi_a.init():
            intersect_ids = self.psi_a.intersect(ID_train)
        _id_order = dict(zip(ID_train, range(len(ID_train))))
        _selector = np.array([_id_order[inter_id] for inter_id in intersect_ids])

        X_train = X_train[_selector]
        Y_train = Y_train[_selector]

        self.logger.info(f"start training ...")
        with self.model_a.init():
            self.restore(non_exist='None')
            for epoch in range(5):
                self.logger.info(f"start epoch<{epoch}>")
                self.comm.send('VFLB', 'new_epoch', True)
                self.model_a.train(X_train, Y_train)
                self.checkpoint()
        self.comm.send('VFLB', 'new_epoch', False)

    def test(self, ID_test, X_test, Y_test):
        with self.psi_a.init():
            intersect_ids = self.psi_a.intersect(ID_test)
        _id_order = dict(zip(ID_test, range(len(ID_test))))
        _selector = np.array([_id_order[inter_id] for inter_id in intersect_ids])

        X_test = X_test[_selector]
        Y_test = Y_test[_selector]

        self.logger.info(f"start testing ...")
        with self.model_a.init():
            self.restore(non_exist='raise')
            Y_pred = self.model_a.predict(X_test)

        acc = (Y_test == Y_pred).mean()
        self.logger.info(f"test acc : {acc}")

    def state_dict(self) -> Optional[StateDict]:
        return {'model_a': self.model_a}

    def load_state_dict(self, state_dict: StateDict) -> None:
        self.model_a.load_state_dict(state_dict['model_a'])


class VFLB(BaseClient):
    def __init__(self):
        super().__init__("VFL", "VFLB")
        self.psi_b = PsiB()
        self.model_b = ModelB()

    def init(self):
        self.set_sub_client(self.psi_b,
                            role_bind_mapping={"PsiA": "VFLA", "PsiB": "VFLB"})
        self.set_sub_client(self.model_b,
                            role_bind_mapping={"ModelA": "VFLA", "ModelB": "VFLB"})
        return self

    def train(self, ID_train, X_train):
        with self.psi_b.init():
            intersect_ids = self.psi_b.intersect(ID_train)
        _id_order = dict(zip(ID_train, range(len(ID_train))))
        _selector = np.array([_id_order[inter_id] for inter_id in intersect_ids])

        X_train = X_train[_selector]

        with self.model_b.init():
            self.model_b.restore(non_exist='None')
            while self.comm.receive('VFLA', 'new_epoch'):
                self.model_b.train(X_train)
                self.model_b.checkpoint()

    def test(self, ID_test, X_test):
        with self.psi_b.init():
            intersect_ids = self.psi_b.intersect(ID_test)
        _id_order = dict(zip(ID_test, range(len(ID_test))))
        _selector = np.array([_id_order[inter_id] for inter_id in intersect_ids])

        X_test = X_test[_selector]

        with self.model_b.init():
            self.model_b.restore(non_exist='raise')
            self.model_b.predict(X_test)


def make_dataset():
    from sklearn import datasets
    from sklearn.model_selection import train_test_split
    X, Y = datasets.make_classification(n_samples=1000, n_features=20, random_state=47621)
    A_X, B_X = np.hsplit(X, [10])
    ID = np.arange(Y.size)
    ID_train, ID_test, A_X_train, A_X_test, \
        B_X_train, B_X_test, Y_train, Y_test = train_test_split(ID, A_X, B_X, Y, test_size=0.3)

    A_ID_train, _, A_X_train, _, Y_train, _ = train_test_split(ID_train, A_X_train, Y_train, test_size=0.1)
    B_ID_train, _, B_X_train, _ = train_test_split(ID_train, B_X_train, test_size=0.1)

    A_ID_test, _, A_X_test, _, Y_test, _ = train_test_split(ID_test, A_X_test, Y_test, test_size=0.1)
    B_ID_test, _, B_X_test, _ = train_test_split(ID_test, B_X_test, test_size=0.1)
    return A_ID_train, A_X_train, Y_train, \
        A_ID_test, A_X_test, Y_test, \
        B_ID_train, B_X_train, \
        B_ID_test, B_X_test


if __name__ == '__main__':
    from fedprotocol.envs import LocalEnv

    A_ID_train, A_X_train, Y_train, \
        A_ID_test, A_X_test, Y_test, \
        B_ID_train, B_X_train, \
        B_ID_test, B_X_test = make_dataset()

    fp.set_env(name='Local') \
        .add_client(VFLA(), entry_func='train', ID_train=A_ID_train, X_train=A_X_train, Y_train=Y_train) \
        .add_client(VFLB(), entry_func='train', ID_train=B_ID_train, X_train=B_X_train) \
        .set_checkpoint_home(r'D:\Temp\fedPrototype') \
        .run()

    fp.set_env(name='Local') \
        .add_client(VFLA(), entry_func='test', ID_test=A_ID_test, X_test=A_X_test, Y_test=Y_test) \
        .add_client(VFLB(), entry_func='test', ID_test=B_ID_test, X_test=B_X_test) \
        .set_checkpoint_home(r'D:\Temp\fedPrototype') \
        .run()
