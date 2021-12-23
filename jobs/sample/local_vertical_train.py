import logging
import random
import typing as T

import numpy as np
from torch import Tensor

from components.algorithms.vertical.dataset import MnistDataset
from components.algorithms.vertical.train_clients import (
    ActiveTrainClient,
    PassiveTrainClient,
)
from components.smc.psi.psi import PSIClient, PSIServer
from fedprototype import BaseClient
from tools.log import LoggerFactory


class ActiveClient(BaseClient):
    def __init__(self, **kwargs):
        super().__init__("Active")
        self.psi_server = PSIServer()
        self.train_client = ActiveTrainClient(**kwargs)

    def init(self) -> None:
        self.set_sub_client(
            self.psi_server,
            role_rename_dict={"PSIServer": "Active", "PSIClient": "Passive"},
        )
        self.set_sub_client(self.train_client)

    def run(
        self,
        train_ids: T.List[int],
        test_ids: T.List[int],
        train_features: Tensor,
        train_labels: Tensor,
        test_features: Tensor,
        test_labels: Tensor,
    ) -> None:
        train_psi_ids = self._psi(train_ids)
        test_psi_ids = self._psi(test_ids)

        train_features = train_features[train_psi_ids, :392]
        train_labels = train_labels[train_psi_ids]
        test_features = test_features[test_psi_ids, :392]
        test_labels = test_labels[test_psi_ids]

        self.train_client.init()
        self.train_client.run(train_features, train_labels, test_features, test_labels)

    def _psi(self, ids: T.List[int]) -> T.List[int]:
        # self.psi_server.init()
        # psi_ids = self.psi_server.run(ids)
        # self.psi_server.close()
        # return psi_ids
        return ids

    def close(self):
        pass


class PassiveClient(BaseClient):
    def __init__(self, **kwargs):
        super().__init__("Passive")
        self.psi_client = PSIClient()
        self.train_client = PassiveTrainClient(**kwargs)

    def init(self) -> None:
        self.set_sub_client(
            self.psi_client,
            role_rename_dict={"PSIServer": "Active", "PSIClient": "Passive"},
        )
        self.set_sub_client(self.train_client)

    def run(
        self,
        train_ids: T.List[int],
        test_ids: T.List[int],
        train_features: Tensor,
        test_features: Tensor,
    ) -> None:
        train_psi_ids = self._psi(train_ids)
        test_psi_ids = self._psi(test_ids)

        train_features = train_features[train_psi_ids, 392:]
        test_features = test_features[test_psi_ids, 392:]

        self.train_client.init()
        self.train_client.run(train_features, test_features)

    def _psi(self, ids: T.List[int]) -> T.List[int]:
        # self.psi_client.init()
        # psi_ids = self.psi_client.run(ids)
        # self.psi_client.close()
        # return psi_ids
        return ids

    def close(self):
        pass


def get_run_kwargs(data_dir: str) -> T.Tuple[dict, dict]:
    mnist = MnistDataset(data_dir=data_dir)
    data_dict = mnist.prepare()
    return data_dict["Active"], data_dict["Passive"]


if __name__ == "__main__":
    from fedprototype.envs import LocalEnv

    random.seed(42)
    np.random.seed(31)

    data_dir = "/home/scott/Projects/ECNU/FedPrototype/datasets"
    active_run_kwargs, passive_run_kwargs = get_run_kwargs(data_dir)

    LoggerFactory.LEVEL = logging.ERROR

    alpha = 1e-3
    batch_size = 64
    epoch = 100

    env = LocalEnv()
    env.add_client(
        client=ActiveClient(alpha=alpha, batch_size=batch_size, epoch=epoch),
        **active_run_kwargs
    )
    env.add_client(
        client=PassiveClient(alpha=alpha, batch_size=batch_size, epoch=epoch),
        **passive_run_kwargs
    )
    env.run()
