import typing as T

import numpy as np
import torch
from torch import Tensor, nn
from torch.utils.data.dataloader import DataLoader
from torch.utils.data.dataset import TensorDataset

from components.algorithms.vertical.models import ActiveModel, PassiveModel
from encrypt.paillier import PaillierKeypair, PaillierPublicKey
from fedprotocol import BaseWorker


class ActiveTrainClient(BaseWorker):
    encrypt_func: np.ufunc
    decrypt_func: np.ufunc

    def __init__(
        self,
        alpha: float = 1e-3,
        batch_size: int = 16,
        epoch: int = 100,
        in_features: int = 392,
        out_features: int = 10,
        encrypt_key_size: int = 1024,
    ):
        super(ActiveTrainClient, self).__init__("Active")
        self.encrypt_key_size = encrypt_key_size
        self.batch_size = batch_size
        self.epoch = epoch
        self.model = ActiveModel(in_features, out_features)
        self.loss_func = nn.CrossEntropyLoss()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=alpha)

    def init(self) -> None:
        self._init_encrypt_keys()

    def run(
        self,
        train_features: Tensor,
        train_labels: Tensor,
        test_features: Tensor,
        test_labeles: Tensor,
    ):
        train_dataloader, test_dataloader = self._get_dataloader(
            train_features, train_labels, test_features, test_labeles
        )
        for step in range(self.epoch):
            total_loss = 0.0
            idx = 0
            for features, labels in train_dataloader:
                loss = self._train_one_batch(features, labels)
                total_loss += loss
                if idx % 100 == 0:
                    print(f"[{idx} / {len(train_dataloader)}]   ", f"loss: {loss}")
                idx += 1
            acc = self._eval(test_dataloader)
            print(
                f"[{step + 1}] / [{self.epoch}]]   ",
                f"loss: {total_loss / len(train_dataloader)}   ",
                f"acc: {acc}",
            )
        return {"model": self.model}

    def _eval(self, test_dataloader: DataLoader) -> float:
        total_acc = 0.0
        with torch.no_grad():
            for features, labels in test_dataloader:
                output = self.model.forward(features)
                passive_output: Tensor = self.comm.receive(
                    "Passive", "eval_passive_output"
                )
                full_output = output + passive_output
                acc = (full_output.argmax(dim=1) == labels).float().mean()
                total_acc += acc
        return total_acc / len(test_dataloader)

    def _get_dataloader(
        self,
        train_features: Tensor,
        train_labels: Tensor,
        test_features: Tensor,
        test_labeles: Tensor,
    ) -> T.Tuple[DataLoader, DataLoader]:
        return (
            DataLoader(
                TensorDataset(train_features, train_labels),
                batch_size=self.batch_size,
                shuffle=False,
            ),
            DataLoader(
                TensorDataset(test_features, test_labeles), batch_size=self.batch_size
            ),
        )

    def _train_one_batch(self, features, labels) -> float:

        # STEP2: Compute Θ^A * x_i^A for i ∈ D_A,receive Θ^B x_i^B from B
        output = self.model.forward(features)
        passive_output: Tensor = self.comm.receive("Passive", "passive_output")
        self.logger.info(f"Successfully receive passive_output from Passive")

        self.optimizer.zero_grad()
        passive_output.requires_grad_()
        passive_output.grad = None

        # STEP3: Compute Θ * x_i = Θ^A * x_i^A + Θ^B * x_i^B
        # STEP3: Compute y_i_hat = h_Θ(x_i)
        # STEP4: Compute ΔL/ΔΘ^A and the loss L
        full_output = output + passive_output
        batch_loss: Tensor = self.loss_func(full_output, labels)
        batch_loss.backward()
        # batch_acc = (full_output.argmax(dim=1) == labels).float().mean()

        # STEP6: Update Θ^A
        self.optimizer.step()

        # STEP3: Compute [[(y_i-y_i_hat)]]
        passive_bias_y_grad: np.ndarray = passive_output.grad.numpy()

        passive_bias_y_grad_encrypted: np.ndarray = self.encrypt_func(
            passive_bias_y_grad
        )

        # STEP3: send [[(y_i-y_i_hat)]] to B for i ∈ D_A
        self.comm.send(
            f"Passive", "passive_bias_y_grad_encrypted", passive_bias_y_grad_encrypted
        )
        self.logger.info(f"Successfully send passive_bias_y_grad_encrypted to Passive!")

        # STEP4: receive [[ΔL/ΔΘ^B]] + [[R_B]] from B
        passive_grad_encrypted_noised: np.ndarray = self.comm.receive(
            "Passive", "passive_grad_encrypted_noised"
        )
        self.logger.info(
            f"Successfully receive passive_grad_encrypted_noised from Passive!"
        )

        # STEP5: Decrypt [[ΔL/ΔΘ^B]] + [[R_B]]
        passive_grad_noised: np.ndarray = self.decrypt_func(
            passive_grad_encrypted_noised
        )

        # STEP5: send ΔL/ΔΘ^B + R_B to B
        self.comm.send(f"Passive", "passive_grad_noised", passive_grad_noised)
        self.logger.info(f"Successfully send passive_grad_noised to Passive!")

        return float(batch_loss.item())

    def _init_encrypt_keys(self) -> None:
        public_key, private_key = PaillierKeypair.generate_keypair(
            n_length=self.encrypt_key_size, precision=1e-8
        )
        self.encrypt_func = np.frompyfunc(public_key.encrypt, 1, 1)
        self.decrypt_func = np.frompyfunc(private_key.decrypt, 1, 1)
        self.comm.send("Passive", "public_key", public_key)
        self.logger.info(f"Successfully send public_key to Passive!")

    def close(self):
        pass


class PassiveTrainClient(BaseWorker):
    encrypt_func: np.ufunc

    def __init__(
        self,
        alpha: float = 1e-3,
        batch_size: int = 16,
        epoch: int = 100,
        in_features: int = 392,
        out_features: int = 10,
        encrypt_key_size: int = 1024,
    ):
        super(PassiveTrainClient, self).__init__("Passive")
        self.encrypt_key_size = encrypt_key_size
        self.batch_size = batch_size
        self.epoch = epoch
        self.model = PassiveModel(in_features, out_features)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=alpha)

    def init(self) -> None:
        self._init_encrypt_keys()

    def run(self, train_features: Tensor, test_features: Tensor):
        train_dataloader, test_dataloader = self._get_dataloader(
            train_features, test_features
        )
        for _ in range(self.epoch):
            for features in train_dataloader:
                features = features[0]
                # features will be List[Tensor]
                self._train_one_batch(features)
            self._eval(test_dataloader)
        return {"model": self.model}

    def _eval(self, test_dataloader: DataLoader):
        with torch.no_grad():
            for features in test_dataloader:
                features = features[0]
                output = self.model.forward(features)
                self.comm.send("Active", "eval_passive_output", output)

    def _get_dataloader(
        self, train_features: Tensor, test_features: Tensor
    ) -> T.Tuple[DataLoader, DataLoader]:
        return (
            DataLoader(
                TensorDataset(train_features), batch_size=self.batch_size, shuffle=False
            ),
            DataLoader(
                TensorDataset(test_features), batch_size=self.batch_size, shuffle=False
            ),
        )

    def _train_one_batch(self, features) -> None:

        # STEP2: Compute Θ^B x_i^B for i ∈ D_B
        output = self.model.forward(features)

        # STEP2: send Θ^B x_i^B to A
        self.comm.send("Active", "passive_output", output.detach())
        self.logger.info("Successfully send passive_output to Active!")

        # STEP3: receive [[(y_i-y_i_hat)]] from A
        passive_bias_y_grad_encrypted: np.ndarray = self.comm.receive(
            "Active", "passive_bias_y_grad_encrypted"
        )
        self.logger.info(
            f"Successfully receive passive_bias_y_grad_encrypted from Active."
        )

        # STEP4: Compute [[ΔL/ΔΘ^B]]
        passive_grad_encrypted: np.ndarray = np.dot(
            passive_bias_y_grad_encrypted.T, features
        )

        # STEP4: generate random number R_B
        grad_noise = np.random.random(passive_grad_encrypted.shape) * 0.01
        self.logger.info(f"Successfully initialize random noise.")

        # STEP4: send [[ΔL/ΔΘ^B]] + [[R_B]] to A
        passive_grad_encrypted_noised: np.ndarray = (
            passive_grad_encrypted + self.encrypt_func(grad_noise)
        )
        self.comm.send(
            "Active", "passive_grad_encrypted_noised", passive_grad_encrypted_noised
        )
        self.logger.info("Successfully send passive_grad_encrypted_noised to Active!")

        # STEP5: receive ΔL/ΔΘ^B + R_B from A
        passive_grad_noised: np.ndarray = self.comm.receive(
            "Active", "passive_grad_noised"
        )
        self.logger.info(f"Successfully receive passive_grad_noised from Active.")

        # STEP6: Update Θ^B
        self.optimizer.zero_grad()
        passive_grad = passive_grad_noised - grad_noise
        model_w = self.model.linear.weight
        model_w.grad = torch.tensor(passive_grad.astype(float), dtype=torch.float32)
        self.optimizer.step()

    def _init_encrypt_keys(self) -> None:
        public_key: PaillierPublicKey = self.comm.receive("Active", "public_key")
        self.encrypt_func = np.frompyfunc(public_key.encrypt, 1, 1)
        self.logger.info(f"Successfully receive public_key from Active.")

    def close(self):
        pass
