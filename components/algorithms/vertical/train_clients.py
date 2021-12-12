import numpy as np
import torch
import torch.nn as nn
from components.algorithms.vertical.models import ActiveModel, PassiveModel
from encrypt.paillier import PaillierKeypair
from fedprototype import BaseClient


class ActiveTrainClient(BaseClient):
    def __init__(self, alpha=1e-3, batch_size=16, epoch=100, feature_size=4):
        super(ActiveTrainClient, self).__init__("Active")
        self.encrypt_key_size = 0
        self.batch_size = batch_size
        self.epoch = epoch
        self.model = ActiveModel(feature_size)
        self.loss_func = nn.BCEWithLogitsLoss()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=alpha)

    def init(self):
        self._init_encrypt_keys()

    def run(self, features, label):
        for i in range(self.epoch):
            batch_loss, batch_acc = self.train_a_epoch(label, features)
            print(
                f"epoch: [{i} / {self.epoch}] epoch_loss: {batch_loss} epoch_acc: {batch_acc}"
            )
        return {"model": self.model}

    def train_a_epoch(self, label, features):
        # 划分batch并运行
        data_size = len(features)
        num_batches_per_epoch = int(data_size / self.batch_size)
        for batch_num in range(num_batches_per_epoch):
            si = batch_num * self.batch_size
            ei = min((batch_num + 1) * self.batch_size, data_size)
            self.train_a_batch(label[si:ei], features[si:ei])
        return batch_loss, batch_acc

    def train_a_batch(self, batch_label, batch_features):
        batch_feature = torch.tensor(
            batch_features, dtype=torch.float, requires_grad=True
        )
        batch_label = torch.tensor(batch_label, dtype=torch.float)

        # STEP2: Compute Θ^A * x_i^A for i ∈ D_A,receive Θ^B x_i^B from B
        batch_output = self.model.forward(batch_feature)
        passive_output = self.comm.receive("Passive", "passive_output")
        self.logger.debug(f"Successfully receive passive_output from Passive")

        self.optimizer.zero_grad()
        passive_output.requires_grad_(requires_grad=True)
        passive_output.grad = None

        # STEP3: Compute Θ_x_i = Θ^A * x_i^A + Θ^B * x_i^B
        # STEP3: Compute y_i_hat = h_Θ(x_i)
        # STEP4: Compute ΔL/ΔΘ^A and the loss L
        full_output = batch_output + passive_output
        batch_loss = self.loss_func(full_output, batch_label)
        batch_loss.backward()
        batch_acc = (
            (torch.round(torch.sigmoid(full_output)) == batch_label).float().mean()
        )

        # STEP6: Update Θ^A
        self.optimizer.step()

        # STEP3: Compute [[(y_i-y_i_hat)]]
        passive_bias_y_grad = passive_output.grad.numpy()
        passive_bias_y_grad_encrypted = self.encrypt_func(passive_bias_y_grad)

        # STEP3: send [[(y_i-y_i_hat)]] to B for i ∈ D_A
        self.comm.send(
            f"Passive", "passive_bias_y_grad_encrypted", passive_bias_y_grad_encrypted
        )
        self.logger.debug(
            f"Successfully send passive_bias_y_grad_encrypted to Passive!"
        )

        # STEP4: receive [[ΔL/ΔΘ^B]] + [[R_B]] from B
        passive_grad_encrypted_noised = self.comm.receive(
            "Passive", "passive_grad_encrypted_noised"
        )
        self.logger.debug(
            f"Successfully receive passive_grad_encrypted_noised from Passive!"
        )

        # STEP5: Decrypt [[ΔL/ΔΘ^B]] + [[R_B]]
        passive_grad_noised = self.decrypt_func(passive_grad_encrypted_noised)

        # STEP5: send ΔL/ΔΘ^B + R_B to B
        self.comm.send(f"Passive", "passive_grad_noised", passive_grad_noised)
        self.logger.debug(f"Successfully send passive_grad_noised to Passive!")

    def _init_encrypt_keys(self):
        self.public_key, self.private_key = PaillierKeypair.generate_keypair(
            n_length=1024, precision=1e-8
        )
        self.encrypt_func = np.frompyfunc(self.public_key.encrypt, 1, 1)
        self.decrypt_func = np.frompyfunc(self.private_key.decrypt, 1, 1)
        self.comm.send("Passive", "public_key", self.public_key)
        self.logger.debug(f"Successfully send public_key to Passive!")

    def close(self):
        pass


class PassiveTrainClient(BaseClient):
    def __init__(self, alpha=1e-3, batch_size=16, epoch=100, feature_size=4):
        super(PassiveTrainClient, self).__init__("Passive")
        self.encrypt_key_size = 0
        self.batch_size = batch_size
        self.epoch = epoch
        self.model = PassiveModel(feature_size)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=alpha)

    def init(self):
        self._init_encrypt_keys()

    def run(self, features):
        for i in range(self.epoch):
            self.train_a_epoch(features)
        return {"model": self.model}

    def train_a_epoch(self, features):
        # 划分batch并运行
        data_size = len(features)
        num_batches_per_epoch = int(data_size / self.batch_size)
        for batch_num in range(num_batches_per_epoch):
            si = batch_num * self.batch_size
            ei = min((batch_num + 1) * self.batch_size, data_size)
            self.train_a_batch(features[si:ei])

    def train_a_batch(self, batch_features):
        batch_feature = torch.tensor(
            batch_features, dtype=torch.float, requires_grad=True
        )

        # STEP2: Compute Θ^B x_i^B for i ∈ D_B
        batch_output = self.model.forward(batch_feature)

        # STEP2: send Θ^B x_i^B to A
        self.comm.send("Active", "passive_output", batch_output.detach())
        self.logger.debug("Successfully send passive_output to Active!")

        # STEP3: receive [[(y_i-y_i_hat)]] from A
        passive_bias_y_grad_encrypted = self.comm.receive(
            "Active", "passive_bias_y_grad_encrypted"
        )
        self.logger.debug(
            f"Successfully receive passive_bias_y_grad_encrypted from Active."
        )

        # STEP4: Compute [[ΔL/ΔΘ^B]]
        passive_grad_encrypted = np.dot(passive_bias_y_grad_encrypted.T, batch_features)

        # STEP4: generate random number R_B
        grad_noise = np.random.random(passive_grad_encrypted.shape) * 0.01
        self.logger.debug(f"Successfully initialize random noise.")

        # STEP4: send [[ΔL/ΔΘ^B]] + [[R_B]] to A
        passive_grad_encrypted_noised = passive_grad_encrypted + self.encrypt_func(
            grad_noise
        )
        self.comm.send(
            "Active", "passive_grad_encrypted_noised", passive_grad_encrypted_noised
        )
        self.logger.debug("Successfully send passive_grad_encrypted_noised to Active!")

        # STEP5: receive ΔL/ΔΘ^B + R_B from A
        passive_grad_noised = self.comm.receive("Active", "passive_grad_noised")
        self.logger.debug(f"Successfully receive passive_grad_noised from Active.")

        # STEP6: Update Θ^B
        self.optimizer.zero_grad()
        passive_grad = passive_grad_noised - grad_noise
        model_w = self.model.linear.weight
        model_w.grad = torch.tensor(passive_grad.astype(float), dtype=torch.float32)
        self.optimizer.step()

    def _init_encrypt_keys(self):
        self.public_key = self.comm.receive("Active", "public_key")
        self.encrypt_func = np.frompyfunc(self.public_key.encrypt, 1, 1)
        self.logger.debug(f"Successfully receive public_key from Active.")

    def close(self):
        pass
