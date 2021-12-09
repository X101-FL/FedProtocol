import torch.nn as nn


class ActiveModel(nn.Module):

    def __init__(self, feature_size):
        super().__init__()
        self.linear = nn.Linear(in_features=feature_size, out_features=1, bias=False)

    def forward(self, x):
        return self.linear(x)


class PassiveModel(nn.Module):
    def __init__(self, feature_size):
        super().__init__()
        self.linear = nn.Linear(in_features=feature_size, out_features=1, bias=False)

    def forward(self, x):
        return self.linear(x)
