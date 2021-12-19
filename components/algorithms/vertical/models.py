from torch import Tensor, nn


class ActiveModel(nn.Module):
    def __init__(self, in_features: int, out_features: int):
        super().__init__()
        self.linear = nn.Linear(
            in_features=in_features, out_features=out_features, bias=False
        )

    def forward(self, x: Tensor) -> Tensor:
        return self.linear(x)


class PassiveModel(nn.Module):
    def __init__(self, in_features: int, out_features: int):
        super().__init__()
        self.linear = nn.Linear(
            in_features=in_features, out_features=out_features, bias=False
        )

    def forward(self, x: Tensor) -> Tensor:
        return self.linear(x)
