import typing as T

from torchvision import datasets


class MnistDataset:
    def __init__(self, data_dir: str):
        self.train_dataset = datasets.MNIST(root=data_dir, train=True, download=True)
        self.test_dataset = datasets.MNIST(root=data_dir, train=False, download=True)

    def prepare(self) -> dict:
        train_features = self.train_dataset.data.flatten(start_dim=1, end_dim=2) / 255.0
        test_features = self.test_dataset.data.flatten(start_dim=1, end_dim=2) / 255.0
        train_labels = self.train_dataset.targets
        test_labels = self.test_dataset.targets
        active_train_ids, passive_train_ids = self._split_ids(len(train_features), 0)
        active_test_ids, passive_test_ids = self._split_ids(len(test_features), 0)
        return {
            "Active": {
                "train_ids": active_train_ids,
                "test_ids": active_test_ids,
                "train_features": train_features,
                "test_features": test_features,
                "train_labels": train_labels,
                "test_labels": test_labels,
            },
            "Passive": {
                "train_ids": passive_train_ids,
                "test_ids": passive_test_ids,
                "train_features": train_features,
                "test_features": test_features,
            },
        }

    @classmethod
    def _split_ids(cls, length: int, place: int) -> T.Tuple[T.List[int], T.List[int]]:
        assert place < length, f"Please input valid place ..."
        active_ids = [i for i in range(length - place)]
        passive_ids = [i for i in range(place, length)]
        # active_ids = [i for i in range(180)]
        # passive_ids = [i for i in range(30, 200)]
        return active_ids, passive_ids
