import typing as T
from abc import ABC, abstractmethod


class BaseEnv(ABC):
    @abstractmethod
    def add_client(self, *args, **kwargs) -> T.Any:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass
