from abc import ABC, abstractmethod
from fedprototype.typing import Env


class BaseEnv(ABC):
    @abstractmethod
    def add_client(self, *args, **kwargs) -> Env:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass
