from abc import ABC, abstractmethod

from fedprototype.typing import Env, Client, Logger


class BaseEnv(ABC):
    @abstractmethod
    def add_client(self, *args, **kwargs) -> Env:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def get_logger(self, client: Client) -> Logger:
        pass

    @abstractmethod
    def checkpoint(self, client: Client) -> None:
        pass

    @abstractmethod
    def load_state(self, client: Client) -> None:
        pass
