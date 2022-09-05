from abc import ABC, abstractmethod
from typing import Optional

from fedprotocol.typing import Env, StateSaver


class BaseEnv(ABC):
    def __init__(self):
        self.state_saver: Optional[StateSaver] = None

    @abstractmethod
    def add_client(self, *args, **kwargs) -> Env:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass

    def set_state_saver(self, state_saver: StateSaver) -> Env:
        self.state_saver = state_saver
        return self
