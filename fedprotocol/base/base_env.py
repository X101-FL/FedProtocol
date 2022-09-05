from abc import ABC, abstractmethod
from typing import Optional

from fedprotocol.typing import Env, StateManager


class BaseEnv(ABC):
    def __init__(self):
        self.state_manager: Optional[StateManager] = None

    @abstractmethod
    def add_worker(self, *args, **kwargs) -> Env:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass

    def set_state_manager(self, state_manager: StateManager) -> Env:
        self.state_manager = state_manager
        return self
