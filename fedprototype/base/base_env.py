from abc import ABC, abstractmethod
from typing import Optional
from fedprototype.typing import Env, Logger, LoggerFactory, StateSaver


class BaseEnv(ABC):
    def __init__(self):
        self.state_saver: Optional[StateSaver] = None
        self.logger_factory: Optional[LoggerFactory] = None
        self.logger: Optional[Logger] = None

    @abstractmethod
    def add_client(self, *args, **kwargs) -> Env:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass

    def set_state_saver(self, state_saver: StateSaver) -> Env:
        self.state_saver = state_saver
        return self

    def set_logger_factory(self, logger_factory: LoggerFactory) -> Env:
        self.logger_factory = logger_factory
        self.logger = logger_factory.get_logger(self.__class__.__name__)
        return self
