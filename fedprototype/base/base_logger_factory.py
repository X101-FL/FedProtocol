from abc import abstractmethod

from fedprototype.typing import Logger


class BaseLoggerFactory:
    @staticmethod
    @abstractmethod
    def get_logger(name: str) -> Logger:
        pass
