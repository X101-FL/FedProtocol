from abc import ABC, abstractmethod
from typing import Any, Union

from fedprototype.typing import Env, Logger, FilePath, FileDir, LoggerFactory


class BaseEnv(ABC):
    def __init__(self):
        self.checkpoint_home: FileDir = ''
        self.logger_factory: LoggerFactory = None
        self.logger: Logger = None

    @abstractmethod
    def add_client(self, *args, **kwargs) -> Env:
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def save(self, obj: Any, file_path: FilePath) -> None:
        pass

    @abstractmethod
    def load(self, file_path: FilePath, non_exist: Union[str, Any] = 'raise') -> Any:
        """
        从文件系统中加载python对象
        :param file_path: 文件路径
        :param non_exist: 文件不存在时如何处理：
            str：可选值{'raise', 'None'}
                'raise'：抛出异常
                'None'：返回None
            Any: 可传入任何python对象，文件不存在时返回该对象
        :return: pyton对象
        """
        pass

    def set_checkpoint_home(self, checkpoint_home: FileDir) -> Env:
        self.checkpoint_home = checkpoint_home
        return self

    def set_logger_factory(self, logger_factory: LoggerFactory) -> Env:
        self.logger_factory = logger_factory
        self.logger = logger_factory.get_logger(self.__class__.__name__)
        return self
