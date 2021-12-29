from abc import ABC, abstractmethod
from typing import Any, Union
from fedprototype.typing import Env, Client, Logger, FilePath


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
