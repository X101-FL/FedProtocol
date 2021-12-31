from typing import Union
from abc import ABC, abstractmethod

from fedprototype import BaseClient
from fedprototype.typing import FilePath, StateDict


class BaseStateSaver(ABC):

    def save(self, file_path: FilePath, state_dict: StateDict) -> None:
        state_dict = BaseStateSaver._client_to_state_dict(state_dict)
        self._save(file_path, state_dict)

    def load(self, file_path: FilePath, non_exist: Union[str, StateDict] = 'raise') -> StateDict:
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
        if self.exists(file_path):
            return self._load(file_path)
        elif non_exist == 'raise':
            raise FileNotFoundError(f"not such file : {file_path}")
        elif non_exist == 'None':
            return None
        else:
            return non_exist

    @staticmethod
    def _client_to_state_dict(state_dict: StateDict) -> StateDict:
        if state_dict is None:
            return None
        result_state_dict = {}
        for state_key, state_value in state_dict.items():
            if isinstance(state_value, BaseClient):
                state_value = state_value.state_dict()
                state_value = BaseStateSaver._client_to_state_dict(state_value)
            result_state_dict[state_key] = state_value
        return result_state_dict

    @abstractmethod
    def exists(self, file_path: FilePath) -> bool:
        pass

    @abstractmethod
    def _save(self, file_path: FilePath, state_dict: StateDict) -> None:
        pass

    @abstractmethod
    def _load(self, file_path: FilePath) -> StateDict:
        pass
