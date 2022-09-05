from abc import ABC, abstractmethod
from typing import Optional, Union

from fedprotocol import BaseClient
from fedprotocol.typing import StateDict, StateKey


class BaseStateSaver(ABC):

    def save(self, state_key: StateKey, state_dict: StateDict) -> None:
        state_dict = BaseStateSaver._client_to_state_dict(state_dict)
        self._save(state_key, state_dict)

    def load(self, state_key: StateKey, non_exist: Union[str, StateDict] = 'raise') -> Optional[StateDict]:
        """
        从文件系统中加载python对象
        :param state_key: 状态保存、加载的唯一主键
        :param non_exist: 状态不存在时如何处理：
            str：可选值{'raise', 'None'}
                'raise'：抛出异常
                'None'：返回None
            Any: 可传入任何python对象，状态不存在时返回该对象
        :return: StateDict
        """
        if self.exists(state_key):
            return self._load(state_key)
        elif non_exist == 'raise':
            raise Exception(f"state : {state_key} not found")
        elif non_exist == 'None':
            return None
        else:
            return non_exist

    @staticmethod
    def _client_to_state_dict(state_dict: Optional[StateDict]) -> Optional[StateDict]:
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
    def exists(self, state_key: StateKey) -> bool:
        pass

    @abstractmethod
    def _save(self, state_key: StateKey, state_dict: StateDict) -> None:
        pass

    @abstractmethod
    def _load(self, state_key: StateKey) -> StateDict:
        pass
