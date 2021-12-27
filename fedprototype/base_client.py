from typing import Any, Optional, Dict
from fedprototype.typing import Comm, Logger, RoleName, SubRoleName, UpperRoleName, \
    Client, TrackName, Env, SubMessageSpaceName

from abc import ABC, abstractmethod
from fedprototype.envs.tools import CommRenameWrapper


class BaseClient(ABC):

    def __init__(self, role_name: RoleName):
        self.role_name = role_name
        self.comm: Comm = None
        self.logger: Logger = None
        self.track_name: TrackName = None
        self._env: Env = None

    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def run(self, **kwargs) -> Any:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def set_sub_client(self,
                       sub_client: Client,
                       sub_message_space_name: SubMessageSpaceName = None,
                       role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None) -> None:
        sub_client \
            ._set_env(self._env) \
            ._set_track_name(self) \
            ._set_logger() \
            ._set_comm(self, sub_message_space_name, role_rename_dict)

    def _set_env(self, env) -> Client:
        self._env = env
        return self

    def _set_track_name(self, upper_client: Client) -> Client:
        for _attr_name, _attr_value in upper_client.__dict__.items():
            if isinstance(_attr_value, BaseClient) and (_attr_value is self):
                self.track_name = f"{upper_client.track_name}/{_attr_name}.{self.role_name}"
                break
        else:
            raise Exception(f"can't find track_name of {self.role_name}")
        return self

    def _set_logger(self) -> Client:
        self.logger = self._env.get_logger(self)
        return self

    def _set_comm(self,
                  upper_client: Client,
                  sub_message_space_name: SubMessageSpaceName = None,
                  role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None
                  ) -> Client:
        comm = upper_client.comm
        if sub_message_space_name:
            comm = comm._sub_comm(sub_message_space_name)
        if role_rename_dict:
            comm = CommRenameWrapper(self.role_name, comm, role_rename_dict)
        self.comm = comm
        return self
