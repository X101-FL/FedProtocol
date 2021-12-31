from abc import ABC
from typing import Any, Optional, Dict, Union

from fedprototype.envs.tools import CommRenameWrapper
from fedprototype.typing import Comm, Logger, RoleName, SubRoleName, UpperRoleName, \
    Client, TrackPath, Env, MessageSpace, FilePath


class BaseClient(ABC):

    def __init__(self, role_name: RoleName):
        self.role_name = role_name
        self.track_path: TrackPath = None
        self.comm: Comm = None
        self.logger: Logger = None
        self.env: Env = None

    def init(self) -> Client:
        return self

    def run(self, **kwargs) -> Any:
        raise NotImplemented("method : 'run' is not implemented")

    def close(self) -> None:
        pass

    def set_sub_client(self,
                       sub_client: Client,
                       message_space: MessageSpace = None,
                       role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None) -> None:
        sub_client \
            ._set_env(self.env) \
            ._set_track_path(self) \
            ._set_comm(self, message_space, role_rename_dict) \
            ._set_client_logger()

    def checkpoint(self,
                   file_path: Optional[FilePath] = None,
                   state_dict: Optional[Dict[str, Any]] = None
                   ) -> None:
        if state_dict is None:
            state_dict = self.state_dict()
        if state_dict is None:
            return

        if file_path is None:
            file_path = f'{self.track_path}.pkl'

        self.logger.debug(f"save state dict to : {file_path}")
        self.env.save(state_dict, file_path)

    def restore(self,
                file_path: Optional[FilePath] = None,
                non_exist: Union[str, Dict[str, Any]] = 'raise'
                ) -> None:
        if file_path is None:
            file_path = f'{self.track_path}.pkl'
        self.logger.debug(f"load state dict from : {file_path}")
        state_dict = self.env.load(file_path, non_exist=non_exist)

        if state_dict is None:
            self.logger.debug(f"no state_dict to load")
            return

        self.load_state_dict(state_dict)

    def state_dict(self) -> Optional[Dict[str, Any]]:
        return None

    def load_state_dict(self, state_dict: Optional[Dict[str, Any]]) -> None:
        pass

    def _set_env(self, env) -> Client:
        self.env = env
        return self

    def _set_track_path(self, upper_client: Client) -> Client:
        for _attr_name, _attr_value in upper_client.__dict__.items():
            if isinstance(_attr_value, BaseClient) and (_attr_value is self):
                self.track_path = f"{upper_client.track_path}/{_attr_name}.{self.role_name}"
                break
        else:
            raise Exception(f"can't find track_name of {self.role_name}")
        return self

    def _set_comm(self,
                  upper_client: Client,
                  message_space: MessageSpace = None,
                  role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None
                  ) -> Client:
        comm = upper_client.comm
        if message_space:
            comm = comm._sub_comm(message_space)
            self._set_comm_logger(comm)
        if role_rename_dict:
            comm = CommRenameWrapper(self.role_name, comm, role_rename_dict)
            self._set_comm_logger(comm)
        self.comm = comm
        return self

    def _set_client_logger(self) -> Client:
        self.logger = self.env.logger_factory.get_logger(self.track_path)
        return self

    def _set_comm_logger(self, comm: Comm) -> Comm:
        comm.logger = self.env.logger_factory.get_logger(f"{self.track_path} [{comm.__class__.__name__}]")
        return comm

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
