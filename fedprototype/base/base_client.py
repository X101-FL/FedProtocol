from abc import ABC
from typing import Any, Dict, Optional, Union

from fedprototype.typing import (
    Client,
    Comm,
    Env,
    Logger,
    MessageSpace,
    RoleName,
    StateDict,
    StateKey,
    SubRoleName,
    TrackPath,
    UpperRoleName,
    ProtocolName
)


class BaseClient(ABC):

    def __init__(self, protocol_name: ProtocolName, role_name: RoleName):
        self.protocol_name: ProtocolName = protocol_name
        self.role_name: RoleName = role_name
        self.track_path: Optional[TrackPath] = None
        self.comm: Optional[Comm] = None
        self.logger: Optional[Logger] = None
        self.env: Optional[Env] = None

    def init(self) -> Client:
        return self

    def run(self, **kwargs) -> Any:
        raise NotImplemented("method : 'run' is not implemented")

    def close(self) -> None:
        pass

    def set_sub_client(self,
                       sub_client: Client,
                       message_space: Optional[MessageSpace] = None,
                       role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None) -> None:
        sub_client \
            ._set_env(self.env) \
            ._set_track_path(self) \
            ._set_comm(self, message_space, role_rename_dict) \
            ._set_comm_logger() \
            ._set_client_logger()

    def checkpoint(self,
                   state_key: Optional[StateKey] = None,
                   state_dict: Optional[StateDict] = None
                   ) -> None:
        if state_dict is None:
            state_dict = self.state_dict()
        if state_dict is None:
            return

        if state_key is None:
            state_key = self.track_path

        self.logger.debug(f"save state dict to : {state_key}")
        self.env.state_saver.save(state_key, state_dict)

    def restore(self,
                state_key: Optional[StateKey] = None,
                non_exist: Union[str, StateDict] = 'raise'
                ) -> None:
        if state_key is None:
            state_key = self.track_path
        self.logger.debug(f"load state dict of : {state_key}")
        state_dict = self.env.state_saver.load(state_key, non_exist=non_exist)

        if state_dict is None:
            self.logger.debug(f"no state_dict to load")
            return

        self.load_state_dict(state_dict)

    def state_dict(self) -> Optional[StateDict]:
        return None

    def load_state_dict(self, state_dict: StateDict) -> None:
        return

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
                  message_space: Optional[MessageSpace] = None,
                  role_rename_dict: Optional[Dict[SubRoleName,
                                                  UpperRoleName]] = None
                  ) -> Client:

        self.comm = upper_client.comm._sub_comm(message_space,
                                                role_rename_dict)
        return self

    def _set_client_logger(self) -> Client:
        self.logger = self.env.logger_factory.get_logger(self.track_path)
        return self

    def _set_comm_logger(self) -> Client:
        self.comm.logger = self.env.logger_factory.get_logger(
            f"{self.track_path} [{self.comm.__class__.__name__}]")
        return self

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
