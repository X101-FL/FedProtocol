from abc import ABC
from logging import Logger
from typing import Any, Dict, Optional, Union

from fedprotocol.tools.log import getLogger
from fedprotocol.typing import (
    Worker,
    Comm,
    Env,
    ProtocolName,
    RoleName,
    StateDict,
    StateKey,
    SubRoleName,
    TrackPath,
    UpperRoleName,
)


class BaseWorker(ABC):
    def __init__(self, protocol_name: ProtocolName, role_name: RoleName):
        self.protocol_name: ProtocolName = protocol_name
        self.role_name: RoleName = role_name
        self.track_path: Optional[TrackPath] = None
        self.comm: Optional[Comm] = None
        self.logger: Optional[Logger] = None
        self.env: Optional[Env] = None

    def init(self) -> Worker:
        return self

    def run(self, **kwargs) -> Any:
        raise NotImplemented("Method 'run' is not implemented")

    def close(self) -> None:
        pass

    def rename_protocol(self, protocol_name: ProtocolName) -> Worker:
        self.protocol_name = protocol_name
        return self

    def set_sub_worker(
        self,
        sub_worker: Worker,
        role_bind_mapping: Optional[Dict[SubRoleName, UpperRoleName]] = None,
    ) -> None:
        sub_worker._set_env(self.env)
        sub_worker._set_track_path(self)
        sub_worker._set_comm(self, role_bind_mapping)
        sub_worker._set_comm_logger()
        sub_worker._active_comm()
        sub_worker._set_logger()

    def checkpoint(
        self,
        state_key: Optional[StateKey] = None,
        state_dict: Optional[StateDict] = None,
    ) -> None:
        if state_dict is None:
            state_dict = self.state_dict()
        if state_dict is None:
            return

        if state_key is None:
            state_key = self.track_path

        self.logger.debug(f"Save state dict to {state_key}")
        self.env.state_manager.save(state_key, state_dict)

    def restore(
        self,
        state_key: Optional[StateKey] = None,
        non_exist: Union[str, StateDict] = 'raise',
    ) -> None:
        if state_key is None:
            state_key = self.track_path
        self.logger.debug(f"Load state dict for {state_key}")
        state_dict = self.env.state_manager.load(state_key, non_exist=non_exist)

        if state_dict is None:
            self.logger.debug(f"No state_dict to load")
            return

        self.load_state_dict(state_dict)

    def state_dict(self) -> Optional[StateDict]:
        return None

    def load_state_dict(self, state_dict: StateDict) -> None:
        return None

    def _set_env(self, env) -> Worker:
        self.env = env
        return self

    def _set_track_path(self, upper_worker: Worker) -> Worker:
        self.track_path = (
            f"{upper_worker.track_path}/{self.protocol_name}.{self.role_name}"
        )
        return self

    def _set_comm(
        self,
        upper_worker: Worker,
        role_bind_mapping: Optional[Dict[SubRoleName, UpperRoleName]] = None,
    ) -> Worker:
        self.comm = upper_worker.comm._sub_comm(
            self.protocol_name, self.role_name, role_bind_mapping
        )
        return self

    def _set_logger(self) -> Worker:
        self.logger = getLogger(f"Worker.{self.track_path}")
        return self

    def _set_comm_logger(self) -> Worker:
        self.comm.logger = getLogger(f"Frame.Comm.{self.track_path}")
        return self

    def _active_comm(self) -> Worker:
        self.comm._active()
        return self

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
