from typing import Any, Optional, Dict
from fedprototype.typing import Comm, Logger, RoleName, SubRoleName, UpperRoleName, Client

from abc import ABC, abstractmethod
from fedprototype.envs.tools import CommRenameWrapper


class BaseClient(ABC):

    def __init__(self, role_name: RoleName):
        self.role_name = role_name
        self.comm: Comm = None
        self.logger: Logger = None

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
                       role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None) -> None:
        if role_rename_dict:
            sub_comm = CommRenameWrapper(sub_client.role_name, self.comm, role_rename_dict)
        else:
            sub_comm = self.comm
        sub_client.set_comm(sub_comm)
        sub_client.set_logger(self.logger)

    def set_comm(self, comm: Comm) -> None:
        self.comm = comm

    def set_logger(self, logger: Logger) -> None:
        self.logger = logger
