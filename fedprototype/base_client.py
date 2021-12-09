from abc import ABC, abstractmethod
from logging import Logger
import typing as T

from fedprototype.envs.base_comm import BaseComm
from fedprototype.envs.tools import CommRenameWrapper


class BaseClient(ABC):
    comm: BaseComm
    logger: Logger

    def __init__(self, role_name: str):
        self.role_name = role_name

    @abstractmethod
    def init(self) -> None:
        pass

    @abstractmethod
    def run(self, **kwargs: T.Any) -> T.Any:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def set_sub_client(
        self,
        sub_client: "BaseClient",
        role_rename_dict: T.Optional[T.Dict[str, str]] = None,
    ) -> None:
        if role_rename_dict:
            sub_comm = CommRenameWrapper(
                sub_client.role_name, self.comm, role_rename_dict
            )
        else:
            sub_comm = self.comm
        sub_client.set_comm(sub_comm)
        sub_client.set_logger(self.logger)

    def set_comm(self, comm: BaseComm) -> None:
        self.comm = comm

    def set_logger(self, logger: Logger) -> None:
        self.logger = logger
