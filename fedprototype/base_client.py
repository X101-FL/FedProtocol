from abc import ABC, abstractmethod
from .envs.tools import CommRenameWrapper


class BaseClient(ABC):
    def __init__(self, role_name):
        self.role_name = role_name
        self.logger = None
        self.comm = None

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def run(self, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass

    def set_sub_client(self, sub_client, role_rename_dict=None):
        sub_comm = CommRenameWrapper(self.comm, role_rename_dict)
        sub_client.set_comm(sub_comm)
        sub_client.set_logger(self.logger)

    def set_comm(self, comm):
        self.comm = comm

    def set_logger(self, logger):
        self.logger = logger
