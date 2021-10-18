from abc import ABC, abstractmethod


class BaseClient(ABC):
    def __init__(self):
        self.comm = None
        self.logger = None
        self.role_name = None
        self.role_index = 0

    @abstractmethod
    def init_client(self, params):
        pass

    @abstractmethod
    def run(self, **kwargs):
        pass

    def set_comm(self, comm):
        self.comm = comm

    def set_logger(self, logger):
        self.logger = logger

    def set_role(self, role_name, role_index):
        self.role_name = role_name
        self.role_index = role_index
