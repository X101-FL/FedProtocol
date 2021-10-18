from abc import ABC, abstractmethod


class BaseEnv(ABC):

    @abstractmethod
    def add_client(self, role_name, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass
