from abc import ABC
from fedprototype import BaseClient


class BasePipline(BaseClient, ABC):

    def init_client(self, params):
        pass

    def init_component(self, component_class, init_params):
        component = component_class()
        component.set_role(self.role_name, self.role_index)
        component.set_comm(self.comm)
        component.set_logger(self.logger)
        component.init_client(init_params)
        return component

    def __call__(self):  # 为了让pipline实例可以作为client_class参数传入env中
        return self
