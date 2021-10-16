from abc import ABC, abstractmethod


class BaseComm(ABC):
    @abstractmethod
    def send(self, obj, message_name, role_name, role_index=0):
        pass

    @abstractmethod
    def get(self, message_name, role_name, role_index=0, timeout=-1):
        pass

    @abstractmethod
    def watch(self, message_role_name_index_tuple_list, timeout=-1):
        """
        监听一组消息的接收，一旦收到某个src发送的消息，则立刻通过迭代器返回结果
        :parameter message_role_name_index_tuple_list: list[(message_name,role_name,role_index)]
        :param timeout:
        :return: iter[((message_name,role_name,role_index),obj)]
        """
        pass
