from abc import ABC, abstractmethod


class BaseComm(ABC):
    @abstractmethod
    def send(self, role_name, message_name, obj):
        pass

    @abstractmethod
    def get(self, role_name, message_name, timeout=-1):
        pass

    @abstractmethod
    def watch(self, role_message_name_tuple_list, timeout=-1):
        """
        监听一组消息的接收，一旦收到某个src发送的消息，则立刻通过迭代器返回结果
        :parameter role_message_name_tuple_list: list[(role_name, message_name)]
        :param timeout:
        :return: iter[((role_name, message_name),obj)]
        """
        pass
