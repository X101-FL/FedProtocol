from abc import ABC, abstractmethod


class BaseComm(ABC):

    def send(self, receiver, message_name, obj, cache=False):
        if cache:
            self._send_cache(receiver, message_name, obj)
        else:
            self._send(receiver, message_name, obj)

    @abstractmethod
    def _send(self, receiver, message_name, obj):
        pass

    @abstractmethod
    def _send_cache(self, receiver, message_name, obj):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def receive(self, sender, message_name, timeout=-1):
        pass

    def watch(self, sender_prefix, message_name, timeout=-1):
        sender_list = self.get_role_name_list(sender_prefix)
        sender_message_name_tuple_list = [(sender, message_name) for sender in sender_list]
        return self.watch_(sender_message_name_tuple_list, timeout)

    @abstractmethod
    def watch_(self, sender_message_name_tuple_list, timeout=-1):
        pass

    @abstractmethod
    def get_role_name_list(self, role_name_prefix):
        pass
