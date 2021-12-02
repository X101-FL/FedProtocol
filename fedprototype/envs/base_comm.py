from abc import ABC, abstractmethod
from collections import defaultdict


class BaseComm(ABC):
    def __init__(self):
        self._message_buffer = defaultdict(lambda: [])

    def send(self, receiver, message_name, obj, flush=True):
        if flush:
            if receiver in self._message_buffer:
                _message_package = self._message_buffer.pop(receiver)
            else:
                _message_package = []
            _message_package.append((message_name, obj))
            self.send_(receiver, _message_package)
        else:
            self._message_buffer[receiver].append((message_name, obj))

    @abstractmethod
    def send_(self, receiver, message_name_obj_list):
        pass

    def flush(self, receiver=None):
        if receiver is None:
            for receiver in list(self._message_buffer.keys()):
                self.flush(receiver)
        else:
            _message_package = self._message_buffer.pop(receiver)
            self.send_(receiver, _message_package)

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
