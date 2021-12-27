import copy
from typing import DefaultDict, List, Tuple, Optional, Generator
from fedprototype.typing import MessageName, MessageObj, Receiver, Sender, \
    RoleName, RoleNamePrefix, Comm, SubMessageSpaceName
from abc import ABC, abstractmethod
from collections import defaultdict

MESSAGE_BUFFER = DefaultDict[Receiver, List[Tuple[MessageName, MessageObj]]]


class BaseComm(ABC):
    def __init__(self):
        self._message_buffer: MESSAGE_BUFFER = defaultdict(lambda: [])

    def send(self, receiver: Receiver, message_name: MessageName, message_obj: MessageObj, flush: bool = True) -> None:
        message_obj = copy.deepcopy(message_obj)
        if flush:
            if receiver in self._message_buffer:
                _message_package = self._message_buffer.pop(receiver)
            else:
                _message_package = []
            _message_package.append((message_name, message_obj))
            self._send(receiver, _message_package)
        else:
            self._message_buffer[receiver].append((message_name, message_obj))

    def flush(self, receiver: Optional[Receiver] = None) -> None:
        if receiver is None:
            for receiver in list(self._message_buffer.keys()):
                self.flush(receiver)
        else:
            _message_package = self._message_buffer.pop(receiver)
            self._send(receiver, _message_package)

    @abstractmethod
    def receive(self, sender: Sender, message_name: MessageName, timeout: Optional[int] = None) -> MessageObj:
        pass

    def watch(self,
              sender_prefix: RoleNamePrefix,
              message_name: MessageName,
              timeout: Optional[int] = None
              ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        sender_list = self.get_role_name_list(sender_prefix)
        sender_message_name_tuple_list = [(sender, message_name) for sender in sender_list]
        return self.watch_(sender_message_name_tuple_list, timeout)

    @abstractmethod
    def watch_(self,
               sender_message_name_tuple_list: List[Tuple[Sender, MessageName]],
               timeout: Optional[int] = None
               ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        pass

    @abstractmethod
    def get_role_name_list(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        pass

    @abstractmethod
    def clear(self, sender: Sender = None, message_name: MessageName = None) -> None:
        pass

    @abstractmethod
    def _sub_comm(self, sub_message_space_name: SubMessageSpaceName) -> Comm:
        pass

    @abstractmethod
    def _send(self, receiver: Receiver, message_package: List[Tuple[MessageName, MessageObj]]) -> None:
        pass
