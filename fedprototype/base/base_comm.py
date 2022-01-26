import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import DefaultDict, Dict, Generator, List, Optional, Tuple

from fedprototype.typing import (
    Comm,
    Logger,
    MessageName,
    MessageObj,
    ProtocolName,
    Receiver,
    RoleName,
    RoleNamePrefix,
    Sender,
    SubRoleName,
    UpperRoleName,
)

_MESSAGE_BUFFER = DefaultDict[Receiver, List[Tuple[MessageName, MessageObj]]]


class BaseComm(ABC):

    def __init__(self):
        self._message_buffer: _MESSAGE_BUFFER = defaultdict(list)
        self.logger: Optional[Logger] = None

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
        sender_list = self.list_role_name(sender_prefix)
        sender_message_name_tuple_list = [
            (sender, message_name) for sender in sender_list]
        return self.watch_(sender_message_name_tuple_list, timeout)

    @abstractmethod
    def watch_(self,
               sender_message_name_tuple_list: List[Tuple[Sender, MessageName]],
               timeout: Optional[int] = None
               ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        pass

    @abstractmethod
    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        pass

    @abstractmethod
    def clear(self, sender: Optional[Sender] = None, message_name: Optional[MessageName] = None) -> None:
        pass

    @abstractmethod
    def _sub_comm(self,
                  protocol_name: ProtocolName,
                  role_name: RoleName,
                  role_name_mapping: Optional[Dict[SubRoleName, UpperRoleName]] = None
                  ) -> Comm:
        pass

    @abstractmethod
    def _send(self, receiver: Receiver, message_package: List[Tuple[MessageName, MessageObj]]) -> None:
        pass
