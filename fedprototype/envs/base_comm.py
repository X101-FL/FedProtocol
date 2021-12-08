from abc import ABC, abstractmethod
from collections import defaultdict
import typing as T

T_MESSAGE_BUFFER = T.DefaultDict[str, T.List[T.Tuple[str, T.Any]]]


class BaseComm(ABC):
    def __init__(self):
        self._message_buffer: T_MESSAGE_BUFFER = defaultdict(lambda: [])

    def send(
        self, receiver: str, message_name: str, obj: T.Any, flush: bool = True
    ) -> None:
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
    def send_(
        self, receiver: str, message_package: T.List[T.Tuple[str, T.Any]]
    ) -> None:
        pass

    def flush(self, receiver: T.Optional[str] = None) -> None:

        if receiver is None:
            for receiver in list(self._message_buffer.keys()):
                self.flush(receiver)
        else:
            _message_package = self._message_buffer.pop(receiver)
            self.send_(receiver, _message_package)

    @abstractmethod
    def receive(
        self, sender: str, message_name: str, timeout: T.Optional[int] = None
    ) -> T.Any:
        pass

    def watch(
        self, sender_prefix: str, message_name: str, timeout: T.Optional[int] = None
    ) -> T.Generator[T.Tuple[str, str, T.Any], None, None]:
        sender_list = self.get_role_name_list(sender_prefix)
        sender_message_name_tuple_list = [
            (sender, message_name) for sender in sender_list
        ]
        return self.watch_(sender_message_name_tuple_list, timeout)

    @abstractmethod
    def watch_(
        self,
        sender_message_name_tuple_list: T.List[T.Tuple[str, str]],
        timeout: T.Optional[int] = None,
    ) -> T.Generator[T.Tuple[str, str, T.Any], None, None]:
        pass

    @abstractmethod
    def get_role_name_list(self, role_name_prefix: str) -> T.List[str]:
        pass
