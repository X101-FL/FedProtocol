from collections import Counter, defaultdict
from queue import Queue
from threading import Lock
from typing import DefaultDict, Dict, Generator, List, Optional, Tuple

from fedprotocol.typing import (
    MessageBytes,
    MessageID,
    MessageName,
    MessageSpace,
    Receiver,
    RoleName,
    RootRoleName,
    Sender,
    Url,
)


class WatchManager:
    def __init__(self, counter: Dict[Tuple[Sender, MessageName], int]):
        self._queue = Queue()
        self._counter = counter

    def empty(self) -> bool:
        return self._queue.empty()

    def get(self, timeout: Optional[int] = None) -> Tuple[Sender, MessageName, MessageBytes]:
        return self._queue.get(timeout=timeout)

    def put(self, sender: Sender, message_name: MessageName, message_bytes: MessageBytes) -> None:
        assert self.is_desired_message(sender, message_name), \
            f"sender={sender},message_name={message_name} isn't be watched"
        self._queue.put((sender, message_name, message_bytes))
        self._deduct_counter(sender, message_name)

    def unreceived(self) -> List[Tuple[Sender, MessageName]]:
        return list(self._counter.keys())

    def is_desired_message(self, sender: Sender, message_name: MessageName) -> bool:
        return (sender, message_name) in self._counter

    def is_all_got(self) -> bool:
        return self._queue.empty() and (not len(self._counter))

    def _deduct_counter(self, sender: Sender, message_name: MessageName) -> None:
        count_key = (sender, message_name)
        self._counter[count_key] -= 1
        if not self._counter[count_key]:
            del self._counter[count_key]


class MessageSpaceManager:
    def __init__(self) -> None:
        self._root_role_bind_mapping: Dict[RoleName, RootRoleName] = {}
        self._message_queue_dict: DefaultDict[MessageID, Queue] = defaultdict(Queue)
        self._watch_queue_dict: Dict[Receiver, WatchManager] = {}
        self._access_lock = Lock()

    def lookup_message_queues(self,
                              sender: Optional[Sender] = None,
                              receiver: Optional[Receiver] = None,
                              message_name: Optional[MessageName] = None
                              ) -> Generator[Tuple[MessageID, Queue], None, None]:
        for message_id, message_queue in self._message_queue_dict.items():
            (_sender, _receiver, _message_name) = message_id
            if ((sender is None) or (sender == _sender)) \
                    and ((receiver is None) or (receiver == _receiver)) \
                    and (message_name is None) or (message_name == _message_name):
                yield message_id, message_queue

    def get_message_queue(self,
                          sender: Sender,
                          receiver: Receiver,
                          message_name: MessageName
                          ) -> Queue:
        return self._message_queue_dict[(sender, receiver, message_name)]

    def get_watch_manager(self, receiver: Receiver) -> WatchManager:
        return self._watch_queue_dict[receiver]

    def is_watching(self, receiver: Receiver) -> bool:
        return receiver in self._watch_queue_dict

    def register_watch(self,
                       receiver: Receiver,
                       sender_message_name_tuple_list: List[Tuple[Sender, MessageName]]
                       ) -> WatchManager:
        sender_msg_counter = dict(Counter(sender_message_name_tuple_list))
        watch_manager = WatchManager(sender_msg_counter)

        for sender, message_name in sender_message_name_tuple_list:  # 把已经接收到的消息移入watch队列
            message_queue = self.get_message_queue(sender, receiver, message_name)
            if not message_queue.empty():
                message_bytes = message_queue.get()
                watch_manager.put(sender, message_name, message_bytes)

        self._watch_queue_dict[receiver] = watch_manager
        return watch_manager

    def cancel_watch(self, receiver: Receiver) -> None:
        del self._watch_queue_dict[receiver]

    def put(self,
            sender: Sender,
            receiver: Receiver,
            message_name: MessageName,
            message_bytes: MessageBytes
            ) -> None:
        if not self.is_watching(receiver):
            self.get_message_queue(sender, receiver, message_name).put(message_bytes)
        else:
            watch_manager = self.get_watch_manager(receiver)
            if not watch_manager.is_desired_message(sender, message_name):
                self.get_message_queue(sender, receiver, message_name).put(message_bytes)
            else:
                watch_manager.put(sender, message_name, message_bytes)

    def set_role_bind_mapping(self, root_role_bind_mapping: Dict[RoleName, RootRoleName]) -> None:
        self._root_role_bind_mapping = root_role_bind_mapping

    def get_root_role_name(self, role_name: RoleName) -> Url:
        return self._root_role_bind_mapping[role_name]

    def __enter__(self) -> 'MessageSpaceManager':
        self._access_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._access_lock.release()


class MessageHub:
    def __init__(self):
        self._message_space_dict: DefaultDict[MessageSpace, MessageSpaceManager] = defaultdict(MessageSpaceManager)

    def get_message_space_manager(self, message_space: MessageSpace) -> MessageSpaceManager:
        return self._message_space_dict[message_space]
