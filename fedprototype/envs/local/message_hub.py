from typing import Tuple, Dict, DefaultDict, Generator, Optional, List
from fedprototype.typing import Sender, Receiver, MessageName, MessageID, MessageObj, SubMessageSpaceName

from collections import defaultdict
from queue import Queue
from collections import Counter


class WatchManager:
    def __init__(self, counter: Dict[Tuple[Sender, MessageName], int]):
        self._queue = Queue()
        self._counter = counter

    def empty(self) -> bool:
        return self._queue.empty()

    def get(self, timeout: Optional[int] = None) -> Tuple[Sender, MessageName, MessageObj]:
        return self._queue.get(timeout=timeout)

    def put(self, sender: Sender, message_name: MessageName, message_obj: MessageObj) -> None:
        assert self.is_desired_message(sender, message_name), \
            f"sender={sender},message_name={message_name} isn't be watched"
        self._queue.put((sender, message_name, message_obj))
        self._deduct_counter(sender, message_name)

    def is_desired_message(self, sender: Sender, message_name: MessageName) -> bool:
        return (sender, message_name) in self._counter

    def is_all_got(self) -> bool:
        return self._queue.empty() and (not len(self._counter))

    def _deduct_counter(self, sender: Sender, message_name: MessageName) -> None:
        count_key = (sender, message_name)
        self._counter[count_key] -= 1
        if not self._counter[count_key]:
            del self._counter[count_key]


class MessageHub:
    def __init__(self):
        self._message_queue_dict: DefaultDict[MessageID, Queue] = defaultdict(Queue)
        self._watch_queue_dict: Dict[Receiver, WatchManager] = {}
        self._sub_message_hub_dict: Dict[SubMessageSpaceName, 'MessageHub'] = {}

    def lookup_message_queues(self,
                              sender: Sender = None,
                              receiver: Receiver = None,
                              message_name: MessageName = None
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
                       sender_message_name_tuple_list: List[Tuple[Sender, MessageName]]) -> WatchManager:
        sender_msg_counter = dict(Counter(sender_message_name_tuple_list))
        watch_manager = WatchManager(sender_msg_counter)
        for sender, message_name in sender_message_name_tuple_list:  # 把已经接收到的消息移入watch队列
            message_queue = self.get_message_queue(sender, receiver, message_name)
            if not message_queue.empty():
                message_obj = message_queue.get()
                watch_manager.put(sender, message_name, message_obj)

        self._watch_queue_dict[receiver] = watch_manager
        return watch_manager

    def cancel_watch(self, receiver: Receiver) -> None:
        del self._watch_queue_dict[receiver]

    def get_sub_message_hub(self, sub_message_space_name: SubMessageSpaceName) -> 'MessageHub':
        if sub_message_space_name is None:
            return self
        else:
            if sub_message_space_name not in self._sub_message_hub_dict:
                self._sub_message_hub_dict[sub_message_space_name] = MessageHub()
            return self._sub_message_hub_dict[sub_message_space_name]
