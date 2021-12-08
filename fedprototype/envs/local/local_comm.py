import typing as T

from copy import deepcopy
from collections import Counter
from threading import Lock

from tools.log import LoggerFactory
from fedprototype.envs.base_comm import BaseComm
from fedprototype.envs.local.message_hub import (
    DeepCopyQueue,
    MessageHub,
    WatchMessageQueue,
)


class LocalComm(BaseComm):
    def __init__(
        self,
        role_name: str,
        other_role_name_set: T.Set[str],
        msg_hub: MessageHub,
        serial_lock: Lock,
    ):
        super().__init__()
        self.role_name = role_name
        self.other_role_name_set = other_role_name_set
        self.msg_hub = msg_hub
        self.serial_lock = serial_lock

        self.logger = LoggerFactory.get_logger(f"{role_name} [{LocalComm.__name__}]")

    def send_(
        self, receiver: str, message_name_obj_list: T.List[T.Tuple[str, T.Any]]
    ) -> None:
        for message_name, obj in message_name_obj_list:
            self._put_message(receiver, message_name, obj)

    def _put_message(self, receiver: str, message_name: str, obj: T.Any) -> None:
        assert (
            receiver in self.other_role_name_set
        ), f"Error: unknown receiver: {receiver}"

        message_id = self._get_message_id(self.role_name, receiver, message_name)

        if receiver not in self.msg_hub.watch_index_dict:
            self.msg_hub.index_dict[message_id].put(obj)
            self.logger.debug(
                f"{receiver} now is not watching, put data into index_dict"
            )
        else:
            watch_message_queue = self.msg_hub.watch_index_dict[receiver]
            counter_id = (self.role_name, message_name)
            if counter_id not in watch_message_queue.counter:
                self.msg_hub.index_dict[message_id].put(obj)
                self.logger.debug(
                    f"({self.role_name}, {message_name}) is not watched by {receiver}, "
                    f"put data into index_dict"
                )
            else:
                watch_message_queue.queue.put((self.role_name, message_name, obj))
                self._modify_count_dict(watch_message_queue.counter, counter_id)
                self.logger.debug(
                    f"({self.role_name}, {message_name}) is watched by {receiver}, "
                    f"put data into watch_index_dict"
                )

    def receive(
        self, sender: str, message_name: str, timeout: T.Optional[int] = None
    ) -> T.Any:
        assert sender in self.other_role_name_set, f"Error: unknown sender: {sender}"

        message_id = self._get_message_id(sender, self.role_name, message_name)
        message_queue = self.msg_hub.index_dict[message_id]
        return self._get_message(message_queue, timeout)

    def watch_(
        self,
        sender_message_name_tuple_list: T.List[T.Tuple[str, str]],
        timeout: int = -1,
    ) -> T.Generator[T.Tuple[str, str, T.Any], None, None]:
        sender_msg_count_dict = dict(Counter(sender_message_name_tuple_list))
        watch_msg_queue = WatchMessageQueue(sender_msg_count_dict)

        for sender, message_name in sender_message_name_tuple_list:
            message_id = self._get_message_id(sender, self.role_name, message_name)
            message_queue = self.msg_hub.index_dict[message_id]
            if not message_queue.empty():
                data = self._get_message(message_queue, timeout)
                self._modify_count_dict(watch_msg_queue.counter, (sender, message_name))
                yield sender, message_name, data

        if len(sender_msg_count_dict):
            self.msg_hub.watch_index_dict[self.role_name] = watch_msg_queue
            count_dict = deepcopy(watch_msg_queue.counter)
            while len(count_dict):
                sender, message_name, data = self._get_message(
                    watch_msg_queue.queue, timeout
                )
                self._modify_count_dict(count_dict, (sender, message_name))
                yield sender, message_name, data
            del self.msg_hub.watch_index_dict[self.role_name]

    def get_role_name_list(self, role_name_prefix: str) -> T.List[str]:
        return [
            role_name
            for role_name in self.other_role_name_set
            if role_name.startswith(role_name_prefix)
        ]

    def _get_message(
        self, message_queue: DeepCopyQueue, timeout: T.Optional[int]
    ) -> T.Any:
        if message_queue.empty():
            self.logger.debug(
                f"Wanna get message, but queue is empty, release serial lock"
            )
            self.serial_lock.release()
            msg = message_queue.get(timeout=timeout)
            self.serial_lock.acquire()
            self.logger.debug(f"Get message, acquire serial lock")
        else:
            msg = message_queue.get()
            self.logger.debug(f"Wanna get message, queue is not empty, get message")
        return msg

    @staticmethod
    def _get_message_id(
        sender: str, receiver: str, message_name: str
    ) -> T.Tuple[str, str, str]:
        return sender, receiver, message_name

    @staticmethod
    def _modify_count_dict(
        count_dict: T.Dict[T.Tuple[str, str], int], key: T.Tuple[str, str]
    ) -> None:
        count_dict[key] -= 1
        if not count_dict[key]:
            del count_dict[key]
