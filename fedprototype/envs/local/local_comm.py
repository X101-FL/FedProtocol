from queue import Queue
from threading import Lock
from typing import Set, List, Tuple, Optional, Generator, Union

from fedprototype.envs.base_comm import BaseComm
from fedprototype.envs.local.message_hub import MessageHub, WatchManager
from fedprototype.typing import RoleName, Receiver, MessageName, MessageObj, Sender, RoleNamePrefix, \
    SubMessageSpaceName, Comm
from tools.log import LoggerFactory


class LocalComm(BaseComm):
    def __init__(self,
                 role_name: RoleName,
                 other_role_name_set: Set[RoleName],
                 msg_hub: MessageHub,
                 serial_lock: Lock):
        super().__init__()
        self.role_name = role_name
        self.other_role_name_set = other_role_name_set
        self.msg_hub = msg_hub
        self.serial_lock = serial_lock

        self.logger = LoggerFactory.get_logger(f"{role_name} [{LocalComm.__name__}]")

    def _send(self, receiver: Receiver, message_package: List[Tuple[MessageName, MessageObj]]) -> None:
        for message_name, message_obj in message_package:
            self._put_message(receiver, message_name, message_obj)

    def _put_message(self, receiver: Receiver, message_name: MessageName, message_obj: MessageObj) -> None:
        assert (receiver in self.other_role_name_set), f"Error: unknown receiver: {receiver}"

        if not self.msg_hub.is_watching(receiver):
            self.msg_hub.get_message_queue(self.role_name, receiver, message_name).put(message_obj)
            self.logger.debug(f"{receiver} now is not watching, put data into index_dict")
        else:
            watch_manager = self.msg_hub.get_watch_manager(receiver)
            if not watch_manager.is_desired_message(self.role_name, message_name):
                self.msg_hub.get_message_queue(self.role_name, receiver, message_name).put(message_obj)
                self.logger.debug(f"({self.role_name}, {message_name}) is not watched by {receiver}, "
                                  f"put data into index_dict")
            else:
                watch_manager.put(self.role_name, message_name, message_obj)
                self.logger.debug(f"({self.role_name}, {message_name}) is watched by {receiver}, "
                                  f"put data into watch_index_dict")

    def receive(self, sender: Sender, message_name: MessageName, timeout: Optional[int] = None) -> MessageObj:
        assert sender in self.other_role_name_set, f"Error: unknown sender: {sender}"

        message_queue = self.msg_hub.get_message_queue(sender, self.role_name, message_name)
        return self._get_message(message_queue, timeout)

    def watch_(self,
               sender_message_name_tuple_list: List[Tuple[Sender, MessageName]],
               timeout: Optional[int] = None
               ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        watch_manager = self.msg_hub.register_watch(self.role_name, sender_message_name_tuple_list)
        while not watch_manager.is_all_got():
            yield self._get_message(watch_manager, timeout)
        self.msg_hub.cancel_watch(self.role_name)

    def clear(self, sender: Sender = None, message_name: MessageName = None) -> None:
        for _, message_queue in self.msg_hub.lookup_message_queues(sender, self.role_name, message_name):
            while not message_queue.empty():
                message_queue.get()

    def get_role_name_list(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]

    def _sub_comm(self, sub_message_space_name: SubMessageSpaceName) -> Comm:
        return LocalComm(self.role_name,
                         self.other_role_name_set,
                         self.msg_hub.get_sub_message_hub(sub_message_space_name),
                         self.serial_lock)

    def _get_message(self,
                     message_queue: Union[Queue, WatchManager],
                     timeout: Optional[int]
                     ) -> Union[MessageObj, Tuple[Sender, MessageName, MessageObj]]:
        if message_queue.empty():
            self.logger.debug(f"Wanna get message, but queue is empty, release serial lock")
            self.serial_lock.release()
            msg = message_queue.get(timeout=timeout)
            self.serial_lock.acquire()
            self.logger.debug(f"Get message, acquire serial lock")
        else:
            msg = message_queue.get()
            self.logger.debug(f"Wanna get message, queue is not empty, get message")
        return msg
