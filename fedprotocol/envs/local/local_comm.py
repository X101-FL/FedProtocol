from queue import Queue
from threading import Lock
from typing import Dict, Generator, List, Optional, Set, Tuple, Union

from fedprotocol.base.base_comm import BaseComm
from fedprotocol.envs.local.local_message_hub import MessageHub, WatchManager
from fedprotocol.typing import (
    Comm,
    MessageName,
    MessageObj,
    MessageSpace,
    ProtocolName,
    Receiver,
    RoleName,
    RoleNamePrefix,
    Sender,
    SubRoleName,
    UpperRoleName,
)


class LocalComm(BaseComm):
    def __init__(self,
                 message_space: MessageSpace,
                 role_name: RoleName,
                 other_role_name_set: Set[RoleName],
                 msg_hub: MessageHub,
                 serial_lock: Lock):
        super().__init__()
        self.message_space = message_space
        self.role_name = role_name
        self.other_role_name_set = other_role_name_set
        self.msg_hub = msg_hub
        self.serial_lock = serial_lock

        self.message_space_manager = msg_hub.get_message_space_manager(message_space)

    def _send(self,
              receiver: Receiver,
              message_package: List[Tuple[MessageName, MessageObj]]
              ) -> None:
        self._assert_role_name(receiver=receiver)
        for message_name, message_obj in message_package:
            self.logger.debug(f"put a message : message_space={self.message_space}, sender={self.role_name}, "
                              f"receiver={receiver}, message_name={message_name}")
            self.message_space_manager \
                .put(self.role_name, receiver, message_name, message_obj)

    def receive(self, sender: Sender, message_name: MessageName, timeout: Optional[int] = None) -> MessageObj:
        self._assert_role_name(sender=sender)
        self.logger.debug(f"get a message : message_space={self.message_space}, sender={sender}, "
                          f"receiver={self.role_name}, message_name={message_name}")
        message_queue = self.message_space_manager \
                            .get_message_queue(sender, self.role_name, message_name)
        return self._get_message(message_queue, timeout)

    def watch_(self,
               sender_message_name_tuple_list: List[Tuple[Sender, MessageName]],
               timeout: Optional[int] = None
               ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        self.logger.debug(f"register watch : message_space={self.message_space}, receiver={self.role_name}")
        watch_manager = self.message_space_manager \
                            .register_watch(self.role_name, sender_message_name_tuple_list)
        while not watch_manager.is_all_got():
            yield self._get_message(watch_manager, timeout)
        self.message_space_manager.cancel_watch(self.role_name)

    def clear(self,
              sender: Optional[Sender] = None,
              message_name: Optional[MessageName] = None
              ) -> None:
        self.logger.debug(f"clear cached messages : message_space={self.message_space}, sender={sender}, "
                          f"receiver={self.role_name}, message_name={message_name}")
        drop_size = 0
        for message_id, message_queue in self.message_space_manager \
                .lookup_message_queues(sender, self.role_name, message_name):
            while not message_queue.empty():
                drop_size += 1
                self.logger.debug(f"drop message <{drop_size}> : {message_id}")
                message_queue.get()

    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]

    def _sub_comm(self, protocol_name: ProtocolName, role_name: RoleName, role_bind_mapping: Optional[Dict[SubRoleName, UpperRoleName]] = None) -> Comm:
        if role_bind_mapping is None:
            assert self.role_name == role_name, \
                f"upper_role_name={self.role_name} should be equal to sub_role_name={role_name}, " \
                f"if role_bind_mapping is not specified"
            sub_other_role_name_set = self.other_role_name_set
        else:
            sub_other_role_name_set = set(role_bind_mapping.keys()) - {role_name}
        return LocalComm(message_space=f"{self.message_space}.{protocol_name}",
                         role_name=role_name,
                         other_role_name_set=sub_other_role_name_set,
                         msg_hub=self.msg_hub,
                         serial_lock=self.serial_lock)

    def _get_message(self,
                     message_queue: Union[Queue, WatchManager],
                     timeout: Optional[int]
                     ) -> Union[MessageObj, Tuple[Sender, MessageName, MessageObj]]:
        if message_queue.empty():
            self.serial_lock.release()
            msg = message_queue.get(timeout=timeout)
            self.serial_lock.acquire()
        else:
            msg = message_queue.get()
        return msg

    def _assert_role_name(self, sender: Optional[Sender] = None, receiver: Optional[Receiver] = None):
        assert (not sender) or (sender in self.other_role_name_set),\
            f"unknown sender : {sender}, acceptable senders are :  {self.other_role_name_set}"
        assert (not receiver) or (receiver in self.other_role_name_set),\
            f"unknown receiver : {receiver}, acceptable receivers are :  {self.other_role_name_set}"
