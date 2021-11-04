from copy import deepcopy
from collections import Counter

from fedprototype.envs.base_comm import BaseComm
from fedprototype.envs.local.message_hub import WatchMessageQueue


class LocalComm(BaseComm):

    def __init__(self, role_name, role_name_set, msg_hub, serial_lock, logger):
        self.role_name = role_name
        self.role_name_set = role_name_set
        self.serial_lock = serial_lock
        self.msg_hub = msg_hub
        self.logger = logger

    def send(self, receiver, message_name, obj):
        assert receiver in self.role_name_set
        message_id = self._get_message_id(self.role_name, receiver, message_name)

        if receiver not in self.msg_hub.watch_index_dict:
            self.msg_hub.index_dict[message_id].put(obj)
            self.logger.debug(f"{receiver} now is not watching, just put data into index_dict")
        else:
            watch_message_queue = self.msg_hub.watch_index_dict[receiver]
            counter_id = (self.role_name, message_name)
            if counter_id not in watch_message_queue.counter:
                self.msg_hub.index_dict[message_id].put(obj)
                self.logger.debug(f"({self.role_name}, {message_name}) is not watched by {receiver}, "
                                  f"just put data into index_dict")
            else:
                watch_message_queue.queue.put((self.role_name, message_name, obj))
                self._modify_count_dict(watch_message_queue.counter, counter_id)
                self.logger.debug(f"({self.role_name}, {message_name}) is watched by {receiver}, "
                                  f"put data into watch_index_dict")

    def receive(self, sender, message_name, timeout=-1):
        assert sender in self.role_name_set
        message_id = self._get_message_id(sender, self.role_name, message_name)
        message_queue = self.msg_hub.index_dict[message_id]
        return self._get_message(message_queue, timeout)

    def watch_(self, sender_message_name_tuple_list, timeout=-1):
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
                sender, message_name, data = self._get_message(watch_msg_queue.queue, timeout)
                self._modify_count_dict(count_dict, (sender, message_name))
                yield sender, message_name, data
            del self.msg_hub.watch_index_dict[self.role_name]

    def get_role_name_list(self, role_name_prefix):
        return [role_name for role_name in self.role_name_set if role_name.startswith(role_name_prefix)]

    def _get_message(self, message_queue, timeout):
        if message_queue.empty():
            self.logger.debug(f"Wanna get message, but queue is empty, so release serial lock")
            self.serial_lock.release()
            msg = message_queue.get(timeout)
            self.serial_lock.acquire()
            self.logger.debug(f"Get message, acquire serial lock")
        else:
            msg = message_queue.get()
            self.logger.debug(f"Wanna get message, queue is not empty, simply get message")
        return msg

    @staticmethod
    def _get_message_id(sender, receiver, message_name):
        return sender, receiver, message_name

    @staticmethod
    def _modify_count_dict(count_dict, key):
        count_dict[key] -= 1
        if not count_dict[key]:
            del count_dict[key]
