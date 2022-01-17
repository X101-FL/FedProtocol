import time
from typing import List, Optional, Tuple, Any, Generator

import requests

from fedprototype.base.base_comm import BaseComm

import pickle

from fedprototype.typing import MessageSpace, Comm, Sender, MessageName, RoleName, RoleNamePrefix


class TCPComm(BaseComm):
    def __init__(self, role_name, local_url, other_role_name_set):
        super().__init__()
        self.role_name = role_name
        self.local_url = local_url
        self.other_role_name_set = other_role_name_set
        # TODO: Add logger
        self.logger = None
        self.message_space = None

        self.put_url = self.local_url + '/message_sender'
        self.get_url = self.local_url + '/get_responder'
        self.clear_url = self.local_url + '/clear'
        self.watch_url = self.local_url + '/watch'

    def _send(self, receiver: str, message_name_obj_list: List[Tuple[str, Any]]) -> None:
        requests.post(self.put_url,
                      files={'message_bytes': pickle.dumps(message_name_obj_list)},
                      headers={'sender': self.role_name,
                               'message-space': self.message_space,
                               'receiver': receiver})

    def receive(self, sender, message_name, timeout=-1):
        r = requests.get(self.get_url, headers={'sender': sender,
                                                'message-space': self.message_space,
                                                'message-name': message_name})
        return r.content

    def watch_(self, sender_message_name_tuple_list: List[Tuple[str, str]], timeout: Optional[int] = None) -> \
            Generator[Tuple[str, str, Any], None, None]:
        start_time = time.time()
        while sender_message_name_tuple_list:
            current_time = time.time()
            if timeout and (current_time - start_time > timeout):
                # TODO: print改成logger
                print("TIMEOUT")
                print("The following message was not processed:")
                for sender, message_name in sender_message_name_tuple_list:
                    print(f"Sender: {sender}  Message Name: {message_name}")
                break
            for sender, message_name in sender_message_name_tuple_list:
                r = requests.get(self.watch_url, headers={'sender': sender,
                                                          'message-space': self.message_space,
                                                          'message-name': message_name})
                if r.status_code == 404:
                    continue
                else:
                    sender_message_name_tuple_list.remove((sender, message_name))
                    yield r.content

    def clear(self, sender: Optional[Sender] = None, message_name: Optional[MessageName] = None) -> None:
        r = requests.post(self.clear_url,
                          headers={'sender': sender,
                                   'message-space': self.message_space,
                                   'message-name': message_name})

    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]

    def _sub_comm(self, message_space: MessageSpace) -> Comm:
        print(f'IN SUB COMM {self.role_name}')
        comm = TCPComm(self.role_name,
                       self.local_url,
                       self.other_role_name_set)
        comm.message_space = message_space
        return comm
