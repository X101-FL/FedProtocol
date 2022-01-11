import time
from typing import List, Optional, Tuple, Any, Generator
import json

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

        self.put_url = self.local_url + '/message_sender'
        self.get_url = self.local_url + '/get_responder'

    def _send(self, receiver: str, message_name_obj_list: List[Tuple[str, Any]]) -> None:
        requests.post(self.put_url,
                      files={'message_bytes': pickle.dumps(message_name_obj_list)},
                      headers={'receiver': receiver})

    def receive(self, sender, message_name, timeout=-1):
        r = requests.get(self.get_url, headers={'sender': sender, 'message-name': message_name})
        return r.json()

    # TODO: 添加watch函数
    def watch_(self, sender_message_name_tuple_list: List[Tuple[str, str]], timeout: Optional[int] = None) -> \
            Generator[Tuple[str, str, Any], None, None]:
        pass

    def clean(self, sender: str, receiver: str, message_name: str) -> None:
        pass

    def get_role_name_list(self, role_name_prefix: str) -> List[str]:
        pass

    def _sub_comm(self, message_space: MessageSpace) -> Comm:
        pass

    def clear(self, sender: Optional[Sender] = None, message_name: Optional[MessageName] = None) -> None:
        pass

    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]
