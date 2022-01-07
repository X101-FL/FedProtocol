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

        self.put_url = self.local_url + '/message_sender'

    def _send(self, receiver: str, message_name_obj_list: List[Tuple[str, Any]]) -> None:
        for message_name, obj in message_name_obj_list:
            self._put_message(receiver, message_name, obj)

    def _put_message(self, receiver: str, message_name: str, obj: Any) -> None:
        r = requests.post(self.put_url,
                          files={'file': pickle.dumps(obj)},
                          headers={
                              'receiver': receiver,
                              'sender': self.role_name,
                              'message-name': message_name})
        # print("runner", r.content)
        print(f"post message: {r.json()}")
        print("------")
        # self.logger.debug(f"Requests now is {r.text}")

    def receive(self, sender, message_name, timeout=-1):
        # if timeout == -1:
        #     timeout = 1000
        # TODO: 打包成参数

        local_url = self.local_url + '/get_responder'
        r = requests.get(local_url, headers={'sender': sender,
                                             'message-name': message_name})

        # count = 0
        # r = requests.get(local_url, headers={'sender': sender,
        #                                      'message-name': message_name})
        # while r.status_code == 404:
        #     time.sleep(1)
        #     r = requests.get(local_url, headers={'sender': sender,
        #                                          'message-name': message_name})
        #     count += 1
        #     if count > timeout:
        #         return '404'
        return r.content

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
        pass
