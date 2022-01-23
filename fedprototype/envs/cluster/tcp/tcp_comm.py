import pickle
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from requests.exceptions import RequestException

from fedprototype.base.base_comm import BaseComm
from fedprototype.typing import (
    Comm,
    MessageName,
    MessageObj,
    MessageSpace,
    Receiver,
    RoleName,
    RoleNamePrefix,
    Sender,
    SubRoleName,
    UpperRoleName,
    Url,
)


class TCPComm(BaseComm):
    def __init__(self,
                 role_name: RoleName,
                 role_name_url_dict: Dict[RoleName, Url],
                 message_space: MessageSpace = 'main'):
        super().__init__()
        self.role_name = role_name
        self.local_url = role_name_url_dict[role_name]
        self.role_name_url_dict = role_name_url_dict
        self.other_role_name_set = set(role_name_url_dict.keys()) - {role_name}
        self.message_space = message_space

        self._send_url = f"{self.local_url}/send"
        self._receive_url = f"{self.local_url}/receive"
        self._clear_url = f"{self.local_url}/clear"
        self._watch_url = f"{self.local_url}/watch"

    def _send(self, receiver: Receiver, message_package: List[Tuple[MessageName, MessageObj]]) -> None:
        self._assert_role_name(receiver=receiver)

        self._post(url=self._send_url,
                   data={'sender': self.role_name,
                         'message_space': self.message_space,
                         'receiver': receiver,
                         'target_url': self.role_name_url_dict[receiver]},
                   files={'message_bytes': pickle.dumps(message_package)})

    def receive(self, sender: Sender, message_name: MessageName, timeout: Optional[int] = None) -> MessageObj:
        self._assert_role_name(sender=sender)

        return self._post(url=self._receive_url,
                          json={'sender': sender,
                                'message_space': self.message_space,
                                'message_name': message_name,
                                'target_url': self.role_name_url_dict[sender]},
                          timeout=timeout)

    def watch_(self, sender_message_name_tuple_list: List[Tuple[Sender, MessageName]], timeout: Optional[int] = None) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        pass

    # def watch_(self, sender_message_name_tuple_list: List[Tuple[str, str]], timeout: Optional[int] = None) -> \
    #         Generator[Tuple[str, str, Any], None, None]:
    #     start_time = time.time()
    #     while sender_message_name_tuple_list:
    #         current_time = time.time()
    #         if timeout and (current_time - start_time > timeout):
    #             # TODO: print改成logger
    #             print("TIMEOUT")
    #             print("Following message was not processed:")
    #             for sender, message_name in sender_message_name_tuple_list:
    #                 print(f"Sender: {sender}  Message Name: {message_name}")
    #             break
    #         for sender, message_name in sender_message_name_tuple_list:
    #             r = requests.get(self._watch_url, headers={'sender': sender,
    #                                                        'message-space': self.message_space,
    #                                                        'message-name': message_name})
    #             if r.json()['status_code'] == 404:
    #                 continue
    #             else:
    #                 sender_message_name_tuple_list.remove(
    #                     (sender, message_name))
    #                 yield r.content

    def clear(self, sender: Optional[Sender] = None, message_name: Optional[MessageName] = None) -> None:
        pass
        # print("-------", sender, message_name)
        # r = requests.post(self._clear_url,
        #                   headers={'sender': sender,
        #                            'message-space': self.message_space,
        #                            'message-name': message_name})

    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]

    def _sub_comm(self, message_space: Optional[MessageSpace] = None, role_rename_dict: Optional[Dict[SubRoleName, UpperRoleName]] = None) -> Comm:
        pass
        # comm = TCPComm(self.role_name,
        #                self.local_url,
        #                self.other_role_name_set)
        # comm.message_space = message_space
        # return comm

    # def set_target_server(self, role_rename_dict):
    #     if not self.target_server_dict:
    #         self.target_server_dict = role_rename_dict
    #     else:
    #         for k, v in self.target_server_dict:
    #             if v in role_rename_dict:
    #                 self.target_server_dict[k] = role_rename_dict[v]

    def _post(self, *args, **kwargs) -> Any:
        res = requests.post(*args, **kwargs)
        if res.status_code != 200:
            raise RequestException(request=res.request, response=res)
        if res.headers.get('content-type', None) == 'application/json':
            return res.json()
        return pickle.loads(res.content)

    def _assert_role_name(self, sender: Optional[Sender] = None, receiver: Optional[Receiver] = None):
        assert (not sender) or (sender in self.other_role_name_set),\
            f"unknown sender : {sender}, acceptable senders are :  {self.other_role_name_set}"
        assert (not receiver) or (receiver in self.other_role_name_set),\
            f"unknown receiver : {receiver}, acceptable receivers are :  {self.other_role_name_set}"
