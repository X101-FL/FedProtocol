import pickle
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from requests.exceptions import RequestException

from fedprotocol.base.base_comm import BaseComm
from fedprotocol.typing import (
    Comm,
    MessageName,
    MessageObj,
    MessageSpace,
    ProtocolName,
    Receiver,
    RoleName,
    RoleNamePrefix,
    RootRoleName,
    Sender,
    SubRoleName,
    UpperRoleName,
    Url,
)


class TCPComm(BaseComm):
    def __init__(self,
                 message_space: MessageSpace,
                 role_name: RoleName,
                 server_url: Url,
                 root_role_bind_mapping: Dict[RoleName, RootRoleName]):
        super().__init__()
        self.message_space = message_space
        self.role_name = role_name
        self.server_url = server_url
        self.root_role_bind_mapping = root_role_bind_mapping
        self.other_role_name_set = set(root_role_bind_mapping.keys()) - {role_name}

    def _active(self) -> None:
        self._post(path="set_message_space",
                   json={"message_space": self.message_space,
                         "root_role_bind_mapping":  self.root_role_bind_mapping})

    def _send(self, receiver: Receiver, message_package: List[Tuple[MessageName, MessageObj]]) -> None:
        self._assert_role_name(receiver=receiver)

        message_package_bytes = [(message_name, pickle.dumps(message_obj)) for
                                 message_name, message_obj in message_package]

        self._post(path="send",
                   data={'message_space': self.message_space,
                         'sender': self.role_name,
                         'receiver': receiver},
                   files={'message_package_bytes': pickle.dumps(message_package_bytes)})

    def receive(self, sender: Sender, message_name: MessageName, timeout: Optional[int] = None) -> MessageObj:
        self._assert_role_name(sender=sender)

        return self._post(path="receive",
                          json={'message_space': self.message_space,
                                'sender': sender,
                                'receiver': self.role_name,
                                'message_name': message_name},
                          timeout=timeout)

    def watch_(self,
               sender_message_name_tuple_list: List[Tuple[Sender, MessageName]],
               timeout: Optional[int] = None
               ) -> Generator[Tuple[Sender, MessageName, MessageObj], None, None]:
        self._post(path="regist_watch",
                   json={'message_space': self.message_space,
                         'receiver': self.role_name,
                         'sender_message_name_tuple_list': sender_message_name_tuple_list})

        watch_res = {'finished': False}
        while not watch_res['finished']:
            watch_res = self._post(path="fetch_watch",
                                   json={'message_space': self.message_space,
                                         'receiver': self.role_name})
            for sender, message_name, message_bytes in watch_res['data']:
                yield sender, message_name, pickle.loads(message_bytes)

    def clear(self, sender: Optional[Sender] = None, message_name: Optional[MessageName] = None) -> None:
        _res = self._post(path="clear",
                          json={'message_space': self.message_space,
                                'sender': sender,
                                'receiver': self.role_name,
                                'message_name': message_name})
        self.logger.debug(f"droped message size : {_res['drop_size']}")

    def list_role_name(self, role_name_prefix: RoleNamePrefix) -> List[RoleName]:
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]

    def _sub_comm(self,
                  protocol_name: ProtocolName,
                  role_name: RoleName,
                  role_bind_mapping: Optional[Dict[SubRoleName, UpperRoleName]] = None
                  ) -> Comm:
        if role_bind_mapping is None:
            assert self.role_name == role_name, \
                f"upper_role_name={self.role_name} should be equal to sub_role_name={role_name}, " \
                f"if role_bind_mapping is not specified"
            sub_root_role_bind_mapping = self.root_role_bind_mapping
        else:
            sub_root_role_bind_mapping = {sub_role_name: self.root_role_bind_mapping[upper_role_name] for
                                          sub_role_name, upper_role_name in role_bind_mapping.items()}
        return TCPComm(message_space=f"{self.message_space}.{protocol_name}",
                       role_name=role_name,
                       server_url=self.server_url,
                       root_role_bind_mapping=sub_root_role_bind_mapping)

    def _post(self, path, **kwargs) -> Any:
        res = requests.post(url=f"{self.server_url}/{path}", **kwargs)
        if res.status_code != 200:
            self.logger.error(f"falied to post : {res.url}\n{res.text}")
            raise RequestException(request=res.request,
                                   response=res)
        content_type = res.headers.get('content-type', None)
        if content_type is None:
            return pickle.loads(res.content)
        elif 'json' in content_type:
            return res.json()
        elif 'text' in content_type:
            return res.text
        else:
            raise Exception(f"unknown content type : {content_type}")

    def _assert_role_name(self, sender: Optional[Sender] = None, receiver: Optional[Receiver] = None):
        assert (not sender) or (sender in self.other_role_name_set),\
            f"unknown sender : {sender}, acceptable senders are :  {self.other_role_name_set}"
        assert (not receiver) or (receiver in self.other_role_name_set),\
            f"unknown receiver : {receiver}, acceptable receivers are :  {self.other_role_name_set}"
