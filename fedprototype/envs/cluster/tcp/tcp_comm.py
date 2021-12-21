import time
import typing as T

import requests

from fedprototype.envs.base_comm import BaseComm
from tools.log import LoggerFactory


class TCPComm(BaseComm):
    def __init__(self, role_name_ip_dict, role_name):
        super().__init__()
        # role_name_ip_dict的是{receiver:ip/port}
        self.role_name_ip_dict = role_name_ip_dict
        self.role_name = role_name
        self.local_url = "127.0.0.1"
        self.logger = LoggerFactory.get_logger(f"{role_name} [{TCPComm.__name__}]")

    def _send(self, receiver: str, message_name_obj_list: T.List[T.Tuple[str, T.Any]]) -> None:
        for message_name, obj in message_name_obj_list:
            self._put_message(receiver, message_name, obj)

    def _put_message(self, receiver: str, message_name: str, obj: T.Any) -> None:
        local_url = self.local_url + '/message_sender'
        target_url = self.role_name_ip_dict[receiver]
        r = requests.post(local_url, files=obj, headers={'sender': self.role_name,
                                                         'target_url': target_url,
                                                         'message_name': message_name})
        self.logger.debug(
            f"Requests now is {r.text}"
        )

    def receive(self, sender, message_name, timeout=-1):
        if timeout == -1:
            timeout = 1000
        local_url = self.local_url + '/get_responder'
        count = 0
        r = requests.get(local_url, headers={'sender': self.role_name,
                                             'message_name': message_name})
        while r.text == '404':
            time.sleep(1)
            r = requests.get(local_url, headers={'sender': self.role_name,
                                                 'message_name': message_name})
            count += 1
            if count > timeout:
                return '404'
        return r.content

    # TODO:Next step
    def watch_(self, sender_message_name_tuple_list: T.List[T.Tuple[str, str]], timeout: T.Optional[int] = None) -> \
            T.Generator[T.Tuple[str, str, T.Any], None, None]:
        pass

    def get_role_name_list(self, role_name_prefix: str) -> T.List[str]:
        pass
