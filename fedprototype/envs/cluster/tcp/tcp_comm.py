import time
import typing as T

import requests

from fedprototype.envs.base_comm import BaseComm
from tools.log import LoggerFactory
import pickle


class TCPComm(BaseComm):
    def __init__(self, role_name, local_url, other_role_name_set):
        super().__init__()
        self.role_name = role_name
        self.local_url = local_url
        self.other_role_name_set = other_role_name_set
        self.logger = LoggerFactory.get_logger(f"{role_name} [{TCPComm.__name__}]")

        self.put_url = self.local_url + '/message_sender'

    def _send(self, receiver: str, message_name_obj_list: T.List[T.Tuple[str, T.Any]]) -> None:
        for message_name, obj in message_name_obj_list:
            self._put_message(receiver, message_name, obj)

    def _put_message(self, receiver: str, message_name: str, obj: T.Any) -> None:
        r = requests.post(self.put_url,
                          files=pickle.dumps(obj),
                          headers={'sender': self.role_name,
                                   'receiver': receiver,
                                   'message_name': message_name})
        self.logger.debug(f"Requests now is {r.text}")

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

    def clean(self, sender: str, receiver: str, message_name: str) -> None:
        pass

    def get_role_name_list(self, role_name_prefix: str) -> T.List[str]:
        pass
