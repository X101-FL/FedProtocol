import threading

from tools.log import LoggerFactory
from fedprototype.envs.base_env import BaseEnv
from fedprototype.envs.local.local_comm import LocalComm
from fedprototype.envs.local.client_thread import ClientThread
from fedprototype.envs.local.message_hub import MessageHub


class LocalEnv(BaseEnv):

    def __init__(self):
        self.serial_lock = threading.Lock()
        self.client_info_dict = {}
        self.logger = LoggerFactory.get_logger(role_name=LocalEnv.__name__)
        self.role_name_set = None
        self.msg_hub = MessageHub()

    def add_client(self, client, **run_kwargs):
        self.client_info_dict[client.role_name] = (client, run_kwargs)
        return self

    def run(self):
        thread_list = []

        self.role_name_set = set(self.client_info_dict.keys())

        for role_name, (client, run_kwargs) in self.client_info_dict.items():
            self._set_client(client)
            thread_list.append(ClientThread(self.serial_lock, client, run_kwargs))
            self.logger.debug(f"Initialize {role_name}")

        self.logger.debug(f"Main thread acquires lock...")
        self.serial_lock.acquire()
        for th in thread_list:
            th.start()
        self.serial_lock.release()
        self.logger.debug(f"Main thread releases lock...")

        for th in thread_list:
            th.join()

        self.logger.debug(f"All task done!!!")

    def _get_comm(self, client):
        return LocalComm(client.role_name, self.role_name_set, self.msg_hub, self.serial_lock, client.logger)

    @classmethod
    def _get_logger(cls, client):
        return LoggerFactory.get_logger(role_name=client.role_name)

    def _set_client(self, client):
        client.set_logger(self._get_logger(client))
        client.set_comm(self._get_comm(client))
