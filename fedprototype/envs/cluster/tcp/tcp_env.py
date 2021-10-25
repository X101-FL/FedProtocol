from fedprototype.envs.base_env import BaseEnv
from collections import defaultdict
from .tcp_comm import TCPComm


class TCPEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.role_name_ip_dict = defaultdict(lambda: [])

    def add_client(self, role_name, ip, port):
        self.role_name_ip_dict[role_name].append((ip, port))
        return self

    def run(self, client, **run_kwargs):
        self._set_client(client)
        client.init()
        ans = client.run(**run_kwargs)
        client.close()
        return ans

    def _set_client(self, client):
        client.set_comm(self._get_comm(client.role_name))
        client.set_logger(self._get_logger(client.role_name))
        return client

    def _get_comm(self, role_name):
        comm = TCPComm(self.role_name_ip_dict, role_name)
        return comm

    def _get_logger(self, role_name, role_index):
        # logger_file_path = f"{ROOT_LOG_PATH}/{role_name}/{role_index}/{job_id}.log"
        # logger = ...
        # return logger
        pass
