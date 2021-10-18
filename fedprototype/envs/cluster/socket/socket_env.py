from fedprototype.envs.cluster.socket.socket_comm import SocketComm
from fedprototype.envs.base_env import BaseEnv
from collections import defaultdict


class SocketEnv(BaseEnv):
    def __init__(self):
        self.role_name_ip_dict = defaultdict(lambda: [])

    def add_client(self, role_name, ip, port):
        self.role_name_ip_dict[role_name].append((ip, port))
        return self

    def run(self, client_class, client_params, role_name, role_index=0, **run_kwargs):
        client = self._get_client(client_class, role_name, role_index)
        client.init_client(client_params)
        return client.run(**run_kwargs)

    def _get_client(self, client_class, role_name, role_index):
        client = client_class()
        client.set_role(role_name, role_index)
        client.set_comm(self._get_comm(role_name, role_index))
        client.set_logger(self._get_logger(role_name, role_index))
        return client

    def _get_comm(self, role_name, role_index):
        comm = SocketComm(self.role_name_ip_dict, role_name, role_index)
        return comm

    def _get_logger(self, role_name, role_index):
        # logger_file_path = f"{ROOT_LOG_PATH}/{role_name}/{role_index}/{job_id}.log"
        # logger = ...
        # return logger
        pass
