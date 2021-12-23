from logging import Logger
from multiprocessing import Process

from fedprototype import BaseClient
from fedprototype.envs.base_env import BaseEnv
from collections import defaultdict

from tools.log import LoggerFactory
from .tcp_comm import TCPComm


class TCPEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.role_name_ip_dict = defaultdict(lambda: [])

    def add_client(self, role_name, ip, port):
        self.role_name_ip_dict[role_name].append((ip, port))
        return self

    def run(self, client: BaseClient, **run_kwargs):
        self._set_client(client)
        client.init()
        # TODO: 加个本机ip 和 port
        print(self.role_name_ip_dict[client.role_name][0])
        local_ip, local_port = self.role_name_ip_dict[client.role_name][0]
        # TODO: 不知道为什么不能run
        client_comm = Process(target=client.comm.run, name=client.role_name, args=(local_ip, local_port,))
        client_comm.start()
        ans = client.run(**run_kwargs)
        client.close()
        client_comm.terminate()
        return ans

    def _set_client(self, client):
        client.set_comm(self._get_comm(client.role_name))
        client.set_logger(self._get_logger(client))
        return client

    def _get_comm(self, role_name):
        comm = TCPComm(self.role_name_ip_dict, role_name)
        return comm

    @classmethod
    def _get_logger(cls, client: BaseClient) -> Logger:
        pass