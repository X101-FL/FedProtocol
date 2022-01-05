from logging import Logger
from multiprocessing import Process

from fedprototype import BaseClient
from fedprototype.base.base_env import BaseEnv
from fedprototype.envs.cluster.tcp.tcp_comm import TCPComm
from fedprototype.envs.cluster.tcp.tcp_http_server import start_server


class TCPEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.role_name_ip_dict = {}
        self.role_name_url_dict = {}

    def add_client(self, role_name, ip, port):
        self.role_name_ip_dict[role_name] = (ip, port)
        self.role_name_url_dict[role_name] = f"http://{ip}:{port}"
        return self

    def run(self, client: BaseClient, **run_kwargs):
        self._set_client(client)

        local_ip, local_port = self.role_name_ip_dict[client.role_name]

        comm_server = Process(target=start_server, name=f"{client.role_name}:server",
                              kwargs={'role_name_url_dict': self.role_name_url_dict,
                                      'role_name': client.role_name,
                                      'host': local_ip,
                                      'port': local_port})
        comm_server.start()
        print(run_kwargs)
        client.init()
        ans = client.run(**run_kwargs)
        client.close()
        comm_server.terminate()
        return ans

    def _set_client(self, client):
        client.comm = self._get_comm(client.role_name)
        # TODO: 添加logger
        # client._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
        # client._set_client_logger()

    def _get_comm(self, role_name):
        comm = TCPComm(role_name,
                       self.role_name_url_dict[role_name],
                       set(self.role_name_url_dict) - {role_name})
        return comm

    @classmethod
    def _get_logger(cls, client: BaseClient) -> Logger:
        pass
