from multiprocessing import Process
from typing import Any, Dict

from fedprototype.base.base_env import BaseEnv
from fedprototype.envs.cluster.tcp.tcp_comm import TCPComm
from fedprototype.envs.cluster.tcp.tcp_http_server import start_server
from fedprototype.tools.log import LocalLoggerFactory
from fedprototype.tools.state_saver import LocalStateSaver
from fedprototype.typing import (
    Client,
    Host,
    Port,
    ProtocolName,
    RoleName,
    RootRoleName,
    Url,
)


class TCPEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.root_role_name_ip_dict: Dict[RootRoleName, (Host, Port)] = {}
        self.root_role_name_url_dict: Dict[RootRoleName, Url] = {}
        self._default_setting()

    def add_client(self, role_name: RoleName, host: Host, port: Port):
        self.root_role_name_ip_dict[role_name] = (host, port)
        self.root_role_name_url_dict[role_name] = f"http://{host}:{port}"
        return self

    def run(self, client: Client, **run_kwargs) -> Any:
        local_ip, local_port = self.root_role_name_ip_dict[client.role_name]
        comm_server = Process(target=start_server,
                              name=f"{client.role_name}:server",
                              kwargs={'root_role_name_url_dict': self.root_role_name_url_dict,
                                      'host': local_ip,
                                      'port': local_port})
        comm_server.start()
        self._set_client(client)
        try:
            with client.init():
                ans = client.run(**run_kwargs)
        finally:
            comm_server.terminate()
            comm_server.close()
        return ans

    def _default_setting(self):
        self.set_logger_factory(LocalLoggerFactory)
        self.set_state_saver(LocalStateSaver())

    def _set_client(self, client: Client):
        client.env = self
        client.track_path = f"{client.protocol_name}/{client.role_name}"
        client.comm = self._get_comm(client)
        client._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
        client._set_client_logger()

    def _get_comm(self, client: Client):
        return TCPComm(message_space=client.protocol_name,
                       role_name=client.role_name,
                       server_url=self.root_role_name_url_dict[client.role_name],
                       role_name_to_root_dict={r: r for r in self.root_role_name_url_dict.keys()})
