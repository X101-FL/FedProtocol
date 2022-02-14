from typing import Iterator, Set

from pyspark.rdd import RDD

from fedprototype.base.base_env import BaseEnv
from fedprototype.typing import Client, Host, Port, RoleName, Url


class SparkEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.role_name_set: Set = set()
        self.job_id: str = None
        self.coordinater_url: Url = None
        self.client: Client = None
        self.entry_func: str = None

    def add_client(self, role_name: RoleName) -> 'SparkEnv':
        self.role_name_set.add(role_name)
        return self

    def run(self, client: Client, rdd: RDD, entry_func: str = 'run') -> RDD:
        self.client = client
        self.entry_func = entry_func
        return rdd.mapPartitionsWithIndex(self._partition_func)

    def set_job_id(self, job_id: str) -> 'SparkEnv':
        self.job_id = job_id
        return self

    def set_coordinater(self, host: Host, port: Port) -> 'SparkEnv':
        self.coordinater_url = f"http://{host}:{port}"
        return self

    def _partition_func(self, splitIndex: int, iterator: Iterator) -> Iterator:
        local_host, local_port = self.root_role_name_ip_dict[client.role_name]
    #     comm_server = Process(target=start_server,
    #                           name=f"{client.role_name}:server",
    #                           kwargs={'host': local_host,
    #                                   'port': local_port,
    #                                   'root_role_name': client.role_name,
    #                                   'root_role_name_url_dict': self.root_role_name_url_dict,
    #                                   'logger_factory': self.logger_factory})
    #     comm_server.start()
    #     try:
    #         self._set_client(client)
    #         with client.init():
    #             ans = getattr(client, entry_func)(**entry_kwargs)
    #     finally:
    #         comm_server.terminate()
    #     return ans
    #     self.
    #     import os
    #     import threading
    #     print()
    #     print(f"{os.getpid()}.{threading.currentThread().name} > splitIndex:{splitIndex}, iterator:{list(iterator)}")
    #     yield splitIndex


    # def _set_client(self, client: Client) -> None:
    #     client.env = self
    #     client.track_path = f"{client.protocol_name}.{client.role_name}"
    #     client.comm = self._get_comm(client)
    #     client._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
    #     client._active_comm()
    #     client._set_client_logger()

    # def _get_comm(self, client: Client) -> TCPComm:
    #     return TCPComm(message_space=client.protocol_name,
    #                    role_name=client.role_name,
    #                    server_url=self.root_role_name_url_dict[client.role_name],
    #                    root_role_bind_mapping={r: r for r in self.root_role_name_url_dict.keys()})
