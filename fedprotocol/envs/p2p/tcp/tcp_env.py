from multiprocessing import Process
from typing import Any, Dict

from fedprotocol.base.base_env import BaseEnv
from fedprotocol.envs.p2p.tcp.tcp_comm import TCPComm
from fedprotocol.envs.p2p.tcp.tcp_fed_server import FedServer
from fedprotocol.tools.state_manager import LocalStateManager
from fedprotocol.typing import Worker, FileDir, Host, Port, RoleName, RootRoleName, Url


class TCPEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.root_role_name_ip_dict: Dict[RootRoleName, (Host, Port)] = {}
        self.root_role_name_url_dict: Dict[RootRoleName, Url] = {}
        self._default_setting()

    def add_worker(self, role_name: RoleName, host: Host, port: Port) -> "TCPEnv":
        self.root_role_name_ip_dict[role_name] = (host, port)
        self.root_role_name_url_dict[role_name] = f"http://{host}:{port}"
        return self

    def run(self, worker: Worker, entry_func: str = 'run', **entry_kwargs) -> Any:
        local_host, local_port = self.root_role_name_ip_dict[worker.role_name]
        with FedServer(host=local_host, port=local_port,
                       root_role_name=worker.role_name,
                       root_role_name_url_dict=self.root_role_name_url_dict):
            self._set_worker(worker)
            with worker.init():
                ans = getattr(worker, entry_func)(**entry_kwargs)
        return ans

    def set_checkpoint_home(self, home_dir: FileDir) -> "TCPEnv":
        assert isinstance(self.state_manager, LocalStateManager), \
            f"set_checkpoint_home is not supported by {self.state_manager.__class__.__name__}"
        self.state_manager.set_home_dir(home_dir)
        return self

    def _default_setting(self) -> None:
        self.set_state_manager(LocalStateManager())

    def _set_worker(self, worker: Worker) -> None:
        worker.env = self
        worker.track_path = f"{worker.protocol_name}.{worker.role_name}"
        worker.comm = self._get_comm(worker)
        worker._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
        worker._active_comm()
        worker._set_logger()

    def _get_comm(self, worker: Worker) -> TCPComm:
        return TCPComm(message_space=worker.protocol_name,
                       role_name=worker.role_name,
                       server_url=self.root_role_name_url_dict[worker.role_name],
                       root_role_bind_mapping={r: r for r in self.root_role_name_url_dict.keys()})
