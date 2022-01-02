from threading import Lock, Thread
from typing import Dict, Tuple, Set, List

from fedprototype.base.base_env import BaseEnv
from fedprototype.typing import RoleName, Client, FileDir
from fedprototype.envs.local.client_thread import ClientThread
from fedprototype.envs.local.local_comm import LocalComm
from fedprototype.envs.local.message_hub import MessageHub
from fedprototype.tools.state_saver import LocalStateSaver
from fedprototype.tools.log import LocalLoggerFactory


class LocalEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.serial_lock: Lock = Lock()
        self.client_info_dict: Dict[RoleName, Tuple[Client, str, dict]] = {}
        self.role_name_set: Set[RoleName] = set()
        self.msg_hub: MessageHub = MessageHub()

        self._default_setting()

    def add_client(self, client: Client, entry_func: str = 'run', **entry_kwargs) -> "LocalEnv":
        self.client_info_dict[client.role_name] = (client, entry_func, entry_kwargs)
        return self

    def run(self) -> None:
        thread_list: List[Thread] = []
        self.role_name_set = set(self.client_info_dict.keys())

        for role_name, (client, entry_func, entry_kwargs) in self.client_info_dict.items():
            self._set_client(client)
            thread_list.append(ClientThread(client, entry_func, entry_kwargs, self.serial_lock))
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

    def set_checkpoint_home(self, home_dir: FileDir) -> "LocalEnv":
        assert isinstance(self.state_saver, LocalStateSaver), \
            f"set_checkpoint_home is not supported by {self.state_saver.__class__.__name__}"
        self.state_saver.set_home_dir(home_dir)
        return self

    def _default_setting(self):
        self.set_logger_factory(LocalLoggerFactory)
        self.set_state_saver(LocalStateSaver())

    def _set_client(self, client: Client) -> None:
        client.env = self
        client.track_path = client.role_name
        client.comm = self._get_comm(client)
        client._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
        client._set_client_logger()

    def _get_comm(self, client: Client) -> LocalComm:
        return LocalComm(role_name=client.role_name,
                         other_role_name_set=(self.role_name_set - {client.role_name}),
                         msg_hub=self.msg_hub,
                         serial_lock=self.serial_lock)
