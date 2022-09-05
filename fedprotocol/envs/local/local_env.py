from threading import Lock, Thread
from typing import Dict, List, Set, Tuple

from fedprotocol.base.base_env import BaseEnv
from fedprotocol.envs.local.worker_thread import WorkerThread
from fedprotocol.envs.local.local_comm import LocalComm
from fedprotocol.envs.local.local_message_hub import MessageHub
from fedprotocol.tools.state_manager import LocalStateManager
from fedprotocol.typing import FileDir, RoleName, Worker
from fedprotocol.tools.log import getLogger


class LocalEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.serial_lock: Lock = Lock()
        self.worker_info_dict: Dict[RoleName, Tuple[Worker, str, dict]] = {}
        self.role_name_set: Set[RoleName] = set()
        self.msg_hub: MessageHub = MessageHub()
        self.logger = getLogger("Frame.Local.Env")
        # Default setting for environment, such as State
        self._default_setting()

    def add_worker(
        self, worker: Worker, entry_func: str = 'run', **entry_kwargs
    ) -> "LocalEnv":
        self.worker_info_dict[worker.role_name] = (worker, entry_func, entry_kwargs)
        return self

    def run(self) -> None:
        thread_list: List[Thread] = []

        self.role_name_set = set(self.worker_info_dict.keys())

        # Put all workers to serial lock
        for role_name, worker_info in self.worker_info_dict.items():
            worker, entry_func, entry_kwargs = worker_info
            self.logger.info(f"Initialize {role_name}")
            self._set_worker(worker)
            thread_list.append(
                WorkerThread(worker, entry_func, entry_kwargs, self.serial_lock)
            )

        self.serial_lock.acquire()
        for th in thread_list:
            th.start()
        self.serial_lock.release()

        for th in thread_list:
            th.join()

        self.logger.info(f"All task done!!!")

    def set_checkpoint_home(self, home_dir: FileDir) -> "LocalEnv":
        assert isinstance(
            self.state_manager, LocalStateManager
        ), f"Please using 'LocalStateManager' in Local mode!"
        self.state_manager.set_home_dir(home_dir)
        return self

    def _default_setting(self):
        self.set_state_manager(LocalStateManager())

    def _set_worker(self, worker: Worker) -> None:
        worker.env = self
        worker.track_path = f"{worker.protocol_name}.{worker.role_name}"
        worker.comm = self._get_comm(worker)
        worker._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
        worker._active_comm()
        worker._set_logger()

    def _get_comm(self, worker: Worker) -> LocalComm:
        return LocalComm(
            message_space=worker.protocol_name,
            role_name=worker.role_name,
            other_role_name_set=(self.role_name_set - {worker.role_name}),
            msg_hub=self.msg_hub,
            serial_lock=self.serial_lock,
        )
