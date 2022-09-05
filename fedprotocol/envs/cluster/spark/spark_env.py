from typing import Any, Callable, Set

from pyspark.rdd import RDD

from fedprotocol.base.base_env import BaseEnv
from fedprotocol.envs.cluster.spark.spark_comm import SparkComm
from fedprotocol.envs.cluster.spark.spark_driver_runner import SparkDriverRunner
from fedprotocol.tools.state_manager import LocalStateManager
from fedprotocol.typing import Client, FileDir, RoleName, RootRoleName, Url
from fedprotocol.tools.log import getLogger

class SparkEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.job_id: str = None
        self.root_role_name_set: Set = set()
        self.root_role_name: RootRoleName = None
        self.partition_num: int = None
        self.coordinater_url: Url = None
        self.logger = getLogger("Frame.Spark.Env")
        self._default_setting()

    def add_worker(self, role_name: RoleName) -> 'SparkEnv':
        self.root_role_name_set.add(role_name)
        return self

    def run(self,
            client: Client,
            rdd: RDD,
            entry_func: str = 'run',
            action_callback: Callable[[RDD], Any] = lambda _rdd: _rdd.count()
            ) -> Any:
        self.root_role_name = client.role_name
        self.partition_num = rdd.getNumPartitions()
        return SparkDriverRunner(self).run(client=client,
                                           rdd=rdd,
                                           entry_func=entry_func,
                                           action_callback=action_callback)

    def set_job_id(self, job_id: str) -> 'SparkEnv':
        self.job_id = job_id
        return self

    def set_coordinater_url(self, coordinater_url: Url) -> 'SparkEnv':
        self.coordinater_url = coordinater_url
        return self

    def set_checkpoint_home(self, home_dir: FileDir) -> "SparkEnv":
        assert isinstance(self.state_manager, LocalStateManager), \
            f"set_checkpoint_home is not supported by {self.state_manager.__class__.__name__}"
        self.state_manager.set_home_dir(home_dir)
        return self

    def _default_setting(self) -> None:
        self.set_state_manager(LocalStateManager())

    def _set_client(self, client: Client, server_url: Url) -> None:
        client.env = self
        client.track_path = f"{client.protocol_name}.{client.role_name}"
        client.comm = self._get_comm(client, server_url)
        client._set_comm_logger()  # 这里调用了私有函数，因为这个函数不应暴露给用户
        client._active_comm()
        client._set_client_logger()

    def _get_comm(self, client: Client, server_url: Url) -> SparkComm:
        return SparkComm(message_space=client.protocol_name,
                         role_name=client.role_name,
                         server_url=server_url,
                         root_role_bind_mapping={r: r for r in self.root_role_name_set})
