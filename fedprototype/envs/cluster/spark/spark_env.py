import os
import signal
import time
from threading import Thread
from typing import Dict, Set

from pyspark.rdd import RDD

from fedprototype.base.base_env import BaseEnv
from fedprototype.envs.cluster.spark.spark_task_runner import SparkTaskRunner
from fedprototype.envs.cluster.spark.spark_comm import SparkComm
from fedprototype.tools.io import post_pro
from fedprototype.tools.log import LocalLoggerFactory
from fedprototype.tools.state_saver import LocalStateSaver
from fedprototype.typing import Client, FileDir, JobID, RoleName, RootRoleName, Url


class SparkEnv(BaseEnv):
    def __init__(self):
        super().__init__()
        self.job_id: str = None
        self.root_role_name_set: Set = set()
        self.root_role_name: RootRoleName = None
        self.partition_num: int = None
        self.coordinater_url: Url = None
        self._default_setting()

    def add_client(self, role_name: RoleName) -> 'SparkEnv':
        self.root_role_name_set.add(role_name)
        return self

    def run(self, client: Client, rdd: RDD, entry_func: str = 'run') -> RDD:
        _spark_conf = rdd.context.getConf()

        self.root_role_name = client.role_name
        self.coordinater_url = _spark_conf.get('fed.coordinater.url')
        self.partition_num = rdd.getNumPartitions()

        _register_driver(self)
        _HeartbeatManager.start_or_skip(self)

        return rdd.mapPartitions(SparkTaskRunner(self, client, entry_func))

    def set_job_id(self, job_id: str) -> 'SparkEnv':
        self.job_id = job_id
        return self

    def set_checkpoint_home(self, home_dir: FileDir) -> "SparkEnv":
        assert isinstance(self.state_saver, LocalStateSaver), \
            f"set_checkpoint_home is not supported by {self.state_saver.__class__.__name__}"
        self.state_saver.set_home_dir(home_dir)
        return self

    def _default_setting(self) -> None:
        self.set_logger_factory(LocalLoggerFactory)
        self.set_state_saver(LocalStateSaver())

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


def _register_driver(spark_env: SparkEnv) -> None:
    post_pro(url=f"{spark_env.coordinater_url}/register_driver",
             json={'job_id': spark_env.job_id,
                   'partition_num': spark_env.partition_num,
                   'root_role_name_set': list(spark_env.root_role_name_set),
                   'root_role_name': spark_env.root_role_name})
    print(f"register driver job_id:{spark_env.job_id}, role_name:{spark_env.root_role_name} successfully")


class _HeartbeatManager:
    heartbeating_threads: Dict[JobID, Thread] = {}

    @staticmethod
    def start_or_skip(spark_env: SparkEnv):
        if spark_env.job_id in _HeartbeatManager.heartbeating_threads:
            print(f"heartbeat thread for job:{spark_env.job_id} is started already ")
        else:
            _heartbeat_thread = Thread(target=_HeartbeatManager._heartbeat,
                                       kwargs={'coordinater_url': spark_env.coordinater_url,
                                               'job_id': spark_env.job_id,
                                               'root_role_name': spark_env.root_role_name},
                                       daemon=True)
            _heartbeat_thread.start()
            _HeartbeatManager.heartbeating_threads[spark_env.job_id] = _heartbeat_thread

    @staticmethod
    def _heartbeat(coordinater_url: Url,
                   job_id: JobID,
                   root_role_name: RootRoleName
                   ) -> None:
        while True:
            heartbeat_res = post_pro(retry_times=3,
                                     retry_interval=3,
                                     error='None',
                                     url=f"{coordinater_url}/driver_heartbeat",
                                     json={'job_id': job_id, 'root_role_name': root_role_name})
            print(f"heartbeat of job_id:{job_id}, role_name:{root_role_name}, heartbeat_res:{heartbeat_res}")
            if heartbeat_res is None:
                print(f"lost connect with coordinater ...")
                os.kill(os.getpid(), signal.SIGTERM)
            if heartbeat_res['job_state'] == 'failed':
                print(f"federated job is failed ...")
                os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(5)
