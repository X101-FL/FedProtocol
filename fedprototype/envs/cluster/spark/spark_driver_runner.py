import typing

if typing.TYPE_CHECKING:
    from .spark_env import SparkEnv

import os
import signal
import time
from threading import Thread
from typing import Any, Callable

from py4j.java_gateway import JavaObject
from pyspark import SparkContext
from pyspark.rdd import RDD

from fedprototype.envs.cluster.spark.spark_task_runner import SparkTaskRunner
from fedprototype.tools.io import post_pro
from fedprototype.typing import Client, JobID, RootRoleName, Url


class HeartbeatThread(Thread):
    def __init__(self,
                 coordinater_url: Url,
                 job_id: JobID,
                 root_role_name: RootRoleName) -> None:
        super().__init__(daemon=True)
        self.coordinater_url = coordinater_url
        self.job_id = job_id
        self.root_role_name = root_role_name
        self._keep_on = True

    def run(self) -> None:
        while self._keep_on:
            heartbeat_res = post_pro(retry_times=3,
                                     retry_interval=3,
                                     error='None',
                                     url=f"{self.coordinater_url}/driver_heartbeat",
                                     json={'job_id': self.job_id, 'root_role_name': self.root_role_name})
            print(f"heartbeat of job_id:{self.job_id}, role_name:{self.root_role_name}, heartbeat_res:{heartbeat_res}")
            if heartbeat_res is None:
                print(f"lost connect with coordinater ...")
                os.kill(os.getpid(), signal.SIGTERM)
            if heartbeat_res['job_state'] == 'failed':
                print(f"federated job is failed ...")
                os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(5)

    def stop(self) -> None:
        self._keep_on = False


class SparkDriverRunner:
    def __init__(self, spark_env: 'SparkEnv',) -> None:
        self.spark_env = spark_env
        self.coordinater_url = spark_env.coordinater_url
        self.job_id = spark_env.job_id
        self._job_listener: JavaObject = None
        self._heartbeat_thread: HeartbeatThread = None

    def run(self,
            client: Client,
            rdd: RDD,
            entry_func: str,
            action_callback: Callable[[RDD], Any]
            ) -> Any:
        try:
            self._register_listener()
            self._register_driver()
            self._start_heartbeat()
            _rdd = rdd.mapPartitions(SparkTaskRunner(self.spark_env, client, entry_func))
            ans = action_callback(_rdd)
        except BaseException as e:
            self.close(success=False)
            raise e
        else:
            self.close(success=True)
            return ans

    def _register_listener(self) -> None:
        sc = SparkContext.getOrCreate()
        _jsc = sc._jvm.org.apache.spark.SparkContext.getOrCreate()
        job_listener = sc._jvm.fedprototype.spark.FedJobListener(self.spark_env.coordinater_url,
                                                                 self.spark_env.job_id,
                                                                 self.spark_env.root_role_name)
        _jsc.addSparkListener(job_listener)
        self._job_listener = job_listener

    def _register_driver(self) -> None:
        post_pro(url=f"{self.coordinater_url}/register_driver",
                 json={'job_id': self.job_id,
                       'partition_num': self.spark_env.partition_num,
                       'root_role_name_set': list(self.spark_env.root_role_name_set),
                       'root_role_name': self.spark_env.root_role_name})
        print(f"register driver job_id:{self.job_id}, role_name:{self.spark_env.root_role_name} successfully")
        self._is_driver_registed = True

    def _start_heartbeat(self) -> None:
        _heartbeat_thread = HeartbeatThread(coordinater_url=self.spark_env.coordinater_url,
                                            job_id=self.spark_env.job_id,
                                            root_role_name=self.spark_env.root_role_name)
        _heartbeat_thread.start()
        self._heartbeat_thread = _heartbeat_thread

    def close(self, success=True):
        if self._job_listener:
            self._job_listener.markJobState(success)
            self._job_listener = None

        if self._heartbeat_thread:
            self._heartbeat_thread.stop()
            self._heartbeat_thread = None
