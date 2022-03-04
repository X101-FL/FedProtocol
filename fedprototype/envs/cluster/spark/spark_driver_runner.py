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
from fedprototype.envs.cluster.spark.constants import *


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
            res = post_pro(retry_times=3,
                           retry_interval=3,
                           error='None',
                           url=f"{self.coordinater_url}/driver/heartbeat",
                           json={'job_id': self.job_id, 'root_role_name': self.root_role_name})
            print(f"heartbeat_res:{res}")
            if res is None:
                print(f"lost connect with coordinater ...")
                os.kill(os.getpid(), signal.SIGTERM)
            elif res['state'] == EXITED:
                if res['is_successed']:
                    self.stop()
                else:
                    print(f"federated job is failed ...")
                    os.kill(os.getpid(), signal.SIGTERM)
            else:
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
        self._is_driver_registed: bool = False

    def run(self,
            client: Client,
            rdd: RDD,
            entry_func: str,
            action_callback: Callable[[RDD], Any]
            ) -> Any:
        try:
            self._register_driver()
            self._start_heartbeat()
            self._wait_for_other_drivers_register()
            self._register_listener()
            _rdd = rdd.mapPartitions(SparkTaskRunner(self.spark_env, client, entry_func))
            ans = action_callback(_rdd)
        except BaseException as e:
            self.close(success=False)
            raise e
        else:
            self.close(success=True)
            return ans

    def _register_driver(self) -> None:
        post_pro(url=f"{self.coordinater_url}/driver/register",
                 json={'job_id': self.job_id,
                       'partition_num': self.spark_env.partition_num,
                       'root_role_name_set': list(self.spark_env.root_role_name_set),
                       'root_role_name': self.spark_env.root_role_name})
        print(f"register driver job_id:{self.job_id}, role_name:{self.spark_env.root_role_name} successfully")
        self._is_driver_registed = True

    def _finish_driver(self, success: bool) -> None:
        post_pro(url=f"{self.coordinater_url}/driver/finish",
                 json={'job_id': self.job_id,
                       'root_role_name': self.spark_env.root_role_name,
                       'success': success})

    def _wait_for_other_drivers_register(self) -> None:
        while True:
            res = post_pro(url=f"{self.coordinater_url}/driver/wait_for_running",
                           json={'job_id': self.job_id,
                                 'root_role_name': self.spark_env.root_role_name})
            print(f"wait_for_running:{res}")
            if res['state'] == STANDBY_FOR_RUN:
                time.sleep(3)
            elif res['state'] == RUNNING:
                return
            else:
                raise Exception(f"failed to wait for other drivers")

    def _wait_for_other_drivers_finish(self):
        while True:
            res = post_pro(url=f"{self.coordinater_url}/driver/wait_for_finish",
                           json={'job_id': self.job_id,
                                 'root_role_name': self.spark_env.root_role_name})
            print(f"wait_for_finish:{res}")
            if res['state'] == STANDBY_FOR_EXIT:
                time.sleep(3)
            elif res['state'] == EXITED:
                job_successed = res['is_successed']
                if job_successed:
                    return
                else:
                    raise Exception(f"Job Failed")
            else:
                raise Exception(f"failed to wait for other drivers")

    def _register_listener(self) -> None:
        sc = SparkContext.getOrCreate()
        _jsc = sc._jvm.org.apache.spark.SparkContext.getOrCreate()
        job_listener = sc._jvm.fedprototype.spark.FedJobListener(self.spark_env.coordinater_url,
                                                                 self.spark_env.job_id,
                                                                 self.spark_env.root_role_name)
        _jsc.addSparkListener(job_listener)
        self._job_listener = job_listener

    def _start_heartbeat(self) -> None:
        _heartbeat_thread = HeartbeatThread(coordinater_url=self.spark_env.coordinater_url,
                                            job_id=self.spark_env.job_id,
                                            root_role_name=self.spark_env.root_role_name)
        _heartbeat_thread.start()
        self._heartbeat_thread = _heartbeat_thread

    def close(self, success=True):
        if self._heartbeat_thread:
            self._heartbeat_thread.stop()
            self._heartbeat_thread = None

        if self._is_driver_registed:
            self._finish_driver(success)
            if success:
                self._wait_for_other_drivers_finish()
