from typing import Set

from pyspark.rdd import RDD

from fedprototype.base.base_env import BaseEnv
from fedprototype.typing import Client, RoleName, Url
from .spark_task_runner import SparkTaskRunner


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
        _spark_conf = rdd.context.getConf()

        self.client = client
        self.entry_func = entry_func
        self.coordinater_url = _spark_conf.get('fed.coordinater.url')

        return rdd.mapPartitions(SparkTaskRunner(self))

    def set_job_id(self, job_id: str) -> 'SparkEnv':
        self.job_id = job_id
        return self
