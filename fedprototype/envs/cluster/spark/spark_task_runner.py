import typing
from typing import Iterator

from pyspark import TaskContext

if typing.TYPE_CHECKING:
    from .spark_env import SparkEnv

from fedprototype.envs.cluster.spark.spark_task_server import SparkTaskServer
from fedprototype.typing import Client


class SparkTaskRunner:
    def __init__(self,
                 spark_env: 'SparkEnv',
                 client: Client,
                 entry_func: str
                 ) -> None:
        self.spark_env = spark_env
        self.client = client
        self.entry_func = entry_func

    @property
    def task_context(self):
        return TaskContext.get()

    def __call__(self, iterator: Iterator) -> Iterator:
        with SparkTaskServer(spark_env=self.spark_env,
                             task_context=self.task_context) as sts:
            self.spark_env._set_client(client=self.client, server_url=sts.get_server_url())
            with self.client.init():
                yield getattr(self.client, self.entry_func)(iterator)
