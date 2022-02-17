import typing
from typing import Iterator

from pyspark import TaskContext

if typing.TYPE_CHECKING:
    from .spark_env import SparkEnv

from .spark_task_server import SparkTaskServer


class SparkTaskRunner:
    def __init__(self, spark_env: 'SparkEnv') -> None:
        self.spark_env: 'SparkEnv' = spark_env

    @property
    def task_context(self):
        return TaskContext.get()

    def __call__(self, iterator: Iterator) -> Iterator:
        with SparkTaskServer(spark_env=self.spark_env,
                             task_context=self.task_context) as sts:
            print(f"get data : {list(iterator)}")
            import time
            time.sleep(10)
        yield 0
