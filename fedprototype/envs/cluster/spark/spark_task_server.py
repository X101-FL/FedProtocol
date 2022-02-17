import os
import signal
import time
from multiprocessing import Process
from typing import Callable, Set

import requests
import uvicorn
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import PlainTextResponse
from pyspark import TaskContext
from requests import Response as ReqResponse
from requests.exceptions import ConnectionError
from fedprototype.tools.io import post_pro
from fedprototype.base.base_client import BaseClient
from fedprototype.base.base_logger_factory import BaseLoggerFactory
from fedprototype.tools.func import get_free_ip_port
from fedprototype.tools.log import LocalLoggerFactory
from fedprototype.typing import (
    Host,
    Port,
    Receiver,
    RoleName,
    RootRoleName,
    Sender,
    Url,
)

SUCCESS_CODE = 200
SUCCESS_RESPONSE = PlainTextResponse(content='OK', status_code=SUCCESS_CODE)


def _start_server(host: Host, port: Port,
                  coordinater_url: Url,
                  job_id: str, stage_id: int, partition_id: int, task_attempt_id: str,
                  root_role_name: RootRoleName, root_role_name_set: Set[RootRoleName],
                  task_pid: int,
                  logger_factory: BaseLoggerFactory = LocalLoggerFactory,
                  maximum_start_latency: int = 20,
                  beat_interval: int = 2,
                  alive_interval: int = 2):
    url_root_role_name_dict = {}
    task_server_url = f"http://{host}:{port}"

    logger = logger_factory.get_logger("[SparkServer]")
    logger.info(f"root_role_name={root_role_name}")

    app = FastAPI()

    @app.post("/test")
    def test():
        logger.debug(f"test.....")
        return SUCCESS_RESPONSE

    @app.post("/fail_task")
    def fail_task():
        logger.debug(f"fail_task.....")
        _exit()
        return SUCCESS_RESPONSE

    def _post(**kwargs) -> ReqResponse:
        try:
            _res = requests.post(**kwargs)
            if _res.status_code != SUCCESS_CODE:
                raise HTTPException(detail=f"internal post error : {_res.text}", status_code=500)
        except ConnectionError as e:
            raise HTTPException(detail=f"internal post error : {e}", status_code=500)

    def _try_func(func: Callable, detail: str, status_code: int, **kwargs) -> None:
        try:
            func(**kwargs)
        except HTTPException as e:
            detail = f"{detail}. {e.detail}"
            raise HTTPException(detail=detail, status_code=status_code)

    def _exit():
        print(f"kill task, pid:{task_pid}")
        os.kill(task_pid, signal.SIGTERM)

    def _register_task():
        post_pro(url=f"{coordinater_url}/register_task",
                 json={'job_id': job_id,
                       'partition_id': partition_id,
                       'root_role_name': root_role_name,
                       'stage_id': stage_id,
                       'task_attempt_id': task_attempt_id,
                       'task_server_url': task_server_url})

    try:
        _register_task()
        uvicorn.run(app=app, host=host, port=port, debug=True,
                    access_log=True, log_level=logger.level, use_colors=True)
    except BaseException as e:
        print(f"error : {e}")
        _exit()


class SparkTaskServer:
    def __init__(self, spark_env: 'SparkEnv', task_context: TaskContext) -> None:
        self.spark_env = spark_env
        self.task_context = task_context
        self._process: Process = None
        self._server_url: Url = None

    def _start(self):
        host, port = get_free_ip_port()
        self._server_url = f"http://{host}:{port}"
        self._process = Process(target=_start_server,
                                kwargs={'host': host,
                                        'port': port,
                                        'stage_id': self.task_context.stageId(),
                                        'partition_id': self.task_context.partitionId(),
                                        'task_attempt_id': self.task_context.taskAttemptId(),
                                        'job_id': self.spark_env.job_id,
                                        'coordinater_url': self.spark_env.coordinater_url,
                                        'root_role_name': self.spark_env.client.role_name,
                                        'root_role_name_set': self.spark_env.role_name_set,
                                        'task_pid': os.getpid()},
                                daemon=True)
        self._process.start()

    def get_server_url(self) -> Url:
        return self._server_url

    def _close(self):
        self._process.terminate()
        self._process = None
        self._server_url = None

    def __enter__(self) -> 'SparkTaskServer':
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._close()


if __name__ == "__main__":
    from fedprototype.envs.cluster.spark.spark_env import SparkEnv

    class _Client(BaseClient):
        def __init__(self):
            super().__init__('DevServer', 'PartA')

    spark_env = SparkEnv() \
        .add_client('PartA') \
        .add_client('PartB') \
        .set_job_id('dev server')
    spark_env.coordinater_url = "http://127.0.0.1:6609"
    spark_env.client = _Client()

    class _TaskContext:

        def stageId(self):
            return 0

        def partitionId(self):
            return 2

        def taskAttemptId(self):
            return "0.0"

    task_context = _TaskContext()
    with SparkTaskServer(spark_env, task_context) as sts:
        import time
        time.sleep(60)
