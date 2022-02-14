import pickle
import time
from collections import defaultdict
from queue import Empty
from typing import Callable, Dict, List, Tuple, Set
from multiprocessing import Process
from fedprototype.tools.func import get_free_ip_port
import numpy as np
import requests
import uvicorn
from fastapi import Body, FastAPI, File, Form
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from requests import Response as ReqResponse
from requests.exceptions import ConnectionError

from fedprototype.base.base_logger_factory import BaseLoggerFactory
from fedprototype.envs.cluster.tcp.tcp_message_hub import MessageHub
from fedprototype.tools.log import LocalLoggerFactory
from fedprototype.typing import (
    Host,
    MessageBytes,
    MessageName,
    MessageSpace,
    Port,
    Receiver,
    RoleName,
    RootRoleName,
    Sender,
    Url,
)

SUCCESS_CODE = 200
SUCCESS_RESPONSE = PlainTextResponse(content='OK', status_code=SUCCESS_CODE)


def _start_server(host: Host,
                  port: Port,
                  partition_num: int,
                  job_id: str,
                  coordinater_url: Url,
                  root_role_name: RootRoleName,
                  root_role_name_set: Set[RootRoleName],
                  logger_factory: BaseLoggerFactory = LocalLoggerFactory,
                  maximum_start_latency: int = 20,
                  beat_interval: int = 2,
                  alive_interval: int = 2):
    url_root_role_name_dict = {}
    local_server_url = f"http://{host}:{port}"

    logger = logger_factory.get_logger("[SparkServer]")
    logger.info(f"root_role_name={root_role_name}")

    app = FastAPI()

    @app.post("/test")
    def test():
        logger.debug(f"test.....")
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

    # https://www.uvicorn.org/settings/#logging
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["default"]["fmt"] = "FastAPI %(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "[FastAPI %(levelname)s] %(asctime)s --> (%(message)s)"
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level=logger.level, use_colors=True)


class SparkHttpServer:
    def __init__(self,
                 partition_num: int,
                 job_id: str,
                 coordinater_url: Url,
                 root_role_name: RootRoleName,
                 root_role_name_set: Set[RootRoleName],
                 **kwargs) -> None:
        self.partition_num = partition_num
        self.job_id = job_id
        self.coordinater_url = coordinater_url
        self.root_role_name = root_role_name
        self.root_role_name_set = root_role_name_set
        self._server_kwargs = kwargs
        self._process: Process = None
        self.host: Host = None
        self.port: Port = None

    def _start(self):
        self.host, self.port = get_free_ip_port()
        _server_kwargs = self._server_kwargs.copy()
        _server_kwargs.update({'host': self.host,
                               'port': self.port,
                               'partition_num': self.partition_num,
                               'job_id': self.job_id,
                               'coordinater_url': self.coordinater_url,
                               'root_role_name': self.root_role_name,
                               'root_role_name_set': self.root_role_name_set})
        self._process = Process(target=_start_server,
                                kwargs=_server_kwargs)
        self._process.start()

    def _close(self):
        self._process.terminate()
        self._process = None
        self.host = None
        self.port = None

    def __enter__(self) -> 'SparkHttpServer':
        print("__enter__ ...")
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        print("__exit__ ...")
        self._close()


if __name__ == "__main__":
    with SparkHttpServer(partition_num=1,
                         job_id="1234",
                         coordinater_url="http://abc:333",
                         root_role_name="PartA",
                         root_role_name_set={'PartA', 'PartB'}) as shs:
        import time
        time.sleep(60)
