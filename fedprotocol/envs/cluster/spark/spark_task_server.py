import logging
import os
import pickle
import signal
import time
import typing
from multiprocessing import Process
from typing import Any, Callable, Dict, List, Tuple

import uvicorn
from fastapi import Body, FastAPI, File, Form, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pyspark import TaskContext

if typing.TYPE_CHECKING:
    from .spark_env import SparkEnv

from fedprotocol.envs.cluster.spark.constants import SUCCESS_RESPONSE
from fedprotocol.envs.cluster.spark.spark_message_hub import MessageHub
from fedprotocol.tools.func import get_free_ip_port
from fedprotocol.tools.io import post_pro
from fedprotocol.tools.log import getLogger
from fedprotocol.typing import (
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


def _start_server(host: Host,
                  port: Port,
                  root_role_name: RootRoleName,
                  task_pid: int,
                  wait_for_target_task_interval: int = 5):
    root_role_name_url_dict: Dict[RootRoleName, Url] = {}
    message_hub: MessageHub = MessageHub()

    logger = getLogger(f"Frame.Server.{root_role_name}")
    logger.info(f"start server...")

    task_group_info: Dict[str, Any] = {}

    app = FastAPI()

    @app.post("/ping")
    def ping():
        logger.info("ping ...")
        return SUCCESS_RESPONSE

    @app.post("/task_group_failed")
    def task_group_failed():
        logger.info(f"fail_task.....")
        os.kill(task_pid, signal.SIGKILL)
        os.kill(os.getpid(), signal.SIGKILL)
        return SUCCESS_RESPONSE

    @app.post("/task_group_successed")
    def task_group_successed():
        logger.info(f"port:{port} task_group_successed ...")
        task_group_info['is_successed'] = True
        return SUCCESS_RESPONSE

    @app.post("/wait_for_group_success")
    def wait_for_group_success():
        while not task_group_info.get('is_successed', False):
            logger.info(f"port:{port} wait_for_group_success ...")
            time.sleep(2)
        return SUCCESS_RESPONSE

    @app.post("/update_task_server_url")
    def update_task_server_url(task_server_url_dict: Dict[RootRoleName, Url] = Body(...)):
        logger.debug(f"update_task_server_url task_server_url_dict:{task_server_url_dict}")
        root_role_name_url_dict.update(task_server_url_dict)
        return SUCCESS_RESPONSE

    @app.post("/set_message_space")
    def set_message_space(message_space: MessageSpace = Body(...),
                          root_role_bind_mapping: Dict[RoleName, RootRoleName] = Body(...)):
        logger.info(f"set_message_space : message_space={message_space}, root_role_bind_mapping={root_role_bind_mapping}")
        message_hub.get_message_space_manager(message_space)\
                   .set_role_bind_mapping(root_role_bind_mapping)
        return SUCCESS_RESPONSE

    @app.post("/receive")
    def receive(message_space: MessageSpace = Body(...),
                sender: Sender = Body(...),
                receiver: Receiver = Body(...),
                message_name: MessageName = Body(...)):
        logger.debug(f"receive : message_space={message_space}, sender={sender}, "
                     f"receiver={receiver}, message_name={message_name}")
        message_space_manager = message_hub.get_message_space_manager(message_space)
        message_queue = message_space_manager.get_message_queue(sender, receiver, message_name)

        message_bytes = message_queue.get()
        return Response(content=message_bytes)

    @app.post("/send")
    def send(message_package_bytes: MessageBytes = File(...),
             message_space: MessageSpace = Form(...),
             sender: Sender = Form(...),
             receiver: Receiver = Form(...)):
        logger.debug(f"send : message_space={message_space}, sender={sender}, "
                     f"receiver={receiver}, message_package_bytes.size={len(message_package_bytes)}")
        message_space_manager = message_hub.get_message_space_manager(message_space)
        receiver_root_role_name = message_space_manager.get_root_role_name(receiver)

        _wait_for_server_registed(receiver_root_role_name)
        receiver_server_url = root_role_name_url_dict[receiver_root_role_name]

        _try_func(func=post_pro,
                  url=f"{receiver_server_url}/put_",
                  data={'message_space': message_space,
                        'sender': sender,
                        'receiver': receiver},
                  files={'message_package_bytes': message_package_bytes},
                  detail=f"failed to put message to server : {receiver_root_role_name}/put_",
                  status_code=422)

        return SUCCESS_RESPONSE

    @app.post("/put_")
    def put_(message_package_bytes: MessageBytes = File(...),
             message_space: MessageSpace = Form(...),
             sender: Sender = Form(...),
             receiver: Receiver = Form(...)):
        # put、register_watch、clear操作会相互影响，需要加锁
        # 加锁的写法就是使用with语法
        with message_hub.get_message_space_manager(message_space) as message_space_manager:
            for message_name, message_bytes in pickle.loads(message_package_bytes):
                logger.debug(f"put a message : message_space={message_space}, sender={sender}, "
                             f"receiver={receiver}, message_name={message_name}, "
                             f"message_bytes.size={len(message_bytes)}")
                message_space_manager.put(sender, receiver, message_name, message_bytes)
        return SUCCESS_RESPONSE

    @app.post("/clear")
    def clear(message_space: MessageSpace = Body(...),
              sender: Sender = Body(None),
              receiver: Receiver = Body(...),
              message_name: MessageName = Body(None)):
        logger.debug(f"clear message hub of message_space={message_space}, sender={sender}, "
                     f"receiver={receiver}, message_name={message_name}")
        # put、register_watch、clear操作会相互影响，需要加锁
        # 加锁的写法就是使用with语法
        with message_hub.get_message_space_manager(message_space) as message_space_manager:
            drop_size = 0
            for message_id, message_queue in message_space_manager.lookup_message_queues(sender, receiver, message_name):
                while not message_queue.empty():
                    drop_size += 1
                    logger.debug(f"drop message <{drop_size}> : {message_id}")
                    message_queue.get()
        return JSONResponse(content={'drop_size': drop_size})

    @app.post("/regist_watch")
    def regist_watch(message_space: MessageSpace = Body(...),
                     receiver: Receiver = Body(...),
                     sender_message_name_tuple_list: List[Tuple[Sender, MessageName]] = Body(...)):
        logger.debug(f"regist_watch : message_space={message_space}, receiver={receiver}, "
                     f"sender_message_name_tuple_list={sender_message_name_tuple_list}")
        # put、register_watch、clear操作会相互影响，需要加锁
        # 加锁的写法就是使用with语法
        with message_hub.get_message_space_manager(message_space) as message_space_manager:
            message_space_manager.register_watch(receiver, sender_message_name_tuple_list)
        return SUCCESS_RESPONSE

    @app.post("/fetch_watch")
    def fetch_watch(message_space: MessageSpace = Body(...),
                    receiver: Receiver = Body(...)):
        logger.debug(f"fetch_watch : message_space={message_space}, receiver={receiver}")
        message_space_manager = message_hub.get_message_space_manager(message_space)
        watch_manager = message_space_manager.get_watch_manager(receiver)
        watched_item_list = []

        watched_item = watch_manager.get()
        watched_item_list.append(watched_item)

        while not watch_manager.empty():
            watched_item_list.append(watch_manager.get())

        fetch_res = {'finished': False, 'data': watched_item_list}
        if watch_manager.is_all_got():
            fetch_res['finished'] = True
            message_space_manager.cancel_watch(receiver)
        return Response(content=pickle.dumps(fetch_res))

    def _wait_for_server_registed(root_role_name: RootRoleName) -> None:
        while root_role_name not in root_role_name_url_dict:
            logger.debug(f"wait for server '{root_role_name}' registed ...")
            time.sleep(wait_for_target_task_interval)

    def _try_func(func: Callable, detail: str, status_code: int, **kwargs) -> Any:
        try:
            return func(**kwargs)
        except Exception as e:
            if isinstance(e, HTTPException):
                err_detail = e.detail
            else:
                err_detail = str(e)
            raise HTTPException(detail=f"{detail}. {err_detail}", status_code=status_code)

    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level=logging.ERROR, use_colors=True)


class SparkTaskServer:
    def __init__(self, spark_env: 'SparkEnv', task_context: TaskContext) -> None:
        self.spark_env = spark_env
        self.task_context = task_context
        self._process: Process = None
        self._server_url: Url = None
        self.logger = getLogger("Frame.Spark.Task.Server")

    def _start(self):
        self.logger.info(f"spark server task_attempt_num:{self.task_context.attemptNumber()}, root_role_name:{self.spark_env.root_role_name}, task_pid:{os.getpid()}")
        host, port = get_free_ip_port()
        self._server_url = f"http://{host}:{port}"
        self._process = Process(target=_start_server,
                                kwargs={'host': host,
                                        'port': port,
                                        'root_role_name': self.spark_env.root_role_name,
                                        'task_pid': os.getpid()},
                                daemon=True)
        self._process.start()
        self._wait_for_server_startup()
        self._register_task_to_coordinater()

    def get_server_url(self) -> Url:
        return self._server_url

    def _wait_for_server_startup(self):
        post_pro(retry_times=10,
                 retry_interval=0.5,
                 url=f"{self._server_url}/ping")

    def _register_task_to_coordinater(self):
        post_pro(url=f"{self.spark_env.coordinater_url}/task/register",
                 json={'job_id': self.spark_env.job_id,
                       'partition_id': self.task_context.partitionId(),
                       'root_role_name': self.spark_env.root_role_name,
                       'stage_id': self.task_context.stageId(),
                       'task_attempt_num': self.task_context.attemptNumber(),
                       'task_server_url': self._server_url})

    def _wait_for_exit(self):
        post_pro(url=f"{self.spark_env.coordinater_url}/task/success",
                 json={'job_id': self.spark_env.job_id,
                       'partition_id': self.task_context.partitionId(),
                       'root_role_name': self.spark_env.root_role_name,
                       'stage_id': self.task_context.stageId(),
                       'task_attempt_num': self.task_context.attemptNumber(),
                       'task_server_url': self._server_url})
        post_pro(url=f"{self._server_url}/wait_for_group_success")

    def _close(self, success: bool = True):
        self.logger.info(f"close task task_attempt_num:{self.task_context.attemptNumber()}, root_role_name:{self.spark_env.root_role_name}, success : {success}")
        if success:
            self._wait_for_exit()
        self._process.terminate()
        self._process = None
        self._server_url = None

    def __enter__(self) -> 'SparkTaskServer':
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_val is None:
            self._close(success=True)
        else:
            self._close(success=False)
