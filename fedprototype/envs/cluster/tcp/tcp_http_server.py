import pickle
import time
from collections import defaultdict
from queue import Empty
from typing import Callable, Dict, List, Tuple

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


def start_server(host: Host, port: Port, root_role_name: RootRoleName,
                 root_role_name_url_dict: Dict[RootRoleName, Url],
                 logger_factory: BaseLoggerFactory = LocalLoggerFactory,
                 maximum_start_latency: int = 20,
                 beat_interval: int = 2,
                 alive_interval: int = 2):
    url_root_role_name_dict = {v: k for k, v in root_role_name_url_dict.items()}
    local_server_url = root_role_name_url_dict[root_role_name]

    target_server_is_ready: Dict[RootRoleName, bool] = defaultdict(bool)
    message_hub: MessageHub = MessageHub(root_role_name_url_dict)

    logger = logger_factory.get_logger("[TcpServer]")
    logger.info(f"root_role_name={root_role_name}")
    logger.info(f"root_role_name_url_dict={root_role_name_url_dict}")

    app = FastAPI()

    @app.post("/heartbeat")
    def heartbeat(requestor_server_url: Url = Body(...)):
        root_role_name = url_root_role_name_dict[requestor_server_url]
        target_server_is_ready[root_role_name] = True
        logger.debug(f"get heartbeat request from {root_role_name}@{requestor_server_url}")
        return SUCCESS_RESPONSE

    @app.post("/set_message_space")
    def set_message_space(message_space: MessageSpace = Body(...),
                          root_role_bind_mapping: Dict[RoleName, RootRoleName] = Body(...)):
        logger.debug(f"set_message_space : message_space={message_space}, root_role_bind_mapping={root_role_bind_mapping}")
        message_hub.set_message_space_url(message_space, root_role_bind_mapping)
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
        target_server_url = message_space_manager.get_target_server_url(sender)

        _wait_for_server_started(target_server_url)

        while True:
            try:
                message_bytes = message_queue.get(timeout=alive_interval)
            except Empty:
                _test_for_server_alived(target_server_url)
                logger.debug(f"waiting for messsage : message_space={message_space}, "
                             f"sender={sender}, receiver={receiver}, message_name={message_name}")
            else:
                return Response(content=message_bytes)

    @app.post("/send")
    def send(message_package_bytes: MessageBytes = File(...),
             message_space: MessageSpace = Form(...),
             sender: Sender = Form(...),
             receiver: Receiver = Form(...)):
        logger.debug(f"send : message_space={message_space}, sender={sender}, "
                     f"receiver={receiver}, message_package_bytes.size={len(message_package_bytes)}")
        message_space_manager = message_hub.get_message_space_manager(message_space)
        target_server_url = message_space_manager.get_target_server_url(receiver)

        _wait_for_server_started(target_server_url)

        _try_func(func=_post,
                  url=f"{target_server_url}/put_",
                  data={'message_space': message_space,
                        'sender': sender,
                        'receiver': receiver},
                  files={'message_package_bytes': message_package_bytes},
                  detail=f"failed to put message to server : {target_server_url}/put_",
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
                             f"receiver={receiver}, message_name={message_name}, message_bytes.size={len(message_bytes)}")
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

        while not watched_item_list:
            try:
                watched_item = watch_manager.get(timeout=alive_interval)
                watched_item_list.append(watched_item)
            except Empty:
                unreceived_list = watch_manager.unreceived()
                # 随机选择一个尚未收到消息的sender，测试该sender是否已经启动/挂掉
                selected_unreceived_tuple: Tuple[Sender, MessageName] = unreceived_list[np.random.randint(low=0, high=len(unreceived_list))]
                selected_unreceived_sender = selected_unreceived_tuple[0]
                selected_unreceived_server_url = message_space_manager.get_target_server_url(selected_unreceived_sender)
                _wait_for_server_started(selected_unreceived_server_url)
                _test_for_server_alived(selected_unreceived_server_url)
                logger.debug(f"waiting for watched messages ...")
            else:
                while not watch_manager.empty():
                    watched_item_list.append(watch_manager.get())
        fetch_res = {'finished': False, 'data': watched_item_list}
        if watch_manager.is_all_got():
            fetch_res['finished'] = True
            message_space_manager.cancel_watch(receiver)
        return Response(content=pickle.dumps(fetch_res))

    def _wait_for_server_started(target_server_url: Url) -> None:
        root_role_name = url_root_role_name_dict[target_server_url]

        for i in range(maximum_start_latency):

            if target_server_is_ready[root_role_name]:
                return

            try:
                _test_for_server_alived(target_server_url)
                target_server_is_ready[root_role_name] = True
                return
            except HTTPException:
                logger.debug(f"wait for server started <{i+1}/{maximum_start_latency}> {root_role_name}@{target_server_url}")
                time.sleep(beat_interval)

        _try_func(func=_test_for_server_alived,
                  target_server_url=target_server_url,
                  detail=f"server is not ready : {root_role_name}@{target_server_url}",
                  status_code=421)

    def _test_for_server_alived(target_server_url: Url) -> None:
        root_role_name = url_root_role_name_dict[target_server_url]
        _try_func(func=_post,
                  url=f"{target_server_url}/heartbeat",
                  json=local_server_url,
                  detail=f"server may be dead : {root_role_name}@{target_server_url}/heartbeat",
                  status_code=420)

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
