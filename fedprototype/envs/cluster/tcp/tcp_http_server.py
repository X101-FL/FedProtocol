from socket import timeout
import time
from collections import deque, defaultdict
from functools import partial
from typing import Optional, Set, Dict, Tuple, Any
from crypten import log

import requests
from requests.exceptions import RequestException
from fastapi import FastAPI, File,  HTTPException, Response, Body, Form
import uvicorn
import pickle
from fedprototype.envs.local.message_hub import MessageHub
from fedprototype.typing import MessageName, MessageSpace, RoleName, Sender, Url, Host, Port, MessageID
import logging


def start_server(host: Host, port: Port,
                 role_name_url_dict: Dict[RoleName, Url],
                 maximum_start_latency: int = 5,
                 beat_interval: int = 2,
                 alive_interval: int = 2):
    url_role_name_dict: Dict[Url, RoleName] = {v: k for k, v
                                               in role_name_url_dict.items()}
    target_server_is_ready: Dict[RoleName, bool] = defaultdict(bool)
    message_hub: Dict[MessageSpace, Dict[MessageID, deque]] = \
        defaultdict(partial(defaultdict, deque))

    app = FastAPI()
    logger = logging.getLogger("comm_server")

    @app.post("/heartbeat")
    def heartbeat():
        return 0

    # @app.post("/clear")
    # def clear(sender: Optional[str] = Header(None),
    #           message_space: Optional[str] = Header(None),
    #           message_name: Optional[str] = Header(None)):
    #     message_id = (sender, message_name)
    #     if message_hub[message_space][message_id]:
    #         del message_hub[message_space][message_id]
    #     return None

    # @app.post("/watch")
    # def watch(sender: Optional[str] = Header(None),
    #           message_space: Optional[str] = Header(None),
    #           message_name: Optional[str] = Header(None)):
    #     message_id = (sender, message_name)
    #     if message_hub[message_space][message_id]:
    #         file = message_hub[message_space][message_id].popleft()
    #         return Response(content=file)
    #     else:
    #         return HTTPException(status_code=404, detail={"Still Not Found!"})

    @app.get("/receive")
    def receive(message_space: str = Body(...),
                sender: str = Body(...),
                receiver: str = Body(...),
                message_name: str = Body(...),
                target_server: str = Body(...)):
        _wait_for_server_started(target_server)

        message_id = (sender, receiver, message_name)

        while not message_hub[message_space][message_id]:  # 消息为空，需要等待
            _assert_server_alived(target_server)
            logger.debug(f"waiting for messsage {message_space}#{message_id}")
            time.sleep(alive_interval)

        message_bytes = message_hub[message_space][message_id].popleft()
        return Response(content=message_bytes)

    @app.post("/send")
    def send(message_bytes: bytes = File(...),
             message_space: str = Form(...),
             sender: str = Form(...),
             receiver: str = Form(...),
             target_server: str = Form(...)):
        _wait_for_server_started()

        if target_server_is_ready[target_server]:  # 心跳：监测另一个Server是否挂掉
            try:  # 在Server B启动后，如果post出错，则应把Server B挂掉
                r = requests.post(f"{role_name_url_dict[target_server]}/message_receiver",
                                  files={'message_bytes': message_bytes},
                                  headers={'sender': sender,
                                           'message-space': message_space})
                print(">>>",
                      {'sender': sender, 'message_space': message_space, 'receiver': receiver, 'target': target_server})
                return {"status": r.status_code}
            except Exception as e:
                raise ConnectionError(f"{role_name} client has been crashed.")

    @app.post("/message_receiver")
    def message_receiver(message_bytes: bytes = File(...),
                         sender: Optional[str] = Header(None),
                         message_space: Optional[str] = Header(None)):
        start = time.time()
        for (message_name, single_message_bytes) in pickle.loads(message_bytes):
            message_hub[message_space][(sender, message_name)].append(
                pickle.dumps(single_message_bytes))
            print("--+--", (sender, message_space, message_name))

        return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}

    # def is_server_start(server_name):
    #     """心跳机制：等待server_name服务器开启"""
    #     start = time.time()

    #     for i in range(maximum_start_latency + 1):
    #         print(
    #             f"{time.time() - start:.0f}s passed, still waiting for the {server_name} server to start")
    #         if one_alive_test(server_name, i):
    #             return True

    # def one_alive_test(server_name, cnt):
    #     """心跳机制：等待server_name服务器开启"""
    #     try:
    #         requests.post(f"{role_name_url_dict[server_name]}/heartbeat")
    #         # print(f"Server {server_name} has been running.")
    #         return True
    #     except Exception:
    #         if cnt < maximum_start_latency:
    #             time.sleep(beat_interval)
    #             return False
    #         else:
    #             print("Reach Max Call Time. Exit process.")
    #             raise ConnectionError(
    #                 f"{role_name} client has call {maximum_start_latency} times,"
    #                 f"but {server_name} service still not starts")

    def _wait_for_server_started(target_server: Url):
        target_role_name = url_role_name_dict[target_server]
        for i in range(maximum_start_latency):

            if target_server_is_ready[target_role_name]:
                return

            try:
                _assert_server_alived(target_server)
                target_server_is_ready[target_role_name] = True
                return
            except RequestException as e:
                pass

            logging.debug(
                f"wait for server started <{i+1}/{maximum_start_latency}> {target_role_name}:{target_server}")
            time.sleep(beat_interval)

        raise e

    def _assert_server_alived(target_server: Url):
        target_role_name = url_role_name_dict[target_server]
        logging.debug(
            f"test for server alived {target_role_name}:{target_server}")
        res = requests.post(f"{target_server}/heartbeat")
        if res.status_code == 200:
            logging.debug(
                f"server {target_role_name}:{target_server} is alived")
        else:
            raise RequestException(request=res.request, response=res)

    def _post(*args, **kwargs) -> Any:
        res = requests.post(*args, **kwargs)
        if res.status_code != 200:
            raise RequestException(request=res.request, response=res)
        if res.headers.get('content-type', None) == 'application/json':
            return res.json()
        return res.content

    # https://www.uvicorn.org/settings/#logging
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["default"]["fmt"] = "FastAPI %(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "[FastAPI %(levelname)s] %(asctime)s --> (%(message)s)"
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level='info', use_colors=True)


if __name__ == "__main__":
    start_server({1: 2, 4: 5}, '12', host="127.0.0.1", port=8081)
