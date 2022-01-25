from socket import timeout
import time
from collections import deque, defaultdict
from functools import partial
from typing import Optional, Set, Dict, Tuple, Any

import requests
from requests.exceptions import RequestException
from fastapi import FastAPI, File,  HTTPException, Response, Body, Form
import uvicorn
import pickle
from fedprototype.typing import MessageName, MessageSpace, RoleName, RootRoleName, Sender, Url, Host, Port, MessageID
import logging
from fedprototype.envs.cluster.tcp.message_hub import MessageHub, MessageSpaceManager


def start_server(host: Host, port: Port,
                 root_role_name_url_dict: Dict[RootRoleName, Url],
                 maximum_start_latency: int = 5,
                 beat_interval: int = 2,
                 alive_interval: int = 2):
    target_server_is_ready: Dict[RootRoleName, bool] = defaultdict(bool)
    url_root_role_name_dict = {v: k for k, v in root_role_name_url_dict}
    message_hub: MessageHub = MessageHub(root_role_name_url_dict)

    app = FastAPI()

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

    @app.post("/set_message_space")
    def set_message_space(message_space: MessageSpace = Body(...),
                          role_name_to_root_dict: Dict[RoleName, RootRoleName] = Body(...)):
        print("server : set_message_space", message_space, role_name_to_root_dict)
        message_hub.set_message_space_url(message_space, role_name_to_root_dict)
        return 0

    @app.post("/receive")
    def receive(message_space: str = Body(...),
                sender: str = Body(...),
                receiver: str = Body(...),
                message_name: str = Body(...)):
        message_space_manager = message_hub.get_message_space_manager(message_space)
        message_queue = message_space_manager.get_message_queue(sender,receiver,message_name)
        target_server_url = message_space_manager.role_name_url_dict[receiver]

        while message_queue.empty():
            
        if not message_queue.empty():
            
        
        _wait_for_server_started(target_server_url)

        

        while not message_hub[message_space][message_id]:  # 消息为空，需要等待
            _assert_server_alived(target_server)
            logger.debug(f"waiting for messsage {message_space}#{message_id}")
            time.sleep(alive_interval)

        message_bytes = message_hub[message_space][message_id].popleft()
        return Response(content=message_bytes)

    @app.post("/send")
    def send(message_package_bytes: bytes = File(...),
             message_space: str = Form(...),
             sender: str = Form(...),
             receiver: str = Form(...)):
        message_space_manager = message_hub.get_message_space_manager(message_space)
        target_server_url = message_space_manager.role_name_url_dict[receiver]
        _wait_for_server_started(target_server_url)

        _post(f"{target_server_url}/put_",
              data={'message_space': message_space,
                    'sender': sender,
                    'receiver': receiver},
              files={'message_package_bytes': message_package_bytes})
        return 0

    @app.post("/put_")
    def put_(message_package_bytes: bytes = File(...),
             message_space: str = Form(...),
             sender: str = Form(...),
             receiver: str = Form(...)):
        with message_hub.get_message_space_manager(message_space) as message_space_manager:
            for message_name, message_bytes in pickle.loads(message_package_bytes):
                message_space_manager.put(sender, receiver, message_name, message_bytes)
        return 0

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

    def _wait_for_server_started(target_server_url: Url):
        root_role_name = url_root_role_name_dict[target_server_url]
        if target_server_is_ready[root_role_name]:
            return
        for i in range(maximum_start_latency):
            try:
                _assert_server_alived(target_server_url)
                target_server_is_ready[root_role_name] = True
                return
            except RequestException as e:
                pass

            logging.debug(
                f"wait for server started <{i+1}/{maximum_start_latency}> {root_role_name}:{target_server_url}")
            time.sleep(beat_interval)

        raise e

    def _assert_server_alived(target_server: Url):
        target_role_name = url_root_role_name_dict[target_server]
        logging.debug(f"test for server alived {target_role_name}:{target_server}")
        _post(url=f"{target_server}/heartbeat")
        logging.debug(f"server {target_role_name}:{target_server} is alived")

    def _post(**kwargs) -> Any:
        res = requests.post(**kwargs)
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
