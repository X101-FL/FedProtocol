import pickle
import time
from collections import defaultdict
from queue import Empty
from typing import Dict

import requests
import uvicorn
from fastapi import Body, FastAPI, File, Form
from fastapi.responses import PlainTextResponse, Response
from requests.exceptions import ConnectionError

from fedprototype.envs.cluster.tcp.tcp_message_hub import MessageHub
from fedprototype.typing import Host, MessageSpace, Port, RoleName, RootRoleName, Url

SUCCESS_CODE = 200
SUCCESS_RESPONSE = PlainTextResponse(content='OK', status_code=SUCCESS_CODE)


def start_server(host: Host, port: Port, root_role_name: RootRoleName,
                 root_role_name_url_dict: Dict[RootRoleName, Url],
                 maximum_start_latency: int = 20,
                 beat_interval: int = 2,
                 alive_interval: int = 2):
    url_root_role_name_dict = {v: k for k, v in root_role_name_url_dict.items()}
    local_server_url = root_role_name_url_dict[root_role_name]

    target_server_is_ready: Dict[RootRoleName, bool] = defaultdict(bool)
    message_hub: MessageHub = MessageHub(root_role_name_url_dict)

    app = FastAPI()

    @app.post("/heartbeat")
    def heartbeat(requestor_server_url: Url = Body(...)):
        root_role_name = url_root_role_name_dict[requestor_server_url]
        target_server_is_ready[root_role_name] = True
        print(f"get heartbeat request from {root_role_name}@{requestor_server_url}")
        return SUCCESS_RESPONSE

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
                          root_role_name_mapping: Dict[RoleName, RootRoleName] = Body(...)):
        print("server : set_message_space", message_space, root_role_name_mapping)
        message_hub.set_message_space_url(message_space, root_role_name_mapping)
        return SUCCESS_RESPONSE

    @app.post("/receive")
    def receive(message_space: str = Body(...),
                sender: str = Body(...),
                receiver: str = Body(...),
                message_name: str = Body(...)):
        print("server : receive", message_space, sender, receiver, message_name)
        message_space_manager = message_hub.get_message_space_manager(message_space)
        message_queue = message_space_manager.get_message_queue(sender, receiver, message_name)
        target_server_url = message_space_manager.get_target_server_url(sender)

        _res = _wait_for_server_started(target_server_url)
        if _res.status_code != SUCCESS_CODE:
            return _res

        while True:
            try:
                message_bytes = message_queue.get(timeout=alive_interval)
            except Empty:
                _res = _test_for_server_alived(target_server_url)
                if _res.status_code != SUCCESS_CODE:
                    return _error_res(message=f"server may be dead : {target_server_url}",
                                      status_code=423,
                                      lower_res=_res)
                print(f"waiting for messsage {message_space}#{(sender,receive,message_name)}")
            else:
                return Response(content=message_bytes)

    @app.post("/send")
    def send(message_package_bytes: bytes = File(...),
             message_space: str = Form(...),
             sender: str = Form(...),
             receiver: str = Form(...)):
        print("server : send", message_space, sender, receiver, len(message_package_bytes))
        message_space_manager = message_hub.get_message_space_manager(message_space)
        target_server_url = message_space_manager.get_target_server_url(receiver)

        _res = _wait_for_server_started(target_server_url)
        if _res.status_code != SUCCESS_CODE:
            return _res

        _res = _post(url=f"{target_server_url}/put_",
                     data={'message_space': message_space,
                           'sender': sender,
                           'receiver': receiver},
                     files={'message_package_bytes': message_package_bytes})
        if _res.status_code == SUCCESS_CODE:
            return SUCCESS_RESPONSE
        else:
            return _error_res(message=f"failed to put message to server : {target_server_url}/put_",
                              status_code=422,
                              lower_res=_res)

    @app.post("/put_")
    def put_(message_package_bytes: bytes = File(...),
             message_space: str = Form(...),
             sender: str = Form(...),
             receiver: str = Form(...)):
        # message_space_manager.put操作需要加锁，避免和watch操作产生冲突
        # 加锁的写法就是使用with语法
        with message_hub.get_message_space_manager(message_space) as message_space_manager:
            for message_name, message_bytes in pickle.loads(message_package_bytes):
                message_space_manager.put(sender, receiver, message_name, message_bytes)
        return SUCCESS_RESPONSE

    def _wait_for_server_started(target_server_url: Url) -> Response:
        root_role_name = url_root_role_name_dict[target_server_url]

        if target_server_is_ready[root_role_name]:
            return SUCCESS_RESPONSE

        for i in range(maximum_start_latency):
            _res = _test_for_server_alived(target_server_url)
            if _res.status_code == SUCCESS_CODE:
                target_server_is_ready[root_role_name] = True
                return SUCCESS_RESPONSE
            else:
                print(f"wait for server started <{i+1}/{maximum_start_latency}> {root_role_name}@{target_server_url}")
                time.sleep(beat_interval)

        return _error_res(message=f"server is not ready : {root_role_name}@{target_server_url}",
                          status_code=421,
                          lower_res=_res)

    def _test_for_server_alived(target_server_url: Url) -> Response:
        root_role_name = url_root_role_name_dict[target_server_url]
        _res = _post(url=f"{target_server_url}/heartbeat",
                     json=local_server_url)
        if _res.status_code == SUCCESS_CODE:
            print(f"server {root_role_name}@{target_server_url} is alived")
            return SUCCESS_RESPONSE
        else:
            print(f"server {root_role_name}@{target_server_url} is not alived")
            return _error_res(message=f"heartbeat failed : {root_role_name}@{target_server_url}/heartbeat",
                              status_code=420,
                              lower_res=_res)

    def _post(**kwargs) -> Response:
        try:
            return requests.post(**kwargs)
        except ConnectionError as e:
            return _error_res(message=f"internal post error : {e}", status_code=500)

    def _error_res(message: str, status_code: int, lower_res: Response = None) -> Response:
        message = f"> {message}"
        if lower_res is not None:

            if hasattr(lower_res, 'text'):
                lower_message = lower_res.text
            else:
                lower_message = lower_res.body.decode(lower_res.charset)

            message = f"{message}\n{lower_message}"

        return PlainTextResponse(content=message, status_code=status_code)

    # https://www.uvicorn.org/settings/#logging
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["default"]["fmt"] = "FastAPI %(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "[FastAPI %(levelname)s] %(asctime)s --> (%(message)s)"
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    uvicorn.run(app=app, host=host, port=port, debug=True,
                access_log=True, log_level='info', use_colors=True)


if __name__ == "__main__":
    start_server(host="127.0.0.1", port=5601,
                 root_role_name_url_dict={'PartA': 'http://127.0.0.1:5601',
                                          'PartB': 'http://127.0.0.1:5602'})
