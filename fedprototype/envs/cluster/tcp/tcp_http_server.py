import io
import time
from collections import deque, defaultdict
from functools import partial
from queue import Queue
from typing import Optional

import requests
from fastapi import FastAPI, File, Header, HTTPException, Response
import uvicorn
import pickle


def start_server(role_name_url_dict, role_name, host="127.0.0.1", port=8081,
                 maximum_start_latency=5, beat_interval=0.1, alive_interval=2):
    """
    maximum_start_latency: 等待role_name服务器开启的最大询问次数
    """
    # TODO: 心跳sleep间隔应该小于对方程序运行时间
    role_server_is_ready = defaultdict(bool)
    message_hub = defaultdict(partial(defaultdict, deque))

    app = FastAPI()

    def is_server_start(server_name):
        """心跳机制：等待server_name服务器开启"""
        start = time.time()

        for i in range(maximum_start_latency + 1):
            try:
                print(f"{time.time() - start:.0f}s passed, still waiting for the {server_name} server to start")
                requests.post(f"{role_name_url_dict[server_name]}/heartbeat")
                print(f"Server {server_name} has been running.")
                return True
            except Exception:
                if i < maximum_start_latency:
                    time.sleep(beat_interval)
                else:
                    print("Reach Max Call Time. Exit process.")
                    raise ConnectionError(
                        f"{role_name} client has call {maximum_start_latency} times,"
                        f"but {server_name} service still not starts")

    @app.post("/heartbeat")
    def heartbeat():
        return None

    @app.post("/clear")
    def clear(sender: Optional[str] = Header(None),
              message_space: Optional[str] = Header(None),
              message_name: Optional[str] = Header(None)):
        message_id = (sender, message_name)
        print(f"+++ clear: {message_hub[message_space][message_id]}")
        if message_hub[message_space][message_id]:
            del message_hub[message_space][message_id]
        return None

    @app.post("/watch")
    def watch(sender: Optional[str] = Header(None),
              message_space: Optional[str] = Header(None),
              message_name: Optional[str] = Header(None)):
        message_id = (sender, message_name)
        if message_hub[message_space][message_id]:
            file = message_hub[message_space][message_id].popleft()
            return Response(content=file)
        else:
            return HTTPException(status_code=404, detail={"Still Not Found!"})

    @app.get("/get_responder")
    def get_responder(sender: Optional[str] = Header(None),
                      message_space: Optional[str] = Header(None),
                      message_name: Optional[str] = Header(None),
                      target_server: Optional[str] = Header(None)):

        MESSAGE_BANK = message_hub
        message_id = (sender, message_name)
        print("get_responder")
        print(message_id, message_space)
        if not role_server_is_ready[target_server]:  # 心跳：等待Server A启动
            role_server_is_ready[target_server] = is_server_start(target_server)

        if role_server_is_ready[target_server]:
            while not MESSAGE_BANK[message_space][message_id]:  # 消息为空，需要等待
                try:
                    time.sleep(alive_interval)  # 每隔几秒问一下另一个server是不是还在服务
                    requests.post(f"{role_name_url_dict[target_server]}/heartbeat")
                except Exception:
                    # exit保证进程能及时停止
                    raise ConnectionError(f"{target_server} client has been crashed.")

            if MESSAGE_BANK[message_space][message_id]:
                file = MESSAGE_BANK[message_space][message_id].popleft()
                return Response(content=file)

    @app.post("/message_sender")
    def message_sender(message_bytes: bytes = File(...),
                       sender: Optional[str] = Header(None),
                       message_space: Optional[str] = Header(None),
                       receiver: Optional[str] = Header(None),
                       target_server: Optional[str] = Header(None)):

        if not role_server_is_ready[target_server]:  # 心跳：等待Server B启动
            role_server_is_ready[target_server] = is_server_start(target_server)

        if role_server_is_ready[target_server]:  # 心跳：监测另一个Server是否挂掉
            try:  # 在Server B启动后，如果post出错，则应把Server B挂掉
                r = requests.post(f"{role_name_url_dict[target_server]}/message_receiver",
                                  files={'message_bytes': message_bytes},
                                  headers={'sender': sender,
                                           'message-space': message_space})
                print("^^^",
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
            message_hub[message_space][(sender, message_name)].append(pickle.dumps(single_message_bytes))
            print("----", (sender, message_space, message_name))

        return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}

    log_config = uvicorn.config.LOGGING_CONFIG  # https://www.uvicorn.org/settings/#logging
    log_config["formatters"]["default"]["fmt"] = "FastAPI %(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "[FastAPI %(levelname)s] %(asctime)s --> (%(message)s)"
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    uvicorn.run(app=app, host=host, port=port, debug=True, access_log=True, log_level='info', use_colors=True)


if __name__ == "__main__":
    start_server({1: 2, 4: 5}, '12', host="127.0.0.1", port=8081)
