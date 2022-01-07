import io
import time
from collections import deque, defaultdict
from typing import Optional

import requests
from fastapi import FastAPI, File, Header, HTTPException
from starlette.responses import StreamingResponse
import uvicorn
import pickle


def start_server(role_name_url_dict, role_name, host="127.0.0.1", port=8081):
    # message_hub: Dict[(Sender, MessagName), Queue] = {}  # TODO: 改成message space（考虑子协议）
    # role_server_is_ready:Dict[RoleName,bool] = {}
    def func():
        return False

    role_server_is_ready = defaultdict(func)
    message_hub = defaultdict(deque)  # TODO: 改成message space（考虑子协议）

    MAX_TIME = 10  # 等待另一个server启动的最大时间

    app = FastAPI()

    @app.get("/")
    async def root():
        pass

    @app.get("/get_role_name_url")
    async def get_role_name_url():
        return role_name_url_dict

    @app.get("/get_message_hub")
    async def get_message_hub():
        return message_hub

    @app.post("/set_message_hub")
    async def set_message_hub(key: str, value: str):
        message_hub[key] = value
        return None

    @app.get("/get_responder")
    async def get_responder(sender: Optional[str] = Header(None),
                            message_name: Optional[str] = Header(None)):

        MESSAGE_BANK = message_hub
        message_id = (sender, message_name)

        if not role_server_is_ready[sender]:  # 心跳：等待Server A启动
            for i in range(MAX_TIME + 1):
                try:
                    r = requests.get(f"{role_name_url_dict[sender]}")
                    # 完成了role_server_is_ready
                    role_server_is_ready[sender] = True
                    print(f"Server {sender} has been running.")
                    break
                except Exception as e:
                    if i < MAX_TIME:
                        interval = 1
                        time.sleep(interval)
                    else:
                        raise ConnectionError(f"{role_name} client has been waiting for {MAX_TIME * interval}s, "
                                              f"but {sender} service has not been started.")

        if role_server_is_ready[sender]:
            while not MESSAGE_BANK[message_id]:  # 消息为空，需要等待
                try:
                    time.sleep(1)  # 每隔1秒问一下另一个server是不是还在服务
                    print(f"{role_name_url_dict[sender]}")
                    r = requests.get(f"{role_name_url_dict[sender]}")
                except Exception as e:
                    raise ConnectionError(f"{sender} client has been crashed.")

            if MESSAGE_BANK[message_id]:
                file = MESSAGE_BANK[message_id].popleft()
                return StreamingResponse(io.BytesIO(file))

    @app.post("/message_sender")
    async def message_sender(file: bytes = File(...),
                             receiver: Optional[str] = Header(None),  # TODO: 把Optional改成必需
                             message_name: Optional[str] = Header(None)):
        start = time.time()

        if not role_server_is_ready[receiver]:  # 心跳：等待Server B启动
            for i in range(MAX_TIME + 1):
                try:
                    r = requests.get(f"{role_name_url_dict[receiver]}")
                    # 完成了role_server_is_ready
                    role_server_is_ready[receiver] = True
                    print(f"Server {receiver} has been running.")
                    break
                except Exception as e:
                    if i < MAX_TIME:
                        interval = 1
                        time.sleep(interval)
                    else:
                        raise ConnectionError(f"{role_name} client has been waiting for {MAX_TIME * interval}s, "
                                              f"but {receiver} service has not been started.")

        if role_server_is_ready[receiver]:  # 心跳：监测另一个Server是否挂掉
            try:  # 在Server B启动后，如果post出错，那应该是Server B挂掉
                r = requests.post(f"{role_name_url_dict[receiver]}/message_receiver",
                                  files={'file': file},
                                  headers={'sender': role_name,
                                           'receiver': receiver,
                                           'message-name': message_name})
                return {"status": 'success', 'time': f"{time.time() - start:.6f}s", 'message_name': message_name}
            except Exception as e:
                raise ConnectionError(f"{role_name} client has been crashed.")

    @app.post("/message_receiver")
    async def message_receiver(file: bytes = File(...),
                               sender: Optional[str] = Header(None),
                               message_name: Optional[str] = Header(None)):
        start = time.time()
        # print(f"get message {sender, message_name} : {pickle.loads(file)}")
        message_hub[(sender, message_name)].append(file)
        # print(message_hub)
        return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}

    # https://www.uvicorn.org/settings/#logging
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["default"]["fmt"] = "FastAPI %(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "[FastAPI %(levelname)s] %(asctime)s --> (%(message)s)"
    log_config["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
    uvicorn.run(app=app, host=host, port=port, debug=True, access_log=True, log_level='info', use_colors=True)


if __name__ == "__main__":
    start_server({1: 2, 4: 5}, '12', host="127.0.0.1", port=8081)
