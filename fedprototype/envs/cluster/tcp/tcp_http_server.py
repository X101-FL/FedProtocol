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
    # TODO: 如果这个server还没开启，就每隔5秒尝试一次，直到它启动后，设为true
    # role_server_is_ready:Dict[RoleName,bool] = {}
    role_server_is_ready = defaultdict(False)
    message_hub = defaultdict(deque)  # TODO: 改成message space（考虑子协议）

    app = FastAPI()

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

        # TODO：添加心跳机制，检测另一个服务是否挂掉
        # TODO: 这responder内sleep，超过5次后询问一次
        MESSAGE_BANK = message_hub
        message_id = (sender, message_name)
        if MESSAGE_BANK[message_id]:
            file = MESSAGE_BANK[message_id].popleft()
            return StreamingResponse(io.BytesIO(file))
        else:
            app.extra.setdefault('heatbeat_count', 0)
            app.extra['heatbeat_count'] += 1
            if app.extra['heatbeat_count'] == 5:
                try:
                    r = requests.get(f"{role_name_url_dict[sender]}")
                except Exception as e:
                    print(e)
                    # TODO: 加入了心跳之后要做什么
            raise HTTPException(status_code=404, detail="Item not found")

    @app.post("/message_sender")
    async def message_sender(file: bytes = File(...),
                             receiver: Optional[str] = Header(None),  # TODO: 把Optional改成必需
                             message_name: Optional[str] = Header(None)):
        print("========== message_sender app post")
        start = time.time()
        # TODO: 加入了重试机制，不过看起来比较粗糙
        for i in range(6):
            try:
                print(f"----- {role_name_url_dict[receiver]}/message_receiver\n")
                r = requests.post(f"{role_name_url_dict[receiver]}/message_receiver",
                                  files={'file': file},
                                  headers={'sender': role_name,
                                           'receiver': receiver,
                                           'message-name': message_name})
                # 完成了role_server_is_ready
                role_server_is_ready[receiver] = True
                print(f"code is:{r.status_code}")
                return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}
            except Exception as e:
                print(e)
                if i < 5:
                    time.sleep(5)
                else:
                    return {"message": repr(e), 'time': time.time() - start, 'message_name': message_name}

    @app.post("/message_receiver")
    async def message_receiver(file: bytes = File(...),
                               sender: Optional[str] = Header(None),
                               message_name: Optional[str] = Header(None)):
        start = time.time()
        try:
            print(f"get message {sender, message_name} : {pickle.loads(file)}")
            message_hub[(sender, message_name)].append(file)
            print(message_hub)
            return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}
        except Exception as e:
            return {"message": str(e), 'time': time.time() - start, 'message_name': message_name}

    uvicorn.run(app=app, host=host, port=port, debug=True)


if __name__ == "__main__":
    start_server({1: 2, 4: 5}, '12', host="127.0.0.1", port=8081)
