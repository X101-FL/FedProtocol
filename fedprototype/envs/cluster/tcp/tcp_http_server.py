import io
import time
from typing import Optional

import requests
from fastapi import FastAPI, File, Header
from starlette.responses import StreamingResponse
import uvicorn
import pickle


def start_server(role_name_url_dict, host="127.0.0.1", port=8081):
    message_hub = {}

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

        MESSAGE_BANK = app.extra['message_hub']
        if sender in MESSAGE_BANK:
            if message_name in MESSAGE_BANK[sender]:
                file = MESSAGE_BANK[sender][message_name]
                del MESSAGE_BANK[sender][message_name]
                return StreamingResponse(io.BytesIO(file))
        return '404'

    @app.post("/message_sender")
    async def message_sender(file: bytes = File(...),
                             receiver: Optional[str] = Header(None),  # TODO: 把Optional改成必需
                             sender: Optional[str] = Header(None),
                             message_name: Optional[str] = Header(None)):
        start = time.time()
        try:
            r = requests.post(f"{role_name_url_dict[receiver]}/message_receiver",
                              files=file,
                              headers={'sender': sender,
                                       'message_name': message_name})
            return {"status": r, 'time': time.time() - start, 'message_name': message_name}
        except Exception as e:
            return {"message": str(e), 'time': time.time() - start, 'message_name': message_name}

    @app.post("/message_receiver")
    async def message_receiver(file: bytes = File(...),
                               sender: Optional[str] = Header(None),
                               message_name: Optional[str] = Header(None)):
        start = time.time()
        try:
            message_hub[(sender, message_name)] = file
            print(f"get message {sender, message_name} : {pickle.loads(file)}")
            return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}
        except Exception as e:
            return {"message": str(e), 'time': time.time() - start, 'message_name': message_name}

    uvicorn.run(app=app, host=host, port=port, debug=True)


if __name__ == "__main__":
    start_server({1: 2, 4: 5}, host="127.0.0.1", port=8081)
