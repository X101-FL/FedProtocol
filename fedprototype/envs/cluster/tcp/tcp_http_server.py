import io
import time

import pickle
from typing import Optional

import requests
from fastapi import FastAPI, UploadFile, File, Form, Header
from pydantic import BaseModel
from starlette.responses import FileResponse, StreamingResponse

app = FastAPI()


MESSAGE_BANK = dict()


@app.get("/get_responder")
def get_responder(sender: Optional[str] = Header(None),
                  message_name: Optional[str] = Header(None)):
    global MESSAGE_BANK
    if sender in MESSAGE_BANK:
        if message_name in MESSAGE_BANK[sender]:
            file = MESSAGE_BANK[sender][message_name]
            del MESSAGE_BANK[sender][message_name]
            return StreamingResponse(io.BytesIO(file))
    return '404'


@app.post("/message_sender")
async def message_sender(file: bytes = File(...),
                         target_url: Optional[str] = Header(None),
                         sender: Optional[str] = Header(None),
                         message_name: Optional[str] = Header(None)):
    start = time.time()
    try:
        r = requests.post(target_url, files=file, headers={'sender': sender, 'message_name': message_name})
        return {"status": r, 'time': time.time() - start, 'message_name': message_name}
    except Exception as e:
        return {"message": str(e), 'time': time.time() - start, 'message_name': message_name}


@app.post("/message_receiver")
async def message_receiver(file: bytes = File(...),
                           sender: Optional[str] = Header(None),
                           message_name: Optional[str] = Header(None)):
    global MESSAGE_BANK
    start = time.time()
    try:
        MESSAGE_BANK.setdefault(sender, dict())
        MESSAGE_BANK[sender][message_name] = file
        return {"status": 'success', 'time': time.time() - start, 'message_name': message_name}
    except Exception as e:
        return {"message": str(e), 'time': time.time() - start, 'message_name': message_name}


def run(host="127.0.0.1", port=8081):
    import uvicorn
    uvicorn.run(app='tcp_http_server:app', host=host, port=port, reload=True, debug=True)


if __name__ == "__main__":
    run()
