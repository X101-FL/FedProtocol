import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
import pickle
from collections import defaultdict
from fastapi import Body, File, Form, Response
from fedprototype.typing import MessageSpace, RoleName, RootRoleName
from typing import Dict


def start_server(host="127.0.0.1", port=8081):
    app = FastAPI()
    message_hub = {}

    @app.post("/set_message_space")
    def set_message_space(message_space: MessageSpace = Body(...),
                          role_name_to_root_dict: Dict[RoleName, RootRoleName] = Body(...)):
        print(message_space, role_name_to_root_dict)
        return 0

    @app.post("/receive")
    def receive(key: str = Body(...), haha: str = Body(...)):
        return Response(content=message_hub[key])

    @app.post("/test")
    def test():
        return 123

    @app.post("/send")
    def send(message_bytes: bytes = File(...),
             key: str = Form(...)):
        message_hub[key] = message_bytes

    uvicorn.run(app=app, host=host, port=port, debug=True)


if __name__ == "__main__":
    start_server(host="127.0.0.1", port=8081)

if 1 == 2:
    import requests
    import pickle
    requests.post("http://127.0.0.1:8081/send",
                  data={'key': 'haha'},
                  files={'message_bytes': pickle.dumps([1, 2, 3])})

    requests.post("http://127.0.0.1:8081/receive",
                  json={'key': 'haha', 'haha': 'biubiu'})

    requests.post("http://127.0.0.1:8081/test")

    requests.post("http://127.0.0.1:8081/set_message_space",
                  json={'message_space': 'haha', 'role_name_to_root_dict': {'a': 'b', 'c': 'c'}})
