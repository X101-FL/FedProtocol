from typing import Dict, Any
from fedprototype.typing import Client
from threading import Lock, Thread


class ClientThread(Thread):
    def __init__(self, serial_lock: Lock, client: Client, run_kwargs: Dict[str, Any]):
        super(ClientThread, self).__init__()
        self.client = client
        self.serial_lock = serial_lock
        self.run_kwargs = run_kwargs

    def run(self) -> None:
        self.serial_lock.acquire()
        self.client.init()
        self.client.run(**self.run_kwargs)
        self.client.close()
        self.serial_lock.release()
