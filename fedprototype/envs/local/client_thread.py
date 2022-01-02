from typing import Dict, Any
from fedprototype.typing import Client
from threading import Lock, Thread


class ClientThread(Thread):
    def __init__(self, client: Client, entry_func: str, entry_kwargs: Dict[str, Any], serial_lock: Lock):
        super(ClientThread, self).__init__()
        self.client = client
        self.entry_func = entry_func
        self.entry_kwargs = entry_kwargs
        self.serial_lock = serial_lock

    def run(self) -> None:
        self.serial_lock.acquire()
        with self.client.init():
            getattr(self.client, self.entry_func)(**self.entry_kwargs)
        self.serial_lock.release()
