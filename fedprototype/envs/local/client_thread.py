import typing as T
from threading import Lock, Thread

from fedprototype.base_client import BaseClient


class ClientThread(Thread):
    def __init__(
        self, serial_lock: Lock, client: BaseClient, run_kwargs: T.Dict[str, T.Any]
    ):
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
