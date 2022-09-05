from threading import Lock, Thread
from typing import Any, Dict

from fedprotocol.typing import Worker


class WorkerThread(Thread):
    def __init__(
        self,
        worker: Worker,
        entry_func: str,
        entry_kwargs: Dict[str, Any],
        serial_lock: Lock,
    ):
        super(WorkerThread, self).__init__()
        self.worker = worker
        self.entry_func = entry_func
        self.entry_kwargs = entry_kwargs
        self.serial_lock = serial_lock

    def run(self) -> None:
        self.serial_lock.acquire()
        with self.worker.init():
            getattr(self.worker, self.entry_func)(**self.entry_kwargs)
        self.serial_lock.release()
