import copy
import typing as T

from collections import defaultdict
from queue import Queue


class DeepCopyQueue(Queue):
    def put(
        self, item: T.Any, block: bool = True, timeout: T.Optional[int] = None
    ) -> None:
        super().put(copy.deepcopy(item), block, timeout)


class WatchMessageQueue:
    def __init__(self, counter: T.Dict[T.Tuple[str, str], int]):
        self.queue = DeepCopyQueue()
        self.counter = counter


T_INDEX_DICT = T.DefaultDict[T.Tuple[str, str, str], DeepCopyQueue]
T_WATCH_INDEX_DICT = T.Dict[str, WatchMessageQueue]


class MessageHub:
    def __init__(self):
        self.index_dict: T_INDEX_DICT = defaultdict(DeepCopyQueue)
        self.watch_index_dict: T_WATCH_INDEX_DICT = {}
