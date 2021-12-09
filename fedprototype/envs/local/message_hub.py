import copy
import typing as T

from collections import defaultdict
from queue import Queue


class WatchMessageQueue:
    def __init__(self, counter: T.Dict[T.Tuple[str, str], int]):
        self.queue = Queue()
        self.counter = counter


T_INDEX_DICT = T.DefaultDict[T.Tuple[str, str, str], Queue]
T_WATCH_INDEX_DICT = T.Dict[str, WatchMessageQueue]


class MessageHub:
    def __init__(self):
        self.index_dict: T_INDEX_DICT = defaultdict(Queue)
        self.watch_index_dict: T_WATCH_INDEX_DICT = {}
