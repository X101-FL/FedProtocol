import copy
from collections import defaultdict
from queue import Queue


class _Queue(Queue):

    def put(self, item, block=True, timeout=None):
        super().put(copy.deepcopy(item), block, timeout)


class WatchMessageQueue:

    def __init__(self, counter):
        self.queue = _Queue()
        self.counter = counter


class MessageHub:

    def __init__(self):
        self.index_dict = defaultdict(_Queue)
        self.watch_index_dict = {}
