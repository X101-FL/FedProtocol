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


class MetaCachePool:
    def __init__(self):
        self.cache_dict = defaultdict(lambda : defaultdict(lambda : []))
        self.key_mapping = defaultdict(lambda : defaultdict(lambda : False))

    def put(self, receiver, message_name, obj):
        self.cache_dict[receiver][message_name].append(obj)
        self.key_mapping[message_name][receiver] = True


class MessageHub:

    def __init__(self):
        self.index_dict = defaultdict(_Queue)
        self.watch_index_dict = {}
        self.cache_pool = defaultdict(MetaCachePool)
