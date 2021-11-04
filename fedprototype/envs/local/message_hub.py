from queue import Queue
from collections import defaultdict


class WatchMessageQueue:

    def __init__(self, counter):
        self.queue = Queue()
        self.counter = counter


class MessageHub:

    def __init__(self):
        self.index_dict = defaultdict(Queue)
        self.watch_index_dict = {}
