from threading import Thread


class ClientThread(Thread):

    def __init__(self, lock, client, run_kwargs):
        super(ClientThread, self).__init__()
        self.client = client
        self.lock = lock
        self.run_kwargs = run_kwargs

    def run(self):
        self.lock.acquire()
        self.client.init()
        self.client.run(**self.run_kwargs)
        self.client.close()
        self.lock.release()
