from threading import Thread


class ClientThread(Thread):

    def __init__(self, serial_lock, client, run_kwargs):
        super(ClientThread, self).__init__()
        self.client = client
        self.serial_lock = serial_lock
        self.run_kwargs = run_kwargs

    def run(self):
        self.serial_lock.acquire()
        self.client.init()
        self.client.run(**self.run_kwargs)
        self.client.close()
        self.serial_lock.release()
