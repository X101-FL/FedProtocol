from fedprototype.envs.base_comm import BaseComm


class LocalComm(BaseComm):

    def __init__(self, role_name, serial_lock, message_body, logger):
        self.role_name = role_name
        self.serial_lock = serial_lock
        self.message_body = message_body
        self.logger = logger

    def send(self, receiver, message_name, obj):
        message_id = self._get_message_id(self.role_name, receiver, message_name)
        self.message_body[message_id].put(obj)
        self.logger.debug(f"Successfully put {message_id} into message body!")

    def receive(self, sender, message_name, timeout=-1):
        message_id = self._get_message_id(sender, self.role_name, message_name)
        message_queue = self.message_body[message_id]
        if message_queue.empty():
            self.serial_lock.release()
            self.logger.debug(f"Can't find {message_id}, release lock.")
            data = message_queue.get(timeout)
            self.serial_lock.acquire()
            self.logger.debug(f"Successfully get {message_id}, acquire lock")
        else:
            data = message_queue.get(timeout)
        return data

    def watch(self, role_message_name_tuple_list, timeout=-1):
        pass

    @staticmethod
    def _get_message_id(sender, receiver, message_name):
        return sender, receiver, message_name
