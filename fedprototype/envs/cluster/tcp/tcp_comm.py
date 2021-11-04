from fedprototype.envs.base_comm import BaseComm


class TCPComm(BaseComm):

    def __init__(self, role_name_ip_dict, role_name):
        super().__init__()
        self.role_name_ip_dict = role_name_ip_dict
        self.role_name = role_name

    def send(self, receiver, message_name, obj):
        pass

    def receive(self, sender, message_name, timeout=-1):
        pass

    def watch(self, sender_message_name_tuple_list, timeout=-1):
        pass
