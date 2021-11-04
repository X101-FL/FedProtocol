from .base_comm import BaseComm


class CommRenameWrapper(BaseComm):

    def __init__(self, comm, rename_dict):
        self.comm = comm
        self.rename_dict = rename_dict

    def send(self, receiver, message_name, obj):
        role_name = self.rename_dict[receiver]
        self.comm.send(role_name, message_name, obj)

    def receive(self, sender, message_name, timeout=-1):
        role_name = self.rename_dict[sender]
        return self.comm.get(role_name, message_name, timeout)

    def watch_(self, sender_message_name_tuple_list, timeout=-1):
        sender_message_name_tuple_list = [(self.rename_dict[role], message) for role, message in
                                          sender_message_name_tuple_list]
        return self.comm.watch(sender_message_name_tuple_list, timeout)

    def get_role_name_list(self, role_name_prefix):
        pass
