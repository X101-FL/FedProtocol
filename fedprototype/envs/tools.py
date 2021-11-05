from .base_comm import BaseComm


class CommRenameWrapper(BaseComm):

    def __init__(self, role_name, comm, rename_dict):
        self.role_name = role_name
        self.comm = comm
        self.rename_dict = rename_dict
        self.reversed_dict = {v: k for k, v in rename_dict.items()}
        self.other_role_name_set = (set(rename_dict.keys()) - {role_name})

    def send(self, receiver, message_name, obj):
        role_name = self.rename_dict[receiver]
        self.comm.send(role_name, message_name, obj)

    def receive(self, sender, message_name, timeout=-1):
        role_name = self.rename_dict[sender]
        return self.comm.receive(role_name, message_name, timeout)

    def watch_(self, sender_message_name_tuple_list, timeout=-1):
        sender_message_name_tuple_list = [(self.rename_dict[role], message) for role, message in
                                          sender_message_name_tuple_list]
        for sender, message_name, data in self.comm.watch_(sender_message_name_tuple_list, timeout):
            yield self.reversed_dict[sender], message_name, data

    def get_role_name_list(self, role_name_prefix):
        return [role_name for role_name in self.other_role_name_set if role_name.startswith(role_name_prefix)]
