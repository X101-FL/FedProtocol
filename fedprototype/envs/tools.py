from .base_comm import BaseComm


class CommRenameWrapper(BaseComm):
    def __init__(self, comm, rename_dict):
        self.comm = comm
        self.rename_dict = rename_dict

    def send(self, role_name, message_name, obj):
        role_name = self.rename_dict[role_name]
        self.comm.send(role_name, message_name, obj)

    def get(self, role_name, message_name, timeout=-1):
        role_name = self.rename_dict[role_name]
        return self.comm.get(role_name, message_name, timeout)

    def watch(self, role_message_name_tuple_list, timeout=-1):
        role_message_name_tuple_list = [(self.rename_dict[role], message) for role, message in
                                        role_message_name_tuple_list]
        return self.comm.watch(role_message_name_tuple_list, timeout)
