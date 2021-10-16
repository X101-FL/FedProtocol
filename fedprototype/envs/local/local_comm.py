from fedprototype.envs.base_comm import BaseComm


class LocalComm(BaseComm):
    def send(self, obj, message_name, role_name, role_index=0):
        pass

    def get(self, message_name, role_name, role_index=0, timeout=-1):
        pass

    def watch(self, message_role_name_index_tuple_list, timeout=-1):
        pass
