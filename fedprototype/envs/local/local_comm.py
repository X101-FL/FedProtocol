from fedprototype.envs.base_comm import BaseComm


class LocalComm(BaseComm):

    def send(self, role_name, message_name, obj):
        pass

    def get(self, role_name, message_name, timeout=-1):
        pass

    def watch(self, role_message_name_tuple_list, timeout=-1):
        pass
