from fedprototype.envs.base_env import BaseEnv
from collections import defaultdict


class LocalEnv(BaseEnv):
    def __init__(self):
        self.client_class_param_dict = defaultdict(lambda: [])

    def add_client(self, role_name, client_class, client_param):
        self.client_class_param_dict[role_name].append((client_class, client_param))
        return self

    def run(self):
        # 初始化每个client
        # 开启多线程执行每个client
        pass
