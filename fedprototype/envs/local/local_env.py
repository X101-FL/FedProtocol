from fedprototype.envs.base_env import BaseEnv
from collections import defaultdict


class LocalEnv(BaseEnv):
    def __init__(self):
        self.client_info_dict = defaultdict(lambda: [])

    def add_client(self, client, **run_kwargs):
        self.client_info_dict[client.role_name].append((client, run_kwargs))
        return self

    def run(self):
        # 初始化每个client
        # 开启多线程执行每个client
        pass
