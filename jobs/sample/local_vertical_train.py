import logging
import random

from components.algorithms.vertical.train_clients import ActiveTrainClient, PassiveTrainClient
from components.smc.psi.psi import PSIServer, PSIClient
from fedprototype import BaseClient
from tools.log import LoggerFactory


def gen_datasets(filename):
    data = []

    with open(filename, 'r') as f:
        for line in f.readlines():
            cur_line = line.strip().split(',')
            data.append([float(num) for num in cur_line])
    total_num = len(data)

    active_id_list = random.sample(range(total_num), int(3 * total_num / 4))
    active_id_list = list(sorted(active_id_list))

    passive_id_list = random.sample(range(total_num), int(3 * total_num / 4))
    passive_id_list = list(sorted(passive_id_list))

    active_feature_list, active_label_list, passive_feature_list = [], [], []
    for i, items in enumerate(data):
        if i in active_id_list:
            active_feature_list.append(items[:4])
            active_label_list.append([items[-1]])
        if i in passive_id_list:
            passive_feature_list.append(items[4:-1])

    return {"features": active_feature_list, "label": active_label_list, "id_list": active_id_list}, \
           {"features": passive_feature_list, "id_list": passive_id_list}


class ActiveClient(BaseClient):
    def __init__(self, **kwargs):
        super().__init__("Active")
        self.psi_server = PSIServer()
        self.train_client = ActiveTrainClient(**kwargs)
        self.intersection_id = None

    def init(self):
        self.set_sub_client(self.psi_server, role_rename_dict={"PSIServer": "Active", "PSIClient": "Passive"})
        self.set_sub_client(self.train_client)

    def run(self, id_list, label, features):
        self.psi_server.init()
        self.intersection_id = self.psi_server.run(id_list)

        new_label, new_features = [], []
        for i, item in enumerate(id_list):
            if item in self.intersection_id:
                new_label.append(label[i])
                new_features.append(features[i])

        self.train_client.init()
        self.train_client.run(new_features, new_label)

    def get_intersection_result(self):
        return self.intersection_id

    def close(self):
        pass


class PassiveClient(BaseClient):

    def __init__(self, **kwargs):
        super().__init__("Passive")
        self.psi_client = PSIClient()
        self.train_client = PassiveTrainClient(**kwargs)
        self.intersection_id = None

    def init(self):
        self.set_sub_client(self.psi_client, role_rename_dict={"PSIServer": "Active", "PSIClient": "Passive"})
        self.set_sub_client(self.train_client)

    def run(self, id_list, features):
        self.psi_client.init()
        self.intersection_id = self.psi_client.run(id_list)
        self.psi_client.close()

        new_features = []
        for i, item in enumerate(id_list):
            if item in self.intersection_id:
                new_features.append(features[i])

        self.train_client.init()
        self.train_client.run(new_features)

    def get_intersection_result(self):
        return self.intersection_id

    def close(self):
        pass


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    datafile = "./diabetes.csv"  # 糖尿病人数据
    alpha = 0.00005  # 学习率
    epoch = 100
    feature_size = 4  # 特征维度
    active_run_kwargs, passive_run_kwargs = gen_datasets(datafile)
    print(active_run_kwargs["id_list"][:5])
    print(active_run_kwargs["features"][:5])
    print(active_run_kwargs["label"][:5])

    LoggerFactory.LEVEL = logging.INFO

    env = LocalEnv()
    env.add_client(client=ActiveClient(alpha=alpha, batch_size=32,
                                       epoch=epoch, feature_size=feature_size), **active_run_kwargs)
    env.add_client(client=PassiveClient(alpha=alpha, batch_size=32,
                                       epoch=epoch, feature_size=feature_size), **passive_run_kwargs)
    env.run()
