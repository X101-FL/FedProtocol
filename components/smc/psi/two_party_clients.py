from fedprototype import BaseClient


class Party1(BaseClient):
    def __init__(self, param1, param2, param3):
        super().__init__("party1")
        self.param1 = param1
        self.param2 = param2
        self.param3 = param3

    def init(self):
        public_key = None
        self.comm.send("party2", "public_key", public_key)

    def run(self, id_list):
        intersect_of_id = [1, 2, 3]
        return intersect_of_id

    def close(self):
        pass


class Party2(BaseClient):
    def __init__(self, param1, param2, param3):
        super().__init__("party2")
        self.param1 = param1
        self.param2 = param2
        self.param3 = param3

    def init(self):
        public_key = self.comm.get("party1", "public_key")

    def run(self, id_list):
        return None

    def close(self):
        pass
