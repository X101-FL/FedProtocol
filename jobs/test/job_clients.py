from fedprototype import BaseClient


#
# class Level3AClient(BaseClient):
#     def __init__(self, name):
#         super(Level3AClient, self).__init__('level3a')
#         self.name = name
#
#     def init(self) -> None:
#         pass
#
#     def run(self):
#         self.logger.info(f"name : {self.name}")
#         self.comm.send('level3b', 'whoami', f'{self._track_name}#{self.name}')
#         self.comm.send('level3b', 'whoami', f'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
#
#     def close(self) -> None:
#         pass


class Level2AClient(BaseClient):
    def __init__(self, name):
        super(Level2AClient, self).__init__('level2a')
        self.name = name
        # self.l3_client1 = Level3AClient('client1')
        # self.l3_client2 = Level3AClient('client2')
        # self.l3_client3 = Level3AClient('client3')

    def init(self) -> None:
        # self.set_sub_client(self.l3_client1, role_rename_dict={'level3a': 'level2a', 'level3b': 'level2b'})
        # self.set_sub_client(self.l3_client2, role_rename_dict={'level3a': 'level2a', 'level3b': 'level2b'})
        # self.set_sub_client(self.l3_client3, role_rename_dict={'level3a': 'level2a', 'level3b': 'level2b'})
        pass

    def run(self):
        self.logger.info(f"name : {self.name}")
        self.comm.send('level2b', 'whoami', f'{self.track_name}')

        # self.l3_client3.init()
        # self.l3_client3.run()
        # self.l3_client3.close()
        #
        # self.l3_client2.init()
        # self.l3_client2.run()
        # self.l3_client2.close()
        #
        # self.l3_client1.init()
        # self.l3_client1.run()
        # self.l3_client1.close()

    def close(self) -> None:
        pass


class Level1AClient(BaseClient):
    def __init__(self):
        super().__init__("level1a")
        self.l2_client1 = Level2AClient('client1')
        self.l2_client2 = Level2AClient('client2')

    def init(self):
        self.set_sub_client(self.l2_client1,
                            sub_message_space_name='sub1',
                            role_rename_dict={"level2a": "level1a", "level2b": "level1b"})
        self.set_sub_client(self.l2_client2,
                            sub_message_space_name='sub2',
                            role_rename_dict={"level2a": "level1a", "level2b": "level1b"})

    def run(self):
        self.l2_client1.init()
        self.l2_client1.run()
        self.l2_client1.run()
        self.l2_client1.close()

        self.comm.send('level1b', 'whoami', f'{self.track_name}')

        self.l2_client2.init()
        self.l2_client2.run()
        self.l2_client2.close()

        return None

    def close(self):
        pass


# class Level3BClient(BaseClient):
#     def __init__(self, name):
#         super(Level3BClient, self).__init__('level3b')
#         self.name = name
#
#     def init(self) -> None:
#         pass
#
#     def run(self):
#         self.logger.info(f"name : {self.track_name}#{self.name}")
#         whoareyou = self.comm.receive('level3a', 'whoami')
#         self.logger.info(f"get whoareyou : {whoareyou}")
#         return None
#
#     def close(self) -> None:
#         pass


class Level2BClient(BaseClient):
    def __init__(self, name):
        super(Level2BClient, self).__init__('level2b')
        self.name = name
        # self.l3_client1 = Level3BClient('client1')
        # self.l3_client2 = Level3BClient('client2')
        # self.l3_client3 = Level3BClient('client3')

    def init(self) -> None:
        # self.set_sub_client(self.l3_client1, role_rename_dict={'level3a': 'level2a', 'level3b': 'level2b'})
        # self.set_sub_client(self.l3_client2, role_rename_dict={'level3a': 'level2a', 'level3b': 'level2b'})
        # self.set_sub_client(self.l3_client3, role_rename_dict={'level3a': 'level2a', 'level3b': 'level2b'})
        pass

    def run(self):
        self.logger.info(f"name : {self.track_name}")
        whoareyou = self.comm.receive('level2a', 'whoami')
        self.logger.info(f"get whoareyou : {whoareyou}")

        # self.l3_client3.init()
        # self.l3_client3.run()
        # self.l3_client3.close()
        #
        # self.l3_client2.init()
        # self.l3_client2.run()
        # self.l3_client2.close()
        #
        # self.l3_client1.init()
        # self.l3_client1.run()
        # self.l3_client1.close()

    def close(self) -> None:
        self.comm.clear()


class Level1BClient(BaseClient):
    def __init__(self):
        super().__init__("level1b")
        self.l2_client1 = Level2BClient('client1')
        self.l2_client2 = Level2BClient('client2')

    def init(self):
        self.set_sub_client(self.l2_client1,
                            sub_message_space_name='sub1',
                            role_rename_dict={"level2a": "level1a", "level2b": "level1b"})
        self.set_sub_client(self.l2_client2,
                            sub_message_space_name='sub2',
                            role_rename_dict={"level2a": "level1a", "level2b": "level1b"})

    def run(self):
        self.l2_client1.init()
        self.l2_client1.run()
        self.l2_client1.close()

        self.logger.info(f"name : {self.role_name}")
        whoareyou = self.comm.receive('level1a', 'whoami')
        self.logger.info(f"get whoareyou : {whoareyou}")

        self.l2_client2.init()
        self.l2_client2.run()
        self.l2_client2.close()

        return None

    def close(self):
        pass


if __name__ == '__main__':
    from fedprototype.envs import LocalEnv

    LocalEnv() \
        .add_client(Level1AClient()) \
        .add_client(Level1BClient()) \
        .run()
