import random
import typing as T

from components.smc.psi.cuckoo import CuckooHashTable, position_hash
from components.smc.psi.oprf import OPRFClient, OPRFServer
from fedprototype import BaseClient


class PSIServer(BaseClient):
    words: T.List[bytes]

    def __init__(self):
        super().__init__("PSIServer")

        self.s = 5
        self.oprf_server = OPRFServer(512)

    def init(self):
        self.set_sub_client(
            self.oprf_server,
            role_rename_dict={"OPRFServer": "PSIServer", "OPRFClient": "PSIClient"},
        )
        self.oprf_server.init()

    def run(self, words: T.List[int]) -> T.List[int]:
        self.words = [word.to_bytes(4, "big") for word in words]
        n = self.oprf_server.max_count - self.s
        for i in range(3):
            words_: T.List[bytes] = random.sample(self.words, len(self.words))
            for word in words_:
                h = position_hash(i + 1, word, n)
                val = self.oprf_server.run(h, word + bytes([i + 1]))
                self.logger.debug(
                    f"hash index {i + 1}, table position {h},"
                    f"word {int.from_bytes(word, 'big')}, val {val.hex()}"
                )
                self.comm.send(receiver="PSIClient", message_name="word", obj=val)
            self.comm.send(receiver="PSIClient", message_name="word", obj=b"end")

        for i in range(self.s):
            words_: T.List[bytes] = random.sample(self.words, len(self.words))
            for word in words_:
                val = self.oprf_server.run(n + i, word)
                self.logger.debug(
                    f"table position {n + i}, "
                    f"word {int.from_bytes(word, 'big')}, val {val.hex()}"
                )
                self.comm.send(receiver="PSIClient", message_name="word", obj=val)
            self.comm.send(receiver="PSIClient", message_name="word", obj=b"end")

        res = []
        while True:
            word = self.comm.receive(sender="PSIClient", message_name="word")
            if word == b"end":
                break
            res.append(word)

        return sorted([int.from_bytes(val, "big") for val in res])

    def close(self):
        pass


class PSIClient(BaseClient):
    n: int
    table: T.List[T.Optional[bytes]]
    stash: T.List[T.Optional[bytes]]
    table_hash_index: T.List[int]

    def __init__(self):
        super().__init__("PSIClient")
        self.oprf_client = OPRFClient(512)

    def oprf_init(self, words: T.List[bytes]) -> None:
        self.n = int(1.2 * len(words))
        s = 5
        cuckoo = CuckooHashTable(self.n, s)
        for word in words:
            cuckoo.update(word)
        self.table = cuckoo.table
        self.stash = cuckoo.stash
        self.table_hash_index = cuckoo.table_hash_index

        dummy_table = []
        for key, hash_index in zip(self.table, self.table_hash_index):
            if key is None:
                key = random.getrandbits(64).to_bytes(8, "big")
            else:
                key = key + bytes([hash_index])
            dummy_table.append(key)
        for key in self.stash:
            if key is None:
                key = random.getrandbits(64).to_bytes(8, "big")
            dummy_table.append(key)

        self.oprf_client.complete_init(dummy_table)

    def init(self) -> None:
        self.set_sub_client(
            self.oprf_client,
            role_rename_dict={"OPRFServer": "PSIServer", "OPRFClient": "PSIClient"},
        )

    def run(self, words: T.List[int]) -> T.List[int]:
        words_: T.List[bytes] = [word.to_bytes(4, "big") for word in words]
        self.oprf_init(words_)
        peer_tables = [{}, {}, {}]
        peer_stash = {}
        for table in peer_tables:
            while True:
                key = self.comm.receive(sender="PSIServer", message_name="word")
                if key == b"end":
                    break
                table[key] = None
        while True:
            key = self.comm.receive(sender="PSIServer", message_name="word")
            if key == b"end":
                break
            peer_stash[key] = None

        res = []
        for i, (key, hash_index) in enumerate(zip(self.table, self.table_hash_index)):
            if key is not None:
                peer_table = peer_tables[hash_index - 1]
                local_val = self.oprf_client.run(i)
                self.logger.debug(
                    f"hash index {hash_index}, table position {i}, "
                    f"word {int.from_bytes(key, 'big')}, val {local_val.hex()}"
                )
                if local_val in peer_table:
                    res.append(key)
                    self.comm.send(receiver="PSIServer", message_name="word", obj=key)

        for i, key in enumerate(self.stash):
            if key is not None:
                local_val = self.oprf_client.run(i + self.n)
                self.logger.debug(
                    f"table position {i + self.n}, "
                    f"word {int.from_bytes(key, 'big')}, val {local_val.hex()}"
                )
                if local_val in peer_stash:
                    res.append(key)
                    self.comm.send(receiver="PSIServer", message_name="word", obj=key)
        self.comm.send(receiver="PSIServer", message_name="word", obj=b"end")
        return sorted([int.from_bytes(val, "big") for val in res])

    def close(self):
        pass
