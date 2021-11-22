import random

import numpy as np
from Crypto.Hash import SHAKE256

from components.smc.psi.cuckoo import CuckooHashTable
from components.smc.tools.arr import rand_binary_arr
from components.smc.tools.serialize import int_to_bytes, bytes_to_bit_arr
from fedprototype import BaseClient


class PSIClient(BaseClient):

    def run(self, **kwargs):
        pass

    def close(self):
        pass

    def __init__(self, words, role_name):
        """
        :param words: List[bytes]
        """
        super().__init__(role_name)

        self.n, self.table, self.stash, self.table_hash_index, dummy_table = self._init_cuckoo(words)

        self._codewords = 512

        self._r = np.empty((len(dummy_table), self._codewords), dtype=np.uint8)
        codewords_bytes = int_to_bytes(self._codewords)
        for i, word in enumerate(dummy_table):
            word_arr = bytes_to_bit_arr(codewords_bytes + self._encode(word))
            self._r[i] = word_arr

    def init(self):
        m = self._r.shape[0]
        self._t = rand_binary_arr((m, self._codewords))
        u = self._t ^ self._r

        self._s = rand_binary_arr()
        # if self._codewords == 128:
        #     for i in range(self._codewords):
        #         ti_bytes = bit_arr_to_bytes(self._t[:, i])
        #         ui_bytes = bit_arr_to_bytes(u[:, i])
        #         base.send(self._pair, ti_bytes, ui_bytes)
        # else:
        #     sender = Sender(self._pair, 128)
        #     sender.prepare()
        #
        #     for i in range(self._codewords):
        #         ti_bytes = bit_arr_to_bytes(self._t[:, i])
        #         ui_bytes = bit_arr_to_bytes(u[:, i])
        #         sender.send(ti_bytes, ui_bytes)
        # self._pair.barrier()

    def _encode(self, data):
        shake = SHAKE256.new(data)
        length = (self._codewords + 7) // 8
        return shake.read(length)

    @classmethod
    def _init_cuckoo(cls, words):
        n = int(1.2 * len(words))
        s = 5
        cuckoo = CuckooHashTable(n, s)
        for word in words:
            cuckoo.update(word)
        table = cuckoo.table
        stash = cuckoo.stash
        table_hash_index = cuckoo.table_hash_index

        dummy_table = []
        for key, hash_index in zip(table, table_hash_index):
            if key is None:
                key = random.getrandbits(64).to_bytes(8, "big")
            else:
                key = key + bytes([hash_index])
            dummy_table.append(key)
        for key in stash:
            if key is None:
                key = random.getrandbits(64).to_bytes(8, "big")
            dummy_table.append(key)
        return n, table, stash, table_hash_index, dummy_table


    def intersect(self) -> List[bytes]:
        peer_tables = [{}, {}, {}]
        peer_stash = {}
        for table in peer_tables:
            while True:
                key = self.pair.recv()
                if key == b"end":
                    break
                table[key] = None
        while True:
            key = self.pair.recv()
            if key == b"end":
                break
            peer_stash[key] = None

        res = []
        for i, (key, hash_index) in enumerate(zip(self.table, self.table_hash_index)):
            if key is not None:
                peer_table = peer_tables[hash_index - 1]
                local_val = self.oprf_client.eval(i)
                _logger.debug(f"hash index {hash_index}, table position {i}, "
                              f"word {int.from_bytes(key, 'big')}, val {local_val.hex()}")
                if local_val in peer_table:
                    res.append(key)
                    self.pair.send(key)

        for i, key in enumerate(self.stash):
            if key is not None:
                local_val = self.oprf_client.eval(i + self.n)
                _logger.debug(f"table position {i + self.n}, "
                              f"word {int.from_bytes(key, 'big')}, val {local_val.hex()}")
                if local_val in peer_stash:
                    res.append(key)
                    self.pair.send(key)
        self.pair.send(b"end")
        return res
