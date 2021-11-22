import hashlib
import random


def position_hash(i, data, n):
    """
    :param i: int
    :param data: bytes
    :param n: int
    :return: int
    """
    length = (n.bit_length() + 7) // 8
    h = hashlib.sha256(bytes([i]) + data).digest()[:length]
    val = int.from_bytes(h, "big")
    return val % n


class CuckooHashTable:
    def __init__(self, n, s):
        """
        :param n: int
        :param s: int
        """
        self._n = n
        self._s = s

        # List[Optional[bytes]]
        self._table = self._n * [None]
        # List[int]
        self._table_hash_index = self._n * [0]
        # List[Optional[bytes]]
        self._stash = self._s * [None]
        self._stash_count = 0

        self._max_depth = 500

    def update(self, data):
        """
        :param data: bytes
        """
        for _ in range(self._max_depth):
            h1 = position_hash(1, data, self._n)
            if self._table[h1] is None:
                self._table[h1] = data
                self._table_hash_index[h1] = 1
                return
            h2 = position_hash(2, data, self._n)
            if self._table[h2] is None:
                self._table[h2] = data
                self._table_hash_index[h2] = 2
                return
            h3 = position_hash(3, data, self._n)
            if self._table[h3] is None:
                self._table[h3] = data
                self._table_hash_index[h3] = 3
                return
            i = random.randrange(3)
            h = [h1, h2, h3][i]
            old_data = self._table[h]
            self._table[h] = data
            self._table_hash_index[h] = i + 1
            data = old_data
        self._stash[self._stash_count] = data
        self._stash_count += 1

    @property
    def table(self):
        """
        :return: List[Optional[bytes]]
        """
        return self._table

    @property
    def stash(self):
        """
        :return: List[Optional[bytes]]
        """
        return self._stash

    @property
    def table_hash_index(self):
        """
        :return: List[int]
        """
        return self._table_hash_index
