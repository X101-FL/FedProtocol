import hashlib
import random
import typing as T


"""
Written in 2021-12-08 by WangShuai:

When I use typing to declare the type, I meet a problem:
I want to create a empty bytes list with a size, so I initialize it with [None, None, ...]
But when I want to get the value from the list and arange it to another bytes variable,
there will be a bug: None can't arange to bytes even if the value I get from the list is a
bytes logically. (Occur: 'data = old_data' in update() function)
So, I must use Optional[bytes] to typing the data in CuckooHashTable.update() and position_hash()
"""


def position_hash(i: int, data: T.Optional[bytes], n: int) -> int:
    assert data
    length = (n.bit_length() + 7) // 8
    h = hashlib.sha256(bytes([i]) + data).digest()[:length]
    val = int.from_bytes(h, "big")
    return val % n


class CuckooHashTable:
    def __init__(self, n: int, s: int):
        self._n = n
        self._s = s

        # Create a empty list with size
        self._table: T.List[T.Optional[bytes]] = [None] * self._n
        self._stash: T.List[T.Optional[bytes]] = [None] * self._s

        self._table_hash_index: T.List[int] = self._n * [0]
        self._stash_count = 0
        self._max_depth = 500

    def update(self, data: T.Optional[bytes]) -> None:
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
    def table(self) -> T.List[T.Optional[bytes]]:
        return self._table

    @property
    def stash(self) -> T.List[T.Optional[bytes]]:
        return self._stash

    @property
    def table_hash_index(self) -> T.List[int]:
        return self._table_hash_index
