import typing as T

from components.smc.psi.base import BaseSender, BaseReceiver
from components.smc.tools.arr import rand_binary_arr
from components.smc.tools.serialize import *
from components.smc.tools.pack import pack, unpack
from encrypt.shake import ShakeCipher
from fedprototype.base_client import BaseClient


class OTESender(BaseClient):
    """ Oblivious transfer extension"""

    # s (select bits for prepare stage)
    _s: np.ndarray
    # q (keys)
    _q: np.ndarray

    def __init__(self, codewords: int = 128):
        super().__init__("OTESender")
        if codewords < 128:
            raise ValueError(
                f"codewords {codewords} is too small,"
                f" it should be greater equal than 128 to ensure security"
            )
        # codewords length, matrix q's width, should be greater equal than 128 (for security)
        self._codewords = codewords
        self._index = 0
        self.base_receiver = BaseReceiver()

    def init(self) -> None:
        self.set_sub_client(
            self.base_receiver,
            role_rename_dict={"BaseSender": "OTEReceiver", "BaseReceiver": "OTESender"},
        )
        self._s = rand_binary_arr(self._codewords)
        m = 0
        q_cols = []

        if self._codewords == 128:
            self.ot_init(m, q_cols)
        else:
            self.ote_init(m, q_cols)

        self._q = np.vstack([bytes_to_bit_arr(col) for col in q_cols]).T

    def ot_init(self, m: int, q_cols: T.List[bytes]) -> None:
        # q (keys)
        for i, b in enumerate(self._s):
            # column of q, bytes of 0,1 arr
            q_col = self.base_receiver.run(b)
            if m == 0:
                m = len(q_col)
            else:
                assert m == len(q_col), (
                    f"base OT message length should be all the same, "
                    f"but the round {i} is not"
                )
            q_cols.append(q_col)

    def ote_init(self, m: int, q_cols: T.List[bytes]) -> None:
        ote_receiver = OTEReceiver(self._s, 128)
        self.set_sub_client(
            ote_receiver,
            role_rename_dict={"OTESender": "OTEReceiver", "OTEReceiver": "OTESender"},
        )
        ote_receiver.init()

        for i in range(self._s.size):
            q_col = ote_receiver.run()
            if m == 0:
                m = len(q_col)
            else:
                assert m == len(
                    q_col
                ), f"base OT message length should be all the same, but the round {i} is not"
            q_cols.append(q_col)

    def run(self, m0: bytes, m1: bytes) -> None:
        if not self.is_available():
            raise ValueError(
                "The sender is not available now. "
                "The sender may be not initialized or have used all ot keys"
            )

        key0 = int_to_bytes(self._index) + bit_arr_to_bytes(self._q[self._index, :])
        key1 = int_to_bytes(self._index) + bit_arr_to_bytes(
            self._q[self._index, :] ^ self._s
        )

        cipher_m0 = ShakeCipher.encrypt(key0, m0)
        cipher_m1 = ShakeCipher.encrypt(key1, m1)

        self.comm.send(
            receiver="OTEReceiver",
            message_name="cipher_message",
            obj=pack(cipher_m0, cipher_m1),
        )
        self._index += 1

    def close(self):
        pass

    @property
    def max_count(self) -> int:
        if self._q is None:
            return 0
        else:
            return self._q.shape[0]

    def is_available(self) -> bool:
        return self._q is not None and self._index < self._q.shape[0]


class OTEReceiver(BaseClient):

    # 1-d uint8 array of 0 and 1, select bits for m OTs, length is m
    _r: np.ndarray
    # OT extension matrix, shape: m * k
    _t: np.ndarray

    def __init__(self, r: T.Union[np.ndarray, list], codewords: int = 128):
        super().__init__("OTEReceiver")
        if codewords < 128:
            raise ValueError(
                f"codewords {codewords} is too small,"
                f" it should be greater equal than 128 to ensure security"
            )
        # codewords length, matrix q's width, should be greater equal than 128 (for security)
        self._codewords = codewords
        self._r = np.array(r, dtype=np.uint8)
        self._index = 0

        self.base_sender = BaseSender()

    def init(self) -> None:
        self.set_sub_client(
            self.base_sender,
            role_rename_dict={"BaseSender": "OTEReceiver", "BaseReceiver": "OTESender"},
        )
        m = self._r.size
        self._t = rand_binary_arr((m, self._codewords))

        # col(u) = col(t) xor r
        u = (self._t.T ^ self._r).T
        if self._codewords == 128:
            self.ot_init(u)
        else:
            self.ote_init(u)

    def ot_init(self, u: np.ndarray) -> None:
        for i in range(self._codewords):
            t_col_bytes = bit_arr_to_bytes(self._t[:, i])
            u_col_bytes = bit_arr_to_bytes(u[:, i])
            self.base_sender.run(t_col_bytes, u_col_bytes)

    def ote_init(self, u: np.ndarray) -> None:
        ote_sender = OTESender(128)
        self.set_sub_client(
            ote_sender,
            role_rename_dict={"OTESender": "OTEReceiver", "OTEReceiver": "OTESender"},
        )
        ote_sender.init()
        for i in range(self._codewords):
            t_col_bytes = bit_arr_to_bytes(self._t[:, i])
            u_col_bytes = bit_arr_to_bytes(u[:, i])
            ote_sender.run(t_col_bytes, u_col_bytes)

    def run(self) -> bytes:
        key = int_to_bytes(self._index) + bit_arr_to_bytes(self._t[self._index, :])

        cipher_m = self.comm.receive(sender="OTESender", message_name="cipher_message")
        cipher_m = unpack(cipher_m)[self._r[self._index]]
        self._index += 1
        return ShakeCipher.decrypt(key, cipher_m)

    def close(self):
        pass

    def is_available(self) -> bool:
        return self._t is not None and self._index < self._r.size

    @property
    def max_count(self) -> int:
        if self._r is None:
            return 0
        return self._r.shape[0]
