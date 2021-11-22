import numpy as np
from Crypto.Hash import SHAKE256, SHA256

from components.smc.psi.base_trans import BaseSender, BaseReceiver
from components.smc.psi.ote import OTESender, OTEReceiver
from components.smc.tools.arr import rand_binary_arr
from components.smc.tools.serialize import int_to_bytes, bytes_to_bit_arr, bit_arr_to_bytes
from fedprototype import BaseClient


class OPRFClient(BaseClient):

    def __init__(self, r, codewords=128):
        """
        :param r: Sequence[bytes]
        :param codewords:
        """
        super().__init__("OPRFClient")

        if codewords < 128:
            raise ValueError(f"codewords {codewords} is too small,"
                             f" it should be greater equal than 128 to ensure security")
        self._codewords = codewords

        self._r = np.empty((len(r), self._codewords), dtype=np.uint8)
        codewords_bytes = int_to_bytes(self._codewords)
        for i, word in enumerate(r):
            word_arr = bytes_to_bit_arr(codewords_bytes + self._encode(word))
            self._r[i] = word_arr

        self._t = None

        self.base_sender = BaseSender()

    def _encode(self, data):
        shake = SHAKE256.new(data)
        length = (self._codewords + 7) // 8
        return shake.read(length)

    def init(self):
        self.set_sub_client(self.base_sender, role_rename_dict={"BaseSender": "OPRFClient",
                                                                "BaseReceiver": "OPRFServer"})
        m = self._r.shape[0]
        self._t = rand_binary_arr((m, self._codewords))
        u = self._t ^ self._r
        if self._codewords == 128:
            self.ot_init(u)
        else:
            self.ote_init(u)

    def ot_init(self, u):
        for i in range(self._codewords):
            ti_bytes = bit_arr_to_bytes(self._t[:, i])
            ui_bytes = bit_arr_to_bytes(u[:, i])
            self.base_sender.run(ti_bytes, ui_bytes)

    def ote_init(self, u):
        ote_sender = OTESender(128)
        self.set_sub_client(ote_sender, role_rename_dict={"OTESender": "OPRFClient",
                                                          "OTEReceiver": "OPRFServer"})
        ote_sender.init()
        for i in range(self._codewords):
            ti_bytes = bit_arr_to_bytes(self._t[:, i])
            ui_bytes = bit_arr_to_bytes(u[:, i])
            ote_sender.run(ti_bytes, ui_bytes)

    def run(self, i):
        """
        :param i: int
        :return: bytes
        """
        if i >= self.max_count:
            raise IndexError(f"i is greater than oprf instance count {self.max_count}")
        ti = self._t[i, :]
        return SHA256.new(bit_arr_to_bytes(ti)[4:]).digest()

    def close(self):
        pass

    @property
    def max_count(self):
        if self._r is None:
            return 0
        return self._r.shape[0]


class OPRFServer(BaseClient):

    def __init__(self, codewords=128):
        super().__init__("OPRFServer")
        if codewords < 128:
            raise ValueError(f"codewords {codewords} is too small,"
                             f" it should be greater equal than 128 to ensure security")
        self._codewords = codewords
        # s (select bits for prepare stage)
        self._s = None
        # q (keys)
        self._q = None

        self.base_receiver = BaseReceiver()

    def _encode(self, data):
        shake = SHAKE256.new(data)
        length = (self._codewords + 7) // 8
        return shake.read(length)

    def init(self):
        self.set_sub_client(self.base_receiver, role_rename_dict={"BaseSender": "OPRFClient",
                                                                  "BaseReceiver": "OPRFServer"})
        self._s = rand_binary_arr(self._codewords)
        m = 0
        q_cols = []

        if self._codewords == 128:
            self.ot_init(m, q_cols)
        else:
            self.ote_init(m, q_cols)

        self._q = np.vstack([bytes_to_bit_arr(col) for col in q_cols]).T

    def ot_init(self, m, q_cols):
        # q (keys)
        for i, b in enumerate(self._s):
            # column of q, bytes of 0,1 arr
            q_col = self.base_receiver.run(b)
            if m == 0:
                m = len(q_col)
            else:
                assert m == len(q_col), f"base OT message length should be all the same, but the round {i} is not"
            q_cols.append(q_col)

    def ote_init(self, m, q_cols):
        ote_receiver = OTEReceiver(self._s, 128)
        self.set_sub_client(ote_receiver, role_rename_dict={"OTESender": "OPRFClient",
                                                            "OTEReceiver": "OPRFServer"})
        ote_receiver.init()

        for i in range(self._s.size):
            q_col = ote_receiver.run()
            if m == 0:
                m = len(q_col)
            else:
                assert m == len(q_col), f"base OT message length should be all the same, but the round {i} is not"
            q_cols.append(q_col)

    def _eval_op(self, i, enc_data):
        """
        :param i: int
        :param enc_data: np.ndarray
        :return:
        """
        qi = self._q[i, :]
        return qi ^ (self._s & enc_data)

    def run(self, i, data):
        """
        :param i: int
        :param data: bytes
        :return: bytes
        """
        if i >= self.max_count:
            raise IndexError(f"i is greater than oprf instance count {self.max_count}")
        enc_data = self._encode(data)
        enc_data_arr = bytes_to_bit_arr(int_to_bytes(self._codewords) + enc_data)

        res = self._eval_op(i, enc_data_arr)
        return SHA256.new(bit_arr_to_bytes(res)[4:]).digest()

    def close(self):
        pass

    @property
    def max_count(self) -> int:
        if self._q is None:
            return 0
        else:
            return self._q.shape[0]
