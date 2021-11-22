import time

from components.smc.tools.arr import rand_binary_arr
from components.smc.tools.serialize import *
from components.smc.tools.pack import pack, unpack
from encrypt.shake import ShakeCipher
from fedprototype.base_client import BaseClient


class OTESender(BaseClient):
    """ Oblivious transfer extension"""

    def __init__(self, codewords=128):
        super().__init__("OTESender")
        if codewords < 128:
            raise ValueError(f"codewords {codewords} is too small,"
                             f" it should be greater equal than 128 to ensure security")
        # codewords length, matrix q's width, should be greater equal than 128 (for security)
        self._codewords = codewords
        self._s = None
        self._q = None
        self._index = 0

    def init(self):
        # s (select bits for prepare stage)
        self._s = rand_binary_arr(self._codewords)
        # q (keys)
        self._q = None
        m = 0
        q_cols = []

        if self._codewords == 128:
            # q (keys)
            for i, b in enumerate(self._s):
                # column of q, bytes of 0,1 arr
                q_col = self.receive(b)
                if m == 0:
                    m = len(q_col)
                else:
                    assert m == len(q_col), f"base OT message length should be all the same, but the round {i} is not"
                q_cols.append(q_col)

        self._q = np.vstack([bytes_to_bit_arr(col) for col in q_cols]).T

    def run(self, m0, m1):
        """
        :param m0: bytes
        :param m1: bytes
        """
        if not self.is_available():
            raise ValueError("The sender is not available now. "
                             "The sender may be not initialized or have used all ot keys")

        key0 = int_to_bytes(self._index) + bit_arr_to_bytes(self._q[self._index, :])
        key1 = int_to_bytes(self._index) + bit_arr_to_bytes(self._q[self._index, :] ^ self._s)

        cipher_m0 = ShakeCipher.encrypt(key0, m0)
        cipher_m1 = ShakeCipher.encrypt(key1, m1)

        self.comm.send(receiver="OTEReceiver", message_name="cipher_message", obj=pack(cipher_m0, cipher_m1))
        self._index += 1

    def receive(self, b):
        assert b == 0 or b == 1, "b should be 0 or 1"
        ak_bytes = self.comm.receive(sender="OTEReceiver", message_name="public_key")
        ak = bytes_to_key(ak_bytes)
        curve = ak.curve

        sk = ECC.generate(curve=curve)
        pk = sk.public_key()

        # choose point and pk
        if b == 1:
            point = pk.pointQ + ak.pointQ
            pk = ECC.EccKey(curve=curve, point=point, d=None)
        # send chosen public key
        self.comm.send(receiver="OTEReceiver", message_name="public_key", obj=key_to_bytes(pk))

        ck = ak.pointQ * sk.d
        # receive cipher message
        cipher_m = unpack(self.comm.receive(sender="OTEReceiver", message_name="cipher_message"))[b]

        res = ShakeCipher.decrypt(point_to_bytes(ck), cipher_m)
        return res

    def close(self):
        pass

    @property
    def max_count(self):
        if self._q is None:
            return 0
        else:
            return self._q.shape[0]

    def is_available(self):
        return self._q is not None and self._index < self._q.shape[0]


class OTEReceiver(BaseClient):

    def __init__(self, r, codewords=128):
        """
        :param r: Sequence[Union[int, bool]]
        :param codewords:
        """
        super().__init__("OTEReceiver")
        if codewords < 128:
            raise ValueError(f"codewords {codewords} is too small,"
                             f" it should be greater equal than 128 to ensure security")
        # codewords length, martix q's width, should be greater equal than 128 (for security)
        self._codewords = codewords

        # 1-d uint8 array of 0 and 1, select bits for m OTs, length is m
        self._r = np.array(r, dtype=np.uint8)
        # OT extension matrix, shape: m * k
        self._t = None
        self._index = 0

    def init(self):
        m = self._r.size
        self._t = rand_binary_arr((m, self._codewords))

        # col(u) = col(t) xor r
        u = (self._t.T ^ self._r).T
        if self._codewords == 128:
            for i in range(self._codewords):
                t_col_bytes = bit_arr_to_bytes(self._t[:, i])
                u_col_bytes = bit_arr_to_bytes(u[:, i])
                self.send(t_col_bytes, u_col_bytes)

    def run(self):
        key = int_to_bytes(self._index) + bit_arr_to_bytes(self._t[self._index, :])

        cipher_m = self.comm.receive(sender="OTESender", message_name="cipher_message")
        cipher_m = unpack(cipher_m)[self._r[self._index]]
        res = ShakeCipher.decrypt(key, cipher_m)
        self._index += 1
        return res

    def send(self, m0, m1, curve="secp256r1"):
        sk = ECC.generate(curve=curve)
        pk = sk.public_key()

        pk_bytes = key_to_bytes(pk)
        self.comm.send(receiver="OTESender", message_name="public_key", obj=pk_bytes)

        bk_bytes = self.comm.receive(sender="OTESender", message_name="public_key")
        bk = bytes_to_key(bk_bytes)

        # cipher key for m0 and m1
        ck0 = bk.pointQ * sk.d
        ck1 = (-pk.pointQ + bk.pointQ) * sk.d

        cipher_m0 = ShakeCipher.encrypt(point_to_bytes(ck0), m0)
        cipher_m1 = ShakeCipher.encrypt(point_to_bytes(ck1), m1)
        self.comm.send(receiver="OTESender", message_name="cipher_message", obj=pack(cipher_m0, cipher_m1))

    def close(self):
        pass

    def is_available(self):
        return self._t is not None and self._index < self._r.size

    @property
    def max_count(self):
        if self._r is None:
            return 0
        return self._r.shape[0]
