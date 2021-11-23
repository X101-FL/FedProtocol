from Crypto.PublicKey import ECC

from components.smc.tools.pack import pack, unpack
from components.smc.tools.serialize import key_to_bytes, bytes_to_key, point_to_bytes
from encrypt.shake import ShakeCipher
from fedprototype import BaseClient


class BaseSender(BaseClient):

    def __init__(self):
        super().__init__("BaseSender")

    def init(self):
        pass

    def run(self, m0, m1, curve="secp256r1"):
        sk = ECC.generate(curve=curve)
        pk = sk.public_key()

        pk_bytes = key_to_bytes(pk)
        self.comm.send(receiver="BaseReceiver", message_name="public_key", obj=pk_bytes)

        bk_bytes = self.comm.receive(sender="BaseReceiver", message_name="public_key")
        bk = bytes_to_key(bk_bytes)

        # cipher key for m0 and m1
        ck0 = bk.pointQ * sk.d
        ck1 = (-pk.pointQ + bk.pointQ) * sk.d

        cipher_m0 = ShakeCipher.encrypt(point_to_bytes(ck0), m0)
        cipher_m1 = ShakeCipher.encrypt(point_to_bytes(ck1), m1)
        self.comm.send(receiver="BaseReceiver", message_name="cipher_message", obj=pack(cipher_m0, cipher_m1))

    def close(self):
        pass


class BaseReceiver(BaseClient):

    def __init__(self):
        super().__init__("BaseReceiver")

    def init(self):
        pass

    def run(self, b):
        assert b == 0 or b == 1, "b should be 0 or 1"
        ak_bytes = self.comm.receive(sender="BaseSender", message_name="public_key")
        ak = bytes_to_key(ak_bytes)
        curve = ak.curve

        sk = ECC.generate(curve=curve)
        pk = sk.public_key()

        # choose point and pk
        if b == 1:
            point = pk.pointQ + ak.pointQ
            pk = ECC.EccKey(curve=curve, point=point, d=None)
        # send chosen public key
        self.comm.send(receiver="BaseSender", message_name="public_key", obj=key_to_bytes(pk))

        ck = ak.pointQ * sk.d
        # receive cipher message
        cipher_m = unpack(self.comm.receive(sender="BaseSender", message_name="cipher_message"))[b]

        return ShakeCipher.decrypt(point_to_bytes(ck), cipher_m)

    def close(self):
        pass
