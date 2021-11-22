from Crypto.Hash import SHAKE256


class ShakeCipher:

    @staticmethod
    def encrypt(secret, plaintext):
        """
        :param secret: bytes
        :param plaintext: bytes
        :return: bytes
        """
        shake = SHAKE256.new(secret)
        key = shake.read(len(plaintext))

        int_key = int.from_bytes(key, "big")
        int_pt = int.from_bytes(plaintext, "big")

        int_ct = int_key ^ int_pt
        return int_ct.to_bytes(len(plaintext), "big")

    @staticmethod
    def decrypt(secret, ciphertext):
        """
        :param secret: bytes
        :param ciphertext: bytes
        :return: bytes
        """
        shake = SHAKE256.new(secret)
        key = shake.read(len(ciphertext))

        int_key = int.from_bytes(key, "big")
        int_ct = int.from_bytes(ciphertext, "big")

        int_pt = int_key ^ int_ct
        return int_pt.to_bytes(len(ciphertext), "big")
