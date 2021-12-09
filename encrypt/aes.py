from Crypto.Cipher import AES
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF
from Crypto.Random import get_random_bytes


class AESCipher:

    @staticmethod
    def encrypt(secret, plaintext):
        """
        :param secret: bytes
        :param plaintext: bytes
        :return: bytes
        """
        key = HKDF(secret, key_len=32, salt=None, hashmod=SHA512)
        nonce = get_random_bytes(16)
        aes = AES.new(key, AES.MODE_EAX, nonce=nonce, mac_len=16)
        cm, tag = aes.encrypt_and_digest(plaintext)
        return nonce + tag + cm

    @staticmethod
    def decrypt(secret, ciphertext):
        """
        :param secret: bytes
        :param ciphertext: bytes
        :return: bytes
        """
        key = HKDF(secret, key_len=32, salt=None, hashmod=SHA512)

        nonce = ciphertext[:16]
        tag = ciphertext[16:32]
        cm = ciphertext[32:]

        aes = AES.new(key, AES.MODE_EAX, nonce=nonce, mac_len=16)
        try:
            res = aes.decrypt_and_verify(cm, tag)
            return res
        except ValueError:
            print("key incorrect or message corrupted")
