from Cryptodome.Util import number
from random import randint
import os

from .tools.gmpy_math import powmod


# 非对称加密
# 支持乘法同态和数值乘法，不支持加法同态
# 乘法可能不能乘太多次，容易溢出
class Elgamal:
    def __init__(self, n_length=256, precision=27):
        self.n_length = n_length
        self.precision = precision

    @staticmethod
    def __get_g__(p):
        p1 = p - 1
        d = (p - 1) // 2
        j = 2
        while j < p1:
            g1 = powmod(j, d, p)
            if g1 == p1:
                return j
            if j > 100:
                return 0
            j += 1
        return 0

    def generate_keypair(self):
        _N = self.n_length
        g = 0
        p = 0
        while g == 0:
            p = number.getPrime(_N, os.urandom)
            g = Elgamal.__get_g__(p)

        x = randint(1, p - 1)
        y = powmod(g, x, p)

        return PublicKey(p, g, y, self.precision), PrivateKey(p, x)


class PublicKey:
    def __init__(self, p, g, y, precision):
        self.p = p
        self.g = g
        self.y = y
        self.half_p = p // 2
        self.precision = precision
        self.float_scale = 2.0 ** precision

    def encrypt(self, text):
        _text = int(round(text * self.float_scale))
        if abs(_text) >= self.half_p:
            raise Exception(f'abs(_text):{_text} > half_p:{self.half_p} is so big')

        k = randint(1, self.p - 1)
        a = powmod(self.g, k, self.p)
        b = powmod(self.y, k, self.p) * _text % self.p
        return CipherText(a, b, self.p, self.precision)


class PrivateKey:
    def __init__(self, p, x):
        self.p = p
        self.x = x
        self.half_p = p // 2

    def decrypt(self, cipher_text):
        _text = powmod(cipher_text.a, (self.p - 1 - self.x), self.p) * cipher_text.b % self.p
        if _text > self.half_p:
            _text -= self.p
        text = _text * 2.0 ** -(cipher_text.precision * cipher_text.mul_times)
        return text


class CipherText:
    def __init__(self, a, b, p, precision, mul_times=1):
        self.a = a
        self.b = b
        self.p = p
        self.precision = precision
        self.float_scale = 2.0 ** precision
        self.mul_times = mul_times

    def __mul__(self, o):
        if isinstance(o, CipherText):
            return CipherText((self.a * o.a) % self.p,
                              (self.b * o.b) % self.p,
                              self.p,
                              self.precision,
                              self.mul_times + o.mul_times)
        else:
            _o = int(round(o * self.float_scale))
            return CipherText(self.a,
                              (self.b * _o) % self.p,
                              self.p,
                              self.precision,
                              self.mul_times + 1)

    def __rmul__(self, o):
        return self.__mul__(o)
