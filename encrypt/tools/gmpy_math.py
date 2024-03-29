import random

import gmpy2

POWMOD_GMP_SIZE = pow(2, 64)


def powmod(a, b, c):
    """
    return int: (a ** b) % c
    """

    if a == 1:
        return 1

    if max(a, b, c) < POWMOD_GMP_SIZE:
        return pow(a, b, c)

    else:
        return int(gmpy2.powmod(a, b, c))


def invert(a, b):
    """return int: x, where a * x == 1 mod b
    """
    x = int(gmpy2.invert(a, b))

    if x == 0:
        raise ZeroDivisionError('invert(a, b) no inverse exists')

    return x


def getprimeover(n):
    """return a random n-bit prime number
    """
    r = gmpy2.mpz(random.SystemRandom().getrandbits(n))
    r = gmpy2.bit_set(r, n - 1)

    return int(gmpy2.next_prime(r))


def isqrt(n):
    """ return the integer square root of N """

    return int(gmpy2.isqrt(n))


def is_prime(n):
    """
    true if n is probably a prime, false otherwise
    :param n:
    :return:
    """
    return gmpy2.is_prime(int(n))


def legendre(a, p):
    return pow(a, (p - 1) // 2, p)


def tonelli(n, p):
    # assert legendre(n, p) == 1, "not a square (mod p)"
    q = p - 1
    s = 0
    while q % 2 == 0:
        q //= 2
        s += 1
    if s == 1:
        return pow(n, (p + 1) // 4, p)
    for z in range(2, p):
        if p - 1 == legendre(z, p):
            break
    c = pow(z, q, p)
    r = pow(n, (q + 1) // 2, p)
    t = pow(n, q, p)
    m = s
    while (t - 1) % p != 0:
        t2 = (t * t) % p
        for i in range(1, m):
            if (t2 - 1) % p == 0:
                break
            t2 = (t2 * t2) % p
        b = pow(c, 1 << (m - i - 1), p)
        r = (r * b) % p
        c = (b * b) % p
        t = (t * c) % p
        m = i
    return r


def gcd(a, b):
    return int(gmpy2.gcd(a, b))


def next_prime(n):
    return int(gmpy2.next_prime(n))
