from components.smc.tools.serialize import int_to_bytes, bytes_to_int


def pack(m0, m1):
    """
    :param m0: bytes
    :param m1: bytes
    :return:
    """
    return int_to_bytes(len(m0)) + m0 + m1


def unpack(m):
    """
    :param m: bytes
    :return:
    """
    m0_length = bytes_to_int(m[:4])
    m0 = m[4: 4 + m0_length]
    m1 = m[4 + m0_length:]
    return m0, m1
