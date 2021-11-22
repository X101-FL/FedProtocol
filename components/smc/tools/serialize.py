import numpy as np
from Crypto.PublicKey import ECC


def int_to_bytes(value: int):
    """revert value [int] to bytes
    the length of the result is 4
    """
    byte_length = (value.bit_length() + 7) // 8
    byte_length = (byte_length + 3) // 4 * 4
    byte_length = 4 if byte_length == 0 else byte_length
    return value.to_bytes(byte_length, "big")


def bytes_to_int(data: bytes):
    return int.from_bytes(data, "big")


def bit_arr_to_bytes(arr):
    """
    :param arr: 1-d uint8 numpy array
    :return: bytes, one element in arr maps to one bit in output bytes, padding in the left
    """
    n = arr.size
    pad_width = (8 - n % 8) % 8
    arr = np.pad(arr, pad_width=((pad_width, 0),), constant_values=0)
    bs = bytes(np.packbits(arr).tolist())

    return int_to_bytes(n) + bs


def bytes_to_bit_arr(data):
    """
    :param data: bytes, first 4 bytes is array length, and the remaining is array data
    :return:
    """
    prefix_length = 4
    n = bytes_to_int(data[:prefix_length])
    while (n + 7) // 8 != len(data) - prefix_length:
        prefix_length += 4
    arr = np.array(list(data[prefix_length:]), dtype=np.uint8)
    res = np.unpackbits(arr)[-n:]
    return res


def point_to_bytes(point):
    """
    :param point [ECC.EccPoint]
    :return: bytes
    """
    xs = point.x.to_bytes()
    ys = bytes([2 + point.y.is_odd()])
    return xs + ys


def key_to_bytes(key):
    """
    :param key [ECC.EccKey]
    :return:  bytes
    """
    if key.has_private():
        raise ValueError("only public key can be serialized to bytes to send")
    return key.export_key(format="DER", compress=True)


def bytes_to_key(data):
    """ convert data to key
    :param data [bytes]
    :return: ECC.EccKey
    """
    return ECC.import_key(data)


if __name__ == '__main__':
    bs = bit_arr_to_bytes(np.array([1, 2, 3]))
    print(bs)
    res = bytes_to_bit_arr(bs)
    print(res)