import secrets
import typing as T
from functools import reduce
from operator import mul

import numpy as np

from components.smc.tools.serialize import bytes_to_bit_arr, int_to_bytes


def rand_binary_arr(shape: T.Union[int, T.Tuple[int, ...]]) -> np.ndarray:
    if isinstance(shape, int):
        size = shape
    else:
        size = reduce(mul, shape)
    bs = secrets.randbits(size).to_bytes((size + 7) // 8, "big")
    res = bytes_to_bit_arr(int_to_bytes(size) + bs)
    res.resize(shape)
    return res
