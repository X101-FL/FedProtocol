import numpy as np

_frac_mask_ = np.uint32(2 ** 23 - 1)
_b_23_ = np.uint32(2 ** 23)
_exp_mask_ = np.uint32(2 ** 8 - 1)


def decompose_float32(f):
    f = np.float32(f)
    _i = f.view(np.uint32)
    _sign = _i >> 31
    _frac = _i & _frac_mask_ | _b_23_
    _exp = (_i >> 23 & _exp_mask_) - 150
    if _sign:
        _frac = -_frac
    return _frac, _exp


def reconstruct_float32(_frac, _exp):
    return np.float32(_frac * 2.0 ** _exp)
