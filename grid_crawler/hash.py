# -*- coding: utf-8 -*-

import numpy as np
from scipy.fftpack import dct
from skimage.transform import resize


class ArrayHash:
    def __init__(self, binary_array):
        assert binary_array.size <= 64
        self.binary_array = binary_array
        self.shape = binary_array.shape
        int_form = np.packbits(binary_array)
        byte_size = int_form.itemsize * int_form.size
        if byte_size >= 8:
            int_form.dtype = np.uint64
        elif byte_size >= 4:
            int_form.dtype = np.uint32
        elif byte_size >= 2:
            int_form.dtype = np.uint16
        assert int_form.size == 1
        self.hash = int_form[0]

    def __repr__(self):
        return f"ArrayHash(shape={self.shape}, hash={self.hash})"

    def dist(self, other):
        return (self.hash ^ other.hash).bit_count()

    def __eq__(self, other):
        if other is None:
            return False
        return (self.shape == other.shape) and (self.hash == other.hash)

    def __hash__(self):
        return int(self.hash)


def phash_2d(arr, hash_size=8, highfreq_factor=4):
    img_size = hash_size * highfreq_factor
    resized = resize(arr, (img_size, img_size), anti_aliasing=True)
    transformed = dct(dct(resized, axis=0), axis=1)
    lowfreq = transformed[:hash_size, :hash_size]
    med = np.median(lowfreq)
    diff = lowfreq > med
    return ArrayHash(diff)


def phash_1d(arr, hash_size=8, highfreq_factor=4):
    img_size = hash_size * highfreq_factor
    resized = resize(arr, (img_size,), anti_aliasing=True)
    transformed = dct(resized, axis=0)
    lowfreq = transformed[:hash_size]
    med = np.median(lowfreq)
    diff = lowfreq > med
    return ArrayHash(diff)
