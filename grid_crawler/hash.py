# -*- coding: utf-8 -*-

import numpy as np
from scipy.fftpack import dct
from skimage.transform import resize


class ArrayHash:
    def __init__(self, binary_array):
        self.binary_array = binary_array
        self.shape = binary_array.shape


def phash_2d(arr, hash_size=8, highfreq_factor=4):
    img_size = hash_size * highfreq_factor
    resized = resize(arr, (img_size, img_size), anti_aliasing=True)
    transformed = dct(dct(resized, axis=0), axis=1)
    lowfreq = transformed[:hash_size, :hash_size]
    med = np.median(lowfreq)
    diff = lowfreq > med
    return ImageHash(diff)
