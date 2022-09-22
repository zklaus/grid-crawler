#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import fire
import iris

from . import db


def get_grid(candidate):
    cube = iris.load_cube(candidate)
    dim_coords = {}
    non_dim_coords = {}
    dims = {}
    for ax in ("x", "y"):
        axis_dim_coords = cube.coords(axis=ax, dim_coords=True)
        assert len(axis_dim_coords) == 1
        dim_coords[ax] = axis_dim_coords[0]
        dims[ax] = cube.coord_dims(axis_dim_coords[0])[0]
        axis_non_dim_coords = cube.coords(axis=ax, dim_coords=False)
        non_dim_coords[ax] = axis_non_dim_coords
    print(dims)
    return cube


def crawl(basedir):
    basepath = Path(basedir)
    candidates = basepath.glob("**/*.nc")
    for candidate in candidates:
        grid = get_grid(candidate)
        break


def main():
    fire.Fire(crawl)


if __name__ == "__main__":
    main()
