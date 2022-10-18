# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, PickleType, String
from sqlalchemy.orm import declarative_base, relationship
from xxhash import xxh3_64_intdigest

from .hash import phash_1d, phash_2d

Base = declarative_base()


class DimCoord(Base):
    __tablename__ = "dim_coord"

    id = Column(Integer, primary_key=True)
    hash = Column(Integer, nullable=False, unique=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)
    bounds_lower_phash = Column(Integer)
    bounds_upper_phash = Column(Integer)
    grids = relationship("Grid", back_populates="dim_coords")

    def __init__(self, dim_coord):
        points = dim_coord.points
        bounds = dim_coord.bounds
        super().__init__(
            points=points,
            points_hash=xxh3_64_intdigest(points),
            points_phash=phash_1d(points).hash,
            bounds=bounds,
            bounds_hash=xxh3_64_intdigest(bounds),
            bounds_lower_phash=phash_1d(bounds[:, 0]).hash,
            bounds_upper_phash=phash_1d(bounds[:, 1]).hash,
        )


class TwoDCoord(Base):
    __tablename__ = "two_d_coord"

    id = Column(Integer, primary_key=True)
    hash = Column(Integer, nullable=False, unique=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)
    grids = relationship("Grid", back_populates="two_d_coords")

    def __init__(self, coord):
        points = coord.points
        bounds = coord.bounds
        super().__init__(
            points=points,
            points_hash=xxh3_64_intdigest(points),
            points_phash=phash_2d(points).hash,
            bounds=bounds,
            bounds_hash=xxh3_64_intdigest(bounds),
        )


class Grid(Base):
    __tablename__ = "grid"

    id = Column(Integer, primary_key=True)
    dim_coords = relationship("DimCoord", back_populates="grids")
    two_d_coords = relationship("TwoDCoord", back_populates="grids")
    files = relationship("File", back_populates="grid")

    def __init__(self, cube):
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


class File(Base):
    __tablename__ = "file"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    tracking_id = Column(String)
    grid = relationship("Grid", back_populates="files")
