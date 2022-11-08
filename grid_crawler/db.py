# -*- coding: utf-8 -*-

# 0.000001 deg = 0.11 m (7 decimals, cm accuracy)

import os
from typing import Mapping, NamedTuple

import iris
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    PickleType,
    String,
    Table,
    UniqueConstraint,
    select,
)
from sqlalchemy.orm import declarative_base, relationship
from xxhash import xxh3_64_hexdigest

from .hash import phash_1d, phash_2d

Base = declarative_base()


grid_dim_coord = Table(
    "grid_dim_coord",
    Base.metadata,
    Column("grid_id", ForeignKey("grid.id")),
    Column("dim_coord_id", ForeignKey("dim_coord.id")),
)


class DimCoordHashes(NamedTuple):
    points_hash: str
    points_phash: int
    bounds_hash: str
    bounds_lower_phash: int
    bounds_upper_phash: int


class AuxCoordHashes(NamedTuple):
    points_hash: str
    points_phash: int
    bounds_hash: str


class GridHashes(NamedTuple):
    dim_coords: Mapping[DimCoordHashes, str]
    two_d_coords: Mapping[AuxCoordHashes, str]


def hash_dim_coord(coord):
    points = coord.points
    points_hash = xxh3_64_hexdigest(points)
    points_phash = phash_1d(points).hash
    bounds = coord.bounds
    if bounds is not None:
        bounds_hash = xxh3_64_hexdigest(bounds)
        bounds_lower_phash = phash_1d(bounds[:, 0]).hash
        bounds_upper_phash = phash_1d(bounds[:, 1]).hash
    else:
        bounds_hash = None
        bounds_lower_phash = None
        bounds_upper_phash = None
    return DimCoordHashes(points_hash,
                          points_phash,
                          bounds_hash,
                          bounds_lower_phash,
                          bounds_upper_phash)


def hash_aux_coord(coord):
    points = coord.points
    points_hash = xxh3_64_hexdigest(points)
    points_phash = phash_2d(points).hash
    bounds = coord.bounds
    if bounds is not None:
        bounds_hash = xxh3_64_hexdigest(bounds)
    else:
        bounds_hash = None
    return AuxCoordHashes(points_hash,
                          points_phash,
                          bounds_hash)


def hash_grid(cube):
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
    dim_coord_hashes = {hash_dim_coord(coord): coord
                        for coord in dim_coords.values()}
    two_d_coord_hashes = {hash_aux_coord(coord): coord
                          for coord in set(sum(non_dim_coords.values(), []))}
    return GridHashes(dim_coord_hashes, two_d_coord_hashes)


class DimCoord(Base):
    __tablename__ = "dim_coord"
    __table_args__ = (
        UniqueConstraint("points_hash", "bounds_hash"),
    )

    id = Column(Integer, primary_key=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)
    bounds_lower_phash = Column(Integer)
    bounds_upper_phash = Column(Integer)

    def __repr__(self):
        return f"DimCoord({self.id})"


grid_two_d_coord = Table(
    "grid_two_d_coord",
    Base.metadata,
    Column("grid_id", ForeignKey("grid.id")),
    Column("two_d_coord_id", ForeignKey("two_d_coord.id")),
)


class TwoDCoord(Base):
    __tablename__ = "two_d_coord"
    __table_args__ = (
        UniqueConstraint("points_hash", "bounds_hash"),
    )

    id = Column(Integer, primary_key=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)

    def __repr__(self):
        return f"TwoDCoord({self.id})"


class Grid(Base):
    __tablename__ = "grid"

    id = Column(Integer, primary_key=True)
    dim_coords = relationship("DimCoord",
                              secondary=grid_dim_coord,
                              backref="grids")
    two_d_coords = relationship("TwoDCoord",
                                secondary=grid_two_d_coord,
                                backref="grids")

    def __init__(self, cube, session, grid_hashes=None):
        if grid_hashes is None:
            grid_hashes = hash_grid(cube)
        dim_coords = []
        for candidate, coord in grid_hashes.dim_coords.items():
            existing = session.scalar(select(DimCoord).where(
                DimCoord.points_hash == candidate.points_hash,
                DimCoord.bounds_hash == candidate.bounds_hash))
            if existing is None:
                dim_coords.append(DimCoord(
                    points=coord.points,
                    bounds=coord.bounds,
                    **candidate._asdict()))
            else:
                dim_coords.append(existing)
        two_d_coords = []
        for candidate, coord in grid_hashes.two_d_coords.items():
            existing = session.scalar(select(TwoDCoord).where(
                TwoDCoord.points_hash == candidate.points_hash,
                TwoDCoord.bounds_hash == candidate.bounds_hash))
            if existing is None:
                two_d_coords.append(TwoDCoord(
                    points=coord.points,
                    bounds=coord.bounds,
                    **candidate._asdict()))
            else:
                two_d_coords.append(existing)
    # unique_dim_coords.append(candidate)
    # unique_two_d_coords = []
    # for coord in set(sum(non_dim_coords.values(), [])):
    #     candidate = 
    #     existing = session.scalar(select(TwoDCoord).where(
    #         TwoDCoord.points_hash == candidate.points_hash,
    #         TwoDCoord.bounds_hash == candidate.bounds_hash))
    #     if existing is None:
    #         candidate = TwoDCoord(
    #             points=coord.points,
    #             bounds=coord.bounds,
    #             **candidate._asdict())
    #     else:
    #         candidate = existing
    #     unique_two_d_coords.append(candidate)
        super().__init__(
            dim_coords=dim_coords,
            two_d_coords=two_d_coords,
        )

    def __repr__(self):
        return f"Grid([{', '.join([str(c) for c in self.dim_coords])}])"


class File(Base):
    __tablename__ = "file"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    tracking_id = Column(String)
    grid_id = Column(ForeignKey("grid.id"))
    grid = relationship("Grid", backref="files")

    def __init__(self, path, session):
        cube = iris.load_cube(path)
        candidate = hash_grid(cube)
        # import pdb; pdb.set_trace()
        # existing = session.scalar(
        #     select(Grid).join(Grid.dim_coords).join(Grid.two_d_coords).where(
        #         Grid.dim_coords.points_hash.in_([c.points_hash for c in candidate.dim_coords])
        #     ))
        # existing = session.get(Grid, candidate.id)
        existing = None
        if existing is not None:
            candidate = existing
        super().__init__(
            filename=os.path.basename(path),
            tracking_id=cube.attributes["tracking_id"],
            grid=Grid(cube, session, candidate),
        )

    def __repr__(self):
        return f"File({self.filename}, {self.tracking_id}, {self.grid})"
