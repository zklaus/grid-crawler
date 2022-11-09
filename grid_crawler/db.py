# -*- coding: utf-8 -*-

# 0.000001 deg = 0.11 m (7 decimals, cm accuracy)

import os
from typing import Mapping, NamedTuple

import iris
from sqlalchemy import (Column, ForeignKey, Integer, PickleType, String, Table,
                        UniqueConstraint, and_, or_, select)
from sqlalchemy.orm import declarative_base, relationship
from xxhash import xxh3_64_hexdigest

from .hash import phash_1d, phash_2d

Base = declarative_base()


class CoordHashes(NamedTuple):
    points_hash: str
    points_phash: int
    bounds_hash: str


class GridHashes(NamedTuple):
    coords: Mapping[CoordHashes, str]


def hash_coord(coord):
    points = coord.points
    points_hash = xxh3_64_hexdigest(points)
    if points.ndim == 1:
        points_phash = phash_1d(points).hash
    elif points.ndim == 2:
        points_phash = phash_2d(points).hash
    else:
        points_hash = None
    bounds = coord.bounds
    if bounds is not None:
        bounds_hash = xxh3_64_hexdigest(bounds)
    else:
        bounds_hash = None
    return CoordHashes(points_hash, points_phash, bounds_hash)


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
    coord_hashes = {
        hash_coord(coord): coord
        for coord in set(dim_coords.values())
        | set(sum(non_dim_coords.values(), []))
    }
    return GridHashes(coord_hashes)


class Coord(Base):
    __tablename__ = "coord"
    __table_args__ = (UniqueConstraint("points_hash", "bounds_hash"), )

    id = Column(Integer, primary_key=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)

    def __repr__(self):
        return f"Coord({self.id})"


grid_coord = Table(
    "grid_coord",
    Base.metadata,
    Column("grid_id", ForeignKey("grid.id")),
    Column("coord_id", ForeignKey("coord.id")),
)


class Grid(Base):
    __tablename__ = "grid"

    id = Column(Integer, primary_key=True)
    coords = relationship("Coord", secondary=grid_coord, backref="grids")

    @classmethod
    def from_cube(cls, cube, session, grid_hashes=None):
        if grid_hashes is None:
            grid_hashes = hash_grid(cube)
        coords = []
        for candidate, coord in grid_hashes.coords.items():
            existing = session.scalar(
                select(Coord).where(
                    Coord.points_hash == candidate.points_hash,
                    Coord.bounds_hash == candidate.bounds_hash,
                ))
            if existing is None:
                coords.append(
                    Coord(points=coord.points,
                          bounds=coord.bounds,
                          **candidate._asdict()))
            else:
                coords.append(existing)
        return cls(coords=coords)

    def __repr__(self):
        return f"Grid([{', '.join([str(c) for c in self.coords])}])"


class File(Base):
    __tablename__ = "file"
    __table_args__ = (UniqueConstraint("filename", "tracking_id"), )

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    tracking_id = Column(String)
    grid_id = Column(ForeignKey("grid.id"))
    grid = relationship("Grid", backref="files")

    @classmethod
    def from_path(cls, path, session):
        cube = iris.load_cube(path)
        filename = os.path.basename(path)
        tracking_id = cube.attributes["tracking_id"]
        existing_file = session.scalar(
            select(File).where(
                File.filename == filename,
                File.tracking_id == tracking_id,
            ))
        if existing_file:
            return existing_file
        candidate = hash_grid(cube)
        coord_subq = session.scalars(
            select(Coord).where(
                or_(*[
                    and_(Coord.points_hash == c.points_hash, Coord.bounds_hash
                         == c.bounds_hash) for c in candidate.coords
                ])))
        existing = session.scalar(
            select(Grid).join(Grid.coords).where(
                or_(*[Grid.coords.contains(c) for c in coord_subq])))
        if existing is None:
            existing = Grid.from_cube(cube, session, candidate)
        return cls(
            filename=os.path.basename(path),
            tracking_id=cube.attributes["tracking_id"],
            grid=existing,
        )

    def __repr__(self):
        return f"File({self.filename}, {self.tracking_id}, {self.grid})"
