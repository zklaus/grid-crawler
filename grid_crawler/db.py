# -*- coding: utf-8 -*-

import os

import iris
from sqlalchemy import Column, ForeignKey, Integer, PickleType, String, Table
from sqlalchemy.orm import declarative_base, relationship
from xxhash import xxh3_64, xxh3_64_intdigest

from .hash import phash_1d, phash_2d

Base = declarative_base()


grid_dim_coord = Table(
    "grid_dim_coord",
    Base.metadata,
    Column("grid_id", ForeignKey("grid.id")),
    Column("dim_coord_id", ForeignKey("dim_coord.id")),
)


class DimCoord(Base):
    __tablename__ = "dim_coord"

    id = Column(Integer, primary_key=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)
    bounds_lower_phash = Column(Integer)
    bounds_upper_phash = Column(Integer)

    def __init__(self, coord):
        points = coord.points
        points_hash = xxh3_64_intdigest(points)
        bounds = coord.bounds
        if bounds is not None:
            bounds_hash = xxh3_64_intdigest(bounds)
            bounds_lower_phash = phash_1d(bounds[:, 0]).hash
            bounds_upper_phash = phash_1d(bounds[:, 1]).hash
        else:
            bounds_hash = None
            bounds_lower_phash = None
            bounds_upper_phash = None
        super().__init__(
            points=points,
            points_hash=points_hash,
            points_phash=phash_1d(points).hash,
            bounds=bounds,
            bounds_hash=bounds_hash,
            bounds_lower_phash=bounds_lower_phash,
            bounds_upper_phash=bounds_upper_phash,
        )

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

    id = Column(Integer, primary_key=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    points_phash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)

    def __init__(self, coord):
        points = coord.points
        points_hash = xxh3_64_intdigest(points)
        bounds = coord.bounds
        if bounds is not None:
            bounds_hash = xxh3_64_intdigest(bounds)
        else:
            bounds_hash = None
        super().__init__(
            points=points,
            points_hash=points_hash,
            points_phash=phash_2d(points).hash,
            bounds=bounds,
            bounds_hash=bounds_hash,
        )

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

    def __init__(self, cube, session):
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
        for coord in dim_coords.values():
            candidate = DimCoord(coord)
            candidate = session.get(DimCoord, candidate.id)
            print(candidate)
        super().__init__(
            dim_coords=[DimCoord(coord)
                        for coord in dim_coords.values()],
            two_d_coords=[TwoDCoord(coord)
                          for coord in set(sum(non_dim_coords.values(), []))],
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
        super().__init__(
            filename=os.path.basename(path),
            tracking_id=cube.attributes["tracking_id"],
            grid=Grid(cube, session),
        )

    def __repr__(self):
        return f"File({self.filename}, {self.tracking_id}, {self.grid})"
