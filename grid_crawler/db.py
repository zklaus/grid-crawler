# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, PickleType
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DimCoord(Base):
    __tablename__ = "dim_coord"

    id = Column(Integer, primary_key=True)
    hash = Column(Integer, nullable=False, unique=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)


class TwoDCoord(Base):
    __tablename__ = "two_d_coord"

    id = Column(Integer, primary_key=True)
    hash = Column(Integer, nullable=False, unique=True)
    points = Column(PickleType, nullable=False)
    points_hash = Column(Integer, nullable=False)
    bounds = Column(PickleType)
    bounds_hash = Column(Integer)
