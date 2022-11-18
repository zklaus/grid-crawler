#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from itertools import chain
from pathlib import Path

import fire
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from . import db


def setup_db():
    engine = create_engine(
        "sqlite+pysqlite:///grids.db",
        echo=False,
        future=True,
    )
    db.Base.metadata.create_all(engine)
    return engine


def crawl(*basedirs):
    engine = setup_db()
    candidates = chain.from_iterable(
        Path(basedir).glob("**/*.nc") for basedir in basedirs)
    with Session(engine) as session:
        # for candidate in islice(candidates, 10):
        start = time.time()
        for i, candidate in enumerate(candidates):
            filer = db.File.from_path(candidate, session)
            session.add(filer)
            now = time.time()
            print(f"{i} {now-start}")
        session.commit()


def main():
    fire.Fire(crawl)


if __name__ == "__main__":
    main()
