#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
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


def crawl(basedir):
    engine = setup_db()
    basepath = Path(basedir)
    candidates = basepath.glob("**/*.nc")
    with Session(engine) as session:
        # for candidate in islice(candidates, 10):
        start = time.time()
        for i, candidate in enumerate(candidates):
            filer = db.File(candidate, session)
            session.add(filer)
            now = time.time()
            print(f"{i} {now-start}")
        session.commit()


def main():
    fire.Fire(crawl)


if __name__ == "__main__":
    main()
