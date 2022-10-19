#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import fire

from . import db


def setup_db():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        echo=True,
        future=True,
    )
    db.Base.metadata.create_all(engine)
    return engine


def crawl(basedir):
    engine = setup_db()
    basepath = Path(basedir)
    candidates = basepath.glob("**/*.nc")
    with Session(engine) as session:
        for candidate in candidates:
            filer = db.File(candidate)
            print(filer)
            break


def main():
    fire.Fire(crawl)


if __name__ == "__main__":
    main()
