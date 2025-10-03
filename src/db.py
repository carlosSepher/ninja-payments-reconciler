from __future__ import annotations

import contextlib
import logging
from typing import Iterator

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from .settings import get_settings

LOGGER = logging.getLogger(__name__)


class Database:
    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 10) -> None:
        self._dsn = dsn
        self._pool = ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, dsn=dsn)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET search_path TO payments")
                conn.commit()

    @contextlib.contextmanager
    def connection(self) -> Iterator[psycopg2.extensions.connection]:
        conn = self._pool.getconn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SET search_path TO payments")
                yield conn
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        LOGGER.info("Closing database pool")
        self._pool.closeall()


def create_database() -> Database:
    settings = get_settings()
    return Database(settings.database_dsn)
