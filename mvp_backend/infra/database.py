from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable


class DatabaseClient:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(
            filename,
            check_same_thread=False,
            isolation_level=None,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys = ON;')
        try:
            self.connection.execute('PRAGMA journal_mode = WAL;')
        except sqlite3.DatabaseError:
            pass
        self.connection.execute('PRAGMA synchronous = NORMAL;')
        self.connection.execute('PRAGMA busy_timeout = 5000;')
        self.run_schema()

    def run_schema(self) -> None:
        schema_path = Path(__file__).with_name('schema.sql')
        sql = schema_path.read_text(encoding='utf-8')
        with self._lock:
            self.connection.executescript(sql)

    def execute(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> sqlite3.Cursor:
        with self._lock:
            if params is None:
                return self.connection.execute(sql)
            return self.connection.execute(sql, params)

    def run(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> sqlite3.Cursor:
        return self.execute(sql, params)

    def get(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> sqlite3.Row | None:
        return self.execute(sql, params).fetchone()

    def all(self, sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None) -> list[sqlite3.Row]:
        return self.execute(sql, params).fetchall()

    def executescript(self, sql: str) -> None:
        with self._lock:
            self.connection.executescript(sql)

    @contextmanager
    def transaction(self) -> Iterable[None]:
        with self._lock:
            self.connection.execute('BEGIN;')
            try:
                yield
                self.connection.execute('COMMIT;')
            except Exception:
                self.connection.execute('ROLLBACK;')
                raise

    def close(self) -> None:
        with self._lock:
            self.connection.close()


def ensure_directory_for_database(database_path: str) -> None:
    if database_path == ':memory:':
        return
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)


def create_database(database_path: str) -> DatabaseClient:
    ensure_directory_for_database(database_path)
    return DatabaseClient(database_path)
