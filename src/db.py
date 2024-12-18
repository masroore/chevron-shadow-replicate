import itertools
from typing import Any

import pyodbc


class Database:
    _conn_string: str | None = None
    _conn: pyodbc.Connection | None = None

    @staticmethod
    def make(config: dict):
        conn_string = ";".join("=".join([k, v]) for k, v in config.items())
        return Database(conn_string)

    def __init__(self, conn_string: str):
        self._conn_string = conn_string
        # self._conn = pyodbc.connect(self._conn_string)

    def commit(self):
        self._conn.commit()

    def cursor(self) -> pyodbc.Cursor:
        return self._conn.cursor()

    def connect(self):
        self._conn = pyodbc.connect(self._conn_string)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc_info):
        self.close()

    @staticmethod
    def column_names(cur: pyodbc.Cursor) -> list[str]:
        return [c[0] for c in cur.description]

    def execute(self, sql: str, *params: Any) -> int:
        with self.cursor() as cur:
            cur.execute(sql, params)
            self.commit()
            return cur.rowcount

    def exec_sproc(self, sproc: str, *params: Any):
        with self.cursor() as cur:
            sql = f"EXEC {sproc}"
            cur.execute(sql, params)
            self.commit()

    def sproc(self, sproc_name: str, *params: Any):
        sproc_params = f"{sproc_name} " + ",".join(itertools.repeat("?", len(params)))
        return self.exec_sproc(sproc_params, *params)

    def fetch_all(self, sql: str, *params: Any) -> list[dict]:
        with self.cursor() as cur:
            rows = cur.execute(sql, params).fetchall()
            cols = self.column_names(cur)
            return list(map(lambda o: dict(zip(cols, o)), rows))

    def fetch(self, sql: str, *params: Any) -> dict | None:
        with self.cursor() as cur:
            cur.execute(sql, params)
            cols = self.column_names(cur)
            row: pyodbc.Row | None = cur.fetchone()

            if row:
                return dict(zip(cols, row))

            return None

    def fetch_val(self, sql: str, *params: Any) -> Any:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchval()

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
        self._conn = None


_conn_string: str | None = None


def set_connection_string(config: dict):
    global _conn_string
    _conn_string = ";".join("=".join([k, v]) for k, v in config.items())


def conn() -> Database:
    return Database(_conn_string)


def fetch(sql: str, *params: Any) -> dict | None:
    with conn() as db_:
        return db_.fetch(sql, *params)


def fetch_scalar(sql: str, column: str, *params: Any) -> Any | None:
    row = fetch(sql, *params)
    return row.get(column) if row else None


def fetch_all(sql: str, *params: Any) -> list[dict]:
    with conn() as db_:
        return db_.fetch_all(sql, *params)


def fetch_all_scalars(sql: str, column: str, *params: Any) -> list[Any]:
    rows = fetch_all(sql, *params)
    return list(map(lambda r: r.get(column), rows)) if rows else []


def execute(sql: str, *params: Any) -> int:
    with conn() as db_:
        return db_.execute(sql, *params)


def sproc(sproc_name: str, *params: Any):
    with conn() as db_:
        return db_.sproc(sproc_name, *params)
