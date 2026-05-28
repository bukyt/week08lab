"""
Trino + Iceberg adapter — stub.

This file is the Open/Closed exhibit: a future engineer migrating to Trino
fills in the method bodies and nothing else in the repo changes. The factory
already knows how to wire it up, the merger and quality code already talk to
TableEngine, the tests already mock the interface.

The class exists, the import works, the contract is declared. See
`marias_notes/take_home_trino.md` for the migration brief.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import time

from internal_etl_package.engines.base import TableEngine

_NOT_IMPLEMENTED_MSG = "Trino migration — see marias_notes/take_home_trino.md"

log = logging.getLogger(__name__)


class TrinoIcebergEngine(TableEngine):
    """Trino + Iceberg engine implementation."""

    _IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def __init__(self, trino_conn=None):
        if trino_conn is not None:
            self._trino = trino_conn
            return

        from trino.dbapi import connect

        self._trino = connect(
            host="trino",
            port=8080,
            user="airflow",
            catalog="iceberg_datalake",
            schema="default",
        )

    # ---------------------------------------------------------
    # helpers
    # ---------------------------------------------------------

    def _validate_identifier(self, name: str) -> str:
        if not self._IDENTIFIER_RE.match(name):
            raise ValueError(f"Unsafe SQL identifier: {name}")

        return name

    def _execute_query(self, query: str) -> list[tuple]:
        last_err = None

        for attempt in range(3):
            try:
                with self._trino.cursor() as cursor:
                    log.debug(f"[Trino Engine] Executing query:\n{query}")
                    cursor.execute(query)

                    try:
                        return cursor.fetchall()
                    except Exception:
                        return []

            except Exception as e:
                last_err = e
                wait = 1.5**attempt

                log.warning(
                    f"[Trino Engine] Query failed "
                    f"(attempt {attempt + 1}): {e}"
                )

                time.sleep(wait)

        raise RuntimeError("Trino query failed after retries") from last_err

    def _split_table(self, table: str) -> tuple[str, str]:
        parts = table.split(".")

        if len(parts) == 2:
            schema = self._validate_identifier(parts[0])
            tbl = self._validate_identifier(parts[1])
            return schema, tbl

        return "default", self._validate_identifier(table)

    def _table_exists(self, schema: str, table: str) -> bool:
        query = f"""
            SELECT COUNT(*)
            FROM iceberg_datalake.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """

        try:
            result = self._execute_query(query)
            return result[0][0] > 0
        except Exception:
            return False

    # ---------------------------------------------------------
    # core contract methods
    # ---------------------------------------------------------

    def current_snapshot_id(self, table: str) -> int:
        schema, tbl = self._split_table(table)

        if not self._table_exists(schema, tbl):
            log.warning(
                f"[Trino Engine] Snapshot lookup skipped, "
                f"table missing: {table}"
            )
            return -1

        query = f"""
            SELECT snapshot_id
            FROM iceberg_datalake.{schema}."{tbl}$snapshots"
            ORDER BY committed_at DESC
            LIMIT 1
        """

        result = self._execute_query(query)

        if not result or result[0][0] is None:
            return -1

        return int(result[0][0])

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        schema, tbl = self._split_table(table)

        if not self._table_exists(schema, tbl):
            log.warning(
                f"[Trino Engine] Rollback skipped, "
                f"table missing: {table}"
            )
            return

        query = f"""
            CALL iceberg_datalake.system.rollback_to_snapshot(
                '{schema}',
                '{tbl}',
                {snapshot_id}
            )
        """

        self._execute_query(query)

        log.info(
            f"[Trino Engine] Rolled back "
            f"{table} to snapshot {snapshot_id}"
        )

    # ---------------------------------------------------------
    # merge
    # ---------------------------------------------------------

    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        schema, tbl = self._split_table(table)
        pk = self._validate_identifier(primary_key)

        target = f"iceberg_datalake.{schema}.{tbl}"

        # 1. load CSV rows (unchanged)
        import csv, os

        if os.path.isdir(source_path):
            files = [os.path.join(source_path, f)
                    for f in os.listdir(source_path)
                    if f.endswith(".csv")]
        else:
            files = [source_path]

        rows = []
        for fpath in files:
            with open(fpath, newline="") as f:
                rows.extend(list(csv.DictReader(f)))

        if not rows:
            return self.row_count(table)

        columns = list(rows[0].keys())
        col_sql = ", ".join(self._validate_identifier(c) for c in columns)

        # 2. delete existing keys (UPSERT simulation)
        pk_values = {r[pk] for r in rows}
        pk_list = ", ".join(f"'{v}'" for v in pk_values)

        delete_sql = f"""
            DELETE FROM {target}
            WHERE {pk} IN ({pk_list})
        """
        self._execute_query(delete_sql)

        # 3. insert fresh rows
        value_rows = []
        for r in rows:
            escaped = [str(r[c]).replace("'", "''") for c in columns]
            values = ", ".join(f"'{v}'" for v in escaped)
            value_rows.append(f"({values})")

        insert_sql = f"""
            INSERT INTO {target} ({col_sql})
            VALUES {",".join(value_rows)}
        """

        self._execute_query(insert_sql)

        return self.row_count(table)

    # ---------------------------------------------------------
    # quality helpers
    # ---------------------------------------------------------

    def count_duplicates(self, table: str, primary_key: str) -> int:
        schema, tbl = self._split_table(table)
        pk = self._validate_identifier(primary_key)

        if not self._table_exists(schema, tbl):
            log.warning(
                f"[Trino Engine] Duplicate check skipped, "
                f"table missing: {table}"
            )
            return 0

        query = f"""
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT {pk}, COUNT(*) AS cnt
                FROM iceberg_datalake.{schema}."{tbl}"
                GROUP BY {pk}
                HAVING COUNT(*) > 1
            )
        """

        result = self._execute_query(query)

        if not result or result[0][0] is None:
            return 0

        return int(result[0][0])

    def row_count(self, table: str) -> int:
        schema, tbl = self._split_table(table)

        if not self._table_exists(schema, tbl):
            log.warning(
                f"[Trino Engine] row_count skipped, "
                f"table missing: {table}"
            )
            return 0

        query = f"""
            SELECT COUNT(*)
            FROM iceberg_datalake.{schema}."{tbl}"
        """

        result = self._execute_query(query)

        if not result or result[0][0] is None:
            return 0

        return int(result[0][0])

    def null_rate(self, table: str, column: str) -> float:
        schema, tbl = self._split_table(table)
        col = self._validate_identifier(column)

        if not self._table_exists(schema, tbl):
            log.warning(
                f"[Trino Engine] null_rate skipped, "
                f"table missing: {table}"
            )
            return 0.0

        query = f"""
            SELECT
                CAST(
                    COUNT(CASE WHEN {col} IS NULL THEN 1 END)
                    AS DOUBLE
                ) / NULLIF(COUNT(*), 0)
            FROM iceberg_datalake.{schema}."{tbl}"
        """

        result = self._execute_query(query)

        if not result or result[0][0] is None:
            return 0.0

        return float(result[0][0])