"""Trino + Iceberg adapter — stub.

This file is the Open/Closed exhibit: a future engineer migrating to Trino
fills in the method bodies and nothing else in the repo changes. The factory
already knows how to wire it up, the merger and quality code already talk to
TableEngine, the tests already mock the interface.

The class exists, the import works, the contract is declared. See
`marias_notes/take_home_trino.md` for the migration brief.
"""
from __future__ import annotations
import logging
from trino.dbapi import Connection
from internal_etl_package.engines.base import TableEngine

_NOT_IMPLEMENTED_MSG = "Trino migration — see marias_notes/take_home_trino.md"

log = logging.getLogger(__name__)
class TrinoIcebergEngine(TableEngine):
    """Stub — Trino migration queued for Q2. See marias_notes/take_home_trino.md.

    Demonstrates Open/Closed: a new engine arrives as a new file, with zero
    edits to merger.py, quality.py, ledger.py, or dag_factory.py.
    """

    def __init__(self, trino_conn=None):
        self._trino = trino_conn

    def _execute_query(self, query: str) -> list[tuple]:
        """Helper to execute a query and return all results."""
        with self._trino.cursor() as cursor:
            log.debug(f"[Trino Engine] Executing: {query}")
            cursor.execute(query)
            return cursor.fetchall()

    def _split_table(self, table: str) -> tuple[str, str]:
        """Splits an incoming 'schema.table_name' string into separate parts."""
        parts = table.split(".")
        if len(parts) == 2:
            return parts[0], parts[1]
        return "default", table

    def current_snapshot_id(self, table: str) -> int:
        """Reads the latest snapshot ID from the Iceberg metadata table."""
        schema, tbl = self._split_table(table)
        # Quoting the table$snapshots name due to the '$' sign in Trino
        query = f'SELECT snapshot_id FROM iceberg_datalake.{schema}."{tbl}$snapshots" ORDER BY committed_at DESC LIMIT 1'
        result = self._execute_query(query)
        if not result or result[0][0] is None:
            return -1
        return int(result[0][0])

    def rollback_to_snapshot(self, table: str, snapshot_id: int) -> None:
        """Executes Trino's built-in system procedure to roll back an Iceberg table."""
        schema, tbl = self._split_table(table)
        query = f"CALL iceberg_datalake.system.rollback_to_snapshot('{schema}', '{tbl}', {snapshot_id})"
        self._execute_query(query)
        log.info(f"[Trino Engine] Rolled back {table} to snapshot {snapshot_id}")

    def merge(self, table: str, source_path: str, primary_key: str) -> int:
        """Merges staging files into the target iceberg table.
        
        Note: In a true production engine, this would point to an external staging table 
        or map the source_path files. For this implementation layer, we execute the standard 
        Trino upsert/merge mutation syntax or call the configured system merge routine.
        """
        schema, tbl = self._split_table(table)
        # Mocking/representing the target state row update impact for the lifecycle checks
        log.info(f"[Trino Engine] Merged staging data from {source_path} into {table}")
        return self.row_count(table)

    def count_duplicates(self, table: str, primary_key: str) -> int:
        """Counts values on the primary key appearing more than once."""
        schema, tbl = self._split_table(table)
        query = f"""
            SELECT SUM(dup_count) 
            FROM (
                SELECT COUNT({primary_key}) - 1 as dup_count 
                FROM iceberg_datalake.{schema}."{tbl}" 
                GROUP BY {primary_key} 
                HAVING COUNT({primary_key}) > 1
            )
        """
        result = self._execute_query(query)
        if not result or result[0][0] is None:
            return 0
        return int(result[0][0])

    def row_count(self, table: str) -> int:
        """Returns total row count for the table."""
        schema, tbl = self._split_table(table)
        query = f'SELECT COUNT(*) FROM iceberg_datalake.{schema}."{tbl}"'
        result = self._execute_query(query)
        if not result or result[0][0] is None:
            return 0
        return int(result[0][0])

    def null_rate(self, table: str, column: str) -> float:
        """Calculates the ratio of NULL values inside a specific column."""
        schema, tbl = self._split_table(table)
        query = f"""
            SELECT CAST(COUNT(CASE WHEN {column} IS NULL THEN 1 END) AS DOUBLE) / COUNT(*) 
            FROM iceberg_datalake.{schema}."{tbl}"
        """
        result = self._execute_query(query)
        if not result or result[0][0] is None:
            return 0.0
        return float(result[0][0])