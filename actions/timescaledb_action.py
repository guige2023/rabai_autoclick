"""TimescaleDB action module for RabAI AutoClick.

Provides time-series database operations via TimescaleDB
for high-throughput data ingestion and time-based queries.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TimescaleDBAction(BaseAction):
    """TimescaleDB integration for time-series data operations.

    Supports hypertable management, continuous aggregates,
    data compression, and time-based queries.

    Args:
        config: TimescaleDB configuration containing host, port, database,
                user, and password
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 5432)
        self.database = self.config.get("database", "postgres")
        self.user = self.config.get("user", "postgres")
        self.password = self.config.get("password", "")
        self._connection = None

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            try:
                import psycopg2
                conn_str = (
                    f"host={self.host} port={self.port} "
                    f"dbname={self.database} user={self.user} "
                    f"password={self.password}"
                )
                self._connection = psycopg2.connect(conn_str)
            except ImportError:
                raise ImportError(
                    "psycopg2 not installed. Run: pip install psycopg2-binary"
                )
        return self._connection

    def _execute(self, query: str, params: Optional[tuple] = None) -> Dict[str, Any]:
        """Execute a SQL query."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = {"columns": columns, "rows": rows}
            else:
                conn.commit()
                result = {"affected": cursor.rowcount}
            cursor.close()
            return result
        except Exception as e:
            return {"error": str(e), "success": False}

    def create_hypertable(
        self,
        table: str,
        time_column: str,
        partitioning_column: Optional[str] = None,
        chunk_time_interval: Optional[str] = None,
    ) -> ActionResult:
        """Create a hypertable.

        Args:
            table: Table name to convert
            time_column: Name of the time column
            partitioning_column: Optional partitioning column
            chunk_time_interval: Time interval for chunks

        Returns:
            ActionResult with creation status
        """
        query = f"SELECT create_hypertable('{table}', '{time_column}'"
        if partitioning_column:
            query += f", partitioning_column => '{partitioning_column}'"
        if chunk_time_interval:
            query += f", chunk_time_interval => '{chunk_time_interval}'"
        query += ")"

        result = self._execute(query)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"hypertable": table, "created": True})

    def create_continuous_aggregate(
        self,
        name: str,
        view_query: str,
        refresh_interval: str = "1 hour",
    ) -> ActionResult:
        """Create a continuous aggregate.

        Args:
            name: Name for the continuous aggregate
            view_query: Materialized view query
            refresh_interval: How often to refresh

        Returns:
            ActionResult with creation status
        """
        create_view = f"""
        CREATE MATERIALIZED VIEW {name}
        WITH (timescaledb.continuous) AS
        {view_query}
        """
        result = self._execute(create_view)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        refresh = f"CALL add_continuous_aggregate_policy('{name}', NULL, NULL, '{refresh_interval}')"
        refresh_result = self._execute(refresh)
        if "error" in refresh_result:
            return ActionResult(success=False, error=refresh_result["error"])

        return ActionResult(success=True, data={"aggregate": name, "created": True})

    def compress_chunks(
        self,
        table: Optional[str] = None,
        older_than: Optional[str] = None,
    ) -> ActionResult:
        """Compress chunks for a hypertable.

        Args:
            table: Hypertable name (optional, compresses all if not provided)
            older_than: Compress chunks older than this interval

        Returns:
            ActionResult with compression status
        """
        if table and older_than:
            query = f"SELECT compress_chunk(c, '{older_than}') FROM show_chunks('{table}') c"
        elif table:
            query = f"SELECT compress_chunk(c) FROM show_chunks('{table}') c"
        else:
            query = "SELECT compress_chunk(c) FROM show_chunks() c"

        result = self._execute(query)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"compressed": True})

    def decompress_chunks(
        self,
        table: Optional[str] = None,
    ) -> ActionResult:
        """Decompress chunks.

        Args:
            table: Hypertable name (optional)

        Returns:
            ActionResult with decompression status
        """
        if table:
            query = f"SELECT decompress_chunk(c) FROM show_chunks('{table}') c"
        else:
            query = "SELECT decompress_chunk(c) FROM show_chunks() c"

        result = self._execute(query)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"decompressed": True})

    def insert_data(
        self,
        table: str,
        columns: List[str],
        values: List[List[Any]],
    ) -> ActionResult:
        """Insert data into a table.

        Args:
            table: Table name
            columns: List of column names
            values: List of row values

        Returns:
            ActionResult with insert count
        """
        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s" for _ in columns])
        query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.executemany(query, values)
            conn.commit()
            cursor.close()
            return ActionResult(success=True, data={"inserted": len(values)})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def time_bucket_query(
        self,
        table: str,
        bucket_size: str,
        time_column: str,
        aggregates: Optional[Dict[str, str]] = None,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
    ) -> ActionResult:
        """Query data using time_bucket.

        Args:
            table: Table name
            bucket_size: Bucket interval (e.g., '1 hour', '5 minutes')
            time_column: Time column name
            aggregates: Dict of column -> aggregate function
            where: Optional WHERE clause
            group_by: Optional additional GROUP BY columns

        Returns:
            ActionResult with bucketed data
        """
        select_parts = [f"time_bucket('{bucket_size}', {time_column}) AS bucket"]
        if aggregates:
            for col, agg in aggregates.items():
                select_parts.append(f"{agg}({col}) AS {col}_{agg}")
        else:
            select_parts.append(f"COUNT(*) AS count")

        select_str = ", ".join(select_parts)
        query = f"SELECT {select_str} FROM {table}"

        if where:
            query += f" WHERE {where}"

        group_parts = ["bucket"]
        if group_by:
            group_parts.append(group_by)
        query += f" GROUP BY {', '.join(group_parts)}"
        query += " ORDER BY bucket"

        result = self._execute(query)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def show_chunks(
        self,
        table: Optional[str] = None,
        older_than: Optional[str] = None,
    ) -> ActionResult:
        """Show chunks for a hypertable.

        Args:
            table: Hypertable name (optional)
            older_than: Show chunks older than interval

        Returns:
            ActionResult with chunk list
        """
        if table and older_than:
            query = f"SELECT show_chunks('{table}', older_than => '{older_than}')"
        elif table:
            query = f"SELECT show_chunks('{table}')"
        else:
            query = "SELECT show_chunks()"

        result = self._execute(query)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"chunks": result.get("rows", [])})

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute TimescaleDB operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "create_hypertable": self.create_hypertable,
            "create_continuous_aggregate": self.create_continuous_aggregate,
            "compress_chunks": self.compress_chunks,
            "decompress_chunks": self.decompress_chunks,
            "insert_data": self.insert_data,
            "time_bucket_query": self.time_bucket_query,
            "show_chunks": self.show_chunks,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
