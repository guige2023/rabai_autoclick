"""DuckDB action module for RabAI AutoClick.

Provides in-process analytical database operations via DuckDB
for fast OLAP queries without external server dependencies.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union, Tuple
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DuckDBAction(BaseAction):
    """DuckDB integration for in-process analytical queries.

    Supports SQL execution, table operations, parquet/csv import,
    and cross-database queries.

    Args:
        config: DuckDB configuration containing database path and read_only flag
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.db_path = self.config.get("database", ":memory:")
        self.read_only = self.config.get("read_only", False)
        self._connection = None
        self._init_connection()

    def _init_connection(self) -> None:
        """Initialize DuckDB connection."""
        try:
            import duckdb
            self._duckdb = duckdb
            if self.db_path == ":memory:":
                self._connection = self._duckdb.connect(self.db_path)
            else:
                self._connection = self._duckdb.connect(
                    self.db_path, read_only=self.read_only
                )
        except ImportError:
            raise ImportError("duckdb not installed. Run: pip install duckdb")

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple] = None,
        fetch: str = "all",
    ) -> ActionResult:
        """Execute a SQL query.

        Args:
            query: SQL query string
            params: Optional query parameters tuple
            fetch: Fetch mode ('all', 'one', 'arrow')

        Returns:
            ActionResult with query results
        """
        try:
            if params:
                result = self._connection.execute(query, params)
            else:
                result = self._connection.execute(query)

            if fetch == "arrow":
                data = result.arrow().to_pydict()
            elif fetch == "one":
                data = result.fetchone()
            else:
                data = result.fetchall()

            return ActionResult(success=True, data={"rows": data})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def select(
        self,
        table: str,
        columns: str = "*",
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
    ) -> ActionResult:
        """Select data from a table.

        Args:
            table: Table name
            columns: Column list (default *)
            where: Optional WHERE clause
            order_by: Optional ORDER BY clause
            limit: Maximum rows

        Returns:
            ActionResult with rows
        """
        query = f"SELECT {columns} FROM {table}"
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        query += f" LIMIT {limit}"

        return self.execute_query(query)

    def create_table(
        self,
        table: str,
        columns: Dict[str, str],
    ) -> ActionResult:
        """Create a table.

        Args:
            table: Table name
            columns: Dict of column_name -> type

        Returns:
            ActionResult with creation status
        """
        col_defs = ", ".join(f"{k} {v}" for k, v in columns.items())
        query = f"CREATE TABLE {table} ({col_defs})"

        return self.execute_query(query)

    def drop_table(self, table: str, if_exists: bool = True) -> ActionResult:
        """Drop a table.

        Args:
            table: Table name
            if_exists: Use IF EXISTS clause

        Returns:
            ActionResult with drop status
        """
        exists_clause = "IF EXISTS" if if_exists else ""
        return self.execute_query(f"DROP TABLE {exists_clause} {table}")

    def insert(
        self,
        table: str,
        columns: List[str],
        values: List[List[Any]],
    ) -> ActionResult:
        """Insert rows into a table.

        Args:
            table: Table name
            columns: List of column names
            values: List of row value lists

        Returns:
            ActionResult with insert count
        """
        col_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        try:
            for row in values:
                self._connection.execute(query, row)
            return ActionResult(success=True, data={"inserted": len(values)})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def import_csv(
        self,
        csv_path: str,
        table: str,
        header: bool = True,
        delimiter: str = ",",
    ) -> ActionResult:
        """Import CSV file into a table.

        Args:
            csv_path: Path to CSV file
            table: Target table name
            header: Whether CSV has header row
            delimiter: Field delimiter

        Returns:
            ActionResult with import status
        """
        header_str = "HEADER" if header else ""
        query = f"""
        CREATE TABLE {table} AS
        SELECT * FROM read_csv_auto('{csv_path}', {header_str}, delim='{delimiter}')
        """

        try:
            self._connection.execute(query)
            return ActionResult(success=True, data={"imported": True})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def import_parquet(
        self,
        parquet_path: str,
        table: Optional[str] = None,
    ) -> ActionResult:
        """Import Parquet file into a table.

        Args:
            parquet_path: Path to Parquet file
            table: Optional target table name (defaults to file name)

        Returns:
            ActionResult with import status
        """
        if table is None:
            table = Path(parquet_path).stem

        query = f"""
        CREATE TABLE {table} AS
        SELECT * FROM read_parquet('{parquet_path}')
        """

        try:
            self._connection.execute(query)
            return ActionResult(success=True, data={"table": table, "imported": True})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def export_parquet(
        self,
        table: str,
        parquet_path: str,
    ) -> ActionResult:
        """Export table to Parquet file.

        Args:
            table: Source table name
            parquet_path: Target Parquet file path

        Returns:
            ActionResult with export status
        """
        query = f"COPY {table} TO '{parquet_path}' (FORMAT PARQUET)"

        try:
            self._connection.execute(query)
            return ActionResult(success=True, data={"exported": parquet_path})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def show_tables(self) -> ActionResult:
        """Show all tables in the database.

        Returns:
            ActionResult with table names
        """
        return self.execute_query("SHOW TABLES")

    def describe_table(self, table: str) -> ActionResult:
        """Describe table structure.

        Args:
            table: Table name

        Returns:
            ActionResult with column info
        """
        return self.execute_query(f"DESCRIBE {table}")

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute DuckDB operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "execute_query": self.execute_query,
            "select": self.select,
            "create_table": self.create_table,
            "drop_table": self.drop_table,
            "insert": self.insert,
            "import_csv": self.import_csv,
            "import_parquet": self.import_parquet,
            "export_parquet": self.export_parquet,
            "show_tables": self.show_tables,
            "describe_table": self.describe_table,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
