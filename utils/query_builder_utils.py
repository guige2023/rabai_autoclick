"""SQL query builder utilities: chainable query construction for common database operations."""

from __future__ import annotations

from typing import Any

__all__ = [
    "QueryBuilder",
    "SelectQuery",
    "InsertQuery",
    "UpdateQuery",
    "DeleteQuery",
]


class QueryBuilder:
    """Base class for SQL query builders."""

    def __init__(self, table: str) -> None:
        self._table = table

    def _escape(self, col: str) -> str:
        """Escape column name."""
        return f'"{col}"'


class SelectQuery(QueryBuilder):
    """Chainable SELECT query builder."""

    def __init__(self, table: str) -> None:
        super().__init__(table)
        self._columns: list[str] = ["*"]
        self._joins: list[str] = []
        self._where_clauses: list[str] = []
        self._where_params: list[Any] = []
        self._group_by: list[str] = []
        self._having: list[str] = []
        self._order_by: list[str] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None

    def select(self, *columns: str) -> "SelectQuery":
        """Set columns to select."""
        self._columns = [c for c in columns]
        return self

    def join(self, table: str, on: str, join_type: str = "INNER") -> "SelectQuery":
        """Add a JOIN clause."""
        self._joins.append(f"{join_type} JOIN {self._escape(table)} ON {on}")
        return self

    def left_join(self, table: str, on: str) -> "SelectQuery":
        return self.join(table, on, "LEFT")

    def where(self, condition: str, *params: Any) -> "SelectQuery":
        """Add a WHERE condition."""
        self._where_clauses.append(condition)
        self._where_params.extend(params)
        return self

    def where_in(self, column: str, values: list[Any]) -> "SelectQuery":
        """Add WHERE column IN (...) clause."""
        placeholders = ", ".join(["?" for _ in values])
        self._where_clauses.append(f"{self._escape(column)} IN ({placeholders})")
        self._where_params.extend(values)
        return self

    def group_by(self, *columns: str) -> "SelectQuery":
        """Add GROUP BY clause."""
        self._group_by.extend(columns)
        return self

    def having(self, condition: str) -> "SelectQuery":
        """Add HAVING clause."""
        self._having.append(condition)
        return self

    def order_by(self, column: str, direction: str = "ASC") -> "SelectQuery":
        """Add ORDER BY clause."""
        self._order_by.append(f"{self._escape(column)} {direction}")
        return self

    def limit(self, n: int) -> "SelectQuery":
        self._limit_val = n
        return self

    def offset(self, n: int) -> "SelectQuery":
        self._offset_val = n
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query string and parameters."""
        cols = ", ".join(self._columns) if self._columns != ["*"] else "*"
        sql = f"SELECT {cols} FROM {self._escape(self._table)}"

        for join in self._joins:
            sql += f" {join}"

        if self._where_clauses:
            sql += " WHERE " + " AND ".join(self._where_clauses)

        if self._group_by:
            sql += " GROUP BY " + ", ".join(self._escape(c) for c in self._group_by)

        if self._having:
            sql += " HAVING " + " AND ".join(self._having)

        if self._order_by:
            sql += " ORDER BY " + ", ".join(self._order_by)

        if self._limit_val is not None:
            sql += f" LIMIT {self._limit_val}"

        if self._offset_val is not None:
            sql += f" OFFSET {self._offset_val}"

        return sql, list(self._where_params)


class InsertQuery(QueryBuilder):
    """Chainable INSERT query builder."""

    def __init__(self, table: str) -> None:
        super().__init__(table)
        self._columns: list[str] = []
        self._values: list[list[Any]] = []

    def insert(self, data: dict[str, Any]) -> "InsertQuery":
        """Add a row to insert."""
        if not self._columns:
            self._columns = list(data.keys())
        self._values.append([data.get(c) for c in self._columns])
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query string and parameters."""
        cols = ", ".join(self._escape(c) for c in self._columns)
        placeholders = ", ".join(["?" for _ in self._columns])
        multi = len(self._values) > 1

        if multi:
            values_clause = ", ".join([f"({placeholders})" for _ in self._values])
            sql = f"INSERT INTO {self._escape(self._table)} ({cols}) VALUES {values_clause}"
            params: list[Any] = []
            for row in self._values:
                params.extend(row)
        else:
            sql = f"INSERT INTO {self._escape(self._table)} ({cols}) VALUES ({placeholders})"
            params = self._values[0] if self._values else []

        return sql, params


class UpdateQuery(QueryBuilder):
    """Chainable UPDATE query builder."""

    def __init__(self, table: str) -> None:
        super().__init__(table)
        self._sets: list[str] = []
        self._params: list[Any] = []
        self._where_clauses: list[str] = []
        self._where_params: list[Any] = []

    def set(self, column: str, value: Any) -> "UpdateQuery":
        """Add a SET clause."""
        self._sets.append(f"{self._escape(column)} = ?")
        self._params.append(value)
        return self

    def where(self, condition: str, *params: Any) -> "UpdateQuery":
        """Add WHERE clause."""
        self._where_clauses.append(condition)
        self._where_params.extend(params)
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query string and parameters."""
        sql = f"UPDATE {self._escape(self._table)} SET {', '.join(self._sets)}"

        if self._where_clauses:
            sql += " WHERE " + " AND ".join(self._where_clauses)

        params = list(self._params) + list(self._where_params)
        return sql, params


class DeleteQuery(QueryBuilder):
    """Chainable DELETE query builder."""

    def __init__(self, table: str) -> None:
        super().__init__(table)
        self._where_clauses: list[str] = []
        self._where_params: list[Any] = []

    def where(self, condition: str, *params: Any) -> "DeleteQuery":
        """Add WHERE clause."""
        self._where_clauses.append(condition)
        self._where_params.extend(params)
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the SQL query string and parameters."""
        sql = f"DELETE FROM {self._escape(self._table)}"

        if self._where_clauses:
            sql += " WHERE " + " AND ".join(self._where_clauses)

        return sql, list(self._where_params)
