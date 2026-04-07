"""SQL query builder with parameterized queries and type safety."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

__all__ = ["QueryBuilder", "SQLQuery", "Condition", "JoinClause"]


@dataclass
class Condition:
    """A single WHERE condition."""
    column: str
    operator: str = "="
    value: Any = None

    def to_sql(self) -> tuple[str, list[Any]]:
        cond = f"{self.column} {self.operator} ?"
        return cond, [self.value]


@dataclass
class JoinClause:
    """A JOIN clause."""
    table: str
    join_type: str = "INNER"
    on_left: str = ""
    on_right: str = ""

    def to_sql(self) -> str:
        return f"{self.join_type} JOIN {self.table} ON {self.on_left} = {self.on_right}"


@dataclass
class SQLQuery:
    """A built SQL query with params."""
    sql: str
    params: list[Any]


class QueryBuilder:
    """Fluent SQL query builder supporting SELECT, INSERT, UPDATE, DELETE."""

    def __init__(self, table: str) -> None:
        self._table = table
        self._columns: list[str] = []
        self._conditions: list[tuple[str, list[Any]]] = []
        self._joins: list[JoinClause] = []
        self._order_by: list[str] = []
        self._group_by: list[str] = []
        self._having: list[tuple[str, list[Any]]] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None
        self._set_clauses: list[tuple[str, list[Any]]] = []
        self._values: list[list[Any]] = []
        self._returning: list[str] = []
        self._operation: str = "SELECT"

    def select(self, *columns: str) -> QueryBuilder:
        self._operation = "SELECT"
        self._columns = list(columns) if columns else ["*"]
        return self

    def insert(self, *columns: str) -> QueryBuilder:
        self._operation = "INSERT"
        self._columns = list(columns)
        return self

    def update(self) -> QueryBuilder:
        self._operation = "UPDATE"
        return self

    def delete(self) -> QueryBuilder:
        self._operation = "DELETE"
        return self

    def values(self, *rows: dict[str, Any]) -> QueryBuilder:
        for row in rows:
            self._values.append([row.get(c) for c in self._columns])
        return self

    def set(self, **kwargs: Any) -> QueryBuilder:
        for k, v in kwargs.items():
            self._set_clauses.append((f"{k} = ?", [v]))
        return self

    def join(self, table: str, on_left: str, on_right: str, join_type: str = "INNER") -> QueryBuilder:
        self._joins.append(JoinClause(table, join_type, on_left, on_right))
        return self

    def left_join(self, table: str, on_left: str, on_right: str) -> QueryBuilder:
        return self.join(table, on_left, on_right, "LEFT")

    def where(self, condition: str | Condition, *params: Any) -> QueryBuilder:
        if isinstance(condition, Condition):
            cond, vals = condition.to_sql()
            self._conditions.append((cond, vals))
        else:
            self._conditions.append((condition, list(params)))
        return self

    def and_where(self, condition: str, *params: Any) -> QueryBuilder:
        self._conditions.append((condition, list(params)))
        return self

    def order_by(self, *columns: str) -> QueryBuilder:
        self._order_by = list(columns)
        return self

    def group_by(self, *columns: str) -> QueryBuilder:
        self._group_by = list(columns)
        return self

    def having(self, condition: str, *params: Any) -> QueryBuilder:
        self._having.append((condition, list(params)))
        return self

    def limit(self, n: int) -> QueryBuilder:
        self._limit_val = n
        return self

    def offset(self, n: int) -> QueryBuilder:
        self._offset_val = n
        return self

    def returning(self, *columns: str) -> QueryBuilder:
        self._returning = list(columns)
        return self

    def build(self) -> SQLQuery:
        params: list[Any] = []

        if self._operation == "SELECT":
            cols = ", ".join(self._columns) or "*"
            sql_parts = [f"SELECT {cols} FROM {self._table}"]
            for join in self._joins:
                sql_parts.append(join.to_sql())
            if self._conditions:
                sql_parts.append("WHERE " + " AND ".join(c for c, _ in self._conditions))
                for _, vals in self._conditions:
                    params.extend(vals)
            if self._group_by:
                sql_parts.append("GROUP BY " + ", ".join(self._group_by))
            if self._having:
                sql_parts.append("HAVING " + " AND ".join(c for c, _ in self._having))
                for _, vals in self._having:
                    params.extend(vals)
            if self._order_by:
                sql_parts.append("ORDER BY " + ", ".join(self._order_by))
            if self._limit_val is not None:
                sql_parts.append("LIMIT ?")
                params.append(self._limit_val)
            if self._offset_val is not None:
                sql_parts.append("OFFSET ?")
                params.append(self._offset_val)

        elif self._operation == "INSERT":
            cols = ", ".join(self._columns)
            placeholders = ", ".join(["?"] * len(self._columns))
            sql_parts = [f"INSERT INTO {self._table} ({cols}) VALUES ({placeholders})"]
            for row in self._values:
                params.extend(row)
            if self._returning:
                sql_parts.append("RETURNING " + ", ".join(self._returning))

        elif self._operation == "UPDATE":
            sql_parts = [f"UPDATE {self._table} SET"]
            set_parts = [c for c, _ in self._set_clauses]
            sql_parts.append(", ".join(set_parts))
            for _, vals in self._set_clauses:
                params.extend(vals)
            if self._conditions:
                sql_parts.append("WHERE " + " AND ".join(c for c, _ in self._conditions))
                for _, vals in self._conditions:
                    params.extend(vals)

        elif self._operation == "DELETE":
            sql_parts = [f"DELETE FROM {self._table}"]
            if self._conditions:
                sql_parts.append("WHERE " + " AND ".join(c for c, _ in self._conditions))
                for _, vals in self._conditions:
                    params.extend(vals)
            if self._limit_val is not None:
                sql_parts.append("LIMIT ?")
                params.append(self._limit_val)

        return SQLQuery(sql=" ".join(sql_parts), params=params)

    def __repr__(self) -> str:
        q = self.build()
        return f"<Query: {q.sql[:80]}>"
