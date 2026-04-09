"""
Query Action Module.

Provides query building and execution capabilities
with support for filtering, sorting, and pagination.
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class SortOrder(Enum):
    """Sort order options."""
    ASC = "asc"
    DESC = "desc"


class FilterOperator(Enum):
    """Filter operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class Filter:
    """Query filter definition."""
    field: str
    operator: FilterOperator
    value: Any = None


@dataclass
class Sort:
    """Query sort definition."""
    field: str
    order: SortOrder = SortOrder.ASC


@dataclass
class Pagination:
    """Pagination parameters."""
    page: int = 1
    page_size: int = 20


@dataclass
class QueryResult:
    """Result of a query execution."""
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool


class QueryBuilder:
    """Builder for constructing queries."""

    def __init__(self):
        self._filters: List[Filter] = []
        self._sorts: List[Sort] = []
        self._pagination: Optional[Pagination] = None
        self._selects: List[str] = []
        self._groups: List[str] = []
        self._limit: Optional[int] = None
        self._offset: int = 0

    def filter(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "QueryBuilder":
        """Add a filter."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)
        self._filters.append(Filter(field, operator, value))
        return self

    def eq(self, field: str, value: Any) -> "QueryBuilder":
        """Add equality filter."""
        return self.filter(field, FilterOperator.EQ, value)

    def ne(self, field: str, value: Any) -> "QueryBuilder":
        """Add not-equal filter."""
        return self.filter(field, FilterOperator.NE, value)

    def gt(self, field: str, value: Any) -> "QueryBuilder":
        """Add greater-than filter."""
        return self.filter(field, FilterOperator.GT, value)

    def gte(self, field: str, value: Any) -> "QueryBuilder":
        return self.filter(field, FilterOperator.GTE, value)

    def lt(self, field: str, value: Any) -> "QueryBuilder":
        return self.filter(field, FilterOperator.LT, value)

    def lte(self, field: str, value: Any) -> "QueryBuilder":
        return self.filter(field, FilterOperator.LTE, value)

    def contains(self, field: str, value: Any) -> "QueryBuilder":
        return self.filter(field, FilterOperator.CONTAINS, value)

    def in_values(self, field: str, values: List[Any]) -> "QueryBuilder":
        return self.filter(field, FilterOperator.IN, values)

    def is_null(self, field: str) -> "QueryBuilder":
        return self.filter(field, FilterOperator.IS_NULL)

    def is_not_null(self, field: str) -> "QueryBuilder":
        return self.filter(field, FilterOperator.IS_NOT_NULL)

    def sort(self, field: str, order: SortOrder = SortOrder.ASC) -> "QueryBuilder":
        """Add sort."""
        self._sorts.append(Sort(field, order))
        return self

    def order_by(self, field: str, descending: bool = False) -> "QueryBuilder":
        """Add sort with convenience."""
        order = SortOrder.DESC if descending else SortOrder.ASC
        return self.sort(field, order)

    def paginate(self, page: int, page_size: int = 20) -> "QueryBuilder":
        """Set pagination."""
        self._pagination = Pagination(page=max(1, page), page_size=page_size)
        return self

    def limit(self, limit: int) -> "QueryBuilder":
        """Set result limit."""
        self._limit = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        """Set result offset."""
        self._offset = offset
        return self

    def select(self, *fields: str) -> "QueryBuilder":
        """Set fields to select."""
        self._selects.extend(fields)
        return self

    def group_by(self, *fields: str) -> "QueryBuilder":
        """Set grouping fields."""
        self._groups.extend(fields)
        return self

    def build(self) -> Dict[str, Any]:
        """Build query dict."""
        return {
            "filters": [
                {"field": f.field, "op": f.operator.value, "value": f.value}
                for f in self._filters
            ],
            "sorts": [
                {"field": s.field, "order": s.order.value}
                for s in self._sorts
            ],
            "pagination": (
                {"page": self._pagination.page, "page_size": self._pagination.page_size}
                if self._pagination
                else None
            ),
            "limit": self._limit,
            "offset": self._offset,
            "selects": self._selects,
            "groups": self._groups,
        }


class QueryAction:
    """
    Action for building and executing queries.

    Example:
        action = QueryAction("data_query")
        results = action.execute(
            data_source,
            QueryBuilder()
                .filter("status", FilterOperator.EQ, "active")
                .sort("created_at", SortOrder.DESC)
                .paginate(1, 20)
        )
    """

    def __init__(self, name: str):
        self.name = name
        self._lock = threading.RLock()
        self._stats = {"queries_executed": 0, "total_duration": 0.0}

    def _apply_filter(self, item: Dict, filter: Filter) -> bool:
        """Apply a single filter to an item."""
        value = self._get_nested(item, filter.field)

        ops = {
            FilterOperator.EQ: lambda v, val: v == val,
            FilterOperator.NE: lambda v, val: v != val,
            FilterOperator.GT: lambda v, val: v is not None and v > val,
            FilterOperator.GTE: lambda v, val: v is not None and v >= val,
            FilterOperator.LT: lambda v, val: v is not None and v < val,
            FilterOperator.LTE: lambda v, val: v is not None and v <= val,
            FilterOperator.IN: lambda v, val: v in val,
            FilterOperator.NOT_IN: lambda v, val: v not in val,
            FilterOperator.CONTAINS: (
                lambda v, val: v is not None and val in str(v)
            ),
            FilterOperator.STARTS_WITH: (
                lambda v, val: v is not None and str(v).startswith(val)
            ),
            FilterOperator.ENDS_WITH: (
                lambda v, val: v is not None and str(v).endswith(val)
            ),
            FilterOperator.IS_NULL: lambda v, val: v is None,
            FilterOperator.IS_NOT_NULL: lambda v, val: v is not None,
        }

        op_func = ops.get(filter.operator)
        if not op_func:
            return True

        return op_func(value, filter.value)

    def _get_nested(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split(".")
        current = data

        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None

            if current is None:
                return None

        return current

    def _apply_sort(self, items: List[Dict], sorts: List[Sort]) -> List[Dict]:
        """Apply sorting to items."""
        if not sorts:
            return items

        def sort_key(item: Dict):
            values = []
            for sort in sorts:
                value = self._get_nested(item, sort.field)
                if sort.order == SortOrder.DESC:
                    value = -value if isinstance(value, (int, float)) else value
                values.append(value)
            return tuple(values)

        return sorted(items, key=sort_key)

    def _apply_pagination(
        self,
        items: List[Any],
        pagination: Optional[Pagination],
    ) -> Tuple[List[Any], int]:
        """Apply pagination and return subset with total count."""
        total = len(items)

        if pagination:
            start = (pagination.page - 1) * pagination.page_size
            end = start + pagination.page_size
            items = items[start:end]

        return items, total

    def execute(
        self,
        data_source: Union[List[Dict], Callable],
        query: QueryBuilder,
    ) -> QueryResult:
        """Execute a query against a data source."""
        start_time = time.time()

        with self._lock:
            self._stats["queries_executed"] += 1

        if callable(data_source):
            items = data_source()
        else:
            items = list(data_source)

        query_dict = query.build()

        for f in query_dict.get("filters", []):
            filter_obj = Filter(
                field=f["field"],
                operator=FilterOperator(f["op"]),
                value=f.get("value"),
            )
            items = [item for item in items if self._apply_filter(item, filter_obj)]

        if query_dict.get("selects"):
            items = [
                {f: self._get_nested(item, f) for f in query_dict["selects"]}
                for item in items
            ]

        if query_dict.get("sorts"):
            sorts = [
                Sort(field=s["field"], order=SortOrder(s["order"]))
                for s in query_dict["sorts"]
            ]
            items = self._apply_sort(items, sorts)

        offset = query_dict.get("offset", 0)
        if offset:
            items = items[offset:]

        limit = query_dict.get("limit")
        if limit:
            items = items[:limit]

        pagination = None
        if query_dict.get("pagination"):
            pagination = Pagination(**query_dict["pagination"])

        subset, total = self._apply_pagination(items, pagination)

        page = pagination.page if pagination else 1
        page_size = pagination.page_size if pagination else total
        total_pages = (total + page_size - 1) // page_size if page_size else 1

        duration = time.time() - start_time
        with self._lock:
            self._stats["total_duration"] += duration

        return QueryResult(
            items=subset,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get query statistics."""
        with self._lock:
            stats = dict(self._stats)
            if stats["queries_executed"] > 0:
                stats["avg_duration"] = (
                    stats["total_duration"] / stats["queries_executed"]
                )
            return stats

    def reset_stats(self) -> None:
        """Reset statistics."""
        with self._lock:
            self._stats = {"queries_executed": 0, "total_duration": 0.0}
