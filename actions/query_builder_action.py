"""Query builder action module for RabAI AutoClick.

Provides query building:
- QueryBuilder: Build query objects
- FilterBuilder: Build filters
- SortBuilder: Build sorts
- PaginationBuilder: Build pagination
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Query:
    """Query object."""
    filters: List[Dict] = field(default_factory=list)
    sorts: List[Dict] = field(default_factory=list)
    pagination: Dict = field(default_factory=dict)
    select_fields: List[str] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    having: List[Dict] = field(default_factory=list)


class FilterBuilder:
    """Build query filters."""

    def __init__(self):
        self._filters: List[Dict] = []

    def eq(self, field: str, value: Any) -> "FilterBuilder":
        """Equal filter."""
        self._filters.append({"field": field, "op": "eq", "value": value})
        return self

    def ne(self, field: str, value: Any) -> "FilterBuilder":
        """Not equal filter."""
        self._filters.append({"field": field, "op": "ne", "value": value})
        return self

    def gt(self, field: str, value: Any) -> "FilterBuilder":
        """Greater than filter."""
        self._filters.append({"field": field, "op": "gt", "value": value})
        return self

    def gte(self, field: str, value: Any) -> "FilterBuilder":
        """Greater than or equal filter."""
        self._filters.append({"field": field, "op": "gte", "value": value})
        return self

    def lt(self, field: str, value: Any) -> "FilterBuilder":
        """Less than filter."""
        self._filters.append({"field": field, "op": "lt", "value": value})
        return self

    def lte(self, field: str, value: Any) -> "FilterBuilder":
        """Less than or equal filter."""
        self._filters.append({"field": field, "op": "lte", "value": value})
        return self

    def contains(self, field: str, value: str) -> "FilterBuilder":
        """Contains filter."""
        self._filters.append({"field": field, "op": "contains", "value": value})
        return self

    def in_list(self, field: str, values: List) -> "FilterBuilder":
        """IN filter."""
        self._filters.append({"field": field, "op": "in", "value": values})
        return self

    def is_null(self, field: str) -> "FilterBuilder":
        """Is null filter."""
        self._filters.append({"field": field, "op": "is_null", "value": None})
        return self

    def and_filter(self, filter_dict: Dict) -> "FilterBuilder":
        """Add filter dict."""
        self._filters.append(filter_dict)
        return self

    def build(self) -> List[Dict]:
        """Build filters."""
        return list(self._filters)


class SortBuilder:
    """Build query sorts."""

    def __init__(self):
        self._sorts: List[Dict] = []

    def asc(self, field: str) -> "SortBuilder":
        """Ascending sort."""
        self._sorts.append({"field": field, "order": "asc"})
        return self

    def desc(self, field: str) -> "SortBuilder":
        """Descending sort."""
        self._sorts.append({"field": field, "order": "desc"})
        return self

    def build(self) -> List[Dict]:
        """Build sorts."""
        return list(self._sorts)


class QueryBuilder:
    """Build complete queries."""

    def __init__(self):
        self.query = Query()

    def select(self, *fields: str) -> "QueryBuilder":
        """Select fields."""
        self.query.select_fields.extend(fields)
        return self

    def where(self, field: str, op: str, value: Any) -> "QueryBuilder":
        """Add filter."""
        self.query.filters.append({"field": field, "op": op, "value": value})
        return self

    def order_by(self, field: str, order: str = "asc") -> "QueryBuilder":
        """Add sort."""
        self.query.sorts.append({"field": field, "order": order})
        return self

    def group_by(self, *fields: str) -> "QueryBuilder":
        """Add group by."""
        self.query.group_by.extend(fields)
        return self

    def limit(self, limit: int) -> "QueryBuilder":
        """Set limit."""
        self.query.pagination["limit"] = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        """Set offset."""
        self.query.pagination["offset"] = offset
        return self

    def page(self, page: int, page_size: int) -> "QueryBuilder":
        """Set page and page size."""
        self.query.pagination["page"] = page
        self.query.pagination["page_size"] = page_size
        self.query.pagination["offset"] = (page - 1) * page_size
        self.query.pagination["limit"] = page_size
        return self

    def build(self) -> Query:
        """Build query."""
        return self.query

    def to_dict(self) -> Dict:
        """Convert to dict."""
        return {
            "filters": self.query.filters,
            "sorts": self.query.sorts,
            "pagination": self.query.pagination,
            "select_fields": self.query.select_fields,
            "group_by": self.query.group_by,
            "having": self.query.having,
        }


class QueryBuilderAction(BaseAction):
    """Query builder action."""
    action_type = "query_builder"
    display_name = "查询构建器"
    description = "构建查询对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "build")

            if operation == "build":
                return self._build_query(params)
            elif operation == "filter":
                return self._build_filter(params)
            elif operation == "sort":
                return self._build_sort(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Query builder error: {str(e)}")

    def _build_query(self, params: Dict) -> ActionResult:
        """Build query."""
        builder = QueryBuilder()

        if "select" in params:
            fields = params["select"]
            if isinstance(fields, list):
                for f in fields:
                    builder.select(f)
            else:
                builder.select(fields)

        filters = params.get("filters", [])
        for f in filters:
            builder.where(f.get("field", ""), f.get("op", "eq"), f.get("value"))

        sorts = params.get("sorts", [])
        for s in sorts:
            builder.order_by(s.get("field", ""), s.get("order", "asc"))

        if "limit" in params:
            builder.limit(params["limit"])
        if "offset" in params:
            builder.offset(params["offset"])
        if "page" in params:
            builder.page(params["page"], params.get("page_size", 10))

        query = builder.build()

        return ActionResult(
            success=True,
            message="Query built",
            data=builder.to_dict(),
        )

    def _build_filter(self, params: Dict) -> ActionResult:
        """Build filters."""
        builder = FilterBuilder()

        ops = params.get("operations", [])
        for op in ops:
            field = op.get("field")
            op_type = op.get("op", "eq")
            value = op.get("value")

            if field and op_type:
                if hasattr(builder, op_type):
                    getattr(builder, op_type)(field, value)
                else:
                    builder.and_filter({"field": field, "op": op_type, "value": value})

        return ActionResult(
            success=True,
            message=f"Built {len(builder._filters)} filters",
            data={"filters": builder.build()},
        )

    def _build_sort(self, params: Dict) -> ActionResult:
        """Build sorts."""
        builder = SortBuilder()

        sorts = params.get("sorts", [])
        for s in sorts:
            field = s.get("field", "")
            order = s.get("order", "asc")
            if order.lower() == "desc":
                builder.desc(field)
            else:
                builder.asc(field)

        return ActionResult(
            success=True,
            message=f"Built {len(sorts)} sorts",
            data={"sorts": builder.build()},
        )
