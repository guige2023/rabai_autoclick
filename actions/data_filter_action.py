"""
Data Filter Action Module.

Advanced data filtering with predicates, combinators,
transformation, and pagination support.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")


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
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterCondition:
    """
    Single filter condition.

    Attributes:
        field: Field name to filter on.
        operator: Filter operator.
        value: Comparison value.
        case_sensitive: Whether string comparison is case-sensitive.
    """
    field: str
    operator: FilterOperator
    value: Any = None
    case_sensitive: bool = False


@dataclass
class SortSpec:
    """Sort specification."""
    field: str
    ascending: bool = True


@dataclass
class PaginationSpec:
    """Pagination specification."""
    page: int = 1
    page_size: int = 20


@dataclass
class FilterResult(Generic[T]):
    """Result of filter operation."""
    items: list[T]
    total: int
    page: int
    page_size: int
    total_pages: int


class DataFilterAction(Generic[T]):
    """
    Advanced data filtering with multiple operators and combinators.

    Example:
        filter = DataFilterAction[dict]()
        filter.where("age", FilterOperator.GTE, 18)
        filter.where("status", FilterOperator.IN, ["active", "pending"])
        filter.sort_by("created_at", ascending=False)
        result = filter.apply(data_records)
    """

    def __init__(self):
        """Initialize data filter action."""
        self.conditions: list[FilterCondition] = []
        self._and_conditions: list[list[FilterCondition]] = []
        self._or_conditions: list[list[FilterCondition]] = []
        self.sort_specs: list[SortSpec] = []
        self.pagination: Optional[PaginationSpec] = None

    def where(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None
    ) -> "DataFilterAction":
        """
        Add AND filter condition.

        Args:
            field: Field name.
            operator: Filter operator.
            value: Comparison value.

        Returns:
            Self for method chaining.
        """
        condition = FilterCondition(field=field, operator=operator, value=value)
        self.conditions.append(condition)
        return self

    def and_where(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None
    ) -> "DataFilterAction":
        """Add condition to current AND group."""
        condition = FilterCondition(field=field, operator=operator, value=value)
        if self.conditions:
            self._and_conditions.append([condition])
        else:
            self.conditions.append(condition)
        return self

    def or_where(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None
    ) -> "DataFilterAction":
        """
        Start OR group with condition.

        Args:
            field: Field name.
            operator: Filter operator.
            value: Comparison value.

        Returns:
            Self for method chaining.
        """
        condition = FilterCondition(field=field, operator=operator, value=value)
        self._or_conditions.append([condition])
        return self

    def sort_by(self, field: str, ascending: bool = True) -> "DataFilterAction":
        """
        Add sort specification.

        Args:
            field: Field to sort by.
            ascending: Sort direction.

        Returns:
            Self for method chaining.
        """
        self.sort_specs.append(SortSpec(field=field, ascending=ascending))
        return self

    def paginate(self, page: int = 1, page_size: int = 20) -> "DataFilterAction":
        """
        Set pagination.

        Args:
            page: Page number (1-indexed).
            page_size: Items per page.

        Returns:
            Self for method chaining.
        """
        self.pagination = PaginationSpec(page=page, page_size=page_size)
        return self

    def apply(self, data: list[T]) -> FilterResult[T]:
        """
        Apply filters to data.

        Args:
            data: List of records to filter.

        Returns:
            FilterResult with filtered and sorted data.
        """
        filtered = self._filter(data)

        if self.sort_specs:
            filtered = self._sort(filtered)

        total = len(filtered)

        if self.pagination:
            page = self.pagination.page
            page_size = self.pagination.page_size
            start = (page - 1) * page_size
            end = start + page_size
            filtered = filtered[start:end]
            total_pages = (len(filtered) + page_size - 1) // page_size
        else:
            page = 1
            page_size = len(filtered)
            total_pages = 1

        return FilterResult(
            items=filtered,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    def _filter(self, data: list[T]) -> list[T]:
        """Apply all filter conditions."""
        results = []

        for record in data:
            if self._matches(record):
                results.append(record)

        return results

    def _matches(self, record: T) -> bool:
        """Check if record matches all conditions."""
        if isinstance(record, dict):
            record_get = lambda f: record.get(f)
        else:
            record_get = lambda f: getattr(record, f, None)

        if self.conditions:
            if not all(self._check_condition(record_get, cond) for cond in self.conditions):
                return False

        for and_group in self._and_conditions:
            if not all(self._check_condition(record_get, cond) for cond in and_group):
                return False

        if self._or_conditions:
            if not any(all(self._check_condition(record_get, cond) for cond in group) for group in self._or_conditions):
                return False

        return True

    def _check_condition(self, get_field: Callable, condition: FilterCondition) -> bool:
        """Check single condition against record."""
        value = get_field(condition.field)

        op = condition.operator

        if op == FilterOperator.EQ:
            return value == condition.value

        elif op == FilterOperator.NE:
            return value != condition.value

        elif op == FilterOperator.GT:
            return value is not None and value > condition.value

        elif op == FilterOperator.GTE:
            return value is not None and value >= condition.value

        elif op == FilterOperator.LT:
            return value is not None and value < condition.value

        elif op == FilterOperator.LTE:
            return value is not None and value <= condition.value

        elif op == FilterOperator.IN:
            return value in condition.value

        elif op == FilterOperator.NOT_IN:
            return value not in condition.value

        elif op == FilterOperator.CONTAINS:
            if value is None:
                return False
            val_str = str(value)
            cmp_str = str(condition.value)
            return cmp_str in val_str if condition.case_sensitive else cmp_str.lower() in val_str.lower()

        elif op == FilterOperator.STARTS_WITH:
            if value is None:
                return False
            val_str = str(value)
            cmp_str = str(condition.value)
            return val_str.startswith(cmp_str) if condition.case_sensitive else val_str.lower().startswith(cmp_str.lower())

        elif op == FilterOperator.ENDS_WITH:
            if value is None:
                return False
            val_str = str(value)
            cmp_str = str(condition.value)
            return val_str.endswith(cmp_str) if condition.case_sensitive else val_str.lower().endswith(cmp_str.lower())

        elif op == FilterOperator.REGEX:
            import re
            if value is None:
                return False
            flags = 0 if condition.case_sensitive else re.IGNORECASE
            return bool(re.search(condition.value, str(value), flags))

        elif op == FilterOperator.IS_NULL:
            return value is None

        elif op == FilterOperator.IS_NOT_NULL:
            return value is not None

        return True

    def _sort(self, data: list[T]) -> list[T]:
        """Sort data by sort specifications."""
        if isinstance(data[0], dict) if data else False:
            get_val = lambda r, f: r.get(f)
        else:
            get_val = lambda r, f: getattr(r, f, None)

        def sort_key(record: T) -> tuple:
            values = []
            for spec in self.sort_specs:
                val = get_val(record, spec.field)
                values.append(val if spec.ascending else (-val if isinstance(val, (int, float)) else val))
            return tuple(values)

        return sorted(data, key=sort_key)

    def clear(self) -> None:
        """Clear all filters and sorts."""
        self.conditions.clear()
        self._and_conditions.clear()
        self._or_conditions.clear()
        self.sort_specs.clear()
        self.pagination = None
