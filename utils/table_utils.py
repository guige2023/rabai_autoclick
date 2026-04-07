"""
Table and grid operations utilities.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


class Table:
    """2D table data structure with row/column operations."""

    def __init__(
        self,
        data: Optional[List[List[Any]]] = None,
        headers: Optional[List[str]] = None
    ):
        self._data: List[List[Any]] = data or []
        self._headers = headers or []

    @property
    def rows(self) -> int:
        return len(self._data)

    @property
    def cols(self) -> int:
        return len(self._data[0]) if self._data else 0

    @property
    def headers(self) -> List[str]:
        return self._headers

    def get(self, row: int, col: int, default: Any = None) -> Any:
        """Get cell value at (row, col)."""
        try:
            return self._data[row][col]
        except IndexError:
            return default

    def set(self, row: int, col: int, value: Any) -> None:
        """Set cell value at (row, col)."""
        while row >= len(self._data):
            self._data.append([])
        while col >= len(self._data[row]):
            self._data[row].append(None)
        self._data[row][col] = value

    def row(self, index: int) -> List[Any]:
        """Get a row by index."""
        return list(self._data[index]) if index < len(self._data) else []

    def col(self, index: int) -> List[Any]:
        """Get a column by index."""
        return [r[index] for r in self._data if index < len(r)]

    def add_row(self, values: List[Any], index: Optional[int] = None) -> None:
        """Insert a new row."""
        if index is None:
            self._data.append(list(values))
        else:
            self._data.insert(index, list(values))

    def add_col(self, values: List[Any], header: Optional[str] = None, index: Optional[int] = None) -> None:
        """Insert a new column."""
        for i, val in enumerate(values):
            while i >= len(self._data):
                self._data.append([])
            if index is None:
                self._data[i].append(val)
            else:
                self._data[i].insert(index, val)
        if self._headers:
            if index is None:
                self._headers.append(header or "")
            else:
                self._headers.insert(index, header or "")
