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


    def delete_row(self, index: int) -> None:
        """Delete a row."""
        if 0 <= index < len(self._data):
            del self._data[index]

    def delete_col(self, index: int) -> None:
        """Delete a column."""
        for row in self._data:
            if 0 <= index < len(row):
                del row[index]
        if self._headers and 0 <= index < len(self._headers):
            del self._headers[index]

    def transpose(self) -> "Table":
        """Return a transposed copy of the table."""
        if not self._data:
            return Table(headers=self._headers)
        new_data = [self.col(i) for i in range(self.cols)]
        return Table(data=new_data, headers=self._headers)

    def map(self, func: Callable[[Any], Any]) -> "Table":
        """Apply function to all cells."""
        new_data = [[func(cell) for cell in row] for row in self._data]
        return Table(data=new_data, headers=self._headers)

    def filter_rows(self, predicate: Callable[[List[Any]], bool]) -> "Table":
        """Filter rows by predicate."""
        new_data = [row for row in self._data if predicate(row)]
        return Table(data=new_data, headers=self._headers)

    def sort_by(
        self,
        col_index: int,
        reverse: bool = False,
        key_func: Optional[Callable[[Any], Any]] = None
    ) -> "Table":
        """Sort table by a column."""
        data = sorted(
            self._data,
            key=lambda r: key_func(r[col_index]) if key_func else r[col_index],
            reverse=reverse
        )
        return Table(data=data, headers=self._headers)

    def group_by(self, col_index: int) -> Dict[Any, "Table"]:
        """Group rows by column value."""
        groups: Dict[Any, List[List[Any]]] = {}
        for row in self._data:
            key = row[col_index]
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        return {k: Table(data=v, headers=self._headers) for k, v in groups.items()}

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert to list of dicts using headers as keys."""
        if not self._headers:
            return [{"_row": i, "_data": row} for i, row in enumerate(self._data)]
        return [dict(zip(self._headers, row)) for row in self._data]

    def __iter__(self) -> Iterator[List[Any]]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


def create_grid(
    rows: int,
    cols: int,
    fill: Any = None
) -> List[List[Any]]:
    """Create a 2D grid initialized with a value."""
    return [[fill for _ in range(cols)] for _ in range(rows)]


def rotate_grid_90(grid: List[List[Any]], clockwise: bool = True) -> List[List[Any]]:
    """Rotate a 2D grid 90 degrees."""
    if not grid:
        return []
    rows, cols = len(grid), len(grid[0])
    if clockwise:
        return [[grid[rows - 1 - r][c] for r in range(rows)] for c in range(cols)]
    else:
        return [[grid[r][cols - 1 - c] for r in range(rows)] for c in range(cols)]


def find_in_grid(
    grid: List[List[Any]],
    value: Any
) -> Optional[Tuple[int, int]]:
    """Find first occurrence of value in grid."""
    for r, row in enumerate(grid):
        for c, val in enumerate(row):
            if val == value:
                return (r, c)
    return None


def print_grid(grid: List[List[Any]], sep: str = " ") -> None:
    """Print a 2D grid."""
    for row in grid:
        print(sep.join(str(cell) for cell in row))



def grid_bfs(
    grid: List[List[Any]],
    start: Tuple[int, int],
    passable: Callable[[Any], bool] = None
) -> Iterator[Tuple[int, int, Any]]:
    """BFS traversal of a grid with obstacle detection."""
    if passable is None:
        passable = lambda x: x != "#"
    rows, cols = len(grid), len(grid[0])
    visited = set()
    queue = [start]
    visited.add(start)
    while queue:
        r, c = queue.pop(0)
        yield (r, c, grid[r][c])
        for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols and (nr, nc) not in visited:
                if passable(grid[nr][nc]):
                    visited.add((nr, nc))
                    queue.append((nr, nc))

