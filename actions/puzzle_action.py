"""
Classic puzzle solvers for automation actions.

Provides solvers for: N-Queens, Sudoku, Word Search, Crossword,
Magic Square, Tower of Hanoi, and sliding puzzles.
"""

from __future__ import annotations

from typing import Iterator


class NQueens:
    """N-Queens problem solver."""

    def __init__(self, n: int) -> None:
        self.n = n
        self.boards: list[list[str]] = []

    def solve(self) -> list[list[str]]:
        """Solve N-Queens and return all valid board configurations."""
        self.boards = []
        self._backtrack([], [], [])
        return self.boards

    def _backtrack(self, queens: list[int], cols: set[int], diag1: set[int]) -> None:
        """Backtracking helper."""
        row = len(queens)
        if row == self.n:
            self._add_board(queens)
            return
        for col in range(self.n):
            if col in cols or (row - col) in diag1 or (row + col) in {
                i + j for i, j in zip(range(row), queens)
            }:
                continue
            queens.append(col)
            cols.add(col)
            diag1.add(row - col)
            self._backtrack(queens, cols, diag1 | {row + col})
            queens.pop()
            cols.discard(col)

    def _add_board(self, queens: list[int]) -> None:
        board = [["."] * self.n for _ in range(self.n)]
        for r, c in enumerate(queens):
            board[r][c] = "Q"
        self.boards.append(board)


class Sudoku:
    """Sudoku solver using backtracking."""

    def __init__(self, board: list[list[int | None]]) -> None:
        self.board = [row[:] for row in board]
        self.n = 9
        self.box_size = 3

    def solve(self) -> list[list[int]] | None:
        """Solve Sudoku. Returns None if unsolvable."""
        if self._backtrack():
            return self.board
        return None

    def _backtrack(self) -> bool:
        """Backtracking solver."""
        for i in range(self.n):
            for j in range(self.n):
                if self.board[i][j] is None:
                    for num in range(1, 10):
                        if self._is_valid(i, j, num):
                            self.board[i][j] = num
                            if self._backtrack():
                                return True
                            self.board[i][j] = None
                    return False
        return True

    def _is_valid(self, row: int, col: int, num: int) -> bool:
        """Check if placing num at (row, col) is valid."""
        if num in self.board[row]:
            return False
        if num in [self.board[r][col] for r in range(self.n)]:
            return False
        box_row, box_col = 3 * (row // 3), 3 * (col // 3)
        for r in range(box_row, box_row + 3):
            for c in range(box_col, box_col + 3):
                if self.board[r][c] == num:
                    return False
        return True

    def get_possible(self, row: int, col: int) -> set[int]:
        """Get all possible values for a cell."""
        if self.board[row][col] is not None:
            return set()
        all_vals = set(range(1, 10))
        row_vals = set(self.board[row])
        col_vals = {self.board[r][col] for r in range(self.n)}
        box_row, box_col = 3 * (row // 3), 3 * (col // 3)
        box_vals = {
            self.board[r][c]
            for r in range(box_row, box_row + 3)
            for c in range(box_col, box_col + 3)
        }
        return all_vals - row_vals - col_vals - box_vals


class WordSearch:
    """Word search puzzle solver."""

    def __init__(self, grid: list[list[str]]) -> None:
        self.grid = grid
        self.rows = len(grid)
        self.cols = len(grid[0]) if grid else 0
        self.directions = [
            (-1, -1), (-1, 0), (-1, 1),
            (0, -1), (0, 1),
            (1, -1), (1, 0), (1, 1),
        ]

    def find_word(self, word: str) -> list[tuple[int, int]] | None:
        """Find word in grid. Returns list of (row, col) positions or None."""
        if not word:
            return []
        for r in range(self.rows):
            for c in range(self.cols):
                if self.grid[r][c] == word[0]:
                    path = self._search_from(r, c, word)
                    if path:
                        return path
        return None

    def _search_from(self, start_r: int, start_c: int, word: str) -> list[tuple[int, int]] | None:
        """DFS search starting from position."""
        for dr, dc in self.directions:
            r, c = start_r, start_c
            path = [(r, c)]
            for char in word[1:]:
                r += dr
                c += dc
                if not (0 <= r < self.rows and 0 <= c < self.cols):
                    break
                if self.grid[r][c] != char:
                    break
                path.append((r, c))
            else:
                if len(path) == len(word):
                    return path
        return None

    def find_all(self, words: list[str]) -> dict[str, list[tuple[int, int]] | None]:
        """Find all words and return results dict."""
        return {word: self.find_word(word) for word in words}


class MagicSquare:
    """Magic square solver for odd-sized squares."""

    def __init__(self, n: int) -> None:
        if n % 2 == 0:
            raise ValueError("Only odd-sized magic squares supported")
        self.n = n
        self.size = n * n

    def generate(self) -> list[list[int]]:
        """Generate a magic square of size n."""
        magic: list[list[int]] = [[0] * self.n for _ in range(self.n)]
        r, c = 0, self.n // 2
        for num in range(1, self.size + 1):
            magic[r][c] = num
            r -= 1
            c += 1
            if num % self.n == 0:
                r += 2
                c -= 1
            else:
                if r < 0:
                    r = self.n - 1
                if c >= self.n:
                    c = 0
        return magic

    def verify(self, square: list[list[int]]) -> bool:
        """Verify if a square is a magic square."""
        n = len(square)
        target = n * (n * n + 1) // 2
        for row in square:
            if sum(row) != target:
                return False
        for c in range(n):
            if sum(square[r][c] for r in range(n)) != target:
                return False
        if sum(square[i][i] for i in range(n)) != target:
            return False
        if sum(square[i][n - 1 - i] for i in range(n)) != target:
            return False
        return True


class TowerOfHanoi:
    """Tower of Hanoi solver."""

    def __init__(self, n: int) -> None:
        self.n = n
        self.moves: list[tuple[str, str]] = []

    def solve(self, source: str = "A", target: str = "C", auxiliary: str = "B") -> list[tuple[str, str]]:
        """Solve Tower of Hanoi. Returns list of (from, to) moves."""
        self.moves = []
        self._move(self.n, source, target, auxiliary)
        return self.moves

    def _move(self, disks: int, source: str, target: str, auxiliary: str) -> None:
        """Recursive move helper."""
        if disks == 1:
            self.moves.append((source, target))
            return
        self._move(disks - 1, source, auxiliary, target)
        self.moves.append((source, target))
        self._move(disks - 1, auxiliary, target, source)


class SlidingPuzzle:
    """3x3 Sliding puzzle solver using BFS."""

    def __init__(self, board: list[list[int]]) -> None:
        self.initial = tuple(tuple(row) for row in board)
        self.goal = ((1, 2, 3), (4, 5, 6), (7, 8, 0))
        self.size = 3

    def solve(self) -> list[list[int]] | None:
        """Solve using BFS. Returns solution path or None."""
        start = self._to_tuple(self.initial)
        if start == self.goal:
            return self.initial
        queue: list[tuple] = [(start, 0)]
        visited = {start}
        parent = {start: None}
        while queue:
            state, depth = queue.pop(0)
            if state == self.goal:
                return self._reconstruct(parent, state)
            zero_pos = self._find_zero(state)
            for neighbor in self._neighbors(zero_pos):
                swap_state = self._swap(state, zero_pos, neighbor)
                if swap_state not in visited:
                    visited.add(swap_state)
                    parent[swap_state] = state
                    queue.append((swap_state, depth + 1))
        return None

    def _to_tuple(self, board: list[list[int]]) -> tuple:
        return tuple(tuple(row) for row in board)

    def _find_zero(self, state: tuple) -> tuple[int, int]:
        for r in range(self.size):
            for c in range(self.size):
                if state[r][c] == 0:
                    return (r, c)
        return (0, 0)

    def _neighbors(self, pos: tuple[int, int]) -> Iterator[tuple[int, int]]:
        for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
            nr, nc = pos[0] + dr, pos[1] + dc
            if 0 <= nr < self.size and 0 <= nc < self.size:
                yield (nr, nc)

    def _swap(self, state: tuple, pos1: tuple, pos2: tuple) -> tuple:
        state_list = [list(row) for row in state]
        r1, c1 = pos1
        r2, c2 = pos2
        state_list[r1][c1], state_list[r2][c2] = state_list[r2][c2], state_list[r1][c1]
        return tuple(tuple(row) for row in state_list)

    def _reconstruct(self, parent: dict, state: tuple) -> list[list[int]]:
        path = []
        while state is not None:
            path.append([list(row) for row in state])
            state = parent[state]
        path.reverse()
        return path


class Crossword:
    """Simple crossword grid builder."""

    def __init__(self, width: int, height: int) -> None:
        self.width = width
        self.height = height
        self.grid = [["#"] * width for _ in range(height)]
        self.words: list[tuple[str, int, int, bool]] = []

    def place_word(self, word: str, row: int, col: int, horizontal: bool) -> bool:
        """Place a word at position. Returns True if valid."""
        if horizontal:
            if col + len(word) > self.width:
                return False
            for i, char in enumerate(word):
                cell = self.grid[row][col + i]
                if cell != "#" and cell != char:
                    return False
            for i, char in enumerate(word):
                self.grid[row][col + i] = char
            self.words.append((word, row, col, True))
        else:
            if row + len(word) > self.height:
                return False
            for i, char in enumerate(word):
                cell = self.grid[row + i][col]
                if cell != "#" and cell != char:
                    return False
            for i, char in enumerate(word):
                self.grid[row + i][col] = char
            self.words.append((word, row, col, False))
        return True

    def get_grid(self) -> list[list[str]]:
        """Get current grid state."""
        return [row[:] for row in self.grid]

    def fill_blanks(self, char: str = ".") -> None:
        """Fill empty cells with character."""
        for r in range(self.height):
            for c in range(self.width):
                if self.grid[r][c] == "#":
                    self.grid[r][c] = char
