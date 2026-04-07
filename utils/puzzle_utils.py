"""Puzzle utilities: grid puzzles, sudoku, word search, and sliding puzzles."""

from __future__ import annotations

from typing import List, Dict, Optional, Set, Tuple, Any
from collections import deque
import copy
import random


# ---------------------------------------------------------------------------
# Sudoku
# ---------------------------------------------------------------------------


class Sudoku:
    """A 9x9 Sudoku solver/generator."""

    BOX_SIZE = 3
    BOARD_SIZE = 9

    def __init__(self, board: Optional[List[List[int]]] = None) -> None:
        """Initialize with optional 9x9 board (0 = empty)."""
        if board is None:
            self.board = [[0] * self.BOARD_SIZE for _ in range(self.BOARD_SIZE)]
        else:
            self.board = [row[:] for row in board]

    @classmethod
    def from_string(cls, s: str) -> Sudoku:
        """Parse from 81-char string (digits and '.' or '0' for empty)."""
        chars = [c for c in s if c.isdigit() or c in ".0 "]
        if len(chars) < 81:
            chars += ["0"] * (81 - len(chars))
        board = [[int(chars[r * 9 + c]) if chars[r * 9 + c].isdigit() else 0
                   for c in range(9)] for r in range(9)]
        return cls(board)

    def is_valid(self) -> bool:
        """Check if current board state is valid (no rule violations)."""
        for i in range(9):
            if not self._is_unit_valid([self.board[i][j] for j in range(9)]):
                return False
            if not self._is_unit_valid([self.board[j][i] for j in range(9)]):
                return False
        for bi in range(3):
            for bj in range(3):
                box = [self.board[bi * 3 + r][bj * 3 + c]
                       for r in range(3) for c in range(3)]
                if not self._is_unit_valid(box):
                    return False
        return True

    @staticmethod
    def _is_unit_valid(unit: List[int]) -> bool:
        seen: Set[int] = set()
        for v in unit:
            if v != 0:
                if v in seen:
                    return False
                seen.add(v)
        return True

    def solve(self) -> bool:
        """Solve in-place using backtracking. Returns True if solved."""
        empty = self._find_empty()
        if empty is None:
            return True
        r, c = empty
        for val in range(1, 10):
            if self._is_safe(r, c, val):
                self.board[r][c] = val
                if self.solve():
                    return True
                self.board[r][c] = 0
        return False

    def _find_empty(self) -> Optional[Tuple[int, int]]:
        for r in range(9):
            for c in range(9):
                if self.board[r][c] == 0:
                    return (r, c)
        return None

    def _is_safe(self, row: int, col: int, val: int) -> bool:
        if val in self.board[row]:
            return False
        if val in [self.board[r][col] for r in range(9)]:
            return False
        br, bc = (row // 3) * 3, (col // 3) * 3
        for r in range(br, br + 3):
            for c in range(bc, bc + 3):
                if self.board[r][c] == val:
                    return False
        return True

    def count_solutions(self, limit: int = 2) -> int:
        """Count solutions (up to `limit`) using backtracking."""
        empty = self._find_empty()
        if empty is None:
            return 1
        r, c = empty
        count = 0
        for val in range(1, 10):
            if self._is_safe(r, c, val):
                self.board[r][c] = val
                count += self.count_solutions(limit - count)
                self.board[r][c] = 0
                if count >= limit:
                    break
        return count

    def __repr__(self) -> str:
        lines = []
        for i, row in enumerate(self.board):
            if i > 0 and i % 3 == 0:
                lines.append("-" * 21)
            line = " ".join(str(v) if v != 0 else "." for v in row)
            lines.append(line[:6] + " " + line[6:9] + " | " + line[9:12] + " " + line[12:])
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Word Search
# ---------------------------------------------------------------------------


class WordSearch:
    """Grid word search puzzle."""

    DIRECTIONS = [(0, 1), (1, 0), (1, 1), (1, -1), (0, -1), (-1, 0), (-1, -1), (-1, 1)]

    def __init__(self, rows: int, cols: int) -> None:
        self.rows = rows
        self.cols = cols
        self.grid: List[List[str]] = [[" "] * cols for _ in range(rows)]

    def place_word(self, word: str, row: int, col: int, direction: Tuple[int, int]) -> bool:
        """Try to place a word at position going in direction. Returns success."""
        dr, dc = direction
        chars = list(word.upper())
        positions = []
        for i, ch in enumerate(chars):
            r, c = row + dr * i, col + dc * i
            if not (0 <= r < self.rows and 0 <= c < self.cols):
                return False
            if self.grid[r][c] not in (" ", ch):
                return False
            positions.append((r, c))
        for (r, c), ch in zip(positions, chars):
            self.grid[r][c] = ch
        return True

    def fill_random(self, alphabet: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ") -> None:
        """Fill empty cells with random letters."""
        for r in range(self.rows):
            for c in range(self.cols):
                if self.grid[r][c] == " ":
                    self.grid[r][c] = random.choice(alphabet)

    def find_word(self, word: str) -> Optional[List[Tuple[int, int]]]:
        """Find positions for a word. Returns list of (row, col) or None."""
        target = word.upper()
        for r in range(self.rows):
            for c in range(self.cols):
                for dr, dc in self.DIRECTIONS:
                    positions = []
                    for i, ch in enumerate(target):
                        nr, nc = r + dr * i, c + dc * i
                        if not (0 <= nr < self.rows and 0 <= nc < self.cols):
                            break
                        if self.grid[nr][nc] != ch:
                            break
                        positions.append((nr, nc))
                    else:
                        return positions
        return None

    def __repr__(self) -> str:
        col_nums = "  " + " ".join(str(i % 10) for i in range(self.cols))
        rows_str = [col_nums]
        for i, row in enumerate(self.grid):
            rows_str.append(str(i % 10) + " " + " ".join(row))
        return "\n".join(rows_str)


# ---------------------------------------------------------------------------
# Sliding Puzzle (8-puzzle / 15-puzzle)
# ---------------------------------------------------------------------------


class SlidingPuzzle:
    """N-puzzle (8 or 15) solver using A* with Manhattan distance."""

    def __init__(self, tiles: Optional[List[int]] = None, size: int = 3) -> None:
        self.size = size
        self.goal = list(range(1, size * size)) + [0]
        if tiles is None:
            self.tiles = list(range(1, size * size)) + [0]
        else:
            self.tiles = list(tiles)

    @classmethod
    def shuffled(cls, size: int = 3, steps: int = 100) -> SlidingPuzzle:
        """Create a shuffled puzzle by making random moves from goal."""
        p = cls(size=size)
        blank_idx = size * size - 1
        for _ in range(steps):
            blank_r, blank_c = divmod(blank_idx, size)
            moves = []
            if blank_r > 0: moves.append(-1)
            if blank_r < size - 1: moves.append(1)
            if blank_c > 0: moves.append(-size)
            if blank_c < size - 1: moves.append(size)
            delta = random.choice(moves)
            p.tiles[blank_idx], p.tiles[blank_idx + delta] = p.tiles[blank_idx + delta], p.tiles[blank_idx]
            blank_idx += delta
        return p

    def _manhattan(self) -> int:
        dist = 0
        for idx, val in enumerate(self.tiles):
            if val == 0:
                continue
            target_idx = val - 1
            r1, c1 = divmod(idx, self.size)
            r2, c2 = divmod(target_idx, self.size)
            dist += abs(r1 - r2) + abs(c1 - c2)
        return dist

    def _neighbors(self) -> List[List[int]]:
        neighbors = []
        blank = self.tiles.index(0)
        br, bc = divmod(blank, self.size)
        deltas = []
        if br > 0: deltas.append(-1)
        if br < self.size - 1: deltiles.append(1)  # noqa
        if bc > 0: deltas.append(-self.size)
        if bc < self.size - 1: deltas.append(self.size)
        for d in deltas:
            new_tiles = self.tiles[:]
            new_tiles[blank], new_tiles[blank + d] = new_tiles[blank + d], new_tiles[blank]
            neighbors.append(new_tiles)
        return neighbors

    def solve_a_star(self, max_iter: int = 100000) -> Optional[List[List[int]]]:
        """A* search. Returns list of board states or None."""
        start = tuple(self.tiles)
        goal = tuple(self.goal)
        if start == goal:
            return [self.tiles]
        open_set: Dict[tuple, Tuple[int, tuple]] = {start: (self._manhattan(), None)}
        g_score: Dict[tuple, int] = {start: 0}
        closed: Set[tuple] = set()
        came_from: Dict[tuple, tuple] = {}
        for _ in range(max_iter):
            if not open_set:
                return None
            current = min(open_set, key=lambda x: open_set[x][0])
            f, parent = open_set.pop(current)
            if current == goal:
                path = []
                node = current
                while node is not None:
                    path.append(list(node))
                    node = came_from.get(node)
                path.reverse()
                return path
            closed.add(current)
            for neighbor in self._iter_neighbors(list(current)):
                if neighbor in closed:
                    continue
                tentative_g = g_score[current] + 1
                if neighbor not in g_score or tentative_g < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g
                    h = sum(1 for i, v in enumerate(neighbor) if v != 0 and v != i + 1)
                    open_set[neighbor] = (tentative_g + h, current)
        return None

    def _iter_neighbors(self, tiles: List[int]) -> List[Tuple[int, ...]]:
        size = self.size
        blank = tiles.index(0)
        br, bc = divmod(blank, size)
        result = []
        if br > 0:
            t = tiles[:]; t[blank], t[blank - 1] = t[blank - 1], t[blank]; result.append(tuple(t))
        if br < size - 1:
            t = tiles[:]; t[blank], t[blank + 1] = t[blank + 1], t[blank]; result.append(tuple(t))
        if bc > 0:
            t = tiles[:]; t[blank], t[blank - size] = t[blank - size], t[blank]; result.append(tuple(t))
        if bc < size - 1:
            t = tiles[:]; t[blank], t[blank + size] = t[blank + size], t[blank]; result.append(tuple(t))
        return result

    def __repr__(self) -> str:
        lines = []
        for r in range(self.size):
            lines.append(" ".join(f"{v:2}" if v != 0 else " ." for v in self.tiles[r * self.size:(r + 1) * self.size]))
        return "\n".join(lines)
