"""
Puzzle utilities v2 — advanced solvers and puzzle generators.

Companion to puzzle_utils.py. Includes Sudoku solver, N-Queens,
crossword generation, and graph coloring puzzles.
"""

from __future__ import annotations

import random
from typing import Iterator


class Sudoku:
    """Sudoku puzzle solver using backtracking with constraint propagation."""

    def __init__(self, board: list[list[int]] | None = None) -> None:
        """Initialize with optional 9x9 board (0 = empty)."""
        if board:
            self.board = [row[:] for row in board]
        else:
            self.board = [[0] * 9 for _ in range(9)]

    def solve(self) -> bool:
        """Solve the Sudoku puzzle. Returns True if solved."""
        empty = self._find_empty()
        if not empty:
            return True
        row, col = empty
        for num in range(1, 10):
            if self._is_valid(row, col, num):
                self.board[row][col] = num
                if self.solve():
                    return True
                self.board[row][col] = 0
        return False

    def _find_empty(self) -> tuple[int, int] | None:
        """Find first empty cell (value 0)."""
        for r in range(9):
            for c in range(9):
                if self.board[r][c] == 0:
                    return r, c
        return None

    def _is_valid(self, row: int, col: int, num: int) -> bool:
        """Check if placing num at (row, col) is valid."""
        if num in self.board[row]:
            return False
        if num in [self.board[r][col] for r in range(9)]:
            return False
        box_r, box_c = 3 * (row // 3), 3 * (col // 3)
        for r in range(box_r, box_r + 3):
            for c in range(box_c, box_c + 3):
                if self.board[r][c] == num:
                    return False
        return True

    def generate(self, difficulty: int = 40) -> None:
        """Generate a puzzle by removing numbers from a solved board."""
        self.board = [[(i * 3 + i // 3 + j) % 9 + 1 for j in range(9)] for i in range(9)]
        self.solve()
        puzzle = [row[:] for row in self.board]
        positions = [(r, c) for r in range(9) for c in range(9)]
        random.shuffle(positions)
        for r, c in positions[:difficulty]:
            puzzle[r][c] = 0
        self.board = puzzle

    def __repr__(self) -> str:
        lines = []
        for i, row in enumerate(self.board):
            if i % 3 == 0 and i > 0:
                lines.append("-" * 21)
            lines.append(" ".join(str(x) if x != 0 else "." for x in row[:3]) + " | "
                        + " ".join(str(x) if x != 0 else "." for x in row[3:6]) + " | "
                        + " ".join(str(x) if x != 0 else "." for x in row[6:]))
        return "\n".join(lines)


def solve_n_queens(n: int = 8) -> list[list[int]]:
    """
    Solve N-Queens problem: place n queens on n×n board.

    Returns:
        List of solutions, each solution is a list where index=row, value=column

    Example:
        >>> solutions = solve_n_queens(4)
        >>> len(solutions)
        2
    """
    solutions: list[list[int]] = []
    cols: list[int] = []
    diag1: set[int] = set()
    diag2: set[int] = set()

    def backtrack(row: int) -> None:
        if row == n:
            solutions.append(cols[:])
            return
        for col in range(n):
            if col in cols or (row + col) in diag1 or (row - col) in diag2:
                continue
            cols.append(col)
            diag1.add(row + col)
            diag2.add(row - col)
            backtrack(row + 1)
            cols.pop()
            diag1.remove(row + col)
            diag2.remove(row - col)

    backtrack(0)
    return solutions


def n_queens_bitwise(n: int = 8) -> list[int]:
    """
    Solve N-Queens using bitwise algorithm (faster).

    Returns:
        List of column positions for each row
    """
    solutions: list[int] = []
    cols = 0
    diag1 = 0
    diag2 = 0
    answer = 0

    def backtrack(columns: int, diag1_mask: int, diag2_mask: int) -> None:
        nonlocal answer
        if columns == (1 << n) - 1:
            solutions.append(answer)
            return
        available = (~(columns | diag1_mask | diag2_mask)) & ((1 << n) - 1)
        while available:
            pos = available & (-available)
            available -= pos
            answer = (answer << n) | (pos.bit_length() - 1)
            backtrack(columns | pos, (diag1_mask | pos) << 1, (diag2_mask | pos) >> 1)
            answer >>= n

    backtrack(cols, diag1, diag2)
    return solutions


def solve_magic_square(n: int) -> list[list[int]] | None:
    """
    Generate a magic square of size n×n using Siamese method (odd n only).

    Returns:
        Magic square matrix, or None if n is even
    """
    if n % 2 == 0:
        return None
    magic = [[0] * n for _ in range(n)]
    i, j = 0, n // 2
    for num in range(1, n * n + 1):
        magic[i][j] = num
        i, j = i - 1, j + 1
        if i < 0:
            i = n - 1
        if j >= n:
            j = 0
        if magic[i][j] != 0:
            i = (i + 2) % n
            j = (j - 1) % n
    return magic


def word_search(grid: list[list[str]], words: list[str]) -> list[tuple[str, list[tuple[int, int]]]] | None:
    """
    Find words in a character grid. Search horizontally and vertically.

    Args:
        grid: 2D character grid
        words: List of words to find

    Returns:
        List of (word, path) tuples where path is list of (row, col) positions
    """
    rows, cols = len(grid), len(grid[0])
    found: list[tuple[str, list[tuple[int, int]]]] = []

    def search(word: str, r: int, c: int, dr: int, dc: int) -> list[tuple[int, int]] | None:
        path = []
        for _ in word:
            if 0 <= r < rows and 0 <= c < cols and grid[r][c] == _:
                path.append((r, c))
                r, c = r + dr, c + dc
            else:
                return None
        return path

    for word in words:
        for r in range(rows):
            for c in range(cols):
                for dr, dc in [(0, 1), (1, 0), (0, -1), (-1, 0), (1, 1), (-1, -1), (1, -1), (-1, 1)]:
                    path = search(word, r, c, dr, dc)
                    if path:
                        found.append((word, path))
                        break
    return found


def generate_crossword(clues: list[str], size: int = 15) -> list[list[str]]:
    """
    Generate a simple crossword grid from clue list.

    Args:
        clues: List of words to place
        size: Grid size

    Returns:
        Character grid with words placed
    """
    grid = [["#"] * size for _ in range(size)]
    placed: list[tuple[int, int, str]] = []

    def can_place(word: str, r: int, c: int, dr: int, dc: int) -> bool:
        for i, ch in enumerate(word):
            nr, nc = r + i * dr, c + i * dc
            if nr < 0 or nr >= size or nc < 0 or nc >= size:
                return False
            if grid[nr][nc] != "#" and grid[nr][nc] != ch:
                return False
            if dr == 0:
                nr_prev = r + (i - 1) * dr if i > 0 else r
                nc_prev = c + (i - 1) * dc if i > 0 else c - 1
                nr_next = r + (i + 1) * dr if i < len(word) - 1 else r
                nc_next = c + (i + 1) * dc if i < len(word) - 1 else c + 1
                if nc_prev >= 0 and grid[nr_prev][nc_prev] != "#":
                    return False
                if nc_next < size and grid[nr_next][nc_next] != "#":
                    return False
            if dc == 0:
                nr_prev = r + (i - 1) * dr if i > 0 else r - 1
                nc_prev = c + (i - 1) * dc if i > 0 else c
                nr_next = r + (i + 1) * dr if i < len(word) - 1 else r + 1
                nc_next = c + (i + 1) * dc if i < len(word) - 1 else c
                if nr_prev >= 0 and grid[nr_prev][nc_prev] != "#":
                    return False
                if nr_next < size and grid[nr_next][nc_next] != "#":
                    return False
        return True

    for word in sorted(clues, key=len, reverse=True):
        for r in range(size):
            for c in range(size):
                for dr, dc in [(0, 1), (1, 0)]:
                    if can_place(word, r, c, dr, dc):
                        for i, ch in enumerate(word):
                            nr, nc = r + i * dr, c + i * dc
                            grid[nr][nc] = ch
                        placed.append((r, c, word))
                        break
                else:
                    continue
                break

    return grid
