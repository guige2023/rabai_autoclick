"""
Bitmap manipulation utilities for automation actions.

Provides 2D bit array operations, region labeling, convolution,
morphological operations, and image-like transformations.
"""

from __future__ import annotations

from typing import Callable, Iterator


class Bitmap:
    """2D bitmap (boolean matrix) with various operations."""

    def __init__(self, width: int, height: int, fill: bool = False) -> None:
        self.width = width
        self.height = height
        self.data = [[fill] * width for _ in range(height)]

    @classmethod
    def from_list(cls, grid: list[list[bool]]) -> "Bitmap":
        """Create Bitmap from 2D boolean list."""
        if not grid:
            raise ValueError("Grid cannot be empty")
        height = len(grid)
        width = len(grid[0])
        bmp = cls(width, height)
        for r in range(height):
            for c in range(width):
                bmp.data[r][c] = grid[r][c]
        return bmp

    def get(self, row: int, col: int) -> bool:
        """Get pixel value at (row, col)."""
        return self.data[row][col]

    def set(self, row: int, col: int, value: bool) -> None:
        """Set pixel value at (row, col)."""
        self.data[row][col] = value

    def flip(self, row: int, col: int) -> None:
        """Toggle pixel value at (row, col)."""
        self.data[row][col] = not self.data[row][col]

    def clear(self, value: bool = False) -> None:
        """Clear all pixels to value."""
        for r in range(self.height):
            for c in range(self.width):
                self.data[r][c] = value

    def fill_rect(self, row: int, col: int, w: int, h: int, value: bool) -> None:
        """Fill rectangle with value."""
        for r in range(row, min(row + h, self.height)):
            for c in range(col, min(col + w, self.width)):
                self.data[r][c] = value

    def draw_line(self, r0: int, c0: int, r1: int, c1: int, value: bool) -> None:
        """Draw line using Bresenham's algorithm."""
        dr = abs(r1 - r0)
        dc = abs(c1 - c0)
        sr = 1 if r0 < r1 else -1
        sc = 1 if c0 < c1 else -1
        err = dr - dc
        while True:
            self.data[r0][c0] = value
            if r0 == r1 and c0 == c1:
                break
            e2 = 2 * err
            if e2 > -dc:
                err -= dc
                r0 += sr
            if e2 < dr:
                err += dr
                c0 += sc

    def draw_circle(self, center_r: int, center_c: int, radius: int, value: bool) -> None:
        """Draw circle using midpoint algorithm."""
        r = 0
        d = 1 - radius
        dc = radius
        while dc >= r:
            for c_offset in [-dc, dc]:
                for r_offset in [-r, r]:
                    self._set_pixel_safe(center_r + r_offset, center_c + c_offset, value)
            for c_offset in [-r, r]:
                for r_offset in [-dc, dc]:
                    self._set_pixel_safe(center_r + r_offset, center_c + c_offset, value)
            r += 1
            if d < 0:
                d += 2 * r + 1
            else:
                d += 2 * (r - dc) + 1
                dc -= 1

    def _set_pixel_safe(self, r: int, c: int, value: bool) -> None:
        """Set pixel with boundary check."""
        if 0 <= r < self.height and 0 <= c < self.width:
            self.data[r][c] = value

    def count(self, value: bool = True) -> int:
        """Count pixels with given value."""
        return sum(1 for r in range(self.height) for c in range(self.width) if self.data[r][c] == value)

    def find_first(self, value: bool = True) -> tuple[int, int] | None:
        """Find first pixel with given value. Returns (row, col) or None."""
        for r in range(self.height):
            for c in range(self.width):
                if self.data[r][c] == value:
                    return (r, c)
        return None

    def find_all(self, value: bool = True) -> Iterator[tuple[int, int]]:
        """Yield all pixels with given value."""
        for r in range(self.height):
            for c in range(self.width):
                if self.data[r][c] == value:
                    yield (r, c)

    def bounding_box(self, value: bool = True) -> tuple[int, int, int, int] | None:
        """Get bounding box of pixels with value. Returns (min_r, min_c, max_r, max_c) or None."""
        min_r = min_c = None
        max_r = max_c = None
        for r, c in self.find_all(value):
            if min_r is None:
                min_r = max_r = r
                min_c = max_c = c
            else:
                min_r = min(min_r, r)
                max_r = max(max_r, r)
                min_c = min(min_c, c)
                max_c = max(max_c, c)
        if min_r is None:
            return None
        return (min_r, min_c, max_r, max_c)

    def transpose(self) -> "Bitmap":
        """Return transposed bitmap."""
        result = Bitmap(self.height, self.width)
        for r in range(self.height):
            for c in range(self.width):
                result.data[c][r] = self.data[r][c]
        return result

    def flip_horizontal(self) -> "Bitmap":
        """Return horizontally flipped bitmap."""
        result = Bitmap(self.width, self.height)
        for r in range(self.height):
            for c in range(self.width):
                result.data[r][self.width - 1 - c] = self.data[r][c]
        return result

    def flip_vertical(self) -> "Bitmap":
        """Return vertically flipped bitmap."""
        result = Bitmap(self.width, self.height)
        for r in range(self.height):
            result.data[self.height - 1 - r] = self.data[r][:]
        return result

    def rotate_90_cw(self) -> "Bitmap":
        """Return bitmap rotated 90 degrees clockwise."""
        return self.transpose().flip_horizontal()

    def rotate_90_ccw(self) -> "Bitmap":
        """Return bitmap rotated 90 degrees counter-clockwise."""
        return self.transpose().flip_vertical()

    def dilate(self) -> "Bitmap":
        """Morphological dilation - expand foreground."""
        result = Bitmap(self.width, self.height)
        for r in range(self.height):
            for c in range(self.width):
                if self.data[r][c]:
                    for dr in [-1, 0, 1]:
                        for dc in [-1, 0, 1]:
                            nr, nc = r + dr, c + dc
                            if 0 <= nr < self.height and 0 <= nc < self.width:
                                result.data[nr][nc] = True
        return result

    def erode(self) -> "Bitmap":
        """Morphological erosion - shrink foreground."""
        result = Bitmap(self.width, self.height)
        for r in range(self.height):
            for c in range(self.width):
                keep = True
                for dr in [-1, 0, 1]:
                    for dc in [-1, 0, 1]:
                        nr, nc = r + dr, c + dc
                        if not (0 <= nr < self.height and 0 <= nc < self.width):
                            keep = False
                            break
                        if not self.data[nr][nc]:
                            keep = False
                            break
                    if not keep:
                        break
                result.data[r][c] = keep
        return result

    def open(self) -> "Bitmap":
        """Morphological opening (erode then dilate)."""
        return self.erode().dilate()

    def close(self) -> "Bitmap":
        """Morphological closing (dilate then erode)."""
        return self.dilate().erode()

    def flood_fill(self, row: int, col: int, new_value: bool) -> None:
        """Flood fill starting from (row, col)."""
        if not (0 <= row < self.height and 0 <= col < self.width):
            return
        target = self.data[row][col]
        if target == new_value:
            return
        stack: list[tuple[int, int]] = [(row, col)]
        while stack:
            r, c = stack.pop()
            if not (0 <= r < self.height and 0 <= c < self.width):
                continue
            if self.data[r][c] != target:
                continue
            self.data[r][c] = new_value
            stack.extend([(r + 1, c), (r - 1, c), (r, c + 1), (r, c - 1)])

    def connected_components(self) -> int:
        """Count connected components using BFS."""
        visited: list[list[bool]] = [[False] * self.width for _ in range(self.height)]
        count = 0
        for r in range(self.height):
            for c in range(self.width):
                if self.data[r][c] and not visited[r][c]:
                    count += 1
                    self._bfs(r, c, visited)
        return count

    def _bfs(self, start_r: int, start_c: int, visited: list[list[bool]]) -> None:
        """BFS to mark connected component."""
        queue: list[tuple[int, int]] = [(start_r, start_c)]
        visited[start_r][start_c] = True
        while queue:
            r, c = queue.pop(0)
            for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
                nr, nc = r + dr, c + dc
                if 0 <= nr < self.height and 0 <= nc < self.width:
                    if self.data[nr][nc] and not visited[nr][nc]:
                        visited[nr][nc] = True
                        queue.append((nr, nc))

    def convolve(self, kernel: list[list[float]], threshold: float = 0.5) -> "Bitmap":
        """Apply convolution with kernel."""
        kh, kw = len(kernel), len(kernel[0])
        result = Bitmap(self.width, self.height)
        offset_r, offset_c = kh // 2, kw // 2
        for r in range(self.height):
            for c in range(self.width):
                total = 0.0
                for kr in range(kh):
                    for kc in range(kw):
                        pr = r + kr - offset_r
                        pc = c + kc - offset_c
                        if 0 <= pr < self.height and 0 <= pc < self.width:
                            total += (1.0 if self.data[pr][pc] else 0.0) * kernel[kr][kc]
                result.data[r][c] = total >= threshold
        return result

    def distance_transform(self) -> list[list[float]]:
        """Compute distance transform (Euclidean distance to nearest zero pixel)."""
        import math
        dist = [[float("inf")] * self.width for _ in range(self.height)]
        for r in range(self.height):
            for c in range(self.width):
                if not self.data[r][c]:
                    dist[r][c] = 0.0
                    continue
                if r > 0:
                    dist[r][c] = min(dist[r][c], dist[r - 1][c] + 1)
                if c > 0:
                    dist[r][c] = min(dist[r][c], dist[r][c - 1] + 1)
        for r in range(self.height - 1, -1, -1):
            for c in range(self.width - 1, -1, -1):
                if r < self.height - 1:
                    dist[r][c] = min(dist[r][c], dist[r + 1][c] + 1)
                if c < self.width - 1:
                    dist[r][c] = min(dist[r][c], dist[r][c + 1] + 1)
                dist[r][c] = math.sqrt(dist[r][c])
        return dist

    def to_list(self) -> list[list[bool]]:
        """Return data as 2D boolean list."""
        return [row[:] for row in self.data]

    def copy(self) -> "Bitmap":
        """Return a deep copy."""
        result = Bitmap(self.width, self.height)
        result.data = [row[:] for row in self.data]
        return result
