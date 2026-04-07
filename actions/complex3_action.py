"""Complex action v3 - visualization and interactive tools.

Complex number utilities for plotting, animation,
and interactive exploration.
"""

from __future__ import annotations

from complex import complex, polar, rect
from cmath import exp, log, sin, cos, sqrt, tan, sinh, cosh, tanh, asin, acos, atan, asinh, acosh, atanh
from typing import Sequence

__all__ = [
    "ComplexPlotter2D",
    "ComplexGrid",
    "ComplexDomain",
    "ComplexVectorField",
    "ComplexMapper",
    "MandelbrotSet",
    "JuliaSet",
    "ComplexSpiral",
    "ComplexPath",
    "ComplexPolygon",
    "ComplexRoots",
    "ComplexLissajous",
    "ComplexTransform",
]


class ComplexPlotter2D:
    """Enhanced 2D complex plane plotter."""

    def __init__(self, x_range: tuple[float, float] = (-5, 5), y_range: tuple[float, float] = (-5, 5), width: int = 80, height: int = 25) -> None:
        self.x_range = x_range
        self.y_range = y_range
        self.width = width
        self.height = height
        self._points: list[complex] = []
        self._labels: dict[complex, str] = {}
        self._segments: list[tuple[complex, complex]] = []

    def add_point(self, c: complex, label: str | None = None) -> None:
        """Add a point."""
        self._points.append(c)
        if label:
            self._labels[c] = label

    def add_segment(self, a: complex, b: complex) -> None:
        """Add line segment."""
        self._segments.append((a, b))

    def _to_screen(self, c: complex) -> tuple[int, int]:
        """Map complex to screen coordinates."""
        sx = int((c.real - self.x_range[0]) / (self.x_range[1] - self.x_range[0]) * self.width)
        sy = self.height - int((c.imag - self.y_range[0]) / (self.y_range[1] - self.y_range[0]) * self.height)
        sx = max(0, min(self.width - 1, sx))
        sy = max(0, min(self.height - 1, sy))
        return (sx, sy)

    def render(self) -> str:
        """Render plot as ASCII."""
        grid = [[" " for _ in range(self.width)] for _ in range(self.height)]
        for x in range(self.width):
            grid[self.height // 2][x] = "-"
        for y in range(self.height):
            grid[y][self.width // 2] = "|"
        grid[self.height // 2][self.width // 2] = "+"
        for a, b in self._segments:
            self._draw_line(grid, a, b, "-")
        for point in self._points:
            px, py = self._to_screen(point)
            if grid[py][px] in ("-", "|", "+"):
                grid[py][px] = "*"
            else:
                grid[py][px] = "o"
        return "\n".join("".join(row) for row in grid)

    def _draw_line(self, grid: list[list[str]], a: complex, b: complex, char: str) -> None:
        """Bresenham line drawing."""
        ax, ay = self._to_screen(a)
        bx, by = self._to_screen(b)
        dx = abs(bx - ax)
        dy = abs(by - ay)
        sx = 1 if ax < bx else -1
        sy = 1 if ay < by else -1
        err = dx - dy
        x, y = ax, ay
        while True:
            if 0 <= y < self.height and 0 <= x < self.width:
                if grid[y][x] == " ":
                    grid[y][x] = char
            if x == bx and y == by:
                break
            e2 = 2 * err
            if e2 > -dy:
                err -= dy
                x += sx
            if e2 < dx:
                err += dx
                y += sy

    def render_colored(self) -> list[list[tuple[str, str]]]:
        """Render with (char, color) pairs."""
        import sys
        result = self.render().split("\n")
        return [[(c, "default") for c in line] for line in result]


class ComplexGrid:
    """Grid of complex numbers."""

    def __init__(self, x_range: tuple[float, float], y_range: tuple[float, float], resolution: int = 20) -> None:
        self.x_range = x_range
        self.y_range = y_range
        self.resolution = resolution
        self._grid: list[list[complex]] = []
        self._build_grid()

    def _build_grid(self) -> None:
        """Build grid points."""
        import numpy as np
        x_vals = np.linspace(self.x_range[0], self.x_range[1], self.resolution)
        y_vals = np.linspace(self.y_range[0], self.y_range[1], self.resolution)
        self._grid = [[complex(x, y) for x in x_vals] for y in y_vals]

    def apply(self, func: callable) -> list[list[complex]]:
        """Apply function to all grid points."""
        return [[func(c) for c in row] for row in self._grid]

    def magnitudes(self) -> list[list[float]]:
        """Get magnitudes of grid points."""
        return [[abs(c) for c in row] for row in self._grid]

    def phases(self) -> list[list[float]]:
        """Get phases of grid points."""
        return [[c.phase for c in row] for row in self._grid]


class ComplexDomain:
    """Domain coloring plot of complex function."""

    def __init__(self, func: callable, x_range: tuple[float, float] = (-2, 2), y_range: tuple[float, float] = (-2, 2), resolution: int = 40) -> None:
        self.func = func
        self.x_range = x_range
        self.y_range = y_range
        self.resolution = resolution

    def phase_colormap(self, c: complex) -> str:
        """Map phase to color."""
        import colorsys
        hue = (c.phase + 3.14159265) / (2 * 3.14159265)
        r, g, b = colorsys.hsv_to_rgb(hue, 1.0, 1.0)
        return f"\033[38;2;{int(r*255)};{int(g*255)};{int(b*255)}m"

    def render(self) -> str:
        """Render domain colored plot."""
        import numpy as np
        x_vals = np.linspace(self.x_range[0], self.x_range[1], self.resolution)
        y_vals = np.linspace(self.y_range[0], self.y_range[1], self.resolution)
        lines = []
        for y in y_vals:
            row = ""
            for x in x_vals:
                z = complex(x, y)
                try:
                    w = self.func(z)
                    hue = (w.phase + 3.14159265) / (2 * 3.14159265)
                    lum = 1.0 / (1 + abs(w) * 0.5)
                    char = self._phase_char(hue)
                    row += char
                except (ZeroDivisionError, OverflowError):
                    row += " "
            lines.append(row)
        return "\n".join(lines)

    def _phase_char(self, hue: float) -> str:
        """Convert hue to ASCII character."""
        if 0 <= hue < 0.125:
            return "R"
        elif 0.125 <= hue < 0.25:
            return "Y"
        elif 0.25 <= hue < 0.375:
            return "G"
        elif 0.375 <= hue < 0.5:
            return "C"
        elif 0.5 <= hue < 0.625:
            return "B"
        elif 0.625 <= hue < 0.75:
            return "M"
        elif 0.75 <= hue < 0.875:
            return "A"
        else:
            return "r"


class ComplexVectorField:
    """Vector field visualization on complex plane."""

    def __init__(self, func: callable, x_range: tuple[float, float] = (-2, 2), y_range: tuple[float, float] = (-2, 2), density: int = 10) -> None:
        self.func = func
        self.x_range = x_range
        self.y_range = y_range
        self.density = density

    def arrows(self) -> list[tuple[complex, complex]]:
        """Get arrow origins and directions."""
        import numpy as np
        x_vals = np.linspace(self.x_range[0], self.x_range[1], self.density)
        y_vals = np.linspace(self.y_range[0], self.y_range[1], self.density)
        arrows = []
        for x in x_vals:
            for y in y_vals:
                z = complex(x, y)
                try:
                    w = self.func(z)
                    arrows.append((z, w))
                except (ZeroDivisionError, OverflowError):
                    pass
        return arrows


class ComplexMapper:
    """Interactive complex function mapper."""

    def __init__(self) -> None:
        self._transforms: list[callable] = []

    def add_transform(self, func: callable) -> ComplexMapper:
        """Add a transformation."""
        self._transforms.append(func)
        return self

    def map(self, c: complex) -> complex:
        """Apply all transforms."""
        result = c
        for t in self._transforms:
            result = t(result)
        return result

    def map_points(self, points: Sequence[complex]) -> list[complex]:
        """Map sequence of points."""
        return [self.map(p) for p in points]

    def reset(self) -> ComplexMapper:
        """Clear all transforms."""
        self._transforms.clear()
        return self


class MandelbrotSet:
    """Mandelbrot set visualization."""

    def __init__(self, x_range: tuple[float, float] = (-2.5, 1), y_range: tuple[float, float] = (-1.5, 1.5), max_iter: int = 100) -> None:
        self.x_range = x_range
        self.y_range = y_range
        self.max_iter = max_iter

    def escape_time(self, c: complex) -> int:
        """Compute escape iteration."""
        z = complex(0)
        for i in range(self.max_iter):
            if abs(z) > 2:
                return i
            z = z * z + c
        return self.max_iter

    def render_ascii(self, width: int = 60, height: int = 30) -> str:
        """ASCII Mandelbrot rendering."""
        import numpy as np
        x_vals = np.linspace(self.x_range[0], self.x_range[1], width)
        y_vals = np.linspace(self.y_range[0], self.y_range[1], height)
        chars = " .:-=+*#%@"
        lines = []
        for y in reversed(y_vals):
            row = ""
            for x in x_vals:
                c = complex(x, y)
                it = self.escape_time(c)
                idx = min(int(it / self.max_iter * len(chars)), len(chars) - 1)
                row += chars[idx]
            lines.append(row)
        return "\n".join(lines)


class JuliaSet:
    """Julia set visualization."""

    def __init__(self, c: complex, x_range: tuple[float, float] = (-2, 2), y_range: tuple[float, float] = (-2, 2), max_iter: int = 100) -> None:
        self.c = c
        self.x_range = x_range
        self.y_range = y_range
        self.max_iter = max_iter

    def escape_time(self, z: complex) -> int:
        """Compute escape iteration."""
        for i in range(self.max_iter):
            if abs(z) > 2:
                return i
            z = z * z + self.c
        return self.max_iter

    def render_ascii(self, width: int = 60, height: int = 30) -> str:
        """ASCII Julia set rendering."""
        import numpy as np
        x_vals = np.linspace(self.x_range[0], self.x_range[1], width)
        y_vals = np.linspace(self.y_range[0], self.y_range[1], height)
        chars = " .:-=+*#%@"
        lines = []
        for y in reversed(y_vals):
            row = ""
            for x in x_vals:
                z = complex(x, y)
                it = self.escape_time(z)
                idx = min(int(it / self.max_iter * len(chars)), len(chars) - 1)
                row += chars[idx]
            lines.append(row)
        return "\n".join(lines)


class ComplexSpiral:
    """Spiral path in complex plane."""

    def __init__(self, center: complex = 0, start_radius: float = 1, growth_rate: float = 0.1, angular_speed: float = 0.5) -> None:
        self.center = center
        self.start_radius = start_radius
        self.growth_rate = growth_rate
        self.angular_speed = angular_speed

    def point(self, t: float) -> complex:
        """Get point at parameter t."""
        r = self.start_radius + self.growth_rate * t
        angle = self.angular_speed * t
        return self.center + rect(r, angle)

    def points(self, count: int, max_t: float = 20) -> list[complex]:
        """Get multiple points along spiral."""
        return [self.point(t * max_t / count) for t in range(count)]


class ComplexPath:
    """Path of complex points."""

    def __init__(self) -> None:
        self._points: list[complex] = []

    def move_to(self, c: complex) -> ComplexPath:
        """Move to point."""
        self._points.append(c)
        return self

    def line_to(self, c: complex) -> ComplexPath:
        """Draw line to point."""
        self._points.append(c)
        return self

    def arc_to(self, c: complex, radius: float) -> ComplexPath:
        """Draw arc to point."""
        self._points.append(c)
        return self

    def close(self) -> ComplexPath:
        """Close path."""
        if self._points:
            self._points.append(self._points[0])
        return self

    def length(self) -> float:
        """Compute total path length."""
        total = 0.0
        for i in range(1, len(self._points)):
            total += abs(self._points[i] - self._points[i - 1])
        return total

    def points(self) -> list[complex]:
        """Get all points."""
        return list(self._points)


class ComplexPolygon:
    """Regular polygon on complex plane."""

    def __init__(self, sides: int, center: complex = 0, radius: float = 1) -> None:
        if sides < 3:
            raise ValueError("Polygon must have at least 3 sides")
        self.sides = sides
        self.center = center
        self.radius = radius
        self._vertices = self._compute_vertices()

    def _compute_vertices(self) -> list[complex]:
        """Compute polygon vertices."""
        vertices = []
        for i in range(self.sides):
            angle = 2 * 3.14159265 * i / self.sides
            vertices.append(self.center + rect(self.radius, angle))
        return vertices

    def vertices(self) -> list[complex]:
        """Get vertices."""
        return list(self._vertices)

    def perimeter(self) -> float:
        """Compute perimeter."""
        total = 0.0
        for i in range(self.sides):
            total += abs(self._vertices[i] - self._vertices[(i + 1) % self.sides])
        return total

    def area(self) -> float:
        """Compute area using shoelace formula."""
        total = 0.0
        for i in range(self.sides):
            j = (i + 1) % self.sides
            total += self._vertices[i].real * self._vertices[j].imag
            total -= self._vertices[j].real * self._vertices[i].imag
        return abs(total) / 2


class ComplexRoots:
    """Find and visualize complex roots."""

    @staticmethod
    def nth_roots(n: int, center: complex = 0, radius: float = 1) -> list[complex]:
        """Get n-th roots of unity scaled and translated."""
        roots = []
        for k in range(n):
            angle = 2 * 3.14159265 * k / n
            roots.append(center + rect(radius, angle))
        return roots

    @staticmethod
    def visualize_roots(n: int, radius: float = 1) -> str:
        """ASCII visualization of n-th roots."""
        roots = ComplexRoots.nth_roots(n, radius=radius)
        plotter = ComplexPlotter2D(x_range=(-2, 2), y_range=(-2, 2))
        for r in roots:
            plotter.add_point(r)
        plotter.add_segment(complex(0), complex(radius, 0))
        return plotter.render()


class ComplexLissajous:
    """Lissajous curve on complex plane."""

    def __init__(self, a: float = 3, b: float = 2, delta: float = 0) -> None:
        self.a = a
        self.b = b
        self.delta = delta

    def point(self, t: float) -> complex:
        """Get point at parameter t."""
        x = self.a * sin(t + self.delta)
        y = self.b * sin(t)
        return complex(x, y)

    def points(self, count: int = 200) -> list[complex]:
        """Get points along curve."""
        return [self.point(2 * 3.14159265 * i / count) for i in range(count)]


class ComplexTransform:
    """Common complex plane transformations."""

    @staticmethod
    def invert(c: complex) -> complex:
        """Inversion: 1/z."""
        if c == 0:
            raise ZeroDivisionError("Cannot invert 0")
        return c.conjugate() / (c.real * c.real + c.imag * c.imag)

    @staticmethod
    def square(c: complex) -> complex:
        """Square: z^2."""
        return c * c

    @staticmethod
    def cube(c: complex) -> complex:
        """Cube: z^3."""
        return c * c * c

    @staticmethod
    def exp_map(c: complex) -> complex:
        """Exponential map: e^z."""
        return exp(c)

    @staticmethod
    def log_map(c: complex) -> complex:
        """Logarithmic map: log(z)."""
        return log(c)

    @staticmethod
    def sin_map(c: complex) -> complex:
        """Sine map: sin(z)."""
        return sin(c)

    @staticmethod
    def cos_map(c: complex) -> complex:
        """Cosine map: cos(z)."""
        return cos(c)
