"""
ASCII art and table utilities for terminal output.

Provides:
- ASCII table generation with alignment and borders
- Box drawing helpers
- Simple ASCII art generation
- Column formatting
- Bar/chart rendering
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


# ------------------------------------------------------------------------------
# ASCII Table
# ------------------------------------------------------------------------------

class ASCIITable:
    """
    Generate ASCII tables for terminal output.

    Example:
        >>> table = ASCIITable(['Name', 'Age', 'City'])
        >>> table.add_row(['Alice', '30', 'NYC'])
        >>> table.add_row(['Bob', '25', 'LA'])
        >>> print(table)
        +-------+-----+-----+
        | Name  | Age |City |
        +-------+-----+-----+
        | Alice | 30  | NYC |
        | Bob   | 25  | LA  |
        +-------+-----+-----+
    """

    DEFAULT_STYLE = {
        "top_left": "+",
        "top_right": "+",
        "bottom_left": "+",
        "bottom_right": "+",
        "top_cross": "+",
        "bottom_cross": "+",
        "left_cross": "+",
        "right_cross": "+",
        "cross": "+",
        "horizontal": "-",
        "vertical": "|",
    }

    DOUBLE_STYLE = {
        "top_left": "+",
        "top_right": "+",
        "bottom_left": "+",
        "bottom_right": "+",
        "top_cross": "+",
        "bottom_cross": "+",
        "left_cross": "+",
        "right_cross": "+",
        "cross": "+",
        "horizontal": "=",
        "vertical": "|",
    }

    ROUNDED_STYLE = {
        "top_left": "+",
        "top_right": "+",
        "bottom_left": "+",
        "bottom_right": "+",
        "top_cross": "+",
        "bottom_cross": "+",
        "left_cross": "+",
        "right_cross": "+",
        "cross": "+",
        "horizontal": "-",
        "vertical": "|",
    }

    def __init__(
        self,
        headers: Sequence[str] | None = None,
        style: dict[str, str] | None = None,
        alignments: Sequence[str] | None = None,
    ) -> None:
        """
        Initialize ASCII table.

        Args:
            headers: Optional list of column headers.
            style: Border character style dict.
            alignments: Per-column alignments ('left', 'center', 'right').
        """
        self._headers: list[str] = list(headers) if headers else []
        self._rows: list[list[str]] = []
        self._style = style or self.DEFAULT_STYLE.copy()
        self._column_widths: list[int] = []
        self._alignments: list[str] = []

        if headers:
            self._column_widths = [len(str(h)) for h in headers]
            self._alignments = list(alignments) if alignments else ["left"] * len(headers)

    def set_alignments(self, alignments: Sequence[str]) -> "ASCIITable":
        """Set column alignments ('left', 'center', 'right')."""
        self._alignments = list(alignments)
        return self

    def add_row(self, row: Sequence[str]) -> "ASCIITable":
        """
        Add a data row.

        Args:
            row: Row values.

        Returns:
            Self for chaining.
        """
        self._rows.append([str(v) for v in row])
        for i, val in enumerate(row):
            col_width = len(str(val))
            if i >= len(self._column_widths):
                self._column_widths.append(col_width)
                if not self._alignments:
                    self._alignments.append("left")
            else:
                self._column_widths[i] = max(self._column_widths[i], col_width)
        return self

    def add_separator(self) -> "ASCIITable":
        """
        Add a visual row separator (dashed line).

        Returns:
            Self for chaining.
        """
        self._rows.append(["---separator---"])
        return self

    def _align_cell(self, value: str, width: int, alignment: str) -> str:
        """Align a cell value within given width."""
        padding = width - len(value)
        if alignment == "right":
            return " " * padding + value
        elif alignment == "center":
            left = padding // 2
            right = padding - left
            return " " * left + value + " " * right
        else:  # left
            return value + " " * padding

    def _build_border(self, corners: tuple[str, str], cross: str) -> str:
        """Build a horizontal border line."""
        chars = [self._style[corners[0]]]
        for width in self._column_widths:
            chars.append(self._style["horizontal"] * (width + 2))
            chars.append(self._style[cross])
        chars[-1] = self._style[corners[1]]
        return "".join(chars)

    def __str__(self) -> str:
        """Render the table as string."""
        if not self._column_widths:
            return ""

        lines: list[str] = []

        # Top border
        lines.append(self._build_border(("top_left", "top_right"), "top_cross"))

        # Header row
        if self._headers:
            header_cells: list[str] = []
            for i, h in enumerate(self._headers):
                align = self._alignments[i] if i < len(self._alignments) else "left"
                cell = self._align_cell(str(h), self._column_widths[i], align)
                header_cells.append(f" {cell} ")
            lines.append(self._style["vertical"] + self._style["vertical"].join(header_cells) + self._style["vertical"])
            lines.append(self._build_border(("left_cross", "right_cross"), "cross"))

        # Data rows
        for row in self._rows:
            if len(row) == 1 and row[0] == "---separator---":
                lines.append(self._build_border(("left_cross", "right_cross"), "cross"))
                continue
            row_cells: list[str] = []
            for i, val in enumerate(row):
                if i >= len(self._column_widths):
                    continue
                align = self._alignments[i] if i < len(self._alignments) else "left"
                cell = self._align_cell(str(val), self._column_widths[i], align)
                row_cells.append(f" {cell} ")
            lines.append(self._style["vertical"] + self._style["vertical"].join(row_cells) + self._style["vertical"])

        # Bottom border
        lines.append(self._build_border(("bottom_left", "bottom_right"), "bottom_cross"))

        return "\n".join(lines)


def make_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    alignments: Sequence[str] | None = None,
    style: str = "default",
) -> str:
    """
    Create an ASCII table from headers and rows.

    Args:
        headers: Column headers.
        rows: Data rows.
        alignments: Column alignments.
        style: 'default', 'double', or 'rounded'.

    Returns:
        Rendered ASCII table string.

    Example:
        >>> print(make_table(['A', 'B'], [[1, 2], [3, 4]]))
        +---+---+
        | A | B |
        +---+---+
        | 1 | 2 |
        | 3 | 4 |
        +---+---+
    """
    style_map = {
        "default": ASCIITable.DEFAULT_STYLE,
        "double": ASCIITable.DOUBLE_STYLE,
        "rounded": ASCIITable.ROUNDED_STYLE,
    }
    table_style = style_map.get(style, ASCIITable.DEFAULT_STYLE).copy()

    table = ASCIITable(headers, style=table_style, alignments=alignments)
    for row in rows:
        table.add_row(list(row))
    return str(table)


# ------------------------------------------------------------------------------
# Box Drawing
# ------------------------------------------------------------------------------

def box_text(text: str, width: int | None = None, style: str = "single") -> str:
    """
    Draw a box around text.

    Args:
        text: Text to box.
        width: Box width (auto if None).
        style: 'single' or 'double'.

    Returns:
        Boxed text string.

    Example:
        >>> print(box_text('Hello', width=12))
        +----------+
        |  Hello   |
        +----------+
    """
    if style == "double":
        h_char = "="
        v_char = "|"
    else:
        h_char = "-"
        v_char = "|"

    lines = text.splitlines()
    if width is None:
        width = max(len(line) for line in lines) if lines else len(text)
        width = max(width, len(text))

    result_lines: list[str] = []
    result_lines.append("+" + h_char * (width + 2) + "+")
    for line in lines:
        result_lines.append(f"{v_char} {line:<{width}} {v_char}")
    result_lines.append("+" + h_char * (width + 2) + "+")
    return "\n".join(result_lines)


# ------------------------------------------------------------------------------
# Column Formatting
# ------------------------------------------------------------------------------

def format_columns(
    items: Iterable[str],
    num_columns: int,
    column_width: int,
    indent: str = "",
) -> str:
    """
    Format items into columns for terminal output.

    Args:
        items: Items to format.
        num_columns: Number of columns.
        column_width: Width of each column.
        indent: Indent prefix for each line.

    Returns:
        Formatted column string.

    Example:
        >>> print(format_columns(['apple', 'banana', 'cherry', 'date'], 2, 10))
        apple      banana
        cherry     date
    """
    rows: list[list[str]] = []
    current_row: list[str] = []
    for item in items:
        current_row.append(item)
        if len(current_row) == num_columns:
            rows.append(current_row)
            current_row = []
    if current_row:
        while len(current_row) < num_columns:
            current_row.append("")
        rows.append(current_row)

    lines: list[str] = []
    for row in rows:
        cells = [item.ljust(column_width) for item in row]
        lines.append(indent + "".join(cells))
    return "\n".join(lines)


def align_text(
    text: str,
    width: int,
    alignment: str = "left",
    fill_char: str = " ",
) -> str:
    """
    Align text within a given width.

    Args:
        text: Text to align.
        width: Target width.
        alignment: 'left', 'right', 'center'.
        fill_char: Character for padding.

    Returns:
        Aligned text string.

    Example:
        >>> align_text('Hi', 6, 'center')
        '  Hi  '
    """
    text = str(text)
    padding = max(0, width - len(text))
    if alignment == "right":
        return fill_char * padding + text
    elif alignment == "center":
        left = padding // 2
        right = padding - left
        return fill_char * left + text + fill_char * right
    else:  # left
        return text + fill_char * padding


# ------------------------------------------------------------------------------
# Bar / Chart Rendering
# ------------------------------------------------------------------------------

def render_bar(
    value: float,
    maximum: float,
    width: int = 20,
    fill: str = "█",
    empty: str = "░",
    show_value: bool = True,
) -> str:
    """
    Render a horizontal bar chart.

    Args:
        value: Current value.
        maximum: Maximum value (scale).
        width: Bar width in characters.
        fill: Fill character.
        empty: Empty bar character.
        show_value: Include percentage.

    Returns:
        Bar chart string.

    Example:
        >>> render_bar(75, 100, width=10)
        '████████░░ 75%'
    """
    if maximum <= 0:
        return empty * width
    ratio = max(0.0, min(1.0, value / maximum))
    filled = int(ratio * width)
    bar = fill * filled + empty * (width - filled)
    if show_value:
        pct = int(ratio * 100)
        return f"{bar} {pct}%"
    return bar


def render_bars(
    items: Iterable[tuple[str, float]],
    maximum: float | None = None,
    width: int = 20,
    fill: str = "█",
    empty: str = "░",
) -> str:
    """
    Render multiple horizontal bars vertically.

    Args:
        items: Iterable of (label, value) tuples.
        maximum: Optional fixed maximum (auto if None).
        width: Bar width.
        fill: Fill character.

    Returns:
        Multi-line bar chart string.

    Example:
        >>> print(render_bars([('A', 30), ('B', 70)], maximum=100))
        A ███░░░░░░░░░░░░░░ 30
        B ████████░░░░░░░░░ 70
    """
    items = list(items)
    if maximum is None:
        max_val = max(v for _, v in items) if items else 1
    else:
        max_val = maximum

    lines: list[str] = []
    for label, value in items:
        bar = render_bar(value, max_val, width, fill, empty, show_value=False)
        lines.append(f"{label} {bar} {value}")
    return "\n".join(lines)


def render_sparkline(values: list[float], width: int = 40) -> str:
    """
    Render a simple sparkline.

    Args:
        values: Numeric values.
        width: Character width.

    Returns:
        Sparkline string using Unicode block characters.
    """
    if not values:
        return ""

    min_val = min(values)
    max_val = max(values)
    span = max_val - min_val
    if span == 0:
        span = 1

    blocks = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
    result: list[str] = []
    for v in values:
        normalized = (v - min_val) / span
        idx = int(normalized * (len(blocks) - 1))
        result.append(blocks[idx])

    return "".join(result)


# ------------------------------------------------------------------------------
# Simple ASCII Art
# ------------------------------------------------------------------------------

def big_text(text: str, font: str = "block") -> str:
    """
    Render text as large ASCII art.

    Args:
        text: Text to render (letters/numbers only).
        font: Font style ('block').

    Returns:
        ASCII art string.

    Example:
        >>> print(big_text('Hi'))
    """
    # Simple block font (5x5 characters per letter)
    font_map: dict[str, list[str]] = {
        "A": ["  A  ", " A A ", "AAAAA", "A   A", "A   A"],
        "B": ["BBBB ", "B   B", "BBBB ", "B   B", "BBBB "],
        "C": [" CCC ", "C    ", "C    ", "C    ", " CCC "],
        "D": ["DDD  ", "D  D ", "D   D", "D  D ", "DDD  "],
        "E": ["EEEEE", "E    ", "EEE  ", "E    ", "EEEEE"],
        "F": ["FFFFF", "F    ", "FFF  ", "F    ", "F    "],
        "G": [" GGG ", "G    ", "G  GG", "G   G", " GGG "],
        "H": ["H   H", "H   H", "HHHHH", "H   H", "H   H"],
        "I": ["IIIII", "  I  ", "  I  ", "  I  ", "IIIII"],
        "J": ["JJJJJ", "   J ", "   J ", "J  J ", " JJ  "],
        "K": ["K   K", "K  K ", "KK   ", "K  K ", "K   K"],
        "L": ["L    ", "L    ", "L    ", "L    ", "LLLLL"],
        "M": ["M   M", "MM MM", "M M M", "M   M", "M   M"],
        "N": ["N   N", "NN  N", "N N N", "N  NN", "N   N"],
        "O": [" OOO ", "O   O", "O   O", "O   O", " OOO "],
        "P": ["PPPP ", "P   P", "PPPP ", "P    ", "P    "],
        "Q": [" QQQ ", "Q   Q", "Q Q Q", "Q  Q ", " QQ Q"],
        "R": ["RRRR ", "R   R", "RRRR ", "R  R ", "R   R"],
        "S": [" SSS ", "S    ", " SSS ", "    S", " SSS "],
        "T": ["TTTTT", "  T  ", "  T  ", "  T  ", "  T  "],
        "U": ["U   U", "U   U", "U   U", "U   U", " UUU "],
        "V": ["V   V", "V   V", "V   V", " V V ", "  V  "],
        "W": ["W   W", "W   W", "W W W", "WW WW", "W   W"],
        "X": ["X   X", " X X ", "  X  ", " X X ", "X   X"],
        "Y": ["Y   Y", " Y Y ", "  Y  ", "  Y  ", "  Y  "],
        "Z": ["ZZZZZ", "   Z ", "  Z  ", " Z   ", "ZZZZZ"],
        "0": [" 00  ", "0  0 ", "0  0 ", "0  0 ", " 00  "],
        "1": ["  1  ", " 11  ", "  1  ", "  1  ", "11111"],
        "2": [" 22  ", "2  2 ", "  22 ", " 2   ", "22222"],
        "3": ["3333 ", "    3", " 333 ", "    3", "3333 "],
        "4": ["4  4 ", "4  4 ", "44444", "    4", "    4"],
        "5": ["55555", "5    ", "5555 ", "    5", "5555 "],
        "6": [" 66  ", "6    ", "6666 ", "6   6", " 66  "],
        "7": ["77777", "    7", "   7 ", "  7  ", "  7  "],
        "8": [" 88  ", "8   8", " 88  ", "8   8", " 88  "],
        "9": [" 99  ", "9   9", " 999 ", "    9", " 99  "],
        " ": ["     ", "     ", "     ", "     ", "     "],
        "!": ["  !  ", "  !  ", "  !  ", "     ", "  !  "],
        ".": ["     ", "     ", "     ", "     ", "  .  "],
        ",": ["     ", "     ", "     ", "  ,  ", "  ,  "],
        "?": [" ??  ", "?  ? ", "   ? ", "     ", "  ?  "],
    }

    text = text.upper()
    char_art: list[list[str]] = [[] for _ in range(5)]

    for ch in text:
        lines = font_map.get(ch, font_map[" "])
        for i, line in enumerate(lines):
            char_art[i].append(line)

    return "\n".join(" ".join(row) for row in char_art)
