"""
Colored console output and ANSI escape sequence utilities.

Provides functions for colored terminal output with support
for styles, RGB colors, and cross-platform compatibility.

Example:
    >>> from utils.color_console_utils import cprint, red, green, bold
    >>> cprint("Success!", color="green")
    >>> print(red("Error!"))
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Optional


class ANSI:
    """ANSI escape codes for terminal formatting."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    REVERSE = "\033[7m"
    HIDDEN = "\033[8m"
    STRIKETHROUGH = "\033[9m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"

    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"

    RESET_ALL = "\033[0m"
    BOLD_OFF = "\033[21m"
    ITALIC_OFF = "\033[23m"
    UNDERLINE_OFF = "\033[24m"


COLOR_NAMES = {
    "black": ANSI.BLACK,
    "red": ANSI.RED,
    "green": ANSI.GREEN,
    "yellow": ANSI.YELLOW,
    "blue": ANSI.BLUE,
    "magenta": ANSI.MAGENTA,
    "cyan": ANSI.CYAN,
    "white": ANSI.WHITE,
    "bright_black": ANSI.BRIGHT_BLACK,
    "bright_red": ANSI.BRIGHT_RED,
    "bright_green": ANSI.BRIGHT_GREEN,
    "bright_yellow": ANSI.BRIGHT_YELLOW,
    "bright_blue": ANSI.BRIGHT_BLUE,
    "bright_magenta": ANSI.BRIGHT_MAGENTA,
    "bright_cyan": ANSI.BRIGHT_CYAN,
    "bright_white": ANSI.BRIGHT_WHITE,
}

BG_COLOR_NAMES = {
    f"bg_{name}": getattr(ANSI, f"BG_{name.upper()}")
    for name in ["black", "red", "green", "yellow", "blue", "magenta", "cyan", "white"]
}

COLOR_NAMES.update(BG_COLOR_NAMES)

STYLE_NAMES = {
    "bold": ANSI.BOLD,
    "dim": ANSI.DIM,
    "italic": ANSI.ITALIC,
    "underline": ANSI.UNDERLINE,
    "blink": ANSI.BLINK,
    "reverse": ANSI.REVERSE,
    "strikethrough": ANSI.STRIKETHROUGH,
}


def strip_ansi(text: str) -> str:
    """
    Remove ANSI escape codes from text.

    Args:
        text: Text potentially containing ANSI codes.

    Returns:
        Plain text without ANSI codes.
    """
    import re
    ansi_pattern = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_pattern.sub("", text)


def supports_color() -> bool:
    """
    Check if the terminal supports color output.

    Returns:
        True if color output is supported.
    """
    if not hasattr(sys.stdout, "fileno"):
        return False

    if not sys.stdout.isatty():
        return False

    if os.environ.get("TERM") == "dumb":
        return False

    if os.environ.get("NO_COLOR"):
        return False

    return True


def get_color_code(color: str) -> str:
    """
    Get ANSI color code from color name or hex.

    Args:
        color: Color name or hex code (e.g., "#FF0000" or "red").

    Returns:
        ANSI color code.
    """
    if color in COLOR_NAMES:
        return COLOR_NAMES[color]

    if color.startswith("#") and len(color) == 7:
        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        return f"\033[38;2;{r};{g};{b}m"

    if color.startswith("#") and len(color) == 4:
        r = int(color[1] * 2, 16)
        g = int(color[2] * 2, 16)
        b = int(color[3] * 2, 16)
        return f"\033[38;2;{r};{g};{b}m"

    return ""


def get_bg_color_code(color: str) -> str:
    """Get ANSI background color code."""
    bg_key = f"bg_{color}"
    if bg_key in COLOR_NAMES:
        return COLOR_NAMES[bg_key]

    if color.startswith("#") and len(color) == 7:
        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        return f"\033[48;2;{r};{g};{b}m"

    return ""


def colorize(
    text: str,
    color: Optional[str] = None,
    bg_color: Optional[str] = None,
    style: Optional[str] = None,
) -> str:
    """
    Apply color and style to text.

    Args:
        text: Text to colorize.
        color: Foreground color name or hex.
        bg_color: Background color name or hex.
        style: Style name (bold, italic, etc.).

    Returns:
        Colorized text with ANSI codes.
    """
    if not supports_color():
        return text

    codes: list[str] = []

    if style and style in STYLE_NAMES:
        codes.append(STYLE_NAMES[style])

    if color:
        codes.append(get_color_code(color))

    if bg_color:
        codes.append(get_bg_color_code(bg_color))

    if not codes:
        return text

    return "".join(codes) + text + ANSI.RESET


def red(text: str) -> str:
    """Color text red."""
    return colorize(text, color="red")


def green(text: str) -> str:
    """Color text green."""
    return colorize(text, color="green")


def yellow(text: str) -> str:
    """Color text yellow."""
    return colorize(text, color="yellow")


def blue(text: str) -> str:
    """Color text blue."""
    return colorize(text, color="blue")


def magenta(text: str) -> str:
    """Color text magenta."""
    return colorize(text, color="magenta")


def cyan(text: str) -> str:
    """Color text cyan."""
    return colorize(text, color="cyan")


def white(text: str) -> str:
    """Color text white."""
    return colorize(text, color="white")


def bold(text: str) -> str:
    """Make text bold."""
    return colorize(text, style="bold")


def italic(text: str) -> str:
    """Make text italic."""
    return colorize(text, style="italic")


def underline(text: str) -> str:
    """Underline text."""
    return colorize(text, style="underline")


def cprint(
    *args: str,
    color: Optional[str] = None,
    bg_color: Optional[str] = None,
    style: Optional[str] = None,
    file=None,
    **kwargs,
) -> None:
    """
    Print with color and styling.

    Args:
        *args: Text to print.
        color: Foreground color.
        bg_color: Background color.
        style: Text style.
        file: Output file (default: stdout).
        **kwargs: Additional arguments for print().
    """
    separator = kwargs.pop("sep", " ")
    text = separator.join(str(arg) for arg in args)
    colored = colorize(text, color=color, bg_color=bg_color, style=style)
    print(colored, file=file, **kwargs)


class ProgressBar:
    """
    Simple colored progress bar for terminal.

    Attributes:
        total: Total progress value.
        current: Current progress value.
        width: Bar width in characters.
    """

    def __init__(
        self,
        total: float = 100,
        width: int = 40,
        color: str = "green",
        show_percent: bool = True,
        show_time: bool = False,
    ) -> None:
        """
        Initialize the progress bar.

        Args:
            total: Total progress value.
            width: Bar width in characters.
            color: Bar color.
            show_percent: Show percentage.
            show_time: Show elapsed time.
        """
        self.total = total
        self.width = width
        self.color = color
        self.show_percent = show_percent
        self.show_time = show_time
        self.current = 0.0
        self._start_time = __import__("time").time()

    def update(self, amount: float = 1) -> None:
        """Update progress by amount."""
        self.current = min(self.total, self.current + amount)
        self.render()

    def set(self, value: float) -> None:
        """Set absolute progress value."""
        self.current = min(self.total, max(0, value))
        self.render()

    def render(self) -> None:
        """Render the progress bar to stdout."""
        import time
        percent = self.current / self.total if self.total > 0 else 0
        filled = int(self.width * percent)
        empty = self.width - filled

        bar = colorize("=" * filled, color=self.color) + " " * empty

        parts = [f"[{bar}]"]

        if self.show_percent:
            parts.append(f"{int(percent * 100):3d}%")

        if self.show_time:
            elapsed = time.time() - self._start_time
            parts.append(f"{elapsed:.1f}s")

        print("\r" + " ".join(parts), end="", flush=True)

        if self.current >= self.total:
            print()


def color_table(
    headers: list[str],
    rows: list[list[str]],
    colors: Optional[list[str]] = None,
) -> None:
    """
    Print a colored table.

    Args:
        headers: Column headers.
        rows: Table rows.
        colors: Column color names (one per column).
    """
    colors = colors or ["cyan"] * len(headers)
    col_widths = [len(h) for h in headers]

    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    header_line = " | ".join(
        colorize(h.ljust(col_widths[i]), color=colors[i])
        for i, h in enumerate(headers)
    )
    print(header_line)
    print("-+-".join("-" * w for w in col_widths))

    for row in rows:
        row_line = " | ".join(
            str(cell).ljust(col_widths[i])
            for i, cell in enumerate(row)
        )
        print(row_line)
