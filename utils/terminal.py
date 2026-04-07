"""Terminal/CLI utilities for RabAI AutoClick.

Provides:
- ANSI color codes
- Progress bars
- Table formatting
- Input helpers
"""

import shutil
import sys
from typing import Any, Callable, List, Optional, Tuple


# ANSI color codes
class Colors:
    """ANSI color codes."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

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


def colorize(text: str, *codes: str) -> str:
    """Colorize text with ANSI codes.

    Args:
        text: Text to colorize.
        *codes: ANSI codes to apply.

    Returns:
        Colorized text.
    """
    return "".join(codes) + text + Colors.RESET


def red(text: str) -> str:
    """Red text."""
    return colorize(text, Colors.RED)


def green(text: str) -> str:
    """Green text."""
    return colorize(text, Colors.GREEN)


def yellow(text: str) -> str:
    """Yellow text."""
    return colorize(text, Colors.YELLOW)


def blue(text: str) -> str:
    """Blue text."""
    return colorize(text, Colors.BLUE)


def cyan(text: str) -> str:
    """Cyan text."""
    return colorize(text, Colors.CYAN)


def bold(text: str) -> str:
    """Bold text."""
    return colorize(text, Colors.BOLD)


def clear_line() -> str:
    """Get ANSI code to clear current line."""
    return "\033[2K\r"


def hide_cursor() -> str:
    """Get ANSI code to hide cursor."""
    return "\033[?25l"


def show_cursor() -> str:
    """Get ANSI code to show cursor."""
    return "\033[?25h"


def get_terminal_size() -> Tuple[int, int]:
    """Get terminal size.

    Returns:
        Tuple of (columns, lines).
    """
    size = shutil.get_terminal_size(fallback=(80, 24))
    return size.columns, size.lines


class ProgressBar:
    """ASCII progress bar.

    Usage:
        bar = ProgressBar(total=100, width=50)
        for i in range(100):
            bar.update(i + 1)
            time.sleep(0.01)
        bar.finish()
    """

    def __init__(
        self,
        total: int = 100,
        width: Optional[int] = None,
        prefix: str = "",
        suffix: str = "",
        fill_char: str = "=",
        empty_char: str = " ",
        show_percent: bool = True,
        show_count: bool = True,
    ) -> None:
        """Initialize progress bar.

        Args:
            total: Total items.
            width: Bar width in characters.
            prefix: Text before bar.
            suffix: Text after bar.
            fill_char: Character for filled portion.
            empty_char: Character for empty portion.
            show_percent: Show percentage.
            show_count: Show count (current/total).
        """
        self.total = total
        self.current = 0
        self.width = width or (get_terminal_size()[0] - 40)
        self.prefix = prefix
        self.suffix = suffix
        self.fill_char = fill_char
        self.empty_char = empty_char
        self.show_percent = show_percent
        self.show_count = show_count

        if self.width < 10:
            self.width = 10

    def update(self, current: Optional[int] = None) -> None:
        """Update progress bar.

        Args:
            current: Current progress value.
        """
        if current is not None:
            self.current = current

        percent = 0
        if self.total > 0:
            percent = self.current / self.total

        filled = int(self.width * percent)
        empty = self.width - filled

        bar = self.fill_char * filled + self.empty_char * empty

        parts = []
        if self.prefix:
            parts.append(self.prefix)
        parts.append(f"[{bar}]")

        if self.show_count:
            parts.append(f"{self.current}/{self.total}")

        if self.show_percent:
            parts.append(f"{percent * 100:.1f}%")

        if self.suffix:
            parts.append(self.suffix)

        line = " ".join(parts)
        if sys.stdout.isatty():
            sys.stdout.write("\r" + line)
            sys.stdout.flush()
        else:
            sys.stdout.write(line + "\n")

    def finish(self) -> None:
        """Finish progress bar."""
        self.current = self.total
        self.update()
        if sys.stdout.isatty():
            sys.stdout.write("\n")
        sys.stdout.flush()

    def __enter__(self) -> 'ProgressBar':
        """Enter context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context manager."""
        self.finish()


class Table:
    """ASCII table formatter.

    Usage:
        table = Table(["Name", "Age", "City"])
        table.add_row(["Alice", "30", "NYC"])
        table.add_row(["Bob", "25", "LA"])
        print(table)
    """

    def __init__(
        self,
        headers: List[str],
        align: Optional[List[str]] = None,
    ) -> None:
        """Initialize table.

        Args:
            headers: Column headers.
            align: Alignment per column ("l", "c", "r").
        """
        self.headers = headers
        self.rows: List[List[str]] = []
        self.align = align or ["l"] * len(headers)
        self._widths: List[int] = [len(h) for h in headers]

    def add_row(self, row: List[str]) -> None:
        """Add a row to the table.

        Args:
            row: Row data.
        """
        if len(row) != len(self.headers):
            raise ValueError("Row length must match header length")

        self.rows.append([str(cell) for cell in row])

        # Update widths
        for i, cell in enumerate(row):
            self._widths[i] = max(self._widths[i], len(str(cell)))

    def _format_cell(self, cell: str, width: int, align: str) -> str:
        """Format a cell."""
        cell = str(cell)
        padding = width - len(cell)

        if align == "r":
            return " " * padding + cell
        elif align == "c":
            left = padding // 2
            right = padding - left
            return " " * left + cell + " " * right
        else:
            return cell + " " * padding

    def __str__(self) -> str:
        """Get table as string."""
        lines = []

        # Header
        header_cells = [
            self._format_cell(h, w, a)
            for h, w, a in zip(self.headers, self._widths, self.align)
        ]
        lines.append(" | ".join(header_cells))

        # Separator
        sep_cells = ["-" * w for w in self._widths]
        lines.append("-+-".join(sep_cells))

        # Rows
        for row in self.rows:
            row_cells = [
                self._format_cell(cell, w, a)
                for cell, w, a in zip(row, self._widths, self.align)
            ]
            lines.append(" | ".join(row_cells))

        return "\n".join(lines)


def confirm(
    prompt: str = "Continue?",
    default: Optional[bool] = None,
) -> bool:
    """Ask for confirmation.

    Args:
        prompt: Prompt text.
        default: Default value (True/False/None for no default).

    Returns:
        True if confirmed.
    """
    choices = []
    if default is True:
        choices.append("Y/n")
    elif default is False:
        choices.append("y/N")
    else:
        choices.append("y/n")

    while True:
        response = input(f"{prompt} [{'/'.join(choices)}]: ").strip().lower()

        if not response and default is not None:
            return default

        if response in ("y", "yes"):
            return True
        if response in ("n", "no"):
            return False

        print("Please enter 'y' or 'n'")


def select(
    options: List[str],
    prompt: str = "Select option",
    default: Optional[int] = None,
) -> int:
    """Select from options.

    Args:
        options: List of option strings.
        prompt: Prompt text.
        default: Default index.

    Returns:
        Selected index.
    """
    if not options:
        raise ValueError("Options list cannot be empty")

    for i, option in enumerate(options):
        marker = " " if default != i else "*"
        print(f"  {marker} [{i + 1}] {option}")

    while True:
        try:
            response = input(f"{prompt} [{default + 1 if default is not None else ''}]: ").strip()

            if not response and default is not None:
                return default

            idx = int(response) - 1
            if 0 <= idx < len(options):
                return idx

            print(f"Please enter a number between 1 and {len(options)}")
        except ValueError:
            print("Please enter a number")


def print_box(text: str, width: Optional[int] = None) -> None:
    """Print text in a box.

    Args:
        text: Text to print.
        width: Box width.
    """
    if width is None:
        width = min(len(text) + 4, get_terminal_size()[0] - 4)

    print("+" + "-" * (width - 2) + "+")
    print("|" + text.center(width - 2) + "|")
    print("+" + "-" * (width - 2) + "+")