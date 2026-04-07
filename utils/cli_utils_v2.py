"""CLI utilities v2 for RabAI AutoClick.

Provides:
- Advanced CLI argument parsing
- Progress bar display
- Interactive prompts
- Table formatting for CLI
"""

import sys
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
)


class ProgressBar:
    """Simple text-based progress bar."""

    def __init__(
        self,
        total: int,
        width: int = 40,
        prefix: str = "",
        suffix: str = "",
        show_percent: bool = True,
    ) -> None:
        self._total = max(1, total)
        self._width = width
        self._prefix = prefix
        self._suffix = suffix
        self._show_percent = show_percent
        self._current = 0

    def update(self, current: int) -> None:
        """Update progress bar.

        Args:
            current: Current progress value.
        """
        self._current = min(current, self._total)
        self._render()

    def increment(self, delta: int = 1) -> None:
        """Increment progress.

        Args:
            delta: Amount to increment.
        """
        self.update(self._current + delta)

    def _render(self) -> None:
        """Render the progress bar."""
        filled = int(self._width * self._current / self._total)
        bar = "=" * filled + "-" * (self._width - filled)

        percent = self._current * 100 // self._total
        suffix = self._suffix or f"{percent}%"

        line = f"\r{self._prefix}[{bar}] {suffix}"
        sys.stdout.write(line)
        sys.stdout.flush()

        if self._current >= self._total:
            sys.stdout.write("\n")
            sys.stdout.flush()

    def finish(self) -> None:
        """Finish and clear the progress bar."""
        self.update(self._total)


def ask(
    question: str,
    default: Optional[str] = None,
) -> str:
    """Ask a question and return the answer.

    Args:
        question: Question to ask.
        default: Default value if user presses enter.

    Returns:
        User's answer.
    """
    if default:
        prompt = f"{question} [{default}]: "
    else:
        prompt = f"{question}: "

    while True:
        try:
            answer = input(prompt).strip()
            if answer:
                return answer
            if default is not None:
                return default
        except (KeyboardInterrupt, EOFError):
            return ""


def ask_yes_no(
    question: str,
    default: Optional[bool] = None,
) -> bool:
    """Ask a yes/no question.

    Args:
        question: Question to ask.
        default: Default answer (True/False/None).

    Returns:
        True for yes, False for no.
    """
    choices = "Y/n"
    if default is False:
        choices = "y/N"
    elif default is True:
        choices = "Y/n"
    else:
        choices = "y/n"

    while True:
        answer = input(f"{question} ({choices}): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        if not answer and default is not None:
            return default


def ask_choice(
    question: str,
    choices: Sequence[str],
    default: Optional[int] = None,
) -> str:
    """Ask user to choose from a list.

    Args:
        question: Question to ask.
        choices: List of choices.
        default: Default choice index.

    Returns:
        Selected choice.
    """
    print(question)
    for i, choice in enumerate(choices):
        marker = " " if default != i else "*"
        print(f"  [{i + 1}] {marker} {choice}")

    while True:
        try:
            answer = input("Choice: ").strip()
            if not answer and default is not None:
                return choices[default]
            idx = int(answer) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except (ValueError, KeyboardInterrupt, EOFError):
            pass


def confirm(
    question: str,
    default: Optional[bool] = None,
) -> bool:
    """Ask for confirmation.

    Args:
        question: Confirmation question.
        default: Default (True/False).

    Returns:
        True if confirmed.
    """
    return ask_yes_no(question + "?", default)


def print_table(
    headers: List[str],
    rows: List[List[Any]],
    *,
    indent: int = 0,
) -> None:
    """Print a formatted table to stdout.

    Args:
        headers: Column headers.
        rows: Row data.
        indent: Left indentation.
    """
    if not rows:
        return

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            cell_str = str(cell)
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(cell_str))
            else:
                col_widths.append(len(cell_str))

    def format_row(cells: List[Any]) -> str:
        parts = []
        for i, cell in enumerate(cells):
            cell_str = str(cell)
            width = col_widths[i] if i < len(col_widths) else len(cell_str)
            parts.append(cell_str.ljust(width))
        return "  ".join(parts)

    print(" " * indent + format_row(headers))
    print(" " * indent + "  ".join("-" * w for w in col_widths))
    for row in rows:
        print(" " * indent + format_row(row))


def print_error(message: str) -> None:
    """Print an error message to stderr.

    Args:
        message: Error message.
    """
    print(f"ERROR: {message}", file=sys.stderr)


def print_warning(message: str) -> None:
    """Print a warning message.

    Args:
        message: Warning message.
    """
    print(f"WARNING: {message}", file=sys.stderr)


def print_success(message: str) -> None:
    """Print a success message.

    Args:
        message: Success message.
    """
    print(f"SUCCESS: {message}")


def print_info(message: str) -> None:
    """Print an info message.

    Args:
        message: Info message.
    """
    print(f"INFO: {message}")


def print_debug(message: str) -> None:
    """Print a debug message (if DEBUG env var is set).

    Args:
        message: Debug message.
    """
    import os
    if os.environ.get("DEBUG"):
        print(f"DEBUG: {message}")


def color_text(text: str, color: str) -> str:
    """Colorize text for terminal.

    Args:
        text: Text to colorize.
        color: Color name (red, green, yellow, blue, magenta, cyan).

    Returns:
        Colored text string.
    """
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "magenta": "\033[95m",
        "cyan": "\033[96m",
    }
    reset = "\033[0m"
    color_code = colors.get(color, "")
    if not color_code:
        return text
    return f"{color_code}{text}{reset}"


def spinner(
    message: str,
    delay: float = 0.1,
) -> Callable[[], None]:
    """Create a spinning animation.

    Args:
        message: Message to display.
        delay: Delay between spins.

    Returns:
        Stop function to call when done.
    """
    import threading
    chars = "|/-\\"
    idx = [0]
    running = [True]

    def spin() -> None:
        while running[0]:
            char = chars[idx[0] % len(chars)]
            print(f"\r{char} {message}", end="", flush=True)
            idx[0] += 1
            time.sleep(delay)

    thread = threading.Thread(target=spin, daemon=True)
    thread.start()

    def stop() -> None:
        running[0] = False
        thread.join()
        print(f"\r{' ' * (len(message) + 2)}\r", end="", flush=True)

    return stop


def create_progress_callback(total: int) -> Callable[[int], None]:
    """Create a simple progress callback.

    Args:
        total: Total items.

    Returns:
        Callback function that updates progress.
    """
    bar = ProgressBar(total)
    return bar.increment


def log_step(step: str, message: str) -> None:
    """Log a step with formatted output.

    Args:
        step: Step name/number.
        message: Step message.
    """
    print(f"[{step}] {message}")


def log_section(title: str) -> None:
    """Log a section header.

    Args:
        title: Section title.
    """
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print(f"{'=' * 60}\n")
