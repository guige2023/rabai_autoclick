"""
Progress bar and spinner utilities for terminal output.

Provides:
- Configurable progress bars (determinate and indeterminate)
- Spinner animations
- Multi-progress tracking
- Progress context managers
- ETA calculations
"""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable

T = TypeVar("T")


# ------------------------------------------------------------------------------
# Progress Bar
# ------------------------------------------------------------------------------

class ProgressBar:
    """
    Configurable terminal progress bar.

    Supports determinate (percentage-based) and indeterminate (looping) modes.

    Example:
        >>> p = ProgressBar(total=100, width=40, desc="Loading")
        >>> for i in range(0, 101, 10):
        ...     p.update(i)
        ...     time.sleep(0.05)
        >>> p.finish()
    """

    DEFAULT_TEMPLATE = "{desc}: {bar} {percentage:>3.0f}% {counter}"
    NO_PERCENTAGE_TEMPLATE = "{desc}: {bar} {counter}"

    def __init__(
        self,
        total: float = 100,
        *,
        width: int = 40,
        desc: str = "Progress",
        template: str | None = None,
        bar_format: str = "{fill}{empty} {percentage:>3.0f}%",
        fill_char: str = "█",
        empty_char: str = "░",
        show_counter: bool = True,
        show_eta: bool = True,
        show_rate: bool = False,
        file: file = None,
    ) -> None:
        """
        Initialize a progress bar.

        Args:
            total: Total value (100 for percentage-based, or item count).
            width: Bar width in characters.
            desc: Description label.
            template: Full display template string.
            bar_format: Bar segment format string.
            fill_char: Filled portion character.
            empty_char: Empty portion character.
            show_counter: Show current/total counter.
            show_eta: Show estimated time remaining.
            show_rate: Show items per second.
            file: Output file (default: stdout).
        """
        self._total = float(total)
        self._current = 0.0
        self._width = width
        self._desc = desc
        self._template = template or self.DEFAULT_TEMPLATE
        self._bar_format = bar_format
        self._fill_char = fill_char
        self._empty_char = empty_char
        self._show_counter = show_counter
        self._show_eta = show_eta
        self._show_rate = show_rate
        self._file = file or sys.stdout
        self._start_time = time.time()
        self._last_update = self._start_time
        self._closed = False

    @property
    def current(self) -> float:
        """Current progress value."""
        return self._current

    @property
    def percentage(self) -> float:
        """Current percentage (0-100)."""
        if self._total <= 0:
            return 0.0
        return min(100.0, max(0.0, self._current / self._total * 100))

    def update(self, current: float | None = None, advance: float = 0) -> None:
        """
        Update progress bar.

        Args:
            current: Absolute value to set as current.
            advance: Amount to advance from current position.
        """
        if self._closed:
            return
        if current is not None:
            self._current = float(current)
        else:
            self._current += float(advance)
        self._current = min(self._current, self._total)
        self._render()

    def set_description(self, desc: str) -> None:
        """Update the description label."""
        self._desc = desc

    def set_postfix(self, **kwargs: str) -> None:
        """Set postfix key-value pairs to display."""
        self._postfix = kwargs

    def _elapsed(self) -> float:
        """Get elapsed time in seconds."""
        return time.time() - self._start_time

    def _eta(self) -> float | None:
        """Calculate estimated seconds remaining."""
        if self._current <= 0:
            return None
        elapsed = self._elapsed()
        rate = self._current / elapsed
        if rate <= 0:
            return None
        remaining = self._total - self._current
        return remaining / rate

    def _format_time(self, seconds: float | None) -> str:
        """Format seconds as HH:MM:SS or MM:SS."""
        if seconds is None:
            return "--:--"
        seconds = int(seconds)
        if seconds >= 3600:
            return f"{seconds // 3600}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"
        return f"{seconds // 60}:{seconds % 60:02d}"

    def _render(self) -> None:
        """Render the progress bar to the output."""
        pct = self.percentage
        filled_len = int(self._width * pct / 100)
        empty_len = self._width - filled_len

        bar = self._fill_char * filled_len + self._empty_char * empty_len

        # Counter
        counter = ""
        if self._show_counter:
            cur = int(self._current)
            tot = int(self._total)
            counter = f"{cur}/{tot}"

        # ETA
        eta_str = ""
        if self._show_eta:
            eta = self._eta()
            eta_str = f" ETA {self._format_time(eta)}"

        # Rate
        rate_str = ""
        if self._show_rate and self._current > 0:
            elapsed = self._elapsed()
            rate = self._current / elapsed
            rate_str = f" {rate:.1f}/s"

        # Build output
        output = f"\r{self._desc}: {bar} {pct:>3.0f}%{counter}{eta_str}{rate_str}"

        self._file.write(output)
        self._file.flush()
        self._last_update = time.time()

    def finish(self, message: str = "Done!") -> None:
        """
        Complete the progress bar.

        Args:
            message: Final message to display.
        """
        if self._closed:
            return
        self._current = self._total
        elapsed = self._elapsed()
        self._file.write("\r" + " " * (self._width + 80) + "\r")
        self._file.write(f"{self._desc}: {self._fill_char * self._width} 100%  ({self._format_time(elapsed)}) {message}\n")
        self._file.flush()
        self._closed = True

    def __enter__(self) -> "ProgressBar":
        return self

    def __exit__(self, *args: object) -> None:
        if not self._closed:
            self.finish()


class IndeterminateProgressBar:
    """
    Indeterminate (looping) progress bar for unknown-duration tasks.

    Example:
        >>> p = IndeterminateProgressBar(desc="Processing")
        >>> for _ in range(50):
        ...     p.spin()
        ...     time.sleep(0.05)
        >>> p.finish()
    """

    SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    BLOCK_FRAMES = ["[    ]", "[=   ]", "[==  ]", "[ ===]", "[====]", "[ ===]", "[  ==]", "[   =]"]

    def __init__(
        self,
        desc: str = "Working",
        width: int = 30,
        frame_type: str = "spinner",
        file: file = None,
    ) -> None:
        """
        Initialize indeterminate progress bar.

        Args:
            desc: Description label.
            width: Bar width (spinner mode ignores this).
            frame_type: 'spinner', 'block', or 'bar'.
            file: Output file.
        """
        self._desc = desc
        self._width = width
        self._frame_type = frame_type
        self._file = file or sys.stdout
        self._frame = 0
        self._closed = False
        self._start_time = time.time()

        frames_map = {
            "spinner": self.SPINNER_FRAMES,
            "block": self.BLOCK_FRAMES,
            "bar": ["-", "\\", "|", "/"],
        }
        self._frames = frames_map.get(frame_type, self.SPINNER_FRAMES)

    def spin(self, steps: int = 1) -> None:
        """Advance the spinner by steps."""
        if self._closed:
            return
        self._frame = (self._frame + steps) % len(self._frames)
        self._render()

    def _render(self) -> None:
        """Render current frame."""
        frame = self._frames[self._frame]
        elapsed = time.time() - self._start_time
        output = f"\r{self._desc} {frame}  ({elapsed:.1f}s)"
        self._file.write(output)
        self._file.flush()

    def finish(self, message: str = "Done!") -> None:
        """Complete the spinner."""
        if self._closed:
            return
        elapsed = time.time() - self._start_time
        self._file.write("\r" + " " * 60 + "\r")
        self._file.write(f"{self._desc} {self._frames[-1]} ({elapsed:.1f}s) {message}\n")
        self._file.flush()
        self._closed = True

    def __enter__(self) -> "IndeterminateProgressBar":
        return self

    def __exit__(self, *args: object) -> None:
        if not self._closed:
            self.finish()


# ------------------------------------------------------------------------------
# Multi-Progress
# ------------------------------------------------------------------------------

class MultiProgress:
    """
    Track multiple progress bars simultaneously.

    Renders bars at different positions on screen.

    Example:
        >>> mp = MultiProgress()
        >>> p1 = mp.add(ProgressBar(total=100, desc="Task 1"))
        >>> p2 = mp.add(ProgressBar(total=50, desc="Task 2"))
        >>> for i in range(101):
        ...     p1.update(i)
        ...     if i % 2 == 0:
        ...         p2.update(i // 2)
        ...     mp.render()
        ...     time.sleep(0.02)
    """

    def __init__(self, file: file = None) -> None:
        self._bars: list[ProgressBar] = []
        self._file = file or sys.stdout

    def add(self, bar: ProgressBar) -> ProgressBar:
        """Add a progress bar."""
        self._bars.append(bar)
        return bar

    def render(self) -> None:
        """Render all bars in their current state."""
        self._file.write("\033[J")  # Clear from cursor to end of screen
        for i, bar in enumerate(self._bars):
            offset = i * 2
            self._file.write(f"\033[{offset};0H")  # Move cursor to line i*2
            bar._file = self._file
            bar._render()
        self._file.flush()

    def update_all(self) -> None:
        """Trigger render for all bars."""
        self.render()


# ------------------------------------------------------------------------------
# Progress Context Manager
# ------------------------------------------------------------------------------

def progress_context(
    iterable: Iterable[T],
    *,
    total: int | None = None,
    desc: str = "Progress",
    width: int = 40,
    show_rate: bool = True,
) -> Iterable[tuple[int, T]]:
    """
    Wrap an iterable with a progress bar.

    Args:
        iterable: Items to iterate over.
        total: Total count (auto-detected if None).
        desc: Description label.
        width: Bar width.
        show_rate: Show items/second.

    Yields:
        Tuples of (index, item).

    Example:
        >>> for i, item in progress_context(range(10), desc="Processing"):
        ...     time.sleep(0.1)
    """
    items = list(iterable)
    total_count = total if total is not None else len(items)
    with ProgressBar(total=total_count, width=width, desc=desc, show_rate=show_rate) as p:
        for idx, item in enumerate(items):
            yield idx, item
            p.update(idx + 1)


# ------------------------------------------------------------------------------
# Progress Callback
# ------------------------------------------------------------------------------

class ProgressCallback(Generic[T]):
    """
    Callback-based progress reporter.

    Useful for wrapping non-iterable operations.

    Example:
        >>> cb = ProgressCallback(total=100, desc="Downloading")
        >>> for i in range(101):
        ...     cb.report(i)
        ...     time.sleep(0.05)
        >>> cb.finish()
    """

    def __init__(
        self,
        total: float = 100,
        *,
        desc: str = "Progress",
        width: int = 40,
        on_update: Callable[[float], None] | None = None,
    ) -> None:
        self._bar = ProgressBar(total=total, width=width, desc=desc)
        self._on_update = on_update

    def report(self, current: float) -> None:
        """Report current progress."""
        self._bar.update(current)
        if self._on_update:
            self._on_update(current)

    def advance(self, amount: float = 1) -> None:
        """Advance by amount."""
        self._bar.update(advance=amount)
        if self._on_update:
            self._on_update(self._bar.current)

    def set_description(self, desc: str) -> None:
        """Update description."""
        self._bar.set_description(desc)

    def finish(self, message: str = "Done!") -> None:
        """Complete progress."""
        self._bar.finish(message)


# ------------------------------------------------------------------------------
# ETA Utilities
# ------------------------------------------------------------------------------

def calculate_eta(
    current: float,
    total: float,
    elapsed_seconds: float,
) -> float | None:
    """
    Calculate estimated seconds remaining.

    Args:
        current: Current progress value.
        total: Total value.
        elapsed_seconds: Seconds elapsed so far.

    Returns:
        Estimated seconds remaining, or None if cannot calculate.
    """
    if current <= 0 or elapsed_seconds <= 0:
        return None
    rate = current / elapsed_seconds
    if rate <= 0:
        return None
    remaining = total - current
    return remaining / rate


def format_eta(seconds: float | None) -> str:
    """
    Format ETA seconds as human-readable string.

    Args:
        seconds: Seconds remaining.

    Returns:
        Formatted string like "2m 30s" or "--:--" if None.
    """
    if seconds is None:
        return "--:--"
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h {minutes}m"


def progress_rate(
    current: float,
    elapsed_seconds: float,
) -> float:
    """
    Calculate items/second rate.

    Args:
        current: Current progress value.
        elapsed_seconds: Seconds elapsed.

    Returns:
        Items per second (0 if elapsed is 0).
    """
    if elapsed_seconds <= 0:
        return 0.0
    return current / elapsed_seconds
