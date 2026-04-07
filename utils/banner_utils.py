"""
Text banner generation utilities.

Provides:
- Stylish terminal banners
- Box banners with configurable borders
- Centered text banners
- Gradient-style text banners (ANSI colors)
- Tagline/star banners
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------

DEFAULT_WIDTH = 80
DEFAULT_FILL_CHAR = "="
DEFAULT_MARGIN = 1


# ------------------------------------------------------------------------------
# Banner Styles
# ------------------------------------------------------------------------------

class BannerStyle:
    """Banner style configuration."""

    def __init__(
        self,
        top_border: str = "=",
        bottom_border: str = "=",
        side_border: str = "|",
        corner_char: str = "+",
        fill_char: str = " ",
        margin: int = 1,
    ) -> None:
        self.top_border = top_border
        self.bottom_border = bottom_border
        self.side_border = side_border
        self.corner_char = corner_char
        self.fill_char = fill_char
        self.margin = margin


SINGLE = BannerStyle(
    top_border="=",
    bottom_border="=",
    side_border="|",
    corner_char="+",
)

DOUBLE = BannerStyle(
    top_border="=",
    bottom_border="=",
    side_border="║",
    corner_char="╔",
)

ROUNDED = BannerStyle(
    top_border="─",
    bottom_border="─",
    side_border="│",
    corner_char="╭",
)

DOTS = BannerStyle(
    top_border="·",
    bottom_border="·",
    side_border=" ",
    corner_char=" ",
    fill_char=" ",
)

STAR = BannerStyle(
    top_border="*",
    bottom_border="*",
    side_border="*",
    corner_char="*",
)

HASH = BannerStyle(
    top_border="#",
    bottom_border="#",
    side_border="#",
    corner_char="#",
)

MINIMAL = BannerStyle(
    top_border="-",
    bottom_border="-",
    side_border="",
    corner_char="",
    fill_char=" ",
)


# ------------------------------------------------------------------------------
# Core Banner Builder
# ------------------------------------------------------------------------------

class Banner:
    """
    Configurable text banner generator.

    Example:
        >>> banner = Banner("Hello World").width(60).style(SINGLE).margin(2)
        >>> print(banner)
        +------------------------------------------------------------+
        |                                                            |
        |                      Hello World                            |
        |                                                            |
        +------------------------------------------------------------+
    """

    def __init__(
        self,
        text: str = "",
        *,
        width: int = DEFAULT_WIDTH,
        style: BannerStyle | None = None,
        margin: int = DEFAULT_MARGIN,
        align: str = "center",
        fill_char: str = " ",
    ) -> None:
        self._text = text
        self._width = width
        self._style = style or SINGLE
        self._margin = margin
        self._align = align
        self._fill_char = fill_char
        self._lines: list[str] = []
        self._subtitle: str | None = None

    def text(self, text: str) -> "Banner":
        """Set the main banner text."""
        self._text = text
        return self

    def width(self, width: int) -> "Banner":
        """Set banner width."""
        self._width = max(4, width)
        return self

    def style(self, style: BannerStyle) -> "Banner":
        """Set banner style."""
        self._style = style
        return self

    def margin(self, margin: int) -> "Banner":
        """Set vertical margin (blank lines between border and text)."""
        self._margin = max(0, margin)
        return self

    def align(self, align: str) -> "Banner":
        """Set text alignment ('left', 'center', 'right')."""
        self._align = align
        return self

    def fill(self, char: str) -> "Banner":
        """Set fill character for alignment padding."""
        self._fill_char = char
        return self

    def subtitle(self, subtitle: str) -> "Banner":
        """Set a subtitle line below the main text."""
        self._subtitle = subtitle
        return self

    def lines(self, lines: list[str]) -> "Banner":
        """Set multiple lines of text."""
        self._lines = list(lines)
        return self

    def _build_top_bottom_border(self) -> str:
        """Build the top or bottom border."""
        s = self._style
        if not s.corner_char:
            return s.top_border * self._width
        return s.corner_char + s.top_border * (self._width - 2) + s.corner_char

    def _pad_line(self, line: str) -> str:
        """Pad/align a single line within the banner width."""
        s = self._style
        inner_width = self._width - 2 - 2 * self._margin if s.side_border else self._width - 2 * self._margin
        if len(line) >= inner_width:
            return line[:inner_width]

        padding = inner_width - len(line)
        if self._align == "left":
            padded = line + self._fill_char * padding
        elif self._align == "right":
            padded = self._fill_char * padding + line
        else:  # center
            left = padding // 2
            right = padding - left
            padded = self._fill_char * left + line + self._fill_char * right

        return padded

    def _wrap_text(self) -> list[str]:
        """Wrap text to fit banner width."""
        s = self._style
        inner_width = self._width - 2 - 2 * self._margin if s.side_border else self._width - 2 * self._margin
        inner_width = max(1, inner_width)
        wrapped = textwrap.wrap(self._text, width=inner_width)
        return wrapped if wrapped else [""]

    def __str__(self) -> str:
        """Render the banner."""
        s = self._style
        lines_out: list[str] = []

        # Top border
        lines_out.append(self._build_top_bottom_border())

        # Vertical margins
        for _ in range(self._margin):
            if s.side_border:
                lines_out.append(s.side_border + " " * (self._width - 2) + s.side_border)
            else:
                lines_out.append(" " * self._width)

        # Content lines
        content_lines: list[str] = list(self._lines)
        if self._text and not self._lines:
            content_lines = self._wrap_text()

        for line in content_lines:
            padded = self._pad_line(line)
            if s.side_border:
                lines_out.append(f"{s.side_border} {padded} {s.side_border}")
            else:
                lines_out.append(padded)

        # Subtitle
        if self._subtitle:
            padded = self._pad_line(self._subtitle)
            if s.side_border:
                lines_out.append(f"{s.side_border} {padded} {s.side_border}")

        # Bottom margins
        for _ in range(self._margin):
            if s.side_border:
                lines_out.append(s.side_border + " " * (self._width - 2) + s.side_border)
            else:
                lines_out.append(" " * self._width)

        # Bottom border
        lines_out.append(self._build_top_bottom_border())

        return "\n".join(lines_out)


# ------------------------------------------------------------------------------
# Convenience Functions
# ------------------------------------------------------------------------------

def banner(
    text: str,
    width: int = DEFAULT_WIDTH,
    style: BannerStyle | None = None,
    margin: int = DEFAULT_MARGIN,
    align: str = "center",
) -> str:
    """
    Create a simple text banner.

    Args:
        text: Banner text.
        width: Banner width in characters.
        style: Banner style (default: SINGLE).
        margin: Vertical margin lines.
        align: Text alignment ('left', 'center', 'right').

    Returns:
        Rendered banner string.

    Example:
        >>> print(banner("Welcome", width=50, style=DOUBLE))
        ╔══════════════════════════════════════════════╗
        ║                                              ║
        ║                   Welcome                    ║
        ║                                              ║
        ╚══════════════════════════════════════════════╝
    """
    return str(Banner(text, width=width, style=style, margin=margin, align=align))


def box_banner(
    text: str,
    width: int | None = None,
    style: BannerStyle | None = None,
) -> str:
    """
    Create a compact box banner.

    Automatically sizes to fit text with 2-space padding.

    Args:
        text: Banner text.
        width: Width (auto if None).
        style: Banner style.

    Returns:
        Boxed banner string.

    Example:
        >>> print(box_banner("ALERT"))
        +-----+
        |ALERT|
        +-----+
    """
    if width is None:
        width = len(text) + 4
    return str(Banner(text, width=width, style=style or SINGLE, margin=0))


def center_banner(text: str, width: int = DEFAULT_WIDTH, char: str = "=") -> str:
    """
    Create a centered text banner with line above and below.

    Args:
        text: Text to center.
        width: Total banner width.
        char: Border character.

    Returns:
        Centered banner string.

    Example:
        >>> print(center_banner("TITLE", width=50))
        ==================== TITLE ====================
    """
    if len(text) + 4 >= width:
        width = len(text) + 4
    half = (width - len(text) - 2) // 2
    extra = (width - len(text) - 2) % 2
    return f"{char * half} {text} {char * (half + extra)}"


def multiline_banner(
    lines: list[str],
    width: int = DEFAULT_WIDTH,
    style: BannerStyle | None = None,
    margin: int = 1,
) -> str:
    """
    Create a banner with multiple lines.

    Args:
        lines: List of text lines.
        width: Banner width.
        style: Banner style.
        margin: Vertical margin.

    Returns:
        Rendered banner string.
    """
    return str(Banner().width(width).style(style or SINGLE).margin(margin).lines(lines))


def subtitle_banner(
    title: str,
    subtitle: str,
    width: int = DEFAULT_WIDTH,
    style: BannerStyle | None = None,
) -> str:
    """
    Create a banner with a subtitle.

    Args:
        title: Main title.
        subtitle: Subtitle text.
        width: Banner width.
        style: Banner style.

    Returns:
        Banner string with subtitle.
    """
    return str(Banner(title, width=width, style=style or SINGLE).subtitle(subtitle))


def mini_banner(text: str, style: BannerStyle | None = None) -> str:
    """
    Create a minimal single-line banner.

    Args:
        text: Banner text.
        style: Banner style.

    Returns:
        Compact single-line banner.

    Example:
        >>> print(mini_banner("NEW", style=STAR))
        *****
        *NEW*
        *****
    """
    s = style or STAR
    width = len(text) + 4
    return f"{s.corner_char}{s.top_border * (width - 2)}{s.corner_char}\n{s.side_border}{text:^{width - 2}}{s.side_border}\n{s.corner_char}{s.bottom_border * (width - 2)}{s.corner_char}"


def separator(
    text: str = "",
    width: int = DEFAULT_WIDTH,
    char: str = "-",
) -> str:
    """
    Create a separator line with optional centered text.

    Args:
        text: Optional text to center in separator.
        width: Line width.
        char: Separator character.

    Returns:
        Separator string.

    Example:
        >>> print(separator("Section 1"))
        -------------------- Section 1 --------------------
    """
    if not text:
        return char * width
    padding = max(0, (width - len(text) - 2) // 2)
    return f"{char * padding} {text} {char * (width - padding - len(text) - 2)}"


# ------------------------------------------------------------------------------
# ANSI Color Banner Helpers
# ------------------------------------------------------------------------------

def color_banner(
    text: str,
    width: int = DEFAULT_WIDTH,
    *,
    fg: int = 37,
    bg: int = 44,
) -> str:
    """
    Create an ANSI-colored banner.

    Args:
        text: Banner text.
        width: Banner width.
        fg: Foreground color code (default white=37).
        bg: Background color code (default cyan=44).

    Returns:
        ANSI-colored banner string.
    """
    inner_width = width - 4
    padded = text.center(inner_width)
    line = f"\033[{fg};{bg}m{padded}\033[0m"
    top = f"\033[{bg}m{' ' * width}\033[0m"
    border = f"\033[{bg}m {' ' * inner_width} \033[0m"
    lines = [top, border, f"\033[{bg}m {padded} \033[0m", border, top]
    return "\n".join(lines)


def gradient_banner(text: str, width: int = DEFAULT_WIDTH) -> str:
    """
    Create a banner with color gradient across text.

    Args:
        text: Banner text.
        width: Banner width.

    Returns:
        Color-gradient banner string.

    Note:
        Colors cycle through ANSI 256-color palette.
    """
    inner_width = width - 4
    wrapped = textwrap.wrap(text, width=inner_width)
    if not wrapped:
        wrapped = [""]

    lines: list[str] = []
    color_codes = [196, 202, 208, 214, 220, 226, 190, 154, 118, 82, 46]

    for line_idx, line in enumerate(wrapped):
        colored_line = ""
        for i, char in enumerate(line):
            color_idx = (line_idx + i) % len(color_codes)
            colored_line += f"\033[38;5;{color_codes[color_idx]}m{char}\033[0m"
        lines.append(f"  {colored_line:<{inner_width}}  ")

    return "\n".join(lines)
