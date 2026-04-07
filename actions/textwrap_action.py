"""textwrap action extensions for rabai_autoclick.

Provides text wrapping, indentation, and formatting utilities
for working with text content.
"""

from __future__ import annotations

import textwrap
import re
from typing import Callable

__all__ = [
    "wrap",
    "fill",
    "dedent",
    "indent",
    " shorten",
    "wrap_text",
    "align_left",
    "align_center",
    "align_right",
    "center_text",
    "left_justify",
    "right_justify",
    "wrap_paragraphs",
    "indent_lines",
    "unindent",
    "strip_indent",
    "normalize_whitespace",
    "collapse_whitespace",
    "word_wrap",
    "character_wrap",
    "prefix_lines",
    "suffix_lines",
    "remove_prefix",
    "remove_suffix",
    "wrap_string",
    "TextWrapper",
    "indent_width",
    "prefix_width",
    "ColumnFormatter",
    "TableFormatter",
]


def wrap(
    text: str,
    width: int = 70,
    *,
    initial_indent: str = "",
    subsequent_indent: str = "",
    break_long_words: bool = True,
    break_on_hyphens: bool = True,
) -> list[str]:
    """Wrap text to specified width.

    Args:
        text: Text to wrap.
        width: Maximum line width.
        initial_indent: Indent for first line.
        subsequent_indent: Indent for subsequent lines.
        break_long_words: Break long words.
        break_on_hyphens: Break on hyphens.

    Returns:
        List of wrapped lines.
    """
    wrapper = textwrap.TextWrapper(
        width=width,
        initial_indent=initial_indent,
        subsequent_indent=subsequent_indent,
        break_long_words=break_long_words,
        break_on_hyphens=break_on_hyphens,
    )
    return wrapper.wrap(text)


def fill(
    text: str,
    width: int = 70,
    *,
    initial_indent: str = "",
    subsequent_indent: str = "",
) -> str:
    """Fill text to specified width.

    Args:
        text: Text to fill.
        width: Maximum line width.
        initial_indent: Indent for first line.
        subsequent_indent: Indent for subsequent lines.

    Returns:
        Filled text.
    """
    return textwrap.fill(
        text,
        width=width,
        initial_indent=initial_indent,
        subsequent_indent=subsequent_indent,
    )


def dedent(text: str) -> str:
    """Remove common leading whitespace.

    Args:
        text: Text to dedent.

    Returns:
        Dedented text.
    """
    return textwrap.dedent(text)


def indent(text: str, prefix: str, predicate: Callable[[str], bool] | None = None) -> str:
    """Indent text with prefix.

    Args:
        text: Text to indent.
        prefix: Prefix to add.
        predicate: Optional line filter.

    Returns:
        Indented text.
    """
    return textwrap.indent(text, prefix, predicate=predicate)


def shorten(
    text: str,
    width: int = 80,
    *,
    placeholder: str = "...",
    break_long_words: bool = True,
) -> str:
    """Shorten text to width.

    Args:
        text: Text to shorten.
        width: Maximum width.
        placeholder: String to append if shortened.
        break_long_words: Break long words.

    Returns:
        Shortened text.
    """
    return textwrap.shorten(
        text,
        width=width,
        placeholder=placeholder,
        break_long_words=break_long_words,
    )


def wrap_text(
    text: str,
    width: int = 70,
    style: str = "left",
) -> str:
    """Wrap text with alignment.

    Args:
        text: Text to wrap.
        width: Maximum line width.
        style: Alignment style ("left", "center", "right").

    Returns:
        Wrapped and aligned text.
    """
    lines = wrap(text, width)
    if style == "center":
        return "\n".join(line.center(width) for line in lines)
    elif style == "right":
        return "\n".join(line.rjust(width) for line in lines)
    return "\n".join(lines)


def align_left(text: str, width: int | None = None) -> str:
    """Align text to left.

    Args:
        text: Text to align.
        width: Total width (uses max line length if None).

    Returns:
        Left-aligned text.
    """
    if width is None:
        width = max(len(line) for line in text.split("\n")) if text else 0
    return "\n".join(line.ljust(width) for line in text.split("\n"))


def align_center(text: str, width: int | None = None) -> str:
    """Align text to center.

    Args:
        text: Text to align.
        width: Total width.

    Returns:
        Center-aligned text.
    """
    if width is None:
        width = max(len(line) for line in text.split("\n")) if text else 0
    return "\n".join(line.center(width) for line in text.split("\n"))


def align_right(text: str, width: int | None = None) -> str:
    """Align text to right.

    Args:
        text: Text to align.
        width: Total width.

    Returns:
        Right-aligned text.
    """
    if width is None:
        width = max(len(line) for line in text.split("\n")) if text else 0
    return "\n".join(line.rjust(width) for line in text.split("\n"))


def center_text(text: str, width: int | None = None) -> str:
    """Center text (alias for align_center).

    Args:
        text: Text to center.
        width: Total width.

    Returns:
        Centered text.
    """
    return align_center(text, width)


def left_justify(text: str, width: int | None = None) -> str:
    """Left justify text.

    Args:
        text: Text to justify.
        width: Total width.

    Returns:
        Left-justified text.
    """
    return align_left(text, width)


def right_justify(text: str, width: int | None = None) -> str:
    """Right justify text.

    Args:
        text: Text to justify.
        width: Total width.

    Returns:
        Right-justified text.
    """
    return align_right(text, width)


def wrap_paragraphs(
    text: str,
    width: int = 70,
    **kwargs: Any,
) -> str:
    """Wrap text preserving paragraphs.

    Args:
        text: Text with paragraphs.
        width: Maximum line width.
        **kwargs: Additional wrap arguments.

    Returns:
        Wrapped text.
    """
    paragraphs = text.split("\n\n")
    wrapped = []
    for para in paragraphs:
        if para.strip():
            wrapped.append(fill(para, width=width, **kwargs))
        else:
            wrapped.append("")
    return "\n\n".join(wrapped)


def indent_lines(
    text: str,
    indent: str,
    skip_first: bool = False,
) -> str:
    """Indent all lines in text.

    Args:
        text: Text to indent.
        indent: Indentation string.
        skip_first: Skip indenting first line.

    Returns:
        Indented text.
    """
    lines = text.split("\n")
    if skip_first:
        return lines[0] + "\n" + "\n".join(indent + line for line in lines[1:])
    return "\n".join(indent + line for line in lines)


def unindent(text: str, spaces: int = 4) -> str:
    """Unindent text by removing leading spaces.

    Args:
        text: Text to unindent.
        spaces: Number of spaces to remove.

    Returns:
        Unindented text.
    """
    prefix = " " * spaces
    return "\n".join(
        line[len(prefix):] if line.startswith(prefix) else line
        for line in text.split("\n")
    )


def strip_indent(text: str) -> str:
    """Strip common leading indentation.

    Args:
        text: Text to strip.

    Returns:
        Stripped text.
    """
    return textwrap.dedent(text)


def normalize_whitespace(text: str) -> str:
    """Normalize all whitespace to single spaces.

    Args:
        text: Text to normalize.

    Returns:
        Normalized text.
    """
    return " ".join(text.split())


def collapse_whitespace(text: str) -> str:
    """Collapse multiple spaces into one.

    Args:
        text: Text to process.

    Returns:
        Collapsed text.
    """
    return re.sub(r"[ \t]+", " ", text)


def word_wrap(
    text: str,
    width: int = 70,
    break_words: bool = False,
) -> list[str]:
    """Word wrap text.

    Args:
        text: Text to wrap.
        width: Maximum line width.
        break_words: Allow breaking words.

    Returns:
        List of wrapped lines.
    """
    return wrap(
        text,
        width=width,
        break_long_words=break_words,
    )


def character_wrap(text: str, width: int) -> list[str]:
    """Wrap text by characters.

    Args:
        text: Text to wrap.
        width: Characters per line.

    Returns:
        List of character-wrapped lines.
    """
    return [text[i:i+width] for i in range(0, len(text), width)]


def prefix_lines(text: str, prefix: str) -> str:
    """Add prefix to all lines.

    Args:
        text: Text to prefix.
        prefix: Prefix to add.

    Returns:
        Prefixed text.
    """
    return "\n".join(prefix + line for line in text.split("\n"))


def suffix_lines(text: str, suffix: str) -> str:
    """Add suffix to all lines.

    Args:
        text: Text to suffix.
        suffix: Suffix to add.

    Returns:
        Suffixed text.
    """
    return "\n".join(line + suffix for line in text.split("\n"))


def remove_prefix(text: str, prefix: str) -> str:
    """Remove prefix from string.

    Args:
        text: Text to process.
        prefix: Prefix to remove.

    Returns:
        Text without prefix.
    """
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text: str, suffix: str) -> str:
    """Remove suffix from string.

    Args:
        text: Text to process.
        suffix: Suffix to remove.

    Returns:
        Text without suffix.
    """
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text


def wrap_string(
    s: str,
    width: int = 80,
    wrap_chars: str = "\n",
) -> str:
    """Wrap string at width.

    Args:
        s: String to wrap.
        width: Wrap width.
        wrap_chars: Character to use for line breaks.

    Returns:
        Wrapped string.
    """
    return wrap_chars.join([s[i:i+width] for i in range(0, len(s), width)])


class TextWrapper(textwrap.TextWrapper):
    """Extended TextWrapper with more options."""

    def __init__(
        self,
        width: int = 70,
        *,
        tabsize: int = 8,
        **kwargs: Any,
    ) -> None:
        super().__init__(width=width, tabsize=tabsize, **kwargs)


def indent_width(text: str) -> int:
    """Get the minimum indentation width.

    Args:
        text: Text to measure.

    Returns:
        Minimum indentation in spaces.
    """
    lines = text.split("\n")
    non_empty = [line for line in lines if line.strip()]
    if not non_empty:
        return 0

    min_indent = float("inf")
    for line in non_empty:
        match = re.match(r"^(\s*)", line)
        if match:
            indent = len(match.group(1))
            min_indent = min(min_indent, indent)

    return int(min_indent) if min_indent != float("inf") else 0


def prefix_width(text: str) -> int:
    """Get the common prefix width.

    Args:
        text: Text to analyze.

    Returns:
        Prefix width in characters.
    """
    lines = text.split("\n")
    if not lines:
        return 0

    common = []
    for chars in zip(*lines):
        if len(set(chars)) == 1:
            common.append(chars[0])
        else:
            break

    return len("".join(common))


class ColumnFormatter:
    """Format text into columns."""

    def __init__(
        self,
        columns: int = 2,
        spacing: int = 2,
        align: str = "left",
    ) -> None:
        self._columns = columns
        self._spacing = spacing
        self._align = align

    def format(self, items: list[str], width: int = 80) -> str:
        """Format items into columns.

        Args:
            items: Items to format.
            width: Total width.

        Returns:
            Formatted text.
        """
        col_width = (width - (self._columns - 1) * self._spacing) // self._columns
        rows = []
        for i in range(0, len(items), self._columns):
            row_items = items[i:i + self._columns]
            if self._align == "right":
                row = " " * self._spacing.join(
                    item.rjust(col_width) for item in row_items
                )
            elif self._align == "center":
                row = " " * self._spacing.join(
                    item.center(col_width) for item in row_items
                )
            else:
                row = " " * self._spacing.join(
                    item.ljust(col_width) for item in row_items
                )
            rows.append(row)

        return "\n".join(rows)


class TableFormatter:
    """Format text into a table."""

    def __init__(
        self,
        headers: list[str],
        alignments: list[str] | None = None,
    ) -> None:
        self._headers = headers
        self._alignments = alignments or ["left"] * len(headers)
        self._rows: list[list[str]] = []

    def add_row(self, *values: str) -> None:
        """Add a row to the table.

        Args:
            *values: Column values.
        """
        self._rows.append(list(values))

    def format(self) -> str:
        """Format the table.

        Returns:
            Formatted table string.
        """
        if not self._rows:
            return ""

        col_widths = [len(h) for h in self._headers]
        for row in self._rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        lines = []

        header_line = "|".join(
            h.center(w) if a == "center"
            else h.rjust(w) if a == "right"
            else h.ljust(w)
            for h, w, a in zip(self._headers, col_widths, self._alignments)
        )
        lines.append(header_line)

        sep = "+".join("-" * w for w in col_widths)
        lines.append(sep)

        for row in self._rows:
            row_line = "|".join(
                str(cell).center(w) if a == "center"
                else str(cell).rjust(w) if a == "right"
                else str(cell).ljust(w)
                for cell, w, a in zip(row, col_widths, self._alignments)
            )
            lines.append(row_line)

        return "\n".join(lines)
