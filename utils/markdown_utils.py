"""
Markdown parsing and rendering utilities.

Provides:
- Markdown parsing (headers, lists, code blocks, tables, links, images)
- Markdown rendering from structured data
- Markdown sanitization
- Markdown extraction (strip tags, get headings)
- Table of contents generation
- Word/character counting
"""

from __future__ import annotations

import re
import unicodedata
from html import escape as html_escape
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence


# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------

MARKDOWN_SPECIAL_CHARS = r"\*\_\`\[\]\(\)\#\+\-\!\>"


# ------------------------------------------------------------------------------
# Parsing
# ------------------------------------------------------------------------------

def parse_frontmatter(lines: Sequence[str]) -> tuple[dict[str, str], list[str]]:
    """
    Parse YAML-like frontmatter from markdown.

    Expects content starting with lines like:
        ---
        key: value
        ---
    Returns the metadata dict and remaining content lines.

    Args:
        lines: Lines of markdown text.

    Returns:
        Tuple of (metadata dict, remaining content lines).

    Example:
        >>> meta, content = parse_frontmatter(['---', 'title: Hello', '---', '', 'Content'])
        >>> meta['title']
        'Hello'
        >>> content
        ['', 'Content']
    """
    if not lines or len(lines) < 3:
        return {}, list(lines)

    if lines[0].strip() != "---":
        return {}, list(lines)

    meta: dict[str, str] = {}
    content_start = 1
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            content_start = i + 1
            break
        if ":" in line:
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip()

    return meta, list(lines[content_start:])


def extract_headings(text: str) -> list[dict[str, str]]:
    """
    Extract all headings from markdown text.

    Args:
        text: Markdown text.

    Returns:
        List of dicts with 'level', 'text', and 'id' keys.

    Example:
        >>> headings = extract_headings('# Hello\\n## World\\n### Test')
        >>> len(headings)
        3
        >>> headings[0]['level']
        1
        >>> headings[0]['text']
        'Hello'
    """
    headings: list[dict[str, str]] = []
    for line in text.splitlines():
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            level = len(m.group(1))
            text_content = m.group(2).strip()
            heading_id = re.sub(r"[^\w\s-]", "", text_content.lower())
            heading_id = re.sub(r"[-\s]+", "-", heading_id).strip("-")
            headings.append({
                "level": level,
                "text": text_content,
                "id": heading_id,
            })
    return headings


def extract_code_blocks(text: str) -> list[dict[str, str]]:
    """
    Extract all fenced code blocks from markdown.

    Args:
        text: Markdown text.

    Returns:
        List of dicts with 'language', 'code', 'start', 'end'.

    Example:
        >>> blocks = extract_code_blocks('\\n```python\\nprint(1)\\n```\\n')
        >>> blocks[0]['language']
        'python'
        >>> blocks[0]['code']
        'print(1)'
    """
    blocks: list[dict[str, str]] = []
    pattern = re.compile(r"```(\w*)\n(.*?)```", re.DOTALL)
    for m in pattern.finditer(text):
        blocks.append({
            "language": m.group(1),
            "code": m.group(2).rstrip("\n"),
            "start": m.start(),
            "end": m.end(),
        })
    return blocks


def extract_links(text: str) -> list[dict[str, str]]:
    """
    Extract all markdown links from text.

    Args:
        text: Markdown text.

    Returns:
        List of dicts with 'text', 'url', 'title'.

    Example:
        >>> links = extract_links('[Google](https://google.com "Google Search")')
        >>> links[0]['url']
        'https://google.com'
        >>> links[0]['text']
        'Google'
    """
    links: list[dict[str, str]] = []
    # Pattern: [text](url "optional title")
    pattern = re.compile(r"\[([^\]]*)\]\(([^)\s]+)(?:\s+\"([^\"]*)\")?\)")
    for m in pattern.finditer(text):
        links.append({
            "text": m.group(1),
            "url": m.group(2),
            "title": m.group(3) or "",
        })
    return links


def extract_images(text: str) -> list[dict[str, str]]:
    """
    Extract all markdown images from text.

    Args:
        text: Markdown text.

    Returns:
        List of dicts with 'alt', 'url', 'title'.

    Example:
        >>> images = extract_images('![alt text](https://example.com/img.png)')
        >>> images[0]['alt']
        'alt text'
        >>> images[0]['url']
        'https://example.com/img.png'
    """
    images: list[dict[str, str]] = []
    pattern = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)(?:\s+\"([^\"]*)\")?\)")
    for m in pattern.finditer(text):
        images.append({
            "alt": m.group(1),
            "url": m.group(2),
            "title": m.group(3) or "",
        })
    return images


def extract_tables(text: str) -> list[dict[str, str]]:
    """
    Extract all markdown tables from text.

    Args:
        text: Markdown text.

    Returns:
        List of dicts with 'headers', 'alignments', 'rows', 'raw'.

    Example:
        >>> tables = extract_tables('| a | b |\\\\n| - | - |\\\\n| 1 | 2 |')
        >>> len(tables[0]['headers'])
        2
    """
    tables: list[dict[str, str]] = []
    lines = text.splitlines()

    for i, line in enumerate(lines):
        if not re.match(r"^\|.*\|$", line.strip()):
            continue

        # Check if next line is separator
        if i + 1 >= len(lines):
            continue
        sep_line = lines[i + 1]
        if not re.match(r"^\|[\s\-:|]+\|$", sep_line.strip()):
            continue

        # Parse header
        headers = [h.strip() for h in line.strip().strip("|").split("|")]

        # Parse alignment
        alignments: list[str] = []
        for cell in sep_line.strip().strip("|").split("|"):
            cell = cell.strip()
            if cell.startswith(":") and cell.endswith(":"):
                alignments.append("center")
            elif cell.endswith(":"):
                alignments.append("right")
            else:
                alignments.append("left")

        # Parse rows
        rows: list[list[str]] = []
        for row_line in lines[i + 2:]:
            if not re.match(r"^\|.*\|$", row_line.strip()):
                break
            row = [c.strip() for c in row_line.strip().strip("|").split("|")]
            rows.append(row)

        tables.append({
            "headers": headers,
            "alignments": alignments,
            "rows": rows,
            "raw": "\n".join(lines[i:i + 2 + len(rows)]),
        })

    return tables


# ------------------------------------------------------------------------------
# Stripping & Sanitizing
# ------------------------------------------------------------------------------

def strip_markdown(text: str) -> str:
    """
    Remove all markdown formatting from text.

    Args:
        text: Markdown text.

    Returns:
        Plain text with markdown stripped.

    Example:
        >>> strip_markdown('**bold** and *italic*')
        'bold and italic'
        >>> strip_markdown('# Header')
        'Header'
    """
    # Remove code blocks
    text = re.sub(r"```[\s\S]*?```", "", text)
    # Remove inline code
    text = re.sub(r"`[^`]+`", "", text)
    # Remove images
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)
    # Remove links, keep text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    # Remove bold/italic
    text = re.sub(r"\*{1,3}([^*]+)\*{1,3}", r"\1", text)
    text = re.sub(r"_{1,3}([^_]+)_{1,3}", r"\1", text)
    # Remove strikethrough
    text = re.sub(r"~~([^~]+)~~", r"\1", text)
    # Remove headings markers
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Remove blockquotes
    text = re.sub(r"^>\s?", "", text, flags=re.MULTILINE)
    # Remove horizontal rules
    text = re.sub(r"^[-*_]{3,}\s*$", "", text, flags=re.MULTILINE)
    # Remove list markers
    text = re.sub(r"^[\s]*[-*+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^[\s]*\d+\.\s+", "", text, flags=re.MULTILINE)
    return text.strip()


def sanitize_markdown(text: str) -> str:
    """
    Sanitize markdown by escaping potentially dangerous content.

    Args:
        text: Markdown text.

    Returns:
        Sanitized markdown safe for rendering.
    """
    # Unescape already-escaped dangerous sequences
    # Keep basic formatting, escape HTML
    text = re.sub(r"<script[^>]*>", "&lt;script&gt;", text, flags=re.IGNORECASE)
    text = re.sub(r"javascript:", "javascript&#58;", text, flags=re.IGNORECASE)
    return text


# ------------------------------------------------------------------------------
# Rendering
# ------------------------------------------------------------------------------

def render_heading(text: str, level: int = 1) -> str:
    """
    Render a heading.

    Args:
        text: Heading text.
        level: Heading level (1-6).

    Returns:
        Markdown heading string.
    """
    level = max(1, min(6, level))
    return f"{'#' * level} {text}"


def render_bold(text: str) -> str:
    """Render text as bold."""
    return f"**{text}**"


def render_italic(text: str) -> str:
    """Render text as italic."""
    return f"*{text}*"


def render_code(code: str, language: str = "") -> str:
    """
    Render a code block.

    Args:
        code: Code content.
        language: Programming language identifier.

    Returns:
        Markdown code block.
    """
    return f"```{language}\n{code}\n```"


def render_inline_code(code: str) -> str:
    """Render inline code."""
    return f"`{code}`"


def render_link(text: str, url: str, title: str = "") -> str:
    """
    Render a markdown link.

    Args:
        text: Link text.
        url: Link URL.
        title: Optional title.

    Returns:
        Markdown link.
    """
    if title:
        return f"[{text}]({url} \"{title}\")"
    return f"[{text}]({url})"


def render_image(alt: str, url: str, title: str = "") -> str:
    """
    Render a markdown image.

    Args:
        alt: Alt text.
        url: Image URL.
        title: Optional title.

    Returns:
        Markdown image.
    """
    if title:
        return f"![{alt}]({url} \"{title}\")"
    return f"![{alt}]({url})"


def render_list(items: Sequence[str], ordered: bool = False) -> str:
    """
    Render a markdown list.

    Args:
        items: List items.
        ordered: If True, render as ordered list.

    Returns:
        Markdown list string.
    """
    lines: list[str] = []
    for i, item in enumerate(items):
        prefix = f"{i + 1}." if ordered else "-"
        lines.append(f"{prefix} {item}")
    return "\n".join(lines)


def render_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    alignments: Sequence[str] | None = None,
) -> str:
    """
    Render a markdown table.

    Args:
        headers: Column headers.
        rows: Table rows.
        alignments: Optional per-column alignment ('left', 'center', 'right').

    Returns:
        Markdown table string.
    """
    lines: list[str] = []

    # Header row
    lines.append("| " + " | ".join(str(h) for h in headers) + " |")

    # Separator row
    if alignments is None:
        alignments = ["left"] * len(headers)
    sep_cells: list[str] = []
    for i, _ in enumerate(headers):
        align = alignments[i] if i < len(alignments) else "left"
        if align == "center":
            sep_cells.append(":---:")
        elif align == "right":
            sep_cells.append("---:")
        else:
            sep_cells.append("---")
    lines.append("| " + " | ".join(sep_cells) + " |")

    # Data rows
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")

    return "\n".join(lines)


def render_blockquote(text: str) -> str:
    """Render a blockquote."""
    lines = text.splitlines()
    return "\n".join(f"> {line}" for line in lines)


def render_horizontal_rule() -> str:
    """Render a horizontal rule."""
    return "\n---\n"


# ------------------------------------------------------------------------------
# Table of Contents
# ------------------------------------------------------------------------------

def build_toc(text: str, max_level: int = 3) -> str:
    """
    Build a table of contents from markdown headings.

    Args:
        text: Markdown text.
        max_level: Maximum heading level to include.

    Returns:
        Markdown-formatted table of contents.
    """
    headings = extract_headings(text)
    toc_lines: list[str] = []
    for h in headings:
        if h["level"] > max_level:
            continue
        indent = "  " * (h["level"] - 1)
        toc_lines.append(f'{indent}- [{h["text"]}](#{h["id"]})')
    return "\n".join(toc_lines)


# ------------------------------------------------------------------------------
# Statistics
# ------------------------------------------------------------------------------

def word_count(text: str) -> int:
    """
    Count words in markdown (strips markdown first).

    Args:
        text: Markdown text.

    Returns:
        Word count.
    """
    plain = strip_markdown(text)
    words = plain.split()
    return len(words)


def character_count(text: str, include_spaces: bool = True) -> int:
    """
    Count characters in markdown (strips markdown first).

    Args:
        text: Markdown text.
        include_spaces: Whether to include whitespace.

    Returns:
        Character count.
    """
    plain = strip_markdown(text)
    if include_spaces:
        return len(plain)
    return len(plain.replace(" ", "").replace("\n", "").replace("\t", ""))


def reading_time(text: str, words_per_minute: float = 200.0) -> float:
    """
    Estimate reading time in minutes.

    Args:
        text: Markdown text.
        words_per_minute: Reading speed (default 200 wpm).

    Returns:
        Estimated reading time in minutes.
    """
    count = word_count(text)
    return max(0.1, count / words_per_minute)


# ------------------------------------------------------------------------------
# Advanced Parsing
# ------------------------------------------------------------------------------

def parse_markdown_links_inline(text: str) -> str:
    """
    Render markdown links as plain text with URLs in parentheses.

    Args:
        text: Markdown text.

    Returns:
        Text with links rendered as "text (url)".
    """
    return re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        lambda m: f"{m.group(1)} ({m.group(2)})",
        text,
    )


def replace_heading_ids(
    text: str,
    id_func: Callable[[str], str] | None = None,
) -> str:
    """
    Add explicit IDs to headings that don't have them.

    Args:
        text: Markdown text.
        id_func: Optional function to generate IDs from heading text.

    Returns:
        Markdown text with heading IDs.
    """
    if id_func is None:
        def default_id_func(t: str) -> str:
            s = re.sub(r"[^\w\s-]", "", t.lower())
            return re.sub(r"[-\s]+", "-", s).strip("-")
        id_func = default_id_func

    def fix_heading(m: re.Match[str]) -> str:
        level = len(m.group(1))
        content = m.group(2).strip()
        # Check if already has an ID
        if "{#" in content:
            return m.group(0)
        heading_id = id_func(content)
        return f"{'#' * level} {content} {{#{heading_id}}}"

    return re.sub(r"^(#{1,6})\s+(.+)$", fix_heading, text, flags=re.MULTILINE)
