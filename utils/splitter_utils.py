"""Splitter utilities for RabAI AutoClick.

Provides:
- String splitting and parsing
- Record splitting
- Delimiter-based splitting
- Regex-based splitting
"""

from __future__ import annotations

import re
from typing import Callable, Iterator, List, Optional, Pattern


def split_lines(
    text: str,
    strip: bool = True,
    remove_empty: bool = True,
) -> List[str]:
    """Split text into lines.

    Args:
        text: Input text.
        strip: Strip whitespace from each line.
        remove_empty: Remove empty lines.

    Returns:
        List of lines.
    """
    lines = text.splitlines()
    if strip:
        lines = [line.strip() for line in lines]
    if remove_empty:
        lines = [line for line in lines if line]
    return lines


def split_words(
    text: str,
    delimiters: str = " \t\n\r\f\v",
    remove_empty: bool = True,
) -> List[str]:
    """Split text into words.

    Args:
        text: Input text.
        delimiters: Characters to split on.
        remove_empty: Remove empty tokens.

    Returns:
        List of words.
    """
    pattern = f"[{re.escape(delimiters)}]+"
    tokens = re.split(pattern, text)
    if remove_empty:
        tokens = [t for t in tokens if t]
    return tokens


def split_by_length(
    text: str,
    chunk_size: int,
) -> List[str]:
    """Split text into fixed-size chunks.

    Args:
        text: Input text.
        chunk_size: Size of each chunk.

    Returns:
        List of text chunks.
    """
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]


def split_csv_line(
    line: str,
    delimiter: str = ",",
    quote_char: str = '"',
) -> List[str]:
    """Parse a CSV line with quoted fields.

    Args:
        line: CSV line to parse.
        delimiter: Field delimiter.
        quote_char: Quote character.

    Returns:
        List of field values.
    """
    result: List[str] = []
    current = ""
    in_quotes = False

    for char in line:
        if char == quote_char:
            in_quotes = not in_quotes
        elif char == delimiter and not in_quotes:
            result.append(current.strip())
            current = ""
        else:
            current += char

    result.append(current.strip())
    return result


def split_sentences(text: str) -> List[str]:
    """Split text into sentences.

    Args:
        text: Input text.

    Returns:
        List of sentences.
    """
    sentence_end = re.compile(r"[.!?]+\s+")
    sentences = sentence_end.split(text)
    sentences = [s.strip() for s in sentences if s.strip()]
    return sentences


def split_paragraphs(text: str, min_length: int = 10) -> List[str]:
    """Split text into paragraphs.

    Args:
        text: Input text.
        min_length: Minimum paragraph length.

    Returns:
        List of paragraphs.
    """
    paragraphs = re.split(r"\n\s*\n", text)
    return [p.strip() for p in paragraphs if len(p.strip()) >= min_length]


def split_regex(
    text: str,
    pattern: str | Pattern[str],
) -> List[str]:
    """Split by regex pattern.

    Args:
        text: Input text.
        pattern: Regex pattern.

    Returns:
        List of split parts.
    """
    if isinstance(pattern, str):
        pattern = re.compile(pattern)
    return [p for p in pattern.split(text) if p]


def chunk_text(
    text: str,
    chunk_size: int,
    overlap: int = 0,
) -> List[str]:
    """Split text into overlapping chunks.

    Args:
        text: Input text.
        chunk_size: Size of each chunk.
        overlap: Number of overlapping characters.

    Returns:
        List of text chunks.
    """
    if overlap >= chunk_size:
        overlap = chunk_size - 1
    chunks: List[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks


def split_tuples(
    items: list,
    n: int,
) -> Iterator[List]:
    """Split list into tuples of size n.

    Args:
        items: Input list.
        n: Tuple size.

    Yields:
        Tuples of size n.
    """
    for i in range(0, len(items), n):
        yield items[i:i + n]


def split_dict_by_keys(
    data: dict,
    keys: List[str],
) -> tuple[dict, dict]:
    """Split dictionary into two based on key list.

    Args:
        data: Input dictionary.
        keys: Keys for first group.

    Returns:
        Tuple of (matched, unmatched).
    """
    matched = {k: data[k] for k in keys if k in data}
    unmatched = {k: v for k, v in data.items() if k not in keys}
    return matched, unmatched
