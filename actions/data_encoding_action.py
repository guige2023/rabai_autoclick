"""Data Encoding Action module.

Provides data encoding and decoding utilities for various
formats including base64, hex, URL encoding, JSON, and
custom encoding schemes.
"""

from __future__ import annotations

import base64
import json
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable, Optional

import numpy as np


@dataclass
class EncodingResult:
    """Result of encoding operation."""

    success: bool
    encoded: Any
    error: Optional[str] = None
    format: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "format": self.format,
            "error": self.error,
        }


def encode_base64(data: str | bytes) -> EncodingResult:
    """Encode data to base64.

    Args:
        data: String or bytes to encode

    Returns:
        EncodingResult
    """
    try:
        if isinstance(data, str):
            data = data.encode("utf-8")
        encoded = base64.b64encode(data).decode("ascii")
        return EncodingResult(success=True, encoded=encoded, format="base64")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="base64")


def decode_base64(data: str) -> EncodingResult:
    """Decode base64 data.

    Args:
        data: Base64 string to decode

    Returns:
        EncodingResult with decoded bytes
    """
    try:
        decoded = base64.b64decode(data)
        return EncodingResult(success=True, encoded=decoded, format="base64")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="base64")


def encode_hex(data: str | bytes) -> EncodingResult:
    """Encode data to hexadecimal.

    Args:
        data: String or bytes to encode

    Returns:
        EncodingResult
    """
    try:
        if isinstance(data, str):
            data = data.encode("utf-8")
        encoded = data.hex()
        return EncodingResult(success=True, encoded=encoded, format="hex")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="hex")


def decode_hex(data: str) -> EncodingResult:
    """Decode hexadecimal data.

    Args:
        data: Hex string to decode

    Returns:
        EncodingResult with decoded bytes
    """
    try:
        decoded = bytes.fromhex(data)
        return EncodingResult(success=True, encoded=decoded, format="hex")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="hex")


def encode_url(data: str) -> EncodingResult:
    """URL encode a string.

    Args:
        data: String to encode

    Returns:
        EncodingResult
    """
    try:
        encoded = urllib.parse.quote(data)
        return EncodingResult(success=True, encoded=encoded, format="url")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="url")


def decode_url(data: str) -> EncodingResult:
    """URL decode a string.

    Args:
        data: URL-encoded string

    Returns:
        EncodingResult
    """
    try:
        decoded = urllib.parse.unquote(data)
        return EncodingResult(success=True, encoded=decoded, format="url")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="url")


def encode_json(data: Any) -> EncodingResult:
    """Encode data to JSON string.

    Args:
        data: Data to encode

    Returns:
        EncodingResult
    """
    try:
        encoded = json.dumps(data, ensure_ascii=False, default=str)
        return EncodingResult(success=True, encoded=encoded, format="json")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="json")


def decode_json(data: str) -> EncodingResult:
    """Decode JSON string.

    Args:
        data: JSON string to decode

    Returns:
        EncodingResult with decoded data
    """
    try:
        decoded = json.loads(data)
        return EncodingResult(success=True, encoded=decoded, format="json")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="json")


def encode_html(data: str) -> EncodingResult:
    """HTML encode a string.

    Args:
        data: String to encode

    Returns:
        EncodingResult
    """
    html_escape_table = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#x27;",
        "/": "&#x2F;",
    }

    try:
        encoded = "".join(html_escape_table.get(c, c) for c in data)
        return EncodingResult(success=True, encoded=encoded, format="html")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="html")


def decode_html(data: str) -> EncodingResult:
    """HTML decode a string.

    Args:
        data: HTML-encoded string

    Returns:
        EncodingResult
    """
    html_unescape_table = {
        "&amp;": "&",
        "&lt;": "<",
        "&gt;": ">",
        "&quot;": '"',
        "&#x27;": "'",
        "&#x2F;": "/",
        "&#39;": "'",
        "&nbsp;": " ",
    }

    try:
        result = data
        for k, v in html_unescape_table.items():
            result = result.replace(k, v)
        return EncodingResult(success=True, encoded=result, format="html")
    except Exception as e:
        return EncodingResult(success=False, encoded=None, error=str(e), format="html")


class ColumnEncoder:
    """Encode data columns to numeric format."""

    def __init__(self):
        self._mappings: dict[str, dict[Any, int]] = {}
        self._reverse_mappings: dict[str, dict[int, Any]] = {}

    def fit(self, data: list[dict[str, Any]], columns: list[str]) -> "ColumnEncoder":
        """Fit encoder to data.

        Args:
            data: Training data
            columns: Columns to encode

        Returns:
            Self
        """
        for col in columns:
            unique_values = sorted(set(d.get(col) for d in data if col in d))
            mapping = {v: i for i, v in enumerate(unique_values)}
            reverse = {i: v for i, v in enumerate(unique_values)}

            self._mappings[col] = mapping
            self._reverse_mappings[col] = reverse

        return self

    def transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform data using fitted encoder.

        Args:
            data: Data to transform

        Returns:
            Transformed data
        """
        result = []
        for record in data:
            new_record = dict(record)
            for col, mapping in self._mappings.items():
                if col in new_record:
                    value = new_record[col]
                    new_record[col] = mapping.get(value, -1)
            result.append(new_record)
        return result

    def fit_transform(self, data: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
        """Fit and transform in one step.

        Args:
            data: Data to encode
            columns: Columns to encode

        Returns:
            Transformed data
        """
        return self.fit(data, columns).transform(data)

    def inverse_transform(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Reverse the encoding.

        Args:
            data: Encoded data

        Returns:
            Original data
        """
        result = []
        for record in data:
            new_record = dict(record)
            for col, reverse in self._reverse_mappings.items():
                if col in new_record:
                    value = new_record[col]
                    new_record[col] = reverse.get(value, value)
            result.append(new_record)
        return result


def one_hot_encode(
    data: list[Any],
    categories: Optional[list[Any]] = None,
) -> tuple[list[list[int]], list[Any]]:
    """One-hot encode categorical data.

    Args:
        data: Categorical values
        categories: Optional category list

    Returns:
        Tuple of (encoded array, category list)
    """
    if categories is None:
        categories = sorted(set(data))

    cat_to_idx = {cat: i for i, cat in enumerate(categories)}
    encoded = []

    for value in data:
        row = [0] * len(categories)
        if value in cat_to_idx:
            row[cat_to_idx[value]] = 1
        encoded.append(row)

    return encoded, categories


def label_encode(
    data: list[Any],
    categories: Optional[list[Any]] = None,
) -> tuple[list[int], list[Any]]:
    """Label encode categorical data.

    Args:
        data: Categorical values
        categories: Optional category list

    Returns:
        Tuple of (encoded labels, category list)
    """
    if categories is None:
        categories = sorted(set(data))

    cat_to_idx = {cat: i for i, cat in enumerate(categories)}
    encoded = [cat_to_idx.get(v, -1) for v in data]

    return encoded, categories


def target_encode(
    data: list[dict[str, Any]],
    category_col: str,
    target_col: str,
    smoothing: float = 1.0,
) -> dict[Any, float]:
    """Target encode categorical values.

    Args:
        data: Data records
        category_col: Categorical column name
        target_col: Target column name
        smoothing: Smoothing factor

    Returns:
        Dictionary mapping categories to encoded values
    """
    category_stats: dict[Any, tuple[float, int]] = {}

    for record in data:
        cat = record.get(category_col)
        target = record.get(target_col)

        if cat is None or target is None:
            continue

        if cat not in category_stats:
            category_stats[cat] = (0.0, 0)

        total, count = category_stats[cat]
        category_stats[cat] = (total + float(target), count + 1)

    global_mean = sum(s for s, _ in category_stats.values()) / len(category_stats) if category_stats else 0.0

    encoded = {}
    for cat, (total, count) in category_stats.items():
        smoothed = (total + smoothing * global_mean) / (count + smoothing)
        encoded[cat] = smoothed

    return encoded


@dataclass
class EncodingPipeline:
    """Pipeline for chaining encoders."""

    encoders: list[tuple[str, Callable]] = []

    def add(self, name: str, encoder: Callable[[Any], Any]) -> "EncodingPipeline":
        """Add an encoder to the pipeline."""
        self.encoders.append((name, encoder))
        return self

    def encode(self, data: Any) -> Any:
        """Encode data through pipeline."""
        result = data
        for name, encoder in self.encoders:
            try:
                result = encoder(result)
            except Exception:
                pass
        return result

    def decode(self, data: Any) -> Any:
        """Decode data through pipeline (in reverse)."""
        result = data
        for name, encoder in reversed(self.encoders):
            if hasattr(encoder, "inverse_transform"):
                try:
                    result = encoder.inverse_transform(result)
                except Exception:
                    pass
        return result
