"""
Java properties file parsing and manipulation utilities.

Provides support for reading, writing, and manipulating Java-style
properties files with escape sequence handling, Unicode support,
and key-value transformations.

Example:
    >>> from utils.properties_utils import PropertiesHandler
    >>> handler = PropertiesHandler()
    >>> props = handler.parse_file("app.properties")
    >>> handler.set("version", "1.0.0")
"""

from __future__ import annotations

import codecs
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


class PropertiesHandler:
    """
    Handler for Java-style properties files.

    Supports standard properties file format including escape sequences
    (\\n, \\t, \\r, \\\\uXXXX Unicode escapes), multi-line values,
    and comments.

    Attributes:
        encoding: File encoding for read/write operations.
        separator: Key-value separator character.
        comment_chars: Characters that start comment lines.
    """

    ESCAPE_SEQUENCES = {
        r"\n": "\n",
        r"\t": "\t",
        r"\r": "\r",
        r"\\": "\\",
        r"\"": '"',
        r"\'": "'",
    }

    UNICODE_PATTERN = re.compile(r"\\u([0-9a-fA-F]{4})")

    def __init__(
        self,
        encoding: str = "utf-8",
        separator: str = "=",
        comment_chars: str = "#!",
    ) -> None:
        """
        Initialize the properties handler.

        Args:
            encoding: Default file encoding.
            separator: Key-value separator character.
            comment_chars: Characters that start comment lines.
        """
        self.encoding = encoding
        self.separator = separator
        self.comment_chars = comment_chars
        self._properties: Dict[str, str] = {}
        self._comments: List[str] = []

    def parse_string(self, content: str) -> Dict[str, str]:
        """
        Parse properties from a string.

        Args:
            content: Properties-formatted string.

        Returns:
            Dictionary of property key-value pairs.
        """
        self._properties.clear()
        self._comments.clear()

        lines = content.split("\n")
        current_key: Optional[str] = None
        current_value_parts: List[str] = []

        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()

            if not stripped or stripped[0] in self.comment_chars:
                self._comments.append(stripped)
                continue

            if stripped[-1] == "\\":
                if current_key is None:
                    key_part = stripped[:-1].split(self.separator, 1)
                    if len(key_part) == 2:
                        current_key = key_part[0].strip()
                        current_value_parts.append(key_part[1].strip())
                    else:
                        current_value_parts.append(stripped[:-1])
                else:
                    current_value_parts.append(stripped[:-1])
                continue

            if current_key is not None:
                full_value = " ".join(current_value_parts) + " " + stripped
                self._set_property(current_key, full_value)
                current_key = None
                current_value_parts = []
            else:
                parts = stripped.split(self.separator, 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = self._decode_value(parts[1].strip())
                    self._properties[key] = value

        if current_key is not None:
            self._set_property(current_key, " ".join(current_value_parts))

        return dict(self._properties)

    def parse_file(self, path: Union[str, Path]) -> Dict[str, str]:
        """
        Parse a properties file.

        Args:
            path: Path to the properties file.

        Returns:
            Dictionary of property key-value pairs.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Properties file not found: {path}")

        with codecs.open(path, "r", encoding=self.encoding) as f:
            content = f.read()

        return self.parse_string(content)

    def write_file(
        self,
        path: Union[str, Path],
        data: Optional[Dict[str, Any]] = None,
        comments: Optional[List[str]] = None,
    ) -> None:
        """
        Write properties to a file.

        Args:
            path: Destination file path.
            data: Properties dictionary (uses internal state if None).
            comments: Optional header comments.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = data or self._properties
        comments = comments or self._comments

        lines: List[str] = []
        for comment in comments:
            lines.append(comment)

        if lines:
            lines.append("")

        for key, value in data.items():
            encoded = self._encode_value(str(value))
            lines.append(f"{key}{self.separator}{encoded}")

        with codecs.open(path, "w", encoding=self.encoding) as f:
            f.write("\n".join(lines))

    def _decode_value(self, value: str) -> str:
        """Decode escape sequences and Unicode escapes in a value."""
        result = value

        for escape, char in self.ESCAPE_SEQUENCES.items():
            result = result.replace(escape, char)

        def replace_unicode(match: re.Match) -> str:
            return chr(int(match.group(1), 16))

        result = self.UNICODE_PATTERN.sub(replace_unicode, result)
        return result

    def _encode_value(self, value: str) -> str:
        """Encode special characters for properties format."""
        result = value

        for char, escape in [
            ("\n", r"\n"),
            ("\t", r"\t"),
            ("\r", r"\r"),
            ("\\", r"\\"),
        ]:
            result = result.replace(char, escape)

        for char in result:
            if ord(char) > 127:
                result = result.replace(char, f"\\u{ord(char):04x}")

        return result

    def _set_property(self, key: str, value: str) -> None:
        """Set a property, decoding escape sequences."""
        self._properties[key] = self._decode_value(value)

    def get(
        self,
        key: str,
        default: Optional[str] = None
    ) -> Optional[str]:
        """Get a property value."""
        return self._properties.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a property value."""
        self._properties[key] = str(value)

    def remove(self, key: str) -> bool:
        """Remove a property."""
        if key in self._properties:
            del self._properties[key]
            return True
        return False

    def keys(self) -> List[str]:
        """Get all property keys."""
        return list(self._properties.keys())

    def items(self) -> List[tuple]:
        """Get all key-value pairs."""
        return list(self._properties.items())

    def update(self, data: Dict[str, Any]) -> None:
        """Update multiple properties at once."""
        for key, value in data.items():
            self._properties[key] = str(value)

    def filter_keys(self, prefix: str) -> Dict[str, str]:
        """Get all properties with keys starting with a prefix."""
        return {
            k: v
            for k, v in self._properties.items()
            if k.startswith(prefix)
        }

    def diff(
        self,
        other: Dict[str, str]
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """
        Compare properties with another dictionary.

        Returns:
            Dictionary with 'added', 'removed', and 'changed' keys.
        """
        all_keys = set(self._properties.keys()) | set(other.keys())
        result: Dict[str, Dict[str, Optional[str]]] = {
            "added": {},
            "removed": {},
            "changed": {},
        }

        for key in all_keys:
            left = self._properties.get(key)
            right = other.get(key)

            if left is None and right is not None:
                result["added"][key] = right
            elif left is not None and right is None:
                result["removed"][key] = left
            elif left != right:
                result["changed"][key] = {"left": left, "right": right}

        return result


def parse_properties_file(path: Union[str, Path]) -> Dict[str, str]:
    """
    Convenience function to parse a properties file.

    Args:
        path: Path to the properties file.

    Returns:
        Parsed properties dictionary.
    """
    return PropertiesHandler().parse_file(path)


def write_properties_file(
    path: Union[str, Path],
    data: Dict[str, Any]
) -> None:
    """
    Convenience function to write a properties file.

    Args:
        path: Destination file path.
        data: Properties dictionary.
    """
    PropertiesHandler().write_file(path, data)
