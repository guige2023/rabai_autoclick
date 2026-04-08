"""
INI file parsing, manipulation, and serialization utilities.

Provides comprehensive support for reading, writing, and manipulating
INI configuration files with support for sections, comments,
interpolation, and multi-value options.

Example:
    >>> from utils.ini_utils import IniHandler
    >>> handler = IniHandler()
    >>> config = handler.parse_file("settings.ini")
    >>> handler.set_value("database", "host", "localhost")
"""

from __future__ import annotations

import configparser
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


class IniHandler:
    """
    Comprehensive INI file handler with parsing, validation, and serialization.

    Wraps Python's configparser with additional utilities for
    type conversion, comment preservation, and nested structures.

    Attributes:
        interpolation: Enable variable interpolation (e.g., %(var)s).
        allow_no_value: Allow keys without values.
        delimiters: Key-value delimiters.
        comment_prefixes: Comment line prefixes.
    """

    def __init__(
        self,
        interpolation: bool = True,
        allow_no_value: bool = False,
        delimiters: tuple = ("=", ":"),
        comment_prefixes: tuple = (";", "#"),
    ) -> None:
        """
        Initialize the INI handler.

        Args:
            interpolation: Enable %(variable)s interpolation.
            allow_no_value: Allow keys without values.
            delimiters: Key-value delimiter characters.
            comment_prefixes: Characters that start comments.
        """
        self.interpolation = interpolation
        self.allow_no_value = allow_no_value
        self.delimiters = delimiters
        self.comment_prefixes = comment_prefixes
        self._parser = self._create_parser()

    def _create_parser(self) -> configparser.ConfigParser:
        """Create a configured ConfigParser instance."""
        kwargs: Dict[str, Any] = {
            "interpolation": (
                configparser.BasicInterpolation()
                if self.interpolation
                else configparser.RawConfigParser()
            ),
            "allow_no_value": self.allow_no_value,
            "delimiters": self.delimiters,
            "comment_prefixes": self.comment_prefixes,
        }
        return configparser.ConfigParser(**kwargs)

    def parse_string(self, content: str) -> Dict[str, Dict[str, str]]:
        """
        Parse INI content from a string.

        Args:
            content: INI-formatted string.

        Returns:
            Dictionary mapping section names to key-value dictionaries.
        """
        self._parser.clear()
        self._parser.read_string(content)
        return self._to_dict()

    def parse_file(self, path: Union[str, Path]) -> Dict[str, Dict[str, str]]:
        """
        Parse an INI file.

        Args:
            path: Path to the INI file.

        Returns:
            Dictionary mapping section names to key-value dictionaries.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"INI file not found: {path}")

        self._parser.clear()
        self._parser.read(path, encoding="utf-8")
        return self._to_dict()

    def write_file(
        self,
        path: Union[str, Path],
        data: Dict[str, Dict[str, Any]],
        comment: Optional[str] = None
    ) -> None:
        """
        Write data to an INI file.

        Args:
            path: Destination file path.
            data: Dictionary mapping sections to key-value dictionaries.
            comment: Optional file-level comment.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        self._parser.clear()
        for section, options in data.items():
            if not self._parser.has_section(section):
                self._parser.add_section(section)
            for key, value in options.items():
                self._parser.set(section, key, str(value) if value is not None else "")

        with open(path, "w", encoding="utf-8") as f:
            if comment:
                f.write(f"# {comment}\n\n")
            self._parser.write(f)

    def _to_dict(self) -> Dict[str, Dict[str, str]]:
        """Convert parser state to nested dictionary."""
        return {s: dict(self._parser.items(s)) for s in self._parser.sections()}

    def get_sections(self) -> List[str]:
        """Get list of all section names."""
        return self._parser.sections()

    def has_section(self, section: str) -> bool:
        """Check if a section exists."""
        return self._parser.has_section(section)

    def add_section(self, section: str) -> None:
        """Add a new section."""
        if not self._parser.has_section(section):
            self._parser.add_section(section)

    def remove_section(self, section: str) -> bool:
        """Remove a section and all its options."""
        if self._parser.has_section(section):
            return self._parser.remove_section(section)
        return False

    def get_value(
        self,
        section: str,
        option: str,
        fallback: Any = None,
        dtype: type = str
    ) -> Any:
        """
        Get a value with type conversion.

        Args:
            section: Section name.
            option: Option name.
            fallback: Fallback value if not found.
            dtype: Type to convert the value to.

        Returns:
            The value, converted to the specified type.
        """
        if not self._parser.has_option(section, option):
            return fallback

        value = self._parser.get(section, option)

        if dtype == bool:
            return self._parser.getboolean(section, option)
        elif dtype == int:
            return int(value)
        elif dtype == float:
            return float(value)
        elif dtype == str:
            return str(value)
        return value

    def set_value(
        self,
        section: str,
        option: str,
        value: Any
    ) -> None:
        """
        Set a value in a section.

        Args:
            section: Section name.
            option: Option name.
            value: Value to set.
        """
        if not self._parser.has_section(section):
            self._parser.add_section(section)
        self._parser.set(section, option, str(value))

    def remove_option(self, section: str, option: str) -> bool:
        """Remove an option from a section."""
        if self._parser.has_option(section, option):
            return self._parser.remove_option(section, option)
        return False

    def get_section_dict(self, section: str) -> Dict[str, str]:
        """Get all options in a section as a dictionary."""
        if self._parser.has_section(section):
            return dict(self._parser.items(section))
        return {}

    def merge(
        self,
        base: Dict[str, Dict[str, Any]],
        override: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Merge two INI configurations.

        Args:
            base: Base configuration.
            override: Override values (takes precedence).

        Returns:
            Merged configuration.
        """
        result: Dict[str, Dict[str, Any]] = {}

        for section in set(base.keys()) | set(override.keys()):
            result[section] = {}
            if section in base:
                result[section].update(base[section])
            if section in override:
                result[section].update(override[section])

        return result

    def to_string(
        self,
        data: Dict[str, Dict[str, Any]],
        comment: Optional[str] = None
    ) -> str:
        """
        Convert data to INI-formatted string.

        Args:
            data: Dictionary mapping sections to key-value dictionaries.
            comment: Optional file-level comment.

        Returns:
            INI-formatted string.
        """
        self._parser.clear()
        for section, options in data.items():
            if not self._parser.has_section(section):
                self._parser.add_section(section)
            for key, value in options.items():
                self._parser.set(section, key, str(value) if value is not None else "")

        import io
        buffer = io.StringIO()
        if comment:
            buffer.write(f"# {comment}\n\n")
        self._parser.write(buffer)
        return buffer.getvalue()

    def validate_required(
        self,
        data: Dict[str, Dict[str, Any]],
        required_sections: List[str],
        required_options: Dict[str, List[str]]
    ) -> List[str]:
        """
        Validate that required sections and options exist.

        Args:
            data: Parsed INI data.
            required_sections: List of required section names.
            required_options: Dict mapping section names to required option lists.

        Returns:
            List of validation error messages.
        """
        errors: List[str] = []

        for section in required_sections:
            if section not in data:
                errors.append(f"Missing required section: [{section}]")

        for section, options in required_options.items():
            if section not in data:
                continue
            for option in options:
                if option not in data[section]:
                    errors.append(f"Missing required option [{section}] {option}")

        return errors


def parse_ini_file(path: Union[str, Path]) -> Dict[str, Dict[str, str]]:
    """
    Convenience function to parse an INI file.

    Args:
        path: Path to the INI file.

    Returns:
        Parsed configuration dictionary.
    """
    return IniHandler().parse_file(path)


def write_ini_file(
    path: Union[str, Path],
    data: Dict[str, Dict[str, Any]]
) -> None:
    """
    Convenience function to write an INI file.

    Args:
        path: Destination file path.
        data: Configuration dictionary.
    """
    IniHandler().write_file(path, data)


def merge_ini_configs(
    *configs: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    Merge multiple INI configurations.

    Args:
        *configs: Variable number of configuration dictionaries.

    Returns:
        Merged configuration.
    """
    handler = IniHandler()
    result = configs[0] if configs else {}

    for config in configs[1:]:
        result = handler.merge(result, config)

    return result
