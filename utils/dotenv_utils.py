"""
Dotenv (.env) file parsing and environment variable utilities.

Provides support for reading, writing, and manipulating .env files
with variable interpolation, type casting, and schema validation.

Example:
    >>> from utils.dotenv_utils import load_env, set_env_from_file
    >>> load_env(".env")
    >>> config = read_env_file(".env")
"""

from __future__ import annotations

import os
import re
import shlex
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


class DotenvHandler:
    """
    Handler for .env files with interpolation and type support.

    Supports:
    - Quoted and unquoted values
    - Variable interpolation: ${VAR_NAME}
    - Multiple value types (string, int, float, bool, list)
    - Comment lines
    - Export prefix

    Attributes:
        interpolation: Enable ${VAR} interpolation.
        cast_types: Automatically cast values to Python types.
    """

    BOOL_TRUE = ("true", "yes", "1", "on")
    BOOL_FALSE = ("false", "no", "0", "off")

    def __init__(
        self,
        interpolation: bool = True,
        cast_types: bool = True,
    ) -> None:
        """
        Initialize the dotenv handler.

        Args:
            interpolation: Enable ${VAR} interpolation.
            cast_types: Automatically cast values to Python types.
        """
        self.interpolation = interpolation
        self.cast_types = cast_types
        self._data: Dict[str, str] = {}

    def parse_string(self, content: str) -> Dict[str, str]:
        """
        Parse dotenv content from a string.

        Args:
            content: Dotenv-formatted string.

        Returns:
            Dictionary of variable key-value pairs.
        """
        self._data.clear()

        for line in content.split("\n"):
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            line = re.sub(r"^export\s+", "", line)

            if "=" not in line:
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()

            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]

            value = self._unescape_value(value)
            self._data[key] = value

        if self.interpolation:
            self._data = self._interpolate(self._data)

        return dict(self._data)

    def parse_file(self, path: Union[str, Path]) -> Dict[str, str]:
        """
        Parse a dotenv file.

        Args:
            path: Path to the .env file.

        Returns:
            Dictionary of variable key-value pairs.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f".env file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            return self.parse_string(f.read())

    def write_file(
        self,
        path: Union[str, Path],
        data: Optional[Dict[str, Any]] = None,
        include_comments: bool = False,
    ) -> None:
        """
        Write variables to a dotenv file.

        Args:
            path: Destination file path.
            data: Variables to write (uses internal state if None).
            include_comments: Include variable type comments.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = data or self._data
        lines: List[str] = []

        for key, value in data.items():
            if include_comments and self.cast_types:
                lines.append(f"# {key}={self._infer_type(value)}")
            lines.append(f'{key}="{self._escape_value(str(value))}"')

        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    def _unescape_value(self, value: str) -> str:
        """Unescape special characters in a value."""
        value = value.replace("\\n", "\n")
        value = value.replace("\\t", "\t")
        value = value.replace("\\r", "\r")
        value = value.replace('\\"', '"')
        value = value.replace("\\'", "'")
        value = value.replace("\\\\", "\\")
        return value

    def _escape_value(self, value: str) -> str:
        """Escape special characters for writing."""
        value = value.replace("\\", "\\\\")
        value = value.replace('"', '\\"')
        value = value.replace("\n", "\\n")
        value = value.replace("\t", "\\t")
        value = value.replace("\r", "\\r")
        return value

    def _infer_type(self, value: str) -> str:
        """Infer the type of a value."""
        if value.lower() in self.BOOL_TRUE:
            return "bool"
        if value.lower() in self.BOOL_FALSE:
            return "bool"
        if value.startswith("[") and value.endswith("]"):
            return "list"
        try:
            int(value)
            return "int"
        except ValueError:
            pass
        try:
            float(value)
            return "float"
        except ValueError:
            pass
        return "str"

    def _interpolate(self, data: Dict[str, str]) -> Dict[str, str]:
        """Resolve ${VAR} interpolations."""
        pattern = re.compile(r"\$\{([^}]+)\}")
        result: Dict[str, str] = {}

        for key, value in data.items():
            def replacer(m: re.Match) -> str:
                var_name = m.group(1)
                if var_name in data:
                    return data[var_name]
                return os.environ.get(var_name, m.group(0))

            result[key] = pattern.sub(replacer, value)

        return result

    def cast_value(self, value: str) -> Any:
        """
        Cast a string value to its Python type.

        Args:
            value: String value to cast.

        Returns:
            Value cast to appropriate Python type.
        """
        value = value.strip()

        if value.lower() in self.BOOL_TRUE:
            return True
        if value.lower() in self.BOOL_FALSE:
            return False

        if value.startswith("[") and value.endswith("]"):
            inner = value[1:-1]
            if inner:
                items = [item.strip().strip("'\"") for item in inner.split(",")]
                return [self.cast_value(item) for item in items]
            return []

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        return value

    def to_dict(self, cast_types: bool = False) -> Dict[str, Any]:
        """
        Get parsed variables as a dictionary.

        Args:
            cast_types: Cast values to Python types.

        Returns:
            Dictionary of variables.
        """
        if cast_types:
            return {k: self.cast_value(v) for k, v in self._data.items()}
        return dict(self._data)


def load_env(
    path: Union[str, Path] = ".env",
    override: bool = True,
    interpolate: bool = True,
) -> Dict[str, str]:
    """
    Load a .env file into environment variables.

    Args:
        path: Path to the .env file.
        override: Override existing environment variables.
        interpolate: Enable variable interpolation.

    Returns:
        Dictionary of loaded variables.
    """
    handler = DotenvHandler(interpolation=interpolate)
    data = handler.parse_file(path)

    for key, value in data.items():
        if override or key not in os.environ:
            os.environ[key] = value

    return data


def read_env_file(
    path: Union[str, Path] = ".env",
    cast_types: bool = False,
) -> Dict[str, Any]:
    """
    Read a .env file as a dictionary.

    Args:
        path: Path to the .env file.
        cast_types: Cast values to Python types.

    Returns:
        Dictionary of variables.
    """
    handler = DotenvHandler()
    data = handler.parse_file(path)
    if cast_types:
        return {k: handler.cast_value(v) for k, v in data.items()}
    return data


def write_env_file(
    path: Union[str, Path],
    data: Dict[str, Any],
) -> None:
    """
    Write variables to a .env file.

    Args:
        path: Destination file path.
        data: Dictionary of variables to write.
    """
    DotenvHandler().write_file(path, data)


def get_env(
    key: str,
    default: Any = None,
    cast_type: Optional[type] = None,
) -> Any:
    """
    Get an environment variable with optional type casting.

    Args:
        key: Environment variable name.
        default: Default value if not found.
        cast_type: Type to cast the value to.

    Returns:
        Environment variable value or default.
    """
    value = os.environ.get(key)
    if value is None:
        return default

    if cast_type == bool:
        return value.lower() in DotenvHandler.BOOL_TRUE
    if cast_type == int:
        try:
            return int(value)
        except ValueError:
            return default
    if cast_type == float:
        try:
            return float(value)
        except ValueError:
            return default

    return value
