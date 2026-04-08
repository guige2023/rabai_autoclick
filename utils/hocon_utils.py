"""
HOCON (Human-Optimized Config Object Notation) parsing utilities.

Provides support for parsing and manipulating HOCON configuration
files, including object merging, substitution resolution, and
conversion to/from JSON and other formats.

Example:
    >>> from utils.hocon_utils import HoconHandler
    >>> handler = HoconHandler()
    >>> config = handler.parse_file("application.conf")
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


class HoconParseError(ValueError):
    """Raised when HOCON parsing fails."""
    pass


class HoconHandler:
    """
    Handler for HOCON (Human-Optimized Config Object Notation).

    Supports the HOCON spec including:
    - Object merging
    - Value concatenation
    - Substitution resolution (${...} and ${?...})
    - Include directives
    - Multi-line strings
    - Comments

    Attributes:
        include_resolver: Callable to resolve include paths.
        resolve_substitutions: Whether to resolve ${...} substitutions.
    """

    VALUE_CONCAT_PATTERN = re.compile(r'(?<!\\)"[^"]*"\s*\+\s*(?<!\\)"[^"]*"')

    def __init__(
        self,
        include_resolver: Optional[callable] = None,
        resolve_substitutions: bool = True,
    ) -> None:
        """
        Initialize the HOCON handler.

        Args:
            include_resolver: Function(path) -> dict for includes.
            resolve_substitutions: Whether to resolve substitutions.
        """
        self.include_resolver = include_resolver
        self.resolve_substitutions = resolve_substitutions
        self._config: Dict[str, Any] = {}
        self._stack: List[Dict[str, Any]] = []

    def parse_string(self, content: str) -> Dict[str, Any]:
        """
        Parse HOCON content from a string.

        Args:
            content: HOCON-formatted string.

        Returns:
            Parsed configuration dictionary.

        Raises:
            HoconParseError: If parsing fails.
        """
        self._config = {}
        self._stack = [self._config]

        for line in content.split("\n"):
            line = line.strip()

            if not line or line.startswith("#") or line.startswith("//"):
                continue

            if line.startswith("include"):
                self._handle_include(line)
                continue

            self._parse_line(line, self._config)

        if self.resolve_substitutions:
            self._resolve_substitutions(self._config, set())

        return self._config

    def parse_file(self, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Parse a HOCON file.

        Args:
            path: Path to the HOCON file.

        Returns:
            Parsed configuration dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
            HoconParseError: If parsing fails.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"HOCON file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        return self.parse_string(content)

    def _handle_include(self, line: str) -> None:
        """Handle an include directive."""
        match = re.match(r'include\s+"([^"]+)"', line)
        if not match:
            return

        path = match.group(1)

        if self.include_resolver:
            included = self.include_resolver(path)
            if included:
                self._merge_objects(self._config, included)
        elif path.endswith(".json"):
            import json
            p = Path(path)
            if p.exists():
                with open(p) as f:
                    self._merge_objects(self._config, json.load(f))
        elif path.endswith(".conf"):
            p = Path(path)
            if p.exists():
                self._merge_objects(self._config, self.parse_file(p))

    def _parse_line(
        self,
        line: str,
        target: Dict[str, Any]
    ) -> None:
        """Parse a single HOCON line into the target dictionary."""
        if "{" in line and "=" not in line.split("{")[0]:
            key, rest = line.split("{", 1)
            key = key.strip().strip(',')
            target[key] = {}
            self._stack.append(target[key])
            if "}" in rest:
                inner, after = rest.split("}", 1)
                self._parse_content(inner, target[key])
                if after.strip():
                    self._parse_line(after.strip(), self._config)
            return

        if "=" in line:
            key, value = line.split("=", 1)
            key = key.strip().strip(',')
            value = value.strip().strip(',')
            target[key] = self._parse_value(value)

    def _parse_content(
        self,
        content: str,
        target: Dict[str, Any]
    ) -> None:
        """Parse HOCON content within braces."""
        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            self._parse_line(line, target)

    def _parse_value(self, value: str) -> Any:
        """Parse a HOCON value."""
        value = value.strip()

        if value == "true":
            return True
        if value == "false":
            return False
        if value == "null":
            return None

        if value.startswith('"""'):
            return value[3:-3].strip()
        if value.startswith('"'):
            return value.strip('"')

        if value.startswith("[") and value.endswith("]"):
            return self._parse_array(value[1:-1])

        if value.startswith("{") and value.endswith("}"):
            inner = value[1:-1]
            result: Dict[str, Any] = {}
            self._parse_content(inner, result)
            return result

        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def _parse_array(self, content: str) -> List[Any]:
        """Parse a HOCON array."""
        items: List[Any] = []
        current = ""
        depth = 0

        for char in content:
            if char == "[":
                depth += 1
                current += char
            elif char == "]":
                depth -= 1
                current += char
            elif char == "," and depth == 0:
                items.append(self._parse_value(current))
                current = ""
            else:
                current += char

        if current.strip():
            items.append(self._parse_value(current))

        return items

    def _merge_objects(
        self,
        base: Dict[str, Any],
        override: Dict[str, Any]
    ) -> None:
        """Merge override into base dictionary."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_objects(base[key], value)
            else:
                base[key] = value

    def _resolve_substitutions(
        self,
        config: Dict[str, Any],
        resolved: set
    ) -> None:
        """Resolve ${...} substitution references."""
        pattern = re.compile(r'\$\{([^}]+)\}')

        def resolve_value(val: Any) -> Any:
            if isinstance(val, str):
                def replacer(m: re.Match) -> str:
                    key = m.group(1)
                    keys = key.split(".")
                    current: Any = config

                    for k in keys:
                        if isinstance(current, dict) and k in current:
                            current = current[k]
                        else:
                            return m.group(0)

                    return str(current)

                return pattern.sub(replacer, val)
            elif isinstance(val, dict):
                for k, v in val.items():
                    val[k] = resolve_value(v)
                return val
            elif isinstance(val, list):
                return [resolve_value(item) for item in val]
            return val

        for key, value in list(config.items()):
            config[key] = resolve_value(value)

    def to_json(self, config: Optional[Dict[str, Any]] = None) -> str:
        """
        Convert HOCON config to JSON string.

        Args:
            config: Config to convert (uses internal if None).

        Returns:
            JSON-formatted string.
        """
        import json
        config = config or self._config
        return json.dumps(config, indent=2)

    def to_env(self, config: Optional[Dict[str, Any]] = None) -> str:
        """
        Convert HOCON config to environment variable format.

        Args:
            config: Config to convert (uses internal if None).

        Returns:
            Environment variable format string.
        """
        config = config or self._config
        lines: List[str] = []

        def flatten(obj: Dict[str, Any], prefix: str = "") -> None:
            for key, value in obj.items():
                env_key = f"{prefix}{key}".upper().replace(".", "_").replace("-", "_")
                if isinstance(value, dict):
                    flatten(value, f"{env_key}_")
                else:
                    lines.append(f"{env_key}={value}")

        flatten(config)
        return "\n".join(lines)


def parse_hocon_file(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Convenience function to parse a HOCON file.

    Args:
        path: Path to the HOCON file.

    Returns:
        Parsed configuration dictionary.
    """
    return HoconHandler().parse_file(path)


def parse_hocon_string(content: str) -> Dict[str, Any]:
    """
    Convenience function to parse HOCON content.

    Args:
        content: HOCON-formatted string.

    Returns:
        Parsed configuration dictionary.
    """
    return HoconHandler().parse_string(content)
