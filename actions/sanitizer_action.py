"""sanitizer_action module for rabai_autoclick.

Provides sanitization utilities: input validation, output encoding,
SQL/HTML/JSON sanitization, and security helpers.
"""

from __future__ import annotations

import html
import json
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, List, Optional, Pattern

__all__ = [
    "Sanitizer",
    "InputSanitizer",
    "HTMLSanitizer",
    "SQLSanitizer",
    "JSONSanitizer",
    "URLSanitizer",
    "EmailSanitizer",
    "SanitizerConfig",
    "SanitizeResult",
    "validate_input",
    "sanitize_html",
    "sanitize_sql",
    "sanitize_json",
    "escape_html",
    "unescape_html",
]


@dataclass
class SanitizerConfig:
    """Configuration for sanitizers."""
    allow_tags: List[str] = None
    strip_tags: bool = True
    escape_quotes: bool = True
    max_length: Optional[int] = None
    strip_newlines: bool = False

    def __post_init__(self) -> None:
        if self.allow_tags is None:
            self.allow_tags = []


@dataclass
class SanitizeResult:
    """Result of sanitization operation."""
    value: str
    sanitized: bool
    errors: List[str]

    def __bool__(self) -> bool:
        return not bool(self.errors)


class Sanitizer:
    """Base sanitizer class."""

    def __init__(self, config: Optional[SanitizerConfig] = None) -> None:
        self.config = config or SanitizerConfig()

    def sanitize(self, value: str) -> SanitizeResult:
        """Sanitize value. Override in subclasses."""
        raise NotImplementedError

    def _apply_config(self, value: str) -> str:
        """Apply common config options."""
        if self.config.max_length and len(value) > self.config.max_length:
            value = value[: self.config.max_length]
        if self.config.strip_newlines:
            value = re.sub(r"[\r\n]+", "", value)
        return value


class HTMLSanitizer(Sanitizer):
    """Sanitizes HTML content."""

    def __init__(self, config: Optional[SanitizerConfig] = None) -> None:
        super().__init__(config)
        self._allowed_tags = set(self.config.allow_tags) if self.config.allow_tags else set()
        self._block_tags = {
            "script", "style", "iframe", "object", "embed",
            "form", "input", "button", "select", "textarea",
        }

    def sanitize(self, value: str) -> SanitizeResult:
        """Sanitize HTML content."""
        errors: List[str] = []
        sanitized_value = value

        if self.config.strip_tags:
            sanitized_value = self._strip_tags(sanitized_value)
        else:
            sanitized_value = self._escape_unsafe(sanitized_value)

        sanitized_value = self._apply_config(sanitized_value)

        if not sanitized_value:
            sanitized_value = ""

        return SanitizeResult(
            value=sanitized_value,
            sanitized=sanitized_value != value,
            errors=errors,
        )

    def _strip_tags(self, value: str) -> str:
        """Strip disallowed HTML tags."""
        if self._allowed_tags:
            pattern = re.compile(
                r"<(/?)([\w]+)[^>]*(/?)>",
                re.IGNORECASE,
            )
            def replace_tag(match: re.Match) -> str:
                tag = match.group(2).lower()
                if tag in self._allowed_tags:
                    return match.group(0)
                return ""
            return pattern.sub(replace_tag, value)
        else:
            return re.sub(r"<[^>]+>", "", value)

    def _escape_unsafe(self, value: str) -> str:
        """Escape unsafe HTML while allowing safe tags."""
        safe_tags = self._allowed_tags - self._block_tags
        for tag in safe_tags:
            value = re.sub(
                rf"<({tag})([^>]*)>",
                lambda m: f"<{tag}{m.group(2)}>",
                value,
                flags=re.IGNORECASE,
            )
        value = re.sub(r"<script[^>]*>.*?</script>", "", value, flags=re.I | re.S)
        value = re.sub(r"on\w+\s*=\s*['\"][^'\"]*['\"]", "", value, flags=re.I)
        return value


class SQLSanitizer(Sanitizer):
    """Sanitizes SQL queries."""

    def __init__(self, config: Optional[SanitizerConfig] = None) -> None:
        super().__init__(config)
        self._dangerous_patterns = [
            r"(\bOR\b|\bAND\b)\s*\d+\s*[=<>]\s*\d+",
            r"'(?:\\.|[^'\\])*'",
            r";\s*(DROP|DELETE|TRUNCATE|ALTER|CREATE|INSERT|UPDATE)\b",
            r"--",
            r"/\*.*?\*/",
            r"xp_",
            r"0x[0-9a-fA-F]+",
        ]
        self._compiled: List[Pattern] = [
            re.compile(p, re.IGNORECASE | re.DOTALL)
            for p in self._dangerous_patterns
        ]

    def sanitize(self, value: str) -> SanitizeResult:
        """Sanitize SQL input."""
        errors: List[str] = []
        sanitized_value = value

        for i, pattern in enumerate(self._compiled):
            matches = pattern.findall(sanitized_value)
            if matches:
                errors.append(f"Potential SQL injection pattern {i+1} detected")

        sanitized_value = sanitized_value.replace("'", "''")
        sanitized_value = sanitized_value.replace("\\", "\\\\")
        sanitized_value = self._apply_config(sanitized_value)

        return SanitizeResult(
            value=sanitized_value,
            sanitized=sanitized_value != value or bool(errors),
            errors=errors,
        )

    def is_safe_identifier(self, identifier: str) -> bool:
        """Check if identifier is safe for SQL."""
        return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier))


class JSONSanitizer(Sanitizer):
    """Sanitizes JSON input."""

    def sanitize(self, value: str) -> SanitizeResult:
        """Sanitize JSON string."""
        errors: List[str] = []
        sanitized_value = value

        try:
            parsed = json.loads(sanitized_value)
            sanitized_value = json.dumps(parsed)
            sanitized = False
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON: {e}")
            sanitized = True
            sanitized_value = self._apply_config(sanitized_value)

        return SanitizeResult(
            value=sanitized_value,
            sanitized=sanitized,
            errors=errors,
        )


class URLSanitizer(Sanitizer):
    """Sanitizes URLs."""

    def __init__(self, config: Optional[SanitizerConfig] = None) -> None:
        super().__init__(config)
        self._allowed_schemes = {"http", "https", "mailto", "tel"}

    def sanitize(self, value: str) -> SanitizeResult:
        """Sanitize URL."""
        errors: List[str] = []
        sanitized_value = value.strip()

        sanitized_value = self._apply_config(sanitized_value)

        try:
            parsed = urllib.parse.urlparse(sanitized_value)
            if parsed.scheme and parsed.scheme.lower() not in self._allowed_schemes:
                errors.append(f"Disallowed URL scheme: {parsed.scheme}")
                sanitized_value = ""
            if "\n" in sanitized_value or "\r" in sanitized_value:
                errors.append("URL contains newline characters")
                sanitized_value = re.sub(r"[\r\n]", "", sanitized_value)
        except Exception as e:
            errors.append(f"Invalid URL: {e}")

        return SanitizeResult(
            value=sanitized_value,
            sanitized=bool(errors),
            errors=errors,
        )


class EmailSanitizer(Sanitizer):
    """Sanitizes email addresses."""

    _EMAIL_PATTERN = re.compile(
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )

    def sanitize(self, value: str) -> SanitizeResult:
        """Sanitize email address."""
        errors: List[str] = []
        sanitized_value = value.strip().lower()

        sanitized_value = self._apply_config(sanitized_value)

        if not self._EMAIL_PATTERN.match(sanitized_value):
            errors.append("Invalid email format")

        return SanitizeResult(
            value=sanitized_value,
            sanitized=value != sanitized_value,
            errors=errors,
        )


class InputSanitizer:
    """High-level input sanitization."""

    def __init__(self) -> None:
        self._sanitizers = {
            "html": HTMLSanitizer(),
            "sql": SQLSanitizer(),
            "json": JSONSanitizer(),
            "url": URLSanitizer(),
            "email": EmailSanitizer(),
        }

    def sanitize(self, value: str, kind: str) -> SanitizeResult:
        """Sanitize value of given kind."""
        if kind in self._sanitizers:
            return self._sanitizers[kind].sanitize(value)
        return SanitizeResult(value=value, sanitized=False, errors=[])

    def add_sanitizer(self, name: str, sanitizer: Sanitizer) -> None:
        """Add custom sanitizer."""
        self._sanitizers[name] = sanitizer


def sanitize_html(value: str) -> str:
    """Quick HTML sanitization."""
    return HTMLSanitizer().sanitize(value).value


def sanitize_sql(value: str) -> str:
    """Quick SQL sanitization."""
    return SQLSanitizer().sanitize(value).value


def sanitize_json(value: str) -> str:
    """Quick JSON sanitization."""
    return JSONSanitizer().sanitize(value).value


def validate_input(value: str, kind: str) -> bool:
    """Validate input of given kind."""
    sanitizer = InputSanitizer()
    result = sanitizer.sanitize(value, kind)
    return bool(result)


def escape_html(value: str) -> str:
    """Escape HTML special characters."""
    return html.escape(value)


def unescape_html(value: str) -> str:
    """Unescape HTML special characters."""
    return html.unescape(value)
