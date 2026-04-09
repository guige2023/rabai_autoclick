"""Secure Input Handling Utilities.

This module provides utilities for securely handling user input, including
input sanitization, validation, secure text fields, and input event filtering
to prevent injection attacks and data leakage.

Example:
    >>> from secure_input_utils import InputSanitizer, SecureTextField
    >>> sanitizer = InputSanitizer()
    >>> clean = sanitizer.sanitize(user_input, allow_html=False)
"""

from __future__ import annotations

import re
import html
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Tuple


class SecurityLevel(Enum):
    """Input security levels."""
    NONE = auto()
    BASIC = auto()
    STRICT = auto()
    MAXIMUM = auto()


@dataclass
class ValidationResult:
    """Result of input validation."""
    valid: bool
    sanitized: str = ""
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class InputSanitizer:
    """Sanitizes user input to prevent injection attacks.
    
    Provides configurable sanitization for HTML, SQL, shell commands,
    and other potentially dangerous input patterns.
    
    Attributes:
        security_level: Current security level
        allowed_tags: Set of allowed HTML tags
        blocked_patterns: Set of blocked regex patterns
    """
    
    HTML_TAG_PATTERN: Pattern[str] = re.compile(r'<[^>]+>')
    SQL_INJECTION_PATTERNS: List[Pattern[str]] = [
        re.compile(r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|UNION)\b)", re.IGNORECASE),
        re.compile(r"(--|;|/\*|\*/|@@|@)"),
        re.compile(r"'|=|--"),
    ]
    SHELL_INJECTION_PATTERNS: List[Pattern[str]] = [
        re.compile(r'[;&|`$()]'),
        re.compile(r'\b(cat|ls|rm|wget|curl|bash|sh)\b', re.IGNORECASE),
        re.compile(r'[><]'),
    ]
    
    def __init__(
        self,
        security_level: SecurityLevel = SecurityLevel.STRICT,
        allowed_tags: Optional[Set[str]] = None,
    ):
        self.security_level = security_level
        self.allowed_tags = allowed_tags or {'b', 'i', 'u', 'em', 'strong', 'a', 'p', 'br'}
        self.blocked_patterns: Set[str] = set()
        self._custom_sanitizers: Dict[str, Callable[[str], str]] = {}
    
    def sanitize(self, text: str, allow_html: bool = False) -> str:
        """Sanitize input text.
        
        Args:
            text: Input text to sanitize
            allow_html: Whether to allow HTML tags
            
        Returns:
            Sanitized text
        """
        if not isinstance(text, str):
            text = str(text)
        
        text = self._remove_control_characters(text)
        
        if allow_html:
            text = self._sanitize_html(text)
        else:
            text = self.HTML_TAG_PATTERN.sub('', text)
        
        if self.security_level >= SecurityLevel.BASIC:
            text = self._sanitize_sql(text)
        
        if self.security_level >= SecurityLevel.STRICT:
            text = self._sanitize_shell(text)
            text = self._sanitize_path_traversal(text)
        
        return text
    
    def _remove_control_characters(self, text: str) -> str:
        """Remove control characters from text."""
        return ''.join(c for c in text if ord(c) >= 32 or c in '\n\r\t')
    
    def _sanitize_html(self, text: str) -> str:
        """Sanitize HTML while allowing specified tags."""
        for tag in list(self.allowed_tags):
            text = re.sub(
                rf'<{tag}[^>]*>(.*?)</{tag}>',
                r'<\1>\2</\1>',
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        
        allowed_pattern = re.compile(
            r'<' + '|'.join(self.allowed_tags) + r'[^>]*>.*?</(' + '|'.join(self.allowed_tags) + r')>',
            re.IGNORECASE | re.DOTALL,
        )
        
        text = self.HTML_TAG_PATTERN.sub('', text)
        return text
    
    def _sanitize_sql(self, text: str) -> str:
        """Remove SQL injection patterns."""
        for pattern in self.SQL_INJECTION_PATTERNS:
            text = pattern.sub('', text)
        return text
    
    def _sanitize_shell(self, text: str) -> str:
        """Remove shell injection patterns."""
        for pattern in self.SHELL_INJECTION_PATTERNS:
            text = pattern.sub('', text)
        return text
    
    def _sanitize_path_traversal(self, text: str) -> str:
        """Remove path traversal sequences."""
        patterns = [
            re.compile(r'\.\./'),
            re.compile(r'\.\.'),
            re.compile(r'/etc/passwd', re.IGNORECASE),
            re.compile(r'c:\\', re.IGNORECASE),
        ]
        for pattern in patterns:
            text = pattern.sub('', text)
        return text
    
    def validate(
        self,
        text: str,
        min_length: int = 0,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
    ) -> ValidationResult:
        """Validate input text.
        
        Args:
            text: Input text to validate
            min_length: Minimum allowed length
            max_length: Maximum allowed length
            pattern: Optional regex pattern to match
            
        Returns:
            ValidationResult with validation outcome
        """
        errors: List[str] = []
        warnings: List[str] = []
        sanitized = self.sanitize(text)
        
        if len(text) < min_length:
            errors.append(f"Input too short: {len(text)} < {min_length}")
        
        if max_length and len(text) > max_length:
            errors.append(f"Input too long: {len(text)} > {max_length}")
        
        if pattern:
            try:
                if not re.match(pattern, text):
                    errors.append(f"Input does not match required pattern")
            except re.error:
                warnings.append("Invalid validation pattern")
        
        dangerous_patterns = [
            (r'<script', "Possible XSS: script tag"),
            (r'javascript:', "Possible XSS: javascript protocol"),
            (r'on\w+\s*=', "Possible XSS: event handler"),
            (r'(SELECT|INSERT|UPDATE|DELETE)\s+FROM', "Possible SQL injection",),
        ]
        
        for pattern_str, message in dangerous_patterns:
            if re.search(pattern_str, text, re.IGNORECASE):
                warnings.append(message)
        
        return ValidationResult(
            valid=len(errors) == 0,
            sanitized=sanitized,
            errors=errors,
            warnings=warnings,
            metadata={
                'length': len(text),
                'security_level': self.security_level.name,
            },
        )
    
    def register_custom_sanitizer(self, name: str, func: Callable[[str], str]) -> None:
        """Register a custom sanitizer function.
        
        Args:
            name: Sanitizer name
            func: Sanitizer function
        """
        self._custom_sanitizers[name] = func
    
    def apply_custom(self, name: str, text: str) -> str:
        """Apply a custom sanitizer by name.
        
        Args:
            name: Sanitizer name
            text: Text to sanitize
            
        Returns:
            Sanitized text or original if sanitizer not found
        """
        if name in self._custom_sanitizers:
            return self._custom_sanitizers[name](text)
        return text


class SecureTextField:
    """Secure text input field with validation and masking.
    
    Provides a secure text field that masks input, validates content,
    and can optionally log or reject sensitive patterns.
    
    Attributes:
        field_name: Name identifier for this field
        is_sensitive: Whether field contains sensitive data
        max_length: Maximum allowed input length
    """
    
    SENSITIVE_PATTERNS: List[Pattern[str]] = [
        re.compile(r'\b\d{13,16}\b'),
        re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
        re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
        re.compile(r'password\s*[=:]\s*\S+', re.IGNORECASE),
        re.compile(r'secret\s*[=:]\s*\S+', re.IGNORECASE),
        re.compile(r'token\s*[=:]\s*\S+', re.IGNORECASE),
    ]
    
    def __init__(
        self,
        field_name: str,
        is_sensitive: bool = False,
        max_length: int = 1000,
        validator: Optional[Callable[[str], bool]] = None,
    ):
        self.field_name = field_name
        self.is_sensitive = is_sensitive
        self.max_length = max_length
        self.validator = validator
        self._value: str = ""
        self._is_locked: bool = False
    
    @property
    def value(self) -> str:
        """Get the field value."""
        return self._value
    
    @value.setter
    def value(self, new_value: str) -> None:
        """Set the field value with validation."""
        if self._is_locked:
            raise ValueError("Field is locked")
        
        if len(new_value) > self.max_length:
            raise ValueError(f"Value exceeds max length {self.max_length}")
        
        self._value = new_value
    
    def validate(self) -> ValidationResult:
        """Validate current field value."""
        result = ValidationResult(valid=True, sanitized=self._value)
        
        if not self._value:
            return result
        
        for pattern in self.SENSITIVE_PATTERNS:
            if pattern.search(self._value):
                result.warnings.append(f"Potential sensitive data detected in {self.field_name}")
        
        if self.validator and not self.validator(self._value):
            result.valid = False
            result.errors.append("Validation failed")
        
        return result
    
    def lock(self) -> None:
        """Lock the field preventing further edits."""
        self._is_locked = True
    
    def unlock(self) -> None:
        """Unlock the field allowing edits."""
        self._is_locked = False
    
    def clear(self) -> None:
        """Clear the field value."""
        self._value = ""
    
    def get_masked(self) -> str:
        """Get masked version of the value."""
        if not self._value:
            return ""
        
        if len(self._value) <= 4:
            return "*" * len(self._value)
        
        visible_chars = min(4, len(self._value) // 4)
        return self._value[:visible_chars] + "*" * (len(self._value) - visible_chars)


class InputEventFilter:
    """Filters and sanitizes input events.
    
    Provides real-time filtering of keyboard and mouse input events
    to prevent injection through input channels.
    """
    
    def __init__(self, security_level: SecurityLevel = SecurityLevel.STRICT):
        self.security_level = security_level
        self._blocked_keys: Set[str] = set()
        self._allowed_keys: Set[str] = set()
    
    def filter_key_event(self, key: str, modifiers: List[str]) -> bool:
        """Filter a keyboard event.
        
        Args:
            key: Key identifier
            modifiers: List of active modifiers
            
        Returns:
            True if event should be allowed
        """
        if self._blocked_keys:
            return key not in self._blocked_keys
        
        if self._allowed_keys:
            return key in self._allowed_keys
        
        dangerous_combos = [
            {'cmd', 'c'},
            {'cmd', 'v'},
            {'cmd', 'x'},
        ]
        
        if set(modifiers) | {key} in dangerous_combos:
            return self.security_level < SecurityLevel.MAXIMUM
        
        return True
    
    def filter_mouse_event(self, x: int, y: int, button: int) -> bool:
        """Filter a mouse event.
        
        Args:
            x: X coordinate
            y: Y coordinate
            button: Button identifier
            
        Returns:
            True if event should be allowed
        """
        if abs(x) > 10000 or abs(y) > 10000:
            return False
        
        return True
    
    def block_key(self, key: str) -> None:
        """Block a specific key."""
        self._blocked_keys.add(key)
    
    def allow_key(self, key: str) -> None:
        """Allow only a specific key."""
        self._allowed_keys.add(key)
    
    def reset_filters(self) -> None:
        """Reset all filters."""
        self._blocked_keys.clear()
        self._allowed_keys.clear()
