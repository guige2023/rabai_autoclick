"""Data anonymization action for privacy-preserving data processing.

Anonymizes sensitive data fields with techniques including
hashing, pseudonymization, and data masking.
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class AnonymizationTechnique(Enum):
    """Data anonymization techniques."""
    HASH = "hash"
    MASK = "mask"
    PSEUDONYMIZE = "pseudonymize"
    REDACT = "redact"
    GENERALIZE = "generalize"


@dataclass
class FieldRule:
    """Anonymization rule for a field."""
    field_name: str
    technique: AnonymizationTechnique
    salt: str = ""
    mask_char: str = "*"
    keep_first: int = 0
    keep_last: int = 0


@dataclass
class AnonymizationResult:
    """Result of anonymization operation."""
    success: bool
    data: Any
    fields_anonymized: int
    processing_time_ms: float
    error: Optional[str] = None


@dataclass
class AnonymizationStats:
    """Statistics for anonymization operations."""
    records_processed: int = 0
    fields_anonymized: int = 0
    errors: int = 0


class DataAnonymizationAction:
    """Anonymize sensitive data fields.

    Example:
        >>> anon = DataAnonymizationAction()
        >>> anon.add_rule("email", AnonymizationTechnique.HASH, salt="secret")
        >>> result = anon.anonymize(data)
    """

    def __init__(self, default_salt: str = "") -> None:
        self.default_salt = default_salt
        self._rules: dict[str, FieldRule] = {}
        self._stats = AnonymizationStats()

    def add_rule(
        self,
        field_name: str,
        technique: AnonymizationTechnique,
        salt: Optional[str] = None,
        mask_char: str = "*",
        keep_first: int = 0,
        keep_last: int = 0,
    ) -> "DataAnonymizationAction":
        """Add an anonymization rule for a field.

        Args:
            field_name: Name of field to anonymize.
            technique: Anonymization technique to use.
            salt: Optional salt for hashing.
            mask_char: Character to use for masking.
            keep_first: Number of first characters to keep.
            keep_last: Number of last characters to keep.

        Returns:
            Self for method chaining.
        """
        rule = FieldRule(
            field_name=field_name,
            technique=technique,
            salt=salt or self.default_salt,
            mask_char=mask_char,
            keep_first=keep_first,
            keep_last=keep_last,
        )
        self._rules[field_name] = rule
        return self

    def add_pattern_rule(
        self,
        pattern: str,
        technique: AnonymizationTechnique,
        salt: Optional[str] = None,
    ) -> "DataAnonymizationAction":
        """Add a rule based on field name pattern.

        Args:
            pattern: Regex pattern for field names.
            technique: Anonymization technique.
            salt: Optional salt for hashing.

        Returns:
            Self for method chaining.
        """
        self._rules[f"__pattern__{pattern}"] = FieldRule(
            field_name=pattern,
            technique=technique,
            salt=salt or self.default_salt,
        )
        return self

    def anonymize(self, data: Any) -> AnonymizationResult:
        """Anonymize data based on configured rules.

        Args:
            data: Data to anonymize (dict, list, or string).

        Returns:
            Anonymization result.
        """
        import time
        start_time = time.time()

        try:
            fields_count = 0

            if isinstance(data, dict):
                result, count = self._anonymize_dict(data)
                fields_count = count
            elif isinstance(data, list):
                result, count = self._anonymize_list(data)
                fields_count = count
            elif isinstance(data, str):
                result = self._anonymize_string(data, FieldRule(
                    field_name="value",
                    technique=AnonymizationTechnique.HASH,
                    salt=self.default_salt,
                ))
                fields_count = 1
            else:
                result = data

            self._stats.records_processed += 1
            self._stats.fields_anonymized += fields_count

            return AnonymizationResult(
                success=True,
                data=result,
                fields_anonymized=fields_count,
                processing_time_ms=(time.time() - start_time) * 1000,
            )

        except Exception as e:
            self._stats.errors += 1
            logger.error(f"Anonymization failed: {e}")
            return AnonymizationResult(
                success=False,
                data=data,
                fields_anonymized=0,
                processing_time_ms=(time.time() - start_time) * 1000,
                error=str(e),
            )

    def _anonymize_dict(self, data: dict) -> tuple[dict, int]:
        """Anonymize a dictionary.

        Args:
            data: Dictionary to anonymize.

        Returns:
            Tuple of (anonymized dict, fields count).
        """
        result = {}
        fields_count = 0

        for key, value in data.items():
            rule = self._get_rule(key)

            if rule:
                result[key] = self._apply_technique(value, rule)
                fields_count += 1
            elif isinstance(value, dict):
                result[key], sub_count = self._anonymize_dict(value)
                fields_count += sub_count
            elif isinstance(value, list):
                result[key], sub_count = self._anonymize_list(value)
                fields_count += sub_count
            else:
                result[key] = value

        return result, fields_count

    def _anonymize_list(self, data: list) -> tuple[list, int]:
        """Anonymize a list of items.

        Args:
            data: List to anonymize.

        Returns:
            Tuple of (anonymized list, fields count).
        """
        result = []
        fields_count = 0

        for item in data:
            if isinstance(item, dict):
                anonymized, count = self._anonymize_dict(item)
                result.append(anonymized)
                fields_count += count
            elif isinstance(item, list):
                anonymized, count = self._anonymize_list(item)
                result.append(anonymized)
                fields_count += count
            else:
                result.append(item)

        return result, fields_count

    def _get_rule(self, field_name: str) -> Optional[FieldRule]:
        """Get rule for a field name.

        Args:
            field_name: Field name to look up.

        Returns:
            Field rule if found.
        """
        if field_name in self._rules:
            return self._rules[field_name]

        for pattern, rule in self._rules.items():
            if pattern.startswith("__pattern__"):
                regex_pattern = pattern[11:]
                if re.match(regex_pattern, field_name):
                    return rule

        return None

    def _apply_technique(self, value: Any, rule: FieldRule) -> Any:
        """Apply anonymization technique.

        Args:
            value: Value to anonymize.
            rule: Rule to apply.

        Returns:
            Anonymized value.
        """
        if value is None:
            return None

        if rule.technique == AnonymizationTechnique.HASH:
            return self._hash_value(str(value), rule.salt)

        elif rule.technique == AnonymizationTechnique.MASK:
            return self._mask_value(str(value), rule)

        elif rule.technique == AnonymizationTechnique.PSEUDONYMIZE:
            return self._pseudonymize(str(value), rule.salt)

        elif rule.technique == AnonymizationTechnique.REDACT:
            return "[REDACTED]"

        elif rule.technique == AnonymizationTechnique.GENERALIZE:
            return self._generalize_value(str(value))

        return value

    def _hash_value(self, value: str, salt: str) -> str:
        """Hash a value with salt.

        Args:
            value: Value to hash.
            salt: Salt for hashing.

        Returns:
            Hashed value (first 16 chars of hex).
        """
        combined = f"{salt}{value}".encode("utf-8")
        return hashlib.sha256(combined).hexdigest()[:16]

    def _mask_value(self, value: str, rule: FieldRule) -> str:
        """Mask a value keeping some characters visible.

        Args:
            value: Value to mask.
            rule: Masking rule.

        Returns:
            Masked value.
        """
        if not value:
            return value

        keep_first = min(rule.keep_first, len(value) // 2)
        keep_last = min(rule.keep_last, len(value) // 2)
        mask_length = len(value) - keep_first - keep_last

        if mask_length <= 0:
            return value

        masked = rule.mask_char * mask_length
        return f"{value[:keep_first]}{masked}{value[-keep_last:] if keep_last else ''}"

    def _pseudonymize(self, value: str, salt: str) -> str:
        """Pseudonymize a value (consistent hash for same input).

        Args:
            value: Value to pseudonymize.
            salt: Salt for consistency.

        Returns:
            Pseudonymized value.
        """
        return self._hash_value(value, salt)

    def _generalize_value(self, value: str) -> str:
        """Generalize a value to a category.

        Args:
            value: Value to generalize.

        Returns:
            Generalized value.
        """
        import re

        if re.match(r"^\d{3}-\d{3}-\d{4}$", value):
            return "XXX-XX-XXXX"

        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return "XXXX-XX-XX"

        if re.match(r"^[\w.-]+@[\w.-]+\.\w+$", value):
            return "user@domain.example"

        if re.match(r"^\d+\.\d+$", value):
            return "[numeric]"

        if value.lower() in ("true", "false"):
            return "[boolean]"

        if len(value) <= 3:
            return "XXX"

        return f"{value[0]}XXX"

    def _anonymize_string(self, value: str, rule: FieldRule) -> str:
        """Anonymize a string value.

        Args:
            value: String to anonymize.
            rule: Rule to apply.

        Returns:
            Anonymized string.
        """
        return self._apply_technique(value, rule)

    def get_stats(self) -> AnonymizationStats:
        """Get anonymization statistics.

        Returns:
            Current statistics.
        """
        return self._stats
