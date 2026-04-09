"""Data normalization and standardization action."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class NormalizationConfig:
    """Configuration for normalization."""

    input_field: str
    output_field: str
    operation: str  # lowercase, uppercase, trim, strip_html, remove_special, normalize_whitespace, etc.
    custom_func: Optional[Callable[[str], str]] = None


@dataclass
class NormalizationResult:
    """Result of normalization."""

    total_records: int
    success_count: int
    error_count: int
    transformed_count: int
    unchanged_count: int


class DataNormalizerAction:
    """Normalizes and standardizes data values."""

    def __init__(self):
        """Initialize normalizer."""
        self._transformations: dict[str, Callable[[str], str]] = {
            "lowercase": lambda s: s.lower() if s else "",
            "uppercase": lambda s: s.upper() if s else "",
            "titlecase": lambda s: s.title() if s else "",
            "trim": lambda s: s.strip() if s else "",
            "strip_html": self._strip_html,
            "remove_special": self._remove_special_chars,
            "normalize_whitespace": lambda s: " ".join(s.split()) if s else "",
            "remove_digits": lambda s: re.sub(r"\d", "", s) if s else "",
            "remove_alpha": lambda s: re.sub(r"[a-zA-Z]", "", s) if s else "",
            "slugify": self._slugify,
            "camel_to_snake": self._camel_to_snake,
            "snake_to_camel": self._snake_to_camel,
            "remove_punctuation": lambda s: re.sub(r"[^\w\s]", "", s) if s else "",
            "remove_urls": lambda s: re.sub(r"https?://\S+", "", s) if s else "",
            "remove_emails": lambda s: re.sub(r"\S+@\S+", "", s) if s else "",
        }

    def _strip_html(self, s: str) -> str:
        """Remove HTML tags from string."""
        return re.sub(r"<[^>]+>", "", s)

    def _remove_special_chars(self, s: str) -> str:
        """Remove special characters."""
        return re.sub(r"[^a-zA-Z0-9\s]", "", s)

    def _slugify(self, s: str) -> str:
        """Convert string to URL-friendly slug."""
        s = s.lower()
        s = re.sub(r"[^\w\s-]", "", s)
        s = re.sub(r"[-\s]+", "-", s)
        return s.strip("-")

    def _camel_to_snake(self, s: str) -> str:
        """Convert camelCase to snake_case."""
        s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s).lower()

    def _snake_to_camel(self, s: str) -> str:
        """Convert snake_case to camelCase."""
        components = s.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    def normalize_string(
        self,
        value: str,
        operation: str,
        custom_func: Optional[Callable[[str], str]] = None,
    ) -> str:
        """Normalize a single string.

        Args:
            value: Input string.
            operation: Normalization operation name.
            custom_func: Optional custom transformation function.

        Returns:
            Normalized string.
        """
        if value is None:
            return ""

        value = str(value)

        if custom_func:
            return custom_func(value)

        transform = self._transformations.get(operation)
        if transform:
            return transform(value)

        return value

    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number to digits only."""
        if not phone:
            return ""
        return re.sub(r"\D", "", phone)

    def normalize_email(self, email: str) -> str:
        """Normalize email to lowercase and trim."""
        if not email:
            return ""
        return email.lower().strip()

    def normalize_url(self, url: str) -> str:
        """Normalize URL."""
        if not url:
            return ""
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        return url

    def normalize_record(
        self,
        record: dict[str, Any],
        config: NormalizationConfig,
    ) -> tuple[dict[str, Any], bool]:
        """Normalize a single record.

        Args:
            record: Input record.
            config: Normalization configuration.

        Returns:
            Tuple of (normalized_record, was_changed).
        """
        try:
            value = record.get(config.input_field, "")
            original = str(value) if value is not None else ""

            if config.custom_func:
                normalized = config.custom_func(original)
            else:
                normalized = self.normalize_string(original, config.operation)

            result = record.copy()
            result[config.output_field] = normalized

            return result, normalized != original

        except Exception:
            return record, False

    def normalize_batch(
        self,
        records: Sequence[dict[str, Any]],
        configs: list[NormalizationConfig],
    ) -> NormalizationResult:
        """Normalize a batch of records.

        Args:
            records: Input records.
            configs: List of normalization configurations.

        Returns:
            NormalizationResult with statistics.
        """
        success_count = 0
        error_count = 0
        transformed_count = 0
        unchanged_count = 0

        for record in records:
            try:
                was_changed = False
                for config in configs:
                    record, changed = self.normalize_record(record, config)
                    if changed:
                        was_changed = True

                if was_changed:
                    transformed_count += 1
                else:
                    unchanged_count += 1
                success_count += 1

            except Exception:
                error_count += 1

        return NormalizationResult(
            total_records=len(records),
            success_count=success_count,
            error_count=error_count,
            transformed_count=transformed_count,
            unchanged_count=unchanged_count,
        )

    def chain_normalizations(
        self,
        operations: list[str],
    ) -> Callable[[str], str]:
        """Create a chain of normalization operations.

        Args:
            operations: List of operation names.

        Returns:
            Composed transformation function.
        """
        transforms = [self._transformations[op] for op in operations if op in self._transformations]

        def chain(value: str) -> str:
            result = str(value) if value else ""
            for transform in transforms:
                result = transform(result)
            return result

        return chain
