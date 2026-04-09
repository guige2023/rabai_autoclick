"""
Data anonymizer action for PII/PHI data masking and privacy.

Provides field-level encryption, masking, and pseudonymization.
"""

from typing import Any, Callable, Dict, List, Optional
import hashlib
import re


class DataAnonymizerAction:
    """Data anonymization and privacy protection."""

    def __init__(self, salt: str = "") -> None:
        """
        Initialize data anonymizer.

        Args:
            salt: Salt for hashing-based anonymization
        """
        self.salt = salt
        self._patterns: Dict[str, re.Pattern] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute anonymization operation.

        Args:
            params: Dictionary containing:
                - operation: 'anonymize', 'mask', 'pseudonymize', 'register_pattern'
                - data: Data to anonymize
                - fields: Fields to anonymize
                - strategy: Anonymization strategy

        Returns:
            Dictionary with anonymized data
        """
        operation = params.get("operation", "anonymize")

        if operation == "anonymize":
            return self._anonymize(params)
        elif operation == "mask":
            return self._mask_data(params)
        elif operation == "pseudonymize":
            return self._pseudonymize(params)
        elif operation == "register_pattern":
            return self._register_pattern(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _anonymize(self, params: dict[str, Any]) -> dict[str, Any]:
        """Anonymize data based on field definitions."""
        data = params.get("data", {})
        field_config = params.get("field_config", {})

        if not data:
            return {"success": False, "error": "Data is required"}

        result = dict(data)

        for field, config in field_config.items():
            if field not in data:
                continue

            strategy = config.get("strategy", "hash")
            value = data[field]

            if strategy == "hash":
                result[field] = self._hash_value(str(value), config.get("algorithm", "sha256"))
            elif strategy == "mask":
                result[field] = self._mask_value(str(value), config.get("mask_char", "*"))
            elif strategy == "redact":
                result[field] = self._redact_value(str(value))
            elif strategy == "pseudonymize":
                result[field] = self._pseudonymize_value(str(value))
            elif strategy == "generalize":
                result[field] = self._generalize_value(value, config.get("level", "city"))
            elif strategy == "noise":
                result[field] = self._add_noise(value, config.get("range", 0.1))

        return {"success": True, "anonymized_data": result}

    def _mask_data(self, params: dict[str, Any]) -> dict[str, Any]:
        """Mask sensitive fields in data."""
        data = params.get("data", {})
        fields = params.get("fields", [])
        mask_char = params.get("mask_char", "*")
        preserve_chars = params.get("preserve_chars", 4)

        if not data:
            return {"success": False, "error": "Data is required"}

        result = dict(data)

        for field in fields:
            if field in result:
                value = str(result[field])
                result[field] = self._mask_value(value, mask_char, preserve_chars)

        return {"success": True, "masked_data": result, "masked_fields": fields}

    def _pseudonymize(self, params: dict[str, Any]) -> dict[str, Any]:
        """Replace identifiable data with pseudonyms."""
        data = params.get("data", {})
        fields = params.get("fields", [])

        if not data:
            return {"success": False, "error": "Data is required"}

        result = dict(data)
        pseudonym_map = {}

        for field in fields:
            if field in result:
                original = str(result[field])
                result[field] = self._pseudonymize_value(original)
                pseudonym_map[field] = original

        return {"success": True, "pseudonymized_data": result, "pseudonym_map": pseudonym_map}

    def _register_pattern(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register regex pattern for auto-detection."""
        pattern_name = params.get("pattern_name", "")
        pattern = params.get("pattern", "")

        if not pattern_name or not pattern:
            return {"success": False, "error": "pattern_name and pattern are required"}

        try:
            self._patterns[pattern_name] = re.compile(pattern)
        except re.error as e:
            return {"success": False, "error": f"Invalid regex: {e}"}

        return {"success": True, "pattern_registered": pattern_name}

    def _hash_value(self, value: str, algorithm: str = "sha256") -> str:
        """Hash value using specified algorithm."""
        data = f"{self.salt}{value}".encode()
        if algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif algorithm == "sha1":
            return hashlib.sha1(data).hexdigest()
        elif algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        else:
            return hashlib.sha256(data).hexdigest()

    def _mask_value(self, value: str, mask_char: str = "*", preserve_chars: int = 4) -> str:
        """Mask value keeping last N characters visible."""
        if len(value) <= preserve_chars:
            return mask_char * len(value)
        return mask_char * (len(value) - preserve_chars) + value[-preserve_chars:]

    def _redact_value(self, value: str) -> str:
        """Redact entire value."""
        return "[REDACTED]"

    def _pseudonymize_value(self, value: str) -> str:
        """Create consistent pseudonym for value."""
        hash_val = self._hash_value(value, "sha256")[:12]
        return f"user_{hash_val}"

    def _generalize_value(self, value: Any, level: str = "city") -> str:
        """Generalize value to less specific form."""
        if isinstance(value, (int, float)):
            if level == "city":
                return (value // 1000) * 1000
            elif level == "region":
                return (value // 10000) * 10000
        elif isinstance(value, str):
            if level == "city":
                return value[:3] + "***" if len(value) > 3 else "***"
        return str(value)

    def _add_noise(self, value: Any, range_pct: float = 0.1) -> Any:
        """Add random noise to numeric value."""
        import random
        if isinstance(value, (int, float)):
            noise = value * range_pct * (2 * random.random() - 1)
            return value + noise
        return value
