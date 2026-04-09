"""
Data Anonymization and PII Redaction Module.

Anonymizes personally identifiable information (PII) in datasets
using techniques like masking, hashing, generalization, and perturbation.

Author: AutoGen
"""
from __future__ import annotations

import hashlib
import logging
import random
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class AnonymizationTechnique(Enum):
    MASK = auto()
    HASH = auto()
    GENERALIZE = auto()
    PERTURB = auto()
    SUBSTITUTE = auto()
    SWAP = auto()
    REDACT = auto()


@dataclass
class PIIPattern:
    name: str
    pattern: str
    replacement: str
    technique: AnonymizationTechnique
    preserve_format: bool = True


@dataclass
class AnonymizationReport:
    records_processed: int = 0
    fields_anonymized: int = 0
    patterns_detected: int = 0
    patterns_missed: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)


class PIIDetector:
    """Detects personally identifiable information using regex patterns."""

    DEFAULT_PATTERNS: List[PIIPattern] = [
        PIIPattern(
            "email", r"[\w\.\+\-]+@[\w\.\-]+\.\w+",
            "[EMAIL]", AnonymizationTechnique.MASK
        ),
        PIIPattern(
            "phone_us", r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b",
            "[PHONE]", AnonymizationTechnique.MASK
        ),
        PIIPattern(
            "ssn", r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
            "[SSN]", AnonymizationTechnique.MASK
        ),
        PIIPattern(
            "credit_card", r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
            "[CARD]", AnonymizationTechnique.MASK
        ),
        PIIPattern(
            "ip_address", r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
            "[IP]", AnonymizationTechnique.MASK
        ),
        PIIPattern(
            "date_of_birth", r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b",
            "[DATE]", AnonymizationTechnique.GENERALIZE
        ),
    ]

    def __init__(self, custom_patterns: Optional[List[PIIPattern]] = None):
        self._compiled: List[Tuple[PIIPattern, re.Pattern]] = []
        patterns = (custom_patterns or []) + self.DEFAULT_PATTERNS
        for pii in patterns:
            try:
                compiled = re.compile(pii.pattern, re.IGNORECASE)
                self._compiled.append((pii, compiled))
            except re.error as exc:
                logger.warning("Invalid PII pattern '%s': %s", pii.name, exc)

    def detect(self, text: str) -> List[Tuple[str, str, PIIPattern]]:
        matches: List[Tuple[str, str, PIIPattern]] = []
        for pii, pattern in self._compiled:
            for match in pattern.finditer(text):
                matches.append((match.group(), match.group(), pii))
        return matches

    def count_patterns(self, text: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for pii, pattern in self._compiled:
            count = len(pattern.findall(text))
            if count > 0:
                counts[pii.name] = count
        return counts


class DataAnonymizer:
    """
    Anonymizes PII in text and structured data using configurable techniques.
    """

    def __init__(self, salt: Optional[str] = None):
        self.salt = salt or self._generate_salt()
        self.detector = PIIDetector()
        self.report = AnonymizationReport()

    @staticmethod
    def _generate_salt() -> str:
        import os
        return os.urandom(16).hex()

    def anonymize_text(
        self, text: str, technique: AnonymizationTechnique = AnonymizationTechnique.MASK
    ) -> str:
        if not text:
            return text

        result = text
        for pii, pattern in DataAnonymizer._compile_default_patterns():
            if technique == AnonymizationTechnique.MASK:
                result = pattern.sub(pii.replacement, result)
            elif technique == AnonymizationTechnique.HASH:
                def hash_replace(m: re.Match) -> str:
                    val = m.group()
                    h = hashlib.sha256((self.salt + val).encode()).hexdigest()[:12]
                    return f"[HASH:{h}]"
                result = pattern.sub(hash_replace, result)
            elif technique == AnonymizationTechnique.REDACT:
                result = pattern.sub("[REDACTED]", result)

        self.report.patterns_detected += len(self.detector.detect(text))
        return result

    def anonymize_value(
        self, value: Any, field_name: str, technique: AnonymizationTechnique = AnonymizationTechnique.HASH
    ) -> Any:
        if value is None:
            return None

        self.report.fields_anonymized += 1

        if technique == AnonymizationTechnique.HASH:
            if isinstance(value, str):
                h = hashlib.sha256((self.salt + value).encode()).hexdigest()[:16]
                return f"__hash_{h}__"
            return str(value)

        elif technique == AnonymizationTechnique.MASK:
            if isinstance(value, str):
                return self._mask_string(value)
            return value

        elif technique == AnonymizationTechnique.GENERALIZE:
            return self._generalize_value(value)

        elif technique == AnonymizationTechnique.PERTURB:
            return self._perturb_value(value)

        return value

    def _mask_string(self, s: str) -> str:
        if len(s) <= 4:
            return "*" * len(s)
        return s[:2] + "*" * (len(s) - 4) + s[-2:]

    def _generalize_value(self, value: Any) -> Any:
        if isinstance(value, (int, float)):
            magnitude = 10 ** (len(str(abs(int(value)))) - 1)
            if magnitude > 1:
                return (int(value) // magnitude) * magnitude
            return value
        if isinstance(value, str):
            if re.match(r"\d{4}-\d{2}-\d{2}", value):
                return value[:4] + "-01-01"
        return "[GENERALIZED]"

    def _perturb_value(self, value: Any) -> Any:
        if isinstance(value, (int, float)):
            noise = random.uniform(-0.1, 0.1)
            return value * (1 + noise)
        return value

    def anonymize_record(
        self,
        record: Dict[str, Any],
        field_config: Optional[Dict[str, AnonymizationTechnique]] = None,
    ) -> Dict[str, Any]:
        """Anonymize a structured record (dict)."""
        self.report.records_processed += 1
        result = {}

        for key, value in record.items():
            technique = (field_config or {}).get(key, AnonymizationTechnique.HASH)

            if isinstance(value, str):
                result[key] = self.anonymize_text(value, technique)
            elif isinstance(value, dict):
                result[key] = self.anonymize_record(value, field_config)
            elif isinstance(value, list):
                result[key] = [
                    self.anonymize_record(item, field_config) if isinstance(item, dict)
                    else self.anonymize_value(item, key, technique)
                    for item in value
                ]
            else:
                result[key] = self.anonymize_value(value, key, technique)

        return result

    def anonymize_dataset(
        self, records: List[Dict[str, Any]], field_config: Optional[Dict[str, AnonymizationTechnique]] = None
    ) -> List[Dict[str, Any]]:
        """Anonymize a list of records."""
        return [self.anonymize_record(r, field_config) for r in records]

    def k_anonymize(
        self, records: List[Dict[str, Any]], quasi_identifiers: List[str], k: int = 5
    ) -> List[Dict[str, Any]]:
        """Apply k-anonymity by generalizing quasi-identifiers."""
        from collections import Counter

        groups: Dict[Tuple, List[Dict[str, Any]]] = {}
        for record in records:
            key = tuple(record.get(qid, "NULL") for qid in quasi_identifiers)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        result = []
        for key, group in groups.items():
            if len(group) < k:
                for record in group:
                    for qid in quasi_identifiers:
                        record[qid] = self._generalize_value(record.get(qid))
            result.extend(group)

        return result

    def get_report(self) -> AnonymizationReport:
        return self.report

    def reset_report(self) -> None:
        self.report = AnonymizationReport()

    @staticmethod
    def _compile_default_patterns() -> List[Tuple[PIIPattern, re.Pattern]]:
        patterns = []
        default_pii = [
            PIIPattern("email", r"[\w\.\+\-]+@[\w\.\-]+\.\w+", "[EMAIL]", AnonymizationTechnique.MASK),
            PIIPattern("phone", r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", "[PHONE]", AnonymizationTechnique.MASK),
        ]
        for pii in default_pii:
            patterns.append((pii, re.compile(pii.pattern, re.IGNORECASE)))
        return patterns
