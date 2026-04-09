"""
Data Normalization Action Module.

Provides data normalization and standardization for preparing
data for analysis, ML models, and cross-dataset compatibility.

Author: RabAi Team
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class NormalizationMethod(Enum):
    """Normalization methods."""
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    ROBUST = "robust"
    LOG = "log"
    LOG10 = "log10"
    SQRT = "sqrt"
    BOX_COX = "box_cox"


@dataclass
class NormalizationConfig:
    """Configuration for normalization."""
    method: NormalizationMethod = NormalizationMethod.MIN_MAX
    scale_range: Tuple[float, float] = (0, 1)
    handle_outliers: str = "clip"  # clip, winsorize, none
    outlier_threshold: float = 3.0


@dataclass
class TransformStats:
    """Statistics for a transformation."""
    field: str
    mean: float
    std: float
    min_val: float
    max_val: float
    median: float
    q1: float
    q3: float


class DataNormalizer:
    """Main data normalization engine."""

    def __init__(self) -> None:
        self.stats: Dict[str, TransformStats] = {}
        self.config = NormalizationConfig()

    def fit(self, data: List[Dict[str, Any]], field_name: str) -> "DataNormalizer":
        """Compute statistics for a field."""
        values = [record.get(field_name) for record in data if record.get(field_name) is not None]
        values = [v for v in values if isinstance(v, (int, float))]

        if not values:
            return self

        sorted_values = sorted(values)
        n = len(sorted_values)

        stats = TransformStats(
            field=field_name,
            mean=sum(values) / n,
            std=self._calculate_std(values),
            min_val=min(values),
            max_val=max(values),
            median=sorted_values[n // 2],
            q1=sorted_values[n // 4],
            q3=sorted_values[3 * n // 4],
        )

        self.stats[field_name] = stats
        return self

    def _calculate_std(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return variance ** 0.5

    def _handle_outliers(
        self,
        values: List[float],
        method: str,
        threshold: float,
    ) -> List[float]:
        """Handle outliers in values."""
        if method == "none":
            return values

        mean = sum(values) / len(values)
        std = self._calculate_std(values)

        if std == 0:
            return values

        if method == "clip":
            lower = mean - threshold * std
            upper = mean + threshold * std
            return [max(lower, min(upper, v)) for v in values]

        elif method == "winsorize":
            lower = mean - threshold * std
            upper = mean + threshold * std
            return [max(lower, min(upper, v)) for v in values]

        return values

    def normalize(
        self,
        value: float,
        field_name: str,
    ) -> float:
        """Normalize a single value."""
        if field_name not in self.stats:
            return value

        stats = self.stats[field_name]

        if self.config.method == NormalizationMethod.MIN_MAX:
            range_min, range_max = self.config.scale_range
            if stats.max_val == stats.min_val:
                return (range_min + range_max) / 2
            normalized = (value - stats.min_val) / (stats.max_val - stats.min_val)
            return range_min + normalized * (range_max - range_min)

        elif self.config.method == NormalizationMethod.Z_SCORE:
            if stats.std == 0:
                return 0.0
            return (value - stats.mean) / stats.std

        elif self.config.method == NormalizationMethod.ROBUST:
            iqr = stats.q3 - stats.q1
            if iqr == 0:
                return 0.0
            return (value - stats.median) / iqr

        elif self.config.method == NormalizationMethod.LOG:
            import math
            return math.log1p(max(0, value))

        elif self.config.method == NormalizationMethod.LOG10:
            import math
            return math.log10(max(0, value)) if value > 0 else 0.0

        elif self.config.method == NormalizationMethod.SQRT:
            import math
            return math.sqrt(max(0, value))

        return value

    def denormalize(
        self,
        normalized_value: float,
        field_name: str,
    ) -> float:
        """Reverse normalization."""
        if field_name not in self.stats:
            return normalized_value

        stats = self.stats[field_name]

        if self.config.method == NormalizationMethod.MIN_MAX:
            range_min, range_max = self.config.scale_range
            if range_max == range_min:
                return stats.mean
            normalized = (normalized_value - range_min) / (range_max - range_min)
            return stats.min_val + normalized * (stats.max_val - stats.min_val)

        elif self.config.method == NormalizationMethod.Z_SCORE:
            return normalized_value * stats.std + stats.mean

        elif self.config.method == NormalizationMethod.ROBUST:
            return normalized_value * (stats.q3 - stats.q1) + stats.median

        return normalized_value

    def transform_record(
        self,
        record: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, Any]:
        """Transform a single record."""
        result = record.copy()
        for field_name in fields:
            if field_name in self.stats and field_name in result:
                value = result[field_name]
                if isinstance(value, (int, float)):
                    result[f"{field_name}_normalized"] = self.normalize(value, field_name)
        return result

    def transform_batch(
        self,
        data: List[Dict[str, Any]],
        fields: List[str],
    ) -> List[Dict[str, Any]]:
        """Transform batch of records."""
        # Fit on all data first
        for field_name in fields:
            self.fit(data, field_name)

        # Then transform
        return [self.transform_record(record, fields) for record in data]


class TextNormalizer:
    """Text normalization utilities."""

    @staticmethod
    def lowercase(value: str) -> str:
        """Convert to lowercase."""
        return str(value).lower()

    @staticmethod
    def uppercase(value: str) -> str:
        """Convert to uppercase."""
        return str(value).upper()

    @staticmethod
    def trim(value: str) -> str:
        """Remove leading/trailing whitespace."""
        return str(value).strip()

    @staticmethod
    def remove_punctuation(value: str) -> str:
        """Remove punctuation marks."""
        return re.sub(r"[^\w\s]", "", str(value))

    @staticmethod
    def normalize_whitespace(value: str) -> str:
        """Normalize whitespace."""
        return re.sub(r"\s+", " ", str(value)).strip()

    @staticmethod
    def remove_numbers(value: str) -> str:
        """Remove numeric characters."""
        return re.sub(r"\d+", "", str(value))

    @staticmethod
    def keep_only_letters(value: str) -> str:
        """Keep only alphabetic characters."""
        return re.sub(r"[^a-zA-Z]", "", str(value))

    @staticmethod
    def keep_only_alphanumeric(value: str) -> str:
        """Keep only alphanumeric characters."""
        return re.sub(r"[^a-zA-Z0-9]", "", str(value))

    @staticmethod
    def normalize_unicode(value: str) -> str:
        """Normalize unicode characters."""
        import unicodedata
        return unicodedata.normalize("NFKD", str(value))

    @staticmethod
    def remove_accent_marks(value: str) -> str:
        """Remove accent marks from characters."""
        import unicodedata
        nfd = unicodedata.normalize("NFD", str(value))
        return "".join(c for c in nfd if unicodedata.category(c) != "Mn")

    @classmethod
    def normalize_text(
        cls,
        value: str,
        lowercase: bool = True,
        trim: bool = True,
        remove_punct: bool = False,
        normalize_ws: bool = True,
    ) -> str:
        """Apply multiple text normalizations."""
        result = str(value)
        if lowercase:
            result = cls.lowercase(result)
        if trim:
            result = cls.trim(result)
        if remove_punct:
            result = cls.remove_punctuation(result)
        if normalize_ws:
            result = cls.normalize_whitespace(result)
        return result

    @classmethod
    def tokenize(cls, text: str) -> List[str]:
        """Split text into tokens."""
        text = cls.normalize_text(text, lowercase=True, normalize_ws=True)
        return text.split()

    @classmethod
    def remove_stopwords(cls, tokens: List[str], language: str = "en") -> List[str]:
        """Remove common stopwords."""
        stopwords = {
            "en": {
                "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
                "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
                "be", "have", "has", "had", "do", "does", "did", "will", "would",
                "could", "should", "may", "might", "must", "shall", "can", "need",
            }
        }
        lang_stopwords = stopwords.get(language, stopwords["en"])
        return [t for t in tokens if t not in lang_stopwords]


class CategoricalEncoder:
    """Encode categorical variables."""

    def __init__(self) -> None:
        self.mappings: Dict[str, Dict[str, int]] = {}
        self.inverse_mappings: Dict[str, Dict[int, str]] = {}

    def fit(self, data: List[Dict[str, Any]], field_name: str) -> "CategoricalEncoder":
        """Learn encoding from data."""
        unique_values = sorted(set(
            record.get(field_name) for record in data
            if record.get(field_name) is not None
        ))

        mapping = {v: i for i, v in enumerate(unique_values)}
        inverse_mapping = {i: v for v, i in mapping.items()}

        self.mappings[field_name] = mapping
        self.inverse_mappings[field_name] = inverse_mapping

        return self

    def encode(self, value: Any, field_name: str) -> Optional[int]:
        """Encode a single value."""
        if field_name not in self.mappings:
            return None
        return self.mappings[field_name].get(value)

    def decode(self, encoded: int, field_name: str) -> Optional[str]:
        """Decode an encoded value."""
        if field_name not in self.inverse_mappings:
            return None
        return self.inverse_mappings[field_name].get(encoded)

    def one_hot_encode(
        self,
        data: List[Dict[str, Any]],
        field_name: str,
    ) -> Tuple[List[Dict[str, int]], List[str]]:
        """One-hot encode a categorical field."""
        if field_name not in self.mappings:
            self.fit(data, field_name)

        categories = list(self.mappings[field_name].keys())
        encoded_data = []

        for record in data:
            encoded = {cat: 0 for cat in categories}
            value = record.get(field_name)
            if value in self.mappings[field_name]:
                encoded[value] = 1
            encoded_data.append(encoded)

        return encoded_data, categories

    def transform_batch(
        self,
        data: List[Dict[str, Any]],
        field_name: str,
    ) -> List[Dict[str, Any]]:
        """Transform batch of records."""
        if field_name not in self.mappings:
            self.fit(data, field_name)

        result = []
        for record in data:
            new_record = record.copy()
            new_record[f"{field_name}_encoded"] = self.encode(
                record.get(field_name), field_name
            )
            result.append(new_record)

        return result
