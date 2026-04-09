"""Data Inference Action Module.

Infers data types, schemas, and patterns from raw input data.
"""

from typing import Any, Dict, List, Optional, Set, Tuple, Type
import re
import logging
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class InferredType(Enum):
    """Inferred data types."""
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    UUID = "uuid"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    JSON = "json"
    LIST = "list"
    DICT = "dict"
    STRING = "string"
    NULL = "null"
    UNKNOWN = "unknown"


@dataclass
class FieldSchema:
    """Inferred schema for a single field."""
    name: str
    inferred_type: InferredType
    nullable: bool = True
    unique: bool = False
    sample_values: List[Any] = field(default_factory=list)
    match_count: int = 0
    total_count: int = 0


class DataInferenceAction:
    """Infers structure and types from unstructured or semi-structured data.
    
    Analyzes sample records to determine field types, patterns,
    nullability, and uniqueness.
    """

    # Compiled regex patterns for type detection
    UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    URL_RE = re.compile(r"^https?://[^\s]+$")
    PHONE_RE = re.compile(r"^\+?[\d\s\-\(\)]{7,20}$")
    CURRENCY_RE = re.compile(r"^\$[\d,]+\.?\d*$|^\d+[\.,]\d{2}\s*(USD|EUR|GBP|CNY)?$", re.IGNORECASE)
    PERCENTAGE_RE = re.compile(r"^-?\d+\.?\d*%$")

    def __init__(self, sample_size: int = 100) -> None:
        self.sample_size = sample_size

    def infer_type(self, value: Any) -> InferredType:
        """Infer the type of a single value.
        
        Args:
            value: The value to analyze.
        
        Returns:
            InferredType enum value.
        """
        if value is None:
            return InferredType.NULL
        if isinstance(value, bool):
            return InferredType.BOOLEAN
        if isinstance(value, int):
            return InferredType.INTEGER
        if isinstance(value, float):
            return InferredType.FLOAT
        if isinstance(value, (dict, list)):
            return InferredType.DICT if isinstance(value, dict) else InferredType.LIST
        if isinstance(value, str):
            return self._infer_string_type(value)
        return InferredType.UNKNOWN

    def _infer_string_type(self, value: str) -> InferredType:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return InferredType.DATETIME
        except (ValueError, AttributeError):
            pass
        if self.UUID_RE.match(value):
            return InferredType.UUID
        if self.EMAIL_RE.match(value):
            return InferredType.EMAIL
        if self.URL_RE.match(value):
            return InferredType.URL
        if self.PHONE_RE.match(value):
            return InferredType.PHONE
        if self.CURRENCY_RE.match(value):
            return InferredType.CURRENCY
        if self.PERCENTAGE_RE.match(value):
            return InferredType.PERCENTAGE
        try:
            int(value.replace(",", ""))
            return InferredType.INTEGER
        except ValueError:
            pass
        try:
            float(value.replace(",", ""))
            return InferredType.FLOAT
        except ValueError:
            pass
        return InferredType.STRING

    def infer_schema(
        self,
        records: List[Dict[str, Any]],
    ) -> Dict[str, FieldSchema]:
        """Infer field schemas from a list of records.
        
        Args:
            records: List of record dictionaries.
        
        Returns:
            Dict mapping field name to FieldSchema.
        """
        if not records:
            return {}
        samples = records[: self.sample_size]
        all_fields: Set[str] = set()
        for r in samples:
            all_fields.update(r.keys())

        schemas: Dict[str, FieldSchema] = {}
        for field in all_fields:
            type_counts: Dict[InferredType, int] = {}
            null_count = 0
            seen_values: Set[Any] = set()
            total = len(samples)

            for record in samples:
                val = record.get(field)
                if val is None:
                    null_count += 1
                    continue
                t = self.infer_type(val)
                type_counts[t] = type_counts.get(t, 0) + 1
                if len(seen_values) <= 100:
                    seen_values.add(val)

            dominant_type = max(type_counts, key=type_counts.get) if type_counts else InferredType.UNKNOWN
            match_count = type_counts.get(dominant_type, 0)

            schemas[field] = FieldSchema(
                name=field,
                inferred_type=dominant_type,
                nullable=null_count > 0,
                unique=len(seen_values) == match_count and match_count <= len(samples) * 0.5,
                sample_values=list(seen_values)[:5],
                match_count=match_count,
                total_count=total,
            )

        return schemas

    def convert_value(self, value: Any, target_type: InferredType) -> Any:
        """Convert a value to the target inferred type.
        
        Args:
            value: Value to convert.
            target_type: Target InferredType.
        
        Returns:
            Converted value, or original if conversion not possible.
        """
        if value is None:
            return None
        if target_type == InferredType.INTEGER:
            try:
                return int(str(value).replace(",", ""))
            except (ValueError, TypeError):
                return value
        if target_type == InferredType.FLOAT:
            try:
                return float(str(value).replace(",", ""))
            except (ValueError, TypeError):
                return value
        if target_type == InferredType.BOOLEAN:
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes", "on")
        return value
