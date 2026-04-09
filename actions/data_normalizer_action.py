"""
Data Normalizer Action Module.

Data normalization with type coercion, schema validation,
deduplication, and transformation pipelines.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar, Optional
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")


class DataType(Enum):
    """Supported data types for normalization."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    DATE = "date"
    DATETIME = "datetime"
    JSON = "json"
    UUID = "uuid"


@dataclass
class FieldSchema:
    """
    Schema definition for a field.

    Attributes:
        name: Field name.
        data_type: Expected data type.
        required: Whether field is required.
        default: Default value if missing.
        validator: Optional validation function.
        transformer: Optional transformation function.
        unique: Whether values must be unique.
    """
    name: str
    data_type: DataType
    required: bool = False
    default: Any = None
    validator: Optional[Callable[[Any], bool]] = None
    transformer: Optional[Callable[[Any], Any]] = None
    unique: bool = False


@dataclass
class NormalizationResult:
    """Result of data normalization."""
    success: bool
    normalized_data: Optional[dict]
    errors: list[str]
    warnings: list[str]
    deduplicated: bool = False


@dataclass
class ValidationError:
    """Validation error details."""
    field: str
    value: Any
    error: str


class DataNormalizerAction(Generic[T]):
    """
    Normalizes and validates data against schemas.

    Example:
        normalizer = DataNormalizerAction[dict]()
        normalizer.add_field("email", DataType.EMAIL, required=True)
        normalizer.add_field("age", DataType.INTEGER)
        result = normalizer.normalize(raw_data)
    """

    def __init__(self):
        """Initialize data normalizer action."""
        self.schemas: dict[str, FieldSchema] = {}
        self._seen_values: dict[str, set] = {}

    def add_field(
        self,
        name: str,
        data_type: DataType,
        required: bool = False,
        default: Any = None,
        validator: Optional[Callable] = None,
        transformer: Optional[Callable] = None,
        unique: bool = False
    ) -> "DataNormalizerAction":
        """
        Add field schema.

        Args:
            name: Field name.
            data_type: Expected data type.
            required: Whether field is required.
            default: Default value if missing.
            validator: Custom validation function.
            transformer: Custom transformation function.
            unique: Enforce uniqueness.

        Returns:
            Self for method chaining.
        """
        schema = FieldSchema(
            name=name,
            data_type=data_type,
            required=required,
            default=default,
            validator=validator,
            transformer=transformer,
            unique=unique
        )

        self.schemas[name] = schema
        if unique:
            self._seen_values[name] = set()

        return self

    def normalize(self, data: T) -> NormalizationResult:
        """
        Normalize data against defined schemas.

        Args:
            data: Input data to normalize.

        Returns:
            NormalizationResult with normalized data and errors.
        """
        errors: list[str] = []
        warnings: list[str] = []
        normalized: dict = {}

        if data is None:
            return NormalizationResult(
                success=False,
                normalized_data=None,
                errors=["Input data is None"],
                warnings=[]
            )

        if isinstance(data, dict):
            source = data
        elif hasattr(data, "__dict__"):
            source = vars(data)
        else:
            return NormalizationResult(
                success=False,
                normalized_data=None,
                errors=[f"Unsupported data type: {type(data)}"],
                warnings=[]
            )

        for field_name, schema in self.schemas.items():
            value = source.get(field_name)

            if value is None:
                if schema.required:
                    errors.append(f"Required field '{field_name}' is missing")
                    continue
                else:
                    normalized[field_name] = schema.default
                    continue

            try:
                normalized_value = self._transform_value(value, schema)
                validation_passed = self._validate_value(normalized_value, schema)

                if not validation_passed:
                    errors.append(f"Field '{field_name}' validation failed: {normalized_value}")
                    continue

                if schema.unique:
                    if normalized_value in self._seen_values[field_name]:
                        warnings.append(f"Duplicate value for '{field_name}': {normalized_value}")
                        continue
                    self._seen_values[field_name].add(normalized_value)

                normalized[field_name] = normalized_value

            except Exception as e:
                errors.append(f"Field '{field_name}' transformation error: {e}")

        return NormalizationResult(
            success=len(errors) == 0,
            normalized_data=normalized if len(errors) == 0 else None,
            errors=errors,
            warnings=warnings
        )

    def _transform_value(self, value: Any, schema: FieldSchema) -> Any:
        """Transform value to target data type."""
        if schema.transformer:
            return schema.transformer(value)

        if schema.data_type == DataType.STRING:
            return str(value).strip() if value is not None else ""

        elif schema.data_type == DataType.INTEGER:
            if isinstance(value, str):
                value = re.sub(r"[^\d-]", "", value)
            return int(float(value))

        elif schema.data_type == DataType.FLOAT:
            if isinstance(value, str):
                value = re.sub(r"[^\d.-]", "", value)
            return float(value)

        elif schema.data_type == DataType.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)

        elif schema.data_type == DataType.EMAIL:
            email = str(value).lower().strip()
            if not re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", email):
                raise ValueError(f"Invalid email format: {email}")
            return email

        elif schema.data_type == DataType.URL:
            url = str(value).strip()
            if not re.match(r"^https?://", url):
                url = f"https://{url}"
            if not re.match(r"^https?://[\w\.-]+\.\w+", url):
                raise ValueError(f"Invalid URL format: {url}")
            return url

        elif schema.data_type == DataType.PHONE:
            phone = re.sub(r"[^\d+]", "", str(value))
            if len(phone) < 7:
                raise ValueError(f"Invalid phone number: {phone}")
            return phone

        elif schema.data_type == DataType.UUID:
            uuid_str = str(value).strip()
            pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            if not re.match(pattern, uuid_str.lower()):
                raise ValueError(f"Invalid UUID: {uuid_str}")
            return uuid_str

        return value

    def _validate_value(self, value: Any, schema: FieldSchema) -> bool:
        """Validate transformed value."""
        if schema.validator:
            return schema.validator(value)
        return True

    def deduplicate(
        self,
        data: list[T],
        key_fields: list[str]
    ) -> list[T]:
        """
        Deduplicate data based on key fields.

        Args:
            data: List of records.
            key_fields: Fields to use for deduplication.

        Returns:
            Deduplicated list of records.
        """
        seen: set = set()
        result = []

        for record in data:
            if isinstance(record, dict):
                key = tuple(record.get(f) for f in key_fields)
            else:
                key = tuple(getattr(record, f, None) for f in key_fields)

            if key not in seen:
                seen.add(key)
                result.append(record)

        logger.info(f"Deduplicated {len(data)} -> {len(result)} records")
        return result

    def merge(
        self,
        data1: T,
        data2: T,
        strategy: str = "prefer_second"
    ) -> T:
        """
        Merge two records.

        Args:
            data1: First record.
            data2: Second record.
            strategy: Merge strategy (prefer_first, prefer_second, combine).

        Returns:
            Merged record.
        """
        if not isinstance(data1, dict) or not isinstance(data2, dict):
            return data2 if strategy == "prefer_second" else data1

        result = data1.copy()

        for key, value in data2.items():
            if key not in result:
                result[key] = value
            elif strategy == "combine":
                if isinstance(result[key], list) and isinstance(value, list):
                    result[key] = result[key] + value
                elif result[key] != value:
                    result[key] = [result[key], value]
            elif strategy == "prefer_second":
                result[key] = value

        return result

    def clear(self) -> None:
        """Clear all schemas and cached values."""
        self.schemas.clear()
        self._seen_values.clear()
