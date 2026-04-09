"""Data type casting and coercion action."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional, Sequence


@dataclass
class CastConfig:
    """Configuration for type casting."""

    field_name: str
    target_type: str
    default_value: Any = None
    strict: bool = False
    custom_parser: Optional[Callable[[Any], Any]] = None


@dataclass
class CastResult:
    """Result of casting operation."""

    field_name: str
    original_value: Any
    cast_value: Any
    success: bool
    error: Optional[str] = None


@dataclass
class BatchCastResult:
    """Result of batch casting."""

    total_records: int
    success_count: int
    error_count: int
    results: list[CastResult] = field(default_factory=list)


class DataCasterAction:
    """Casts and coerces data types."""

    def __init__(self):
        """Initialize data caster."""
        self._type_converters: dict[str, Callable[[Any], Any]] = {
            "int": self._to_int,
            "float": self._to_float,
            "str": self._to_str,
            "bool": self._to_bool,
            "datetime": self._to_datetime,
            "date": self._to_date,
            "list": self._to_list,
            "dict": self._to_dict,
        }

    def _to_int(self, value: Any) -> int:
        """Convert to integer."""
        if value is None:
            return 0
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            return int(float(value.strip()))
        raise ValueError(f"Cannot cast {type(value)} to int")

    def _to_float(self, value: Any) -> float:
        """Convert to float."""
        if value is None:
            return 0.0
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value.strip())
        raise ValueError(f"Cannot cast {type(value)} to float")

    def _to_str(self, value: Any) -> str:
        """Convert to string."""
        if value is None:
            return ""
        return str(value)

    def _to_bool(self, value: Any) -> bool:
        """Convert to boolean."""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.lower().strip()
            if lowered in ("true", "yes", "1", "on", "enabled"):
                return True
            if lowered in ("false", "no", "0", "off", "disabled", ""):
                return False
            raise ValueError(f"Cannot cast '{value}' to bool")
        return bool(value)

    def _to_datetime(self, value: Any) -> datetime:
        """Convert to datetime."""
        if value is None:
            raise ValueError("Cannot cast None to datetime")
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        if isinstance(value, str):
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d",
                "%d/%m/%Y",
                "%m/%d/%Y",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value.strip(), fmt)
                except ValueError:
                    continue
            raise ValueError(f"Cannot parse '{value}' as datetime")
        raise ValueError(f"Cannot cast {type(value)} to datetime")

    def _to_date(self, value: Any) -> datetime:
        """Convert to date (datetime.date())."""
        dt = self._to_datetime(value)
        return dt.date()

    def _to_list(self, value: Any) -> list:
        """Convert to list."""
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return list(value)
        if isinstance(value, str):
            if value.strip().startswith("["):
                import ast
                return ast.literal_eval(value)
            return [v.strip() for v in value.split(",")]
        return [value]

    def _to_dict(self, value: Any) -> dict:
        """Convert to dict."""
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            import ast
            return ast.literal_eval(value)
        raise ValueError(f"Cannot cast {type(value)} to dict")

    def cast_value(
        self,
        value: Any,
        target_type: str,
        default_value: Any = None,
        strict: bool = False,
        custom_parser: Optional[Callable[[Any], Any]] = None,
    ) -> CastResult:
        """Cast a single value.

        Args:
            value: Value to cast.
            target_type: Target type name.
            default_value: Default if casting fails.
            strict: Whether to raise on error or use default.
            custom_parser: Custom parsing function.

        Returns:
            CastResult with outcome.
        """
        if custom_parser:
            try:
                return CastResult(
                    field_name="",
                    original_value=value,
                    cast_value=custom_parser(value),
                    success=True,
                )
            except Exception as e:
                if strict:
                    raise
                return CastResult(
                    field_name="",
                    original_value=value,
                    cast_value=default_value,
                    success=False,
                    error=str(e),
                )

        converter = self._type_converters.get(target_type)
        if not converter:
            raise ValueError(f"Unknown target type: {target_type}")

        try:
            return CastResult(
                field_name="",
                original_value=value,
                cast_value=converter(value),
                success=True,
            )
        except Exception as e:
            if strict:
                raise
            return CastResult(
                field_name="",
                original_value=value,
                cast_value=default_value,
                success=False,
                error=str(e),
            )

    def cast_record(
        self,
        record: dict[str, Any],
        config: CastConfig,
    ) -> tuple[dict[str, Any], bool, Optional[str]]:
        """Cast a field in a record.

        Args:
            record: Input record.
            config: Casting configuration.

        Returns:
            Tuple of (casted_record, success, error).
        """
        result = record.copy()
        value = record.get(config.field_name)

        cast_result = self.cast_value(
            value,
            config.target_type,
            config.default_value,
            config.strict,
            config.custom_parser,
        )

        result[config.field_name] = cast_result.cast_value

        return result, cast_result.success, cast_result.error

    def cast_batch(
        self,
        records: Sequence[dict[str, Any]],
        configs: list[CastConfig],
    ) -> BatchCastResult:
        """Cast multiple fields in records.

        Args:
            records: Input records.
            configs: List of casting configurations.

        Returns:
            BatchCastResult with statistics.
        """
        all_results: list[CastResult] = []
        success_count = 0
        error_count = 0

        for record in records:
            for config in configs:
                result_record, success, error = self.cast_record(record, config)
                record.update(result_record)

                all_results.append(
                    CastResult(
                        field_name=config.field_name,
                        original_value=record.get(config.field_name),
                        cast_value=record[config.field_name],
                        success=success,
                        error=error,
                    )
                )

                if success:
                    success_count += 1
                else:
                    error_count += 1

        return BatchCastResult(
            total_records=len(records),
            success_count=success_count,
            error_count=error_count,
            results=all_results,
        )
