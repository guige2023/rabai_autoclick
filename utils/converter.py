"""Data conversion utilities for RabAI AutoClick.

Provides:
- Type conversion
- Format conversion
- Unit conversion
"""

import base64
import json
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union


class TypeConverter:
    """Convert between types."""

    @staticmethod
    def to_int(value: Any, default: int = 0) -> int:
        """Convert to integer.

        Args:
            value: Value to convert.
            default: Default if conversion fails.

        Returns:
            Integer value.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def to_float(value: Any, default: float = 0.0) -> float:
        """Convert to float.

        Args:
            value: Value to convert.
            default: Default if conversion fails.

        Returns:
            Float value.
        """
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def to_bool(value: Any) -> bool:
        """Convert to boolean.

        Args:
            value: Value to convert.

        Returns:
            Boolean value.
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    @staticmethod
    def to_str(value: Any) -> str:
        """Convert to string.

        Args:
            value: Value to convert.

        Returns:
            String value.
        """
        if isinstance(value, str):
            return value
        return str(value)

    @staticmethod
    def to_list(value: Any, separator: str = ",", default: Optional[List] = None) -> List:
        """Convert to list.

        Args:
            value: Value to convert.
            separator: Separator for string values.
            default: Default if conversion fails.

        Returns:
            List value.
        """
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [v.strip() for v in value.split(separator)]
        if default is not None:
            return default
        return [value]

    @staticmethod
    def to_dict(value: Any, default: Optional[dict] = None) -> dict:
        """Convert to dictionary.

        Args:
            value: Value to convert.
            default: Default if conversion fails.

        Returns:
            Dictionary value.
        """
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        return default if default is not None else {}


class JSONConverter:
    """Convert to/from JSON."""

    @staticmethod
    def to_json(value: Any, indent: Optional[int] = None) -> str:
        """Convert to JSON string.

        Args:
            value: Value to convert.
            indent: Indentation level.

        Returns:
            JSON string.
        """
        return json.dumps(value, indent=indent, default=str)

    @staticmethod
    def from_json(json_str: str, default: Any = None) -> Any:
        """Parse JSON string.

        Args:
            json_str: JSON string.
            default: Default if parsing fails.

        Returns:
            Parsed value.
        """
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return default

    @staticmethod
    def to_file(value: Any, path: str, indent: Optional[int] = None) -> bool:
        """Write value to JSON file.

        Args:
            value: Value to write.
            path: File path.
            indent: Indentation level.

        Returns:
            True if successful.
        """
        try:
            with open(path, "w") as f:
                json.dump(value, f, indent=indent, default=str)
            return True
        except Exception:
            return False

    @staticmethod
    def from_file(path: str, default: Any = None) -> Any:
        """Read value from JSON file.

        Args:
            path: File path.
            default: Default if reading fails.

        Returns:
            Read value.
        """
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return default


class Base64Converter:
    """Convert to/from Base64."""

    @staticmethod
    def encode(data: Union[str, bytes]) -> str:
        """Encode to Base64.

        Args:
            data: Data to encode.

        Returns:
            Base64 encoded string.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        return base64.b64encode(data).decode("ascii")

    @staticmethod
    def decode(data: str, as_string: bool = True) -> Union[str, bytes]:
        """Decode from Base64.

        Args:
            data: Base64 string.
            as_string: Return as string if True, bytes if False.

        Returns:
            Decoded data.
        """
        result = base64.b64decode(data)
        if as_string:
            return result.decode("utf-8")
        return result


class DateTimeConverter:
    """Convert to/from datetime."""

    @staticmethod
    def to_datetime(value: Any, format: Optional[str] = None) -> Optional[datetime]:
        """Convert to datetime.

        Args:
            value: Value to convert.
            format: Date format string.

        Returns:
            Datetime or None.
        """
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            if format:
                try:
                    return datetime.strptime(value, format)
                except ValueError:
                    pass
            # Try common formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    pass
        return None

    @staticmethod
    def to_timestamp(value: datetime) -> float:
        """Convert datetime to timestamp.

        Args:
            value: Datetime to convert.

        Returns:
            Unix timestamp.
        """
        return value.timestamp()

    @staticmethod
    def from_timestamp(timestamp: float) -> datetime:
        """Convert timestamp to datetime.

        Args:
            timestamp: Unix timestamp.

        Returns:
            Datetime.
        """
        return datetime.fromtimestamp(timestamp)

    @staticmethod
    def to_string(value: datetime, format: str = "%Y-%m-%d %H:%M:%S") -> str:
        """Convert datetime to string.

        Args:
            value: Datetime to convert.
            format: Output format.

        Returns:
            Formatted string.
        """
        return value.strftime(format)


class UnitConverter:
    """Convert between units."""

    # Length
    METERS_TO_FEET = 3.28084
    FEET_TO_METERS = 0.3048
    METERS_TO_INCHES = 39.3701
    INCHES_TO_METERS = 0.0254

    # Weight
    KG_TO_LB = 2.20462
    LB_TO_KG = 0.453592
    GRAMS_TO_OZ = 0.035274
    OZ_TO_GRAMS = 28.3495

    # Temperature
    @staticmethod
    def celsius_to_fahrenheit(c: float) -> float:
        """Convert Celsius to Fahrenheit."""
        return c * 9 / 5 + 32

    @staticmethod
    def fahrenheit_to_celsius(f: float) -> float:
        """Convert Fahrenheit to Celsius."""
        return (f - 32) * 5 / 9

    # Distance
    @staticmethod
    def km_to_miles(km: float) -> float:
        """Convert kilometers to miles."""
        return km * 0.621371

    @staticmethod
    def miles_to_km(miles: float) -> float:
        """Convert miles to kilometers."""
        return miles * 1.60934

    # Time
    @staticmethod
    def hours_to_minutes(hours: float) -> float:
        """Convert hours to minutes."""
        return hours * 60

    @staticmethod
    def minutes_to_hours(minutes: float) -> float:
        """Convert minutes to hours."""
        return minutes / 60

    @staticmethod
    def seconds_to_minutes(seconds: float) -> float:
        """Convert seconds to minutes."""
        return seconds / 60

    @staticmethod
    def minutes_to_seconds(minutes: float) -> float:
        """Convert minutes to seconds."""
        return minutes * 60


class DataFrameConverter:
    """Convert to/from data frames."""

    @staticmethod
    def to_csv(data: List[Dict], path: str) -> bool:
        """Write data to CSV.

        Args:
            data: List of dictionaries.
            path: Output path.

        Returns:
            True if successful.
        """
        if not data:
            return False

        try:
            import csv
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            return True
        except Exception:
            return False

    @staticmethod
    def from_csv(path: str) -> Optional[List[Dict]]:
        """Read data from CSV.

        Args:
            path: Input path.

        Returns:
            List of dictionaries or None.
        """
        try:
            import csv
            with open(path, "r") as f:
                reader = csv.DictReader(f)
                return list(reader)
        except Exception:
            return None

    @staticmethod
    def to_tsv(data: List[Dict], path: str) -> bool:
        """Write data to TSV.

        Args:
            data: List of dictionaries.
            path: Output path.

        Returns:
            True if successful.
        """
        if not data:
            return False

        try:
            import csv
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter="\t")
                writer.writeheader()
                writer.writerows(data)
            return True
        except Exception:
            return False
