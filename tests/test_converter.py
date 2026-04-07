"""Tests for data conversion utilities."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.converter import (
    TypeConverter,
    JSONConverter,
    Base64Converter,
    DateTimeConverter,
    UnitConverter,
    DataFrameConverter,
)


class TestTypeConverter:
    """Tests for TypeConverter."""

    def test_to_int(self) -> None:
        """Test converting to int."""
        assert TypeConverter.to_int("42") == 42
        assert TypeConverter.to_int("abc", 1) == 1

    def test_to_float(self) -> None:
        """Test converting to float."""
        assert TypeConverter.to_float("3.14") == 3.14
        assert TypeConverter.to_float("abc", 1.0) == 1.0

    def test_to_bool(self) -> None:
        """Test converting to bool."""
        assert TypeConverter.to_bool("true") is True
        assert TypeConverter.to_bool("false") is False
        assert TypeConverter.to_bool("1") is True

    def test_to_str(self) -> None:
        """Test converting to string."""
        assert TypeConverter.to_str(42) == "42"

    def test_to_list(self) -> None:
        """Test converting to list."""
        assert TypeConverter.to_list("a,b,c") == ["a", "b", "c"]
        assert TypeConverter.to_list([1, 2]) == [1, 2]

    def test_to_dict(self) -> None:
        """Test converting to dict."""
        assert TypeConverter.to_dict('{"a": 1}') == {"a": 1}
        assert TypeConverter.to_dict("not json") == {}


class TestJSONConverter:
    """Tests for JSONConverter."""

    def test_to_json(self) -> None:
        """Test converting to JSON."""
        assert JSONConverter.to_json({"a": 1}) == '{"a": 1}'

    def test_from_json(self) -> None:
        """Test parsing JSON."""
        assert JSONConverter.from_json('{"a": 1}') == {"a": 1}

    def test_to_file(self) -> None:
        """Test writing JSON file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            path = f.name
        try:
            assert JSONConverter.to_file({"a": 1}, path) is True
            assert JSONConverter.from_file(path) == {"a": 1}
        finally:
            os.unlink(path)


class TestBase64Converter:
    """Tests for Base64Converter."""

    def test_encode_decode(self) -> None:
        """Test encoding and decoding."""
        original = "Hello, World!"
        encoded = Base64Converter.encode(original)
        assert encoded == "SGVsbG8sIFdvcmxkIQ=="
        decoded = Base64Converter.decode(encoded)
        assert decoded == original


class TestDateTimeConverter:
    """Tests for DateTimeConverter."""

    def test_to_datetime(self) -> None:
        """Test converting to datetime."""
        dt = DateTimeConverter.to_datetime("2024-01-15 10:30:00")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1

    def test_to_timestamp(self) -> None:
        """Test converting to timestamp."""
        from datetime import datetime
        dt = datetime(2024, 1, 1, 0, 0, 0)
        ts = DateTimeConverter.to_timestamp(dt)
        assert ts > 0

    def test_to_string(self) -> None:
        """Test converting to string."""
        from datetime import datetime
        dt = datetime(2024, 1, 15)
        s = DateTimeConverter.to_string(dt, "%Y-%m-%d")
        assert s == "2024-01-15"


class TestUnitConverter:
    """Tests for UnitConverter."""

    def test_celsius_fahrenheit(self) -> None:
        """Test temperature conversion."""
        assert UnitConverter.celsius_to_fahrenheit(0) == 32
        assert UnitConverter.fahrenheit_to_celsius(32) == 0

    def test_km_miles(self) -> None:
        """Test distance conversion."""
        assert UnitConverter.km_to_miles(1) == pytest.approx(0.621371, 0.01)
        assert UnitConverter.miles_to_km(1) == pytest.approx(1.60934, 0.01)


class TestDataFrameConverter:
    """Tests for DataFrameConverter."""

    def test_to_csv(self) -> None:
        """Test writing CSV."""
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            assert DataFrameConverter.to_csv(data, path) is True
        finally:
            os.unlink(path)

    def test_from_csv(self) -> None:
        """Test reading CSV."""
        data = [{"a": "1", "b": "2"}]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name
        try:
            DataFrameConverter.to_csv(data, path)
            result = DataFrameConverter.from_csv(path)
            assert result is not None
        finally:
            os.unlink(path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])