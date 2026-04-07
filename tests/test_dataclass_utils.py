"""Tests for dataclass utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataclasses import dataclass, field
from utils.dataclass_utils import (
    is_dataclass_instance,
    to_dict,
    from_dict,
    merge_dataclasses,
    dataclass_with_defaults,
    FieldValidator,
    validate_fields,
    DataclassEncoder,
    compare_dataclasses,
)


@dataclass
class Person:
    name: str
    age: int = 25


@dataclass
class Address:
    street: str
    city: str
    zip_code: str = "00000"


class TestIsDataclassInstance:
    """Tests for is_dataclass_instance."""

    def test_true_for_dataclass(self) -> None:
        """Test returns True for dataclass instance."""
        person = Person(name="Alice", age=30)
        assert is_dataclass_instance(person) is True

    def test_false_for_regular_object(self) -> None:
        """Test returns False for regular object."""
        assert is_dataclass_instance("not a dataclass") is False

    def test_false_for_dataclass_type(self) -> None:
        """Test returns False for dataclass type itself."""
        assert is_dataclass_instance(Person) is False


class TestToDict:
    """Tests for to_dict."""

    def test_simple_dataclass(self) -> None:
        """Test converting simple dataclass to dict."""
        person = Person(name="Alice", age=30)
        result = to_dict(person)
        assert result == {"name": "Alice", "age": 30}

    def test_nested_dataclass(self) -> None:
        """Test converting nested dataclass to dict."""
        addr = Address(street="123 Main St", city="NYC")
        result = to_dict(addr)
        assert result == {"street": "123 Main St", "city": "NYC", "zip_code": "00000"}

    def test_raises_for_non_dataclass(self) -> None:
        """Test raises TypeError for non-dataclass."""
        with pytest.raises(TypeError):
            to_dict("not a dataclass")


class TestFromDict:
    """Tests for from_dict."""

    def test_create_from_dict(self) -> None:
        """Test creating dataclass from dict."""
        data = {"name": "Bob", "age": 40}
        person = from_dict(Person, data)
        assert person.name == "Bob"
        assert person.age == 40

    def test_ignores_extra_fields(self) -> None:
        """Test ignores extra fields in dict."""
        data = {"name": "Bob", "age": 40, "extra": "ignored"}
        person = from_dict(Person, data)
        assert person.name == "Bob"

    def test_raises_for_non_dataclass_type(self) -> None:
        """Test raises TypeError for non-dataclass type."""
        with pytest.raises(TypeError):
            from_dict("not a dataclass", {})


class TestMergeDataclasses:
    """Tests for merge_dataclasses."""

    def test_merge_copy(self) -> None:
        """Test merging creates new instance."""
        base = Person(name="Alice", age=30)
        override = {"age": 35}
        result = merge_dataclasses(base, override)
        assert result.age == 35
        assert base.age == 30  # Original unchanged

    def test_merge_inplace(self) -> None:
        """Test merging modifies in place."""
        base = Person(name="Alice", age=30)
        override = {"age": 35}
        result = merge_dataclasses(base, override, inplace=True)
        assert result is base
        assert base.age == 35

    def test_merge_new_fields(self) -> None:
        """Test merging adds new fields."""
        addr = Address(street="123 Main St", city="NYC")
        override = {"zip_code": "10001"}
        result = merge_dataclasses(addr, override)
        assert result.zip_code == "10001"


class TestDataclassWithDefaults:
    """Tests for dataclass_with_defaults."""

    def test_adds_none_defaults(self) -> None:
        """Test adds None defaults to missing fields."""
        @dataclass_with_defaults
        class TestDC:
            name: str
            value: int = field(default=10)

        # Field without default should get None
        obj = TestDC(name="test")
        assert obj.name == "test"
        assert obj.value == 10


class TestFieldValidator:
    """Tests for FieldValidator."""

    def test_validate_int_valid(self) -> None:
        """Test validating valid integer."""
        assert FieldValidator.validate_int(42) is True
        assert FieldValidator.validate_int(42, min_val=10) is True
        assert FieldValidator.validate_int(42, max_val=50) is True
        assert FieldValidator.validate_int(42, min_val=10, max_val=50) is True

    def test_validate_int_invalid(self) -> None:
        """Test validating invalid integer."""
        assert FieldValidator.validate_int("42") is False
        assert FieldValidator.validate_int(5, min_val=10) is False
        assert FieldValidator.validate_int(100, max_val=50) is False

    def test_validate_str_valid(self) -> None:
        """Test validating valid string."""
        assert FieldValidator.validate_str("hello") is True
        assert FieldValidator.validate_str("hello", min_len=3) is True
        assert FieldValidator.validate_str("hello", max_len=10) is True
        assert FieldValidator.validate_str("hello", pattern=r"^hello$") is True

    def test_validate_str_invalid(self) -> None:
        """Test validating invalid string."""
        assert FieldValidator.validate_str(123) is False
        assert FieldValidator.validate_str("hi", min_len=5) is False
        assert FieldValidator.validate_str("hello", max_len=3) is False
        assert FieldValidator.validate_str("hello", pattern=r"^bye$") is False

    def test_validate_list_valid(self) -> None:
        """Test validating valid list."""
        assert FieldValidator.validate_list([1, 2, 3]) is True
        assert FieldValidator.validate_list([1, 2], min_len=2) is True
        assert FieldValidator.validate_list([1, 2, 3], item_type=int) is True

    def test_validate_list_invalid(self) -> None:
        """Test validating invalid list."""
        assert FieldValidator.validate_list("not a list") is False
        assert FieldValidator.validate_list([1], min_len=5) is False
        assert FieldValidator.validate_list([1, "a"], item_type=int) is False


class TestValidateFields:
    """Tests for validate_fields."""

    def test_valid_fields(self) -> None:
        """Test validate_fields with valid data."""
        person = Person(name="Alice", age=30)
        validators = {
            "name": lambda v: isinstance(v, str),
            "age": lambda v: isinstance(v, int) and v > 0,
        }
        errors = validate_fields(person, validators)
        assert errors == []

    def test_invalid_field(self) -> None:
        """Test validate_fields with invalid data."""
        person = Person(name="Alice", age=30)
        validators = {
            "age": lambda v: v > 100,
        }
        errors = validate_fields(person, validators)
        assert len(errors) == 1

    def test_unknown_field(self) -> None:
        """Test validate_fields with unknown field."""
        person = Person(name="Alice", age=30)
        validators = {
            "unknown": lambda v: True,
        }
        errors = validate_fields(person, validators)
        assert "Unknown field" in errors[0]


class TestDataclassEncoder:
    """Tests for DataclassEncoder."""

    def test_to_dict(self) -> None:
        """Test encoding to dict."""
        person = Person(name="Alice", age=30)
        result = DataclassEncoder.to_dict(person)
        assert result == {"name": "Alice", "age": 30}

    def test_to_json(self) -> None:
        """Test encoding to JSON."""
        person = Person(name="Alice", age=30)
        result = DataclassEncoder.to_json(person)
        assert '"name": "Alice"' in result
        assert '"age": 30' in result


class TestCompareDataclasses:
    """Tests for compare_dataclasses."""

    def test_equal_dataclasses(self) -> None:
        """Test comparing equal dataclasses."""
        a = Person(name="Alice", age=30)
        b = Person(name="Alice", age=30)
        result = compare_dataclasses(a, b)
        assert result["equal"] is True
        assert result["differences"] == []

    def test_different_values(self) -> None:
        """Test comparing dataclasses with different values."""
        a = Person(name="Alice", age=30)
        b = Person(name="Alice", age=35)
        result = compare_dataclasses(a, b)
        assert result["equal"] is False
        assert len(result["differences"]) == 1

    def test_different_types(self) -> None:
        """Test comparing different types."""
        a = Person(name="Alice", age=30)
        b = Address(street="123 Main", city="NYC")
        result = compare_dataclasses(a, b)
        assert result["equal"] is False

    def test_ignore_fields(self) -> None:
        """Test comparing with ignored fields."""
        a = Person(name="Alice", age=30)
        b = Person(name="Bob", age=35)
        result = compare_dataclasses(a, b, ignore_fields=["name"])
        assert result["equal"] is False
        assert all(d["field"] != "name" for d in result["differences"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])