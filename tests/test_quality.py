"""Tests for quality/assertion utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.quality import (
    type_check,
    isinstance_check,
    assert_not_none,
    assert_positive,
    assert_non_empty,
    assert_in_range,
    invariant,
    Contract,
    Validator,
    ValidationError,
)


class TestTypeCheck:
    """Tests for type_check."""

    def test_valid_type(self) -> None:
        """Test type check with valid type."""
        result = type_check(42, int)
        assert result == 42

    def test_invalid_type(self) -> None:
        """Test type check with invalid type."""
        with pytest.raises(TypeError):
            type_check("string", int)


class TestIsinstanceCheck:
    """Tests for isinstance_check."""

    def test_valid_instance(self) -> None:
        """Test isinstance check with valid type."""
        result = isinstance_check(42, (int, str))
        assert result == 42


class TestAssertNotNone:
    """Tests for assert_not_none."""

    def test_not_none(self) -> None:
        """Test value is not None."""
        result = assert_not_none(42)
        assert result == 42

    def test_none_raises(self) -> None:
        """Test None raises."""
        with pytest.raises(ValueError):
            assert_not_none(None)


class TestAssertPositive:
    """Tests for assert_positive."""

    def test_positive(self) -> None:
        """Test positive value."""
        result = assert_positive(5)
        assert result == 5

    def test_negative_raises(self) -> None:
        """Test negative raises."""
        with pytest.raises(ValueError):
            assert_positive(-1)


class TestAssertInRange:
    """Tests for assert_in_range."""

    def test_in_range(self) -> None:
        """Test value in range."""
        result = assert_in_range(5, 0, 10)
        assert result == 5

    def test_out_of_range_raises(self) -> None:
        """Test out of range raises."""
        with pytest.raises(ValueError):
            assert_in_range(15, 0, 10)


class TestInvariant:
    """Tests for invariant."""

    def test_true_condition(self) -> None:
        """Test true condition passes."""
        invariant(True)

    def test_false_condition_raises(self) -> None:
        """Test false condition raises."""
        with pytest.raises(AssertionError):
            invariant(False)


class TestContract:
    """Tests for Contract."""

    def test_pre_condition(self) -> None:
        """Test precondition."""
        @Contract.pre(lambda x: x > 0)
        def positive_sqrt(x):
            return x ** 0.5

        assert positive_sqrt(4) == 2

    def test_post_condition(self) -> None:
        """Test postcondition."""
        @Contract.post(lambda r: r >= 0)
        def always_positive(x):
            return abs(x)

        assert always_positive(-5) == 5


class TestValidator:
    """Tests for Validator."""

    def test_valid_chained(self) -> None:
        """Test chained validators."""
        result = (
            Validator(5, "value")
            .is_not_none()
            .is_type(int)
            .is_positive()
            .validate()
        )
        assert result == 5

    def test_validation_error(self) -> None:
        """Test validation error collection."""
        validator = (
            Validator(5, "value")
            .is_type(str)
            .is_positive()
        )

        with pytest.raises(ValidationError) as exc_info:
            validator.validate()

        assert len(exc_info.value.errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])