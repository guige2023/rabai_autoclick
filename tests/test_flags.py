"""Tests for feature flag utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.flags import (
    FlagType,
    Flag,
    FlagValue,
    FlagEvaluator,
    FeatureFlags,
    FlagPersistence,
    get_flags,
    is_flag_enabled,
    set_flag,
)


class TestFlagType:
    """Tests for FlagType."""

    def test_values(self) -> None:
        """Test flag type values."""
        assert FlagType.BOOLEAN.value == "boolean"
        assert FlagType.STRING.value == "string"


class TestFlag:
    """Tests for Flag."""

    def test_create(self) -> None:
        """Test creating flag."""
        flag = Flag(
            name="test",
            flag_type=FlagType.BOOLEAN,
            default_value=True,
        )
        assert flag.name == "test"
        assert flag.default_value is True


class TestFlagValue:
    """Tests for FlagValue."""

    def test_create(self) -> None:
        """Test creating flag value."""
        flag = Flag(name="test", flag_type=FlagType.BOOLEAN, default_value=True)
        value = FlagValue(flag, True, is_default=False)
        assert value.value is True
        assert value.is_default is False


class TestFlagEvaluator:
    """Tests for FlagEvaluator."""

    def test_create(self) -> None:
        """Test creating evaluator."""
        evaluator = FlagEvaluator()
        assert len(evaluator._rules) == 0

    def test_add_rule(self) -> None:
        """Test adding rule."""
        evaluator = FlagEvaluator()
        evaluator.add_rule("test", lambda ctx: True)
        assert "test" in evaluator._rules

    def test_evaluate_disabled(self) -> None:
        """Test evaluating disabled flag."""
        evaluator = FlagEvaluator()
        flag = Flag(name="test", flag_type=FlagType.BOOLEAN, default_value=True, enabled=False)
        result = evaluator.evaluate(flag)
        assert result.is_default is True


class TestFeatureFlags:
    """Tests for FeatureFlags."""

    def test_create(self) -> None:
        """Test creating flags."""
        flags = FeatureFlags()
        assert len(flags._flags) == 0

    def test_add_flag(self) -> None:
        """Test adding flag."""
        flags = FeatureFlags()
        flags.add_flag("test", FlagType.BOOLEAN, True)
        assert flags.get_flag("test") is not None

    def test_get_flag(self) -> None:
        """Test getting flag."""
        flags = FeatureFlags()
        flags.add_flag("test", FlagType.BOOLEAN, True)
        flag = flags.get_flag("test")
        assert flag.name == "test"

    def test_set_value(self) -> None:
        """Test setting value."""
        flags = FeatureFlags()
        flags.add_flag("test", FlagType.BOOLEAN, True)
        flags.set_value("test", False)
        value = flags.get_value("test")
        assert value.value is False

    def test_is_enabled(self) -> None:
        """Test checking if enabled."""
        flags = FeatureFlags()
        flags.add_flag("test", FlagType.BOOLEAN, True)
        assert flags.is_enabled("test") is True

    def test_enable_disable(self) -> None:
        """Test enabling and disabling."""
        flags = FeatureFlags()
        flags.add_flag("test", FlagType.BOOLEAN, True)
        flags.disable("test")
        assert flags.is_enabled("test") is False
        flags.enable("test")
        assert flags.is_enabled("test") is True

    def test_list_flags(self) -> None:
        """Test listing flags."""
        flags = FeatureFlags()
        flags.add_flag("a", FlagType.BOOLEAN, True)
        flags.add_flag("b", FlagType.STRING, "test")
        flist = flags.list_flags()
        assert len(flist) == 2


class TestFlagPersistence:
    """Tests for FlagPersistence."""

    def test_save_load(self) -> None:
        """Test saving and loading."""
        import tempfile
        import os

        flags = FeatureFlags()
        flags.add_flag("test", FlagType.BOOLEAN, True)
        flags.set_value("test", False)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "flags.json")
            persister = FlagPersistence(path)
            assert persister.save(flags) is True

            flags2 = FeatureFlags()
            persister.load(flags2)
            assert flags2.get_flag("test") is not None


class TestModuleFunctions:
    """Tests for module-level functions."""

    def test_get_flags(self) -> None:
        """Test getting global flags."""
        flags = get_flags()
        assert flags is not None

    def test_is_flag_enabled(self) -> None:
        """Test is_flag_enabled function."""
        result = is_flag_enabled("nonexistent")
        assert result is False

    def test_set_flag(self) -> None:
        """Test set_flag function."""
        result = set_flag("nonexistent", True)
        assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])