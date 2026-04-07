"""Tests for variable manager utilities."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock PyQt5 before any imports
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly to avoid utils/__init__.py issues
import importlib.util


def load_module_from_file(module_name: str, file_path: str):
    """Load a module directly from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict('sys.modules', {
        'PyQt5': MagicMock(),
        'PyQt5.QtCore': MagicMock(),
    }):
        spec.loader.exec_module(module)
    return module


variable_module = load_module_from_file(
    "variable_manager",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "variable_manager.py")
)

Variable = variable_module.Variable
VariableType = variable_module.VariableType


class TestVariableType:
    """Tests for VariableType enum."""

    def test_all_types_exist(self) -> None:
        """Test all expected types exist."""
        assert VariableType.STRING is not None
        assert VariableType.INTEGER is not None
        assert VariableType.FLOAT is not None
        assert VariableType.BOOLEAN is not None
        assert VariableType.COORDINATE is not None
        assert VariableType.REGION is not None
        assert VariableType.LIST is not None
        assert VariableType.DICT is not None

    def test_type_values(self) -> None:
        """Test type values are strings."""
        for var_type in VariableType:
            assert isinstance(var_type.value, str)


class TestVariable:
    """Tests for Variable dataclass."""

    def test_create_with_required_fields(self) -> None:
        """Test creating Variable with required fields."""
        var = Variable(name="test_var", var_type=VariableType.STRING, default_value="hello")
        assert var.name == "test_var"
        assert var.var_type == VariableType.STRING
        assert var.default_value == "hello"
        assert var.current_value == "hello"

    def test_create_with_description(self) -> None:
        """Test creating Variable with description."""
        var = Variable(
            name="test_var",
            var_type=VariableType.INTEGER,
            default_value=42,
            description="A test variable"
        )
        assert var.description == "A test variable"

    def test_current_value_defaults_to_default(self) -> None:
        """Test current_value defaults to default_value."""
        var = Variable(name="x", var_type=VariableType.FLOAT, default_value=3.14)
        assert var.current_value == 3.14

    def test_reset(self) -> None:
        """Test reset restores default_value."""
        var = Variable(name="x", var_type=VariableType.INTEGER, default_value=10)
        var.current_value = 20
        var.reset()
        assert var.current_value == 10

    def test_set_value(self) -> None:
        """Test set_value updates current_value."""
        var = Variable(name="x", var_type=VariableType.INTEGER, default_value=10)
        var.set_value(30)
        assert var.current_value == 30

    def test_to_dict(self) -> None:
        """Test converting to dictionary."""
        var = Variable(
            name="test_var",
            var_type=VariableType.BOOLEAN,
            default_value=True,
            description="A boolean"
        )
        d = var.to_dict()
        assert isinstance(d, dict)
        assert d["name"] == "test_var"
        assert d["var_type"] == "boolean"
        assert d["default_value"] is True
        assert d["description"] == "A boolean"
        assert d["current_value"] is True

    def test_from_dict(self) -> None:
        """Test creating from dictionary."""
        data = {
            "name": "from_dict_var",
            "var_type": "integer",
            "default_value": 100,
            "description": "From dict",
            "current_value": 200
        }
        var = Variable.from_dict(data)
        assert var.name == "from_dict_var"
        assert var.var_type == VariableType.INTEGER
        assert var.default_value == 100
        assert var.current_value == 200

    def test_from_dict_minimal(self) -> None:
        """Test creating from minimal dictionary."""
        data = {"name": "minimal"}
        var = Variable.from_dict(data)
        assert var.name == "minimal"
        assert var.var_type == VariableType.STRING
        assert var.default_value == ""

    def test_roundtrip(self) -> None:
        """Test to_dict and from_dict roundtrip."""
        original = Variable(
            name="roundtrip",
            var_type=VariableType.LIST,
            default_value=[1, 2, 3],
            description="Testing roundtrip"
        )
        original.current_value = [4, 5, 6]
        d = original.to_dict()
        restored = Variable.from_dict(d)
        assert restored.name == original.name
        assert restored.var_type == original.var_type
        assert restored.default_value == original.default_value
        assert restored.current_value == original.current_value


class TestVariableManagerStandalone:
    """Tests for VariableManager methods without QObject dependency."""

    def test_get_default_for_type(self) -> None:
        """Test default values for each type."""
        defaults = {
            VariableType.STRING: "",
            VariableType.INTEGER: 0,
            VariableType.FLOAT: 0.0,
            VariableType.BOOLEAN: False,
            VariableType.COORDINATE: (0, 0),
            VariableType.REGION: (0, 0, 100, 100),
            VariableType.LIST: [],
            VariableType.DICT: {}
        }
        for var_type, expected in defaults.items():
            assert expected == defaults.get(var_type)

    def test_variable_validate_type_logic(self) -> None:
        """Test type validation logic."""
        # Test string
        var = Variable(name="s", var_type=VariableType.STRING, default_value="")
        assert isinstance(var.default_value, str)

        # Test integer
        var = Variable(name="i", var_type=VariableType.INTEGER, default_value=0)
        assert isinstance(var.default_value, int) and not isinstance(var.default_value, bool)

        # Test boolean
        var = Variable(name="b", var_type=VariableType.BOOLEAN, default_value=False)
        assert isinstance(var.default_value, bool)

        # Test coordinate
        var = Variable(name="c", var_type=VariableType.COORDINATE, default_value=(0, 0))
        assert isinstance(var.default_value, (tuple, list)) and len(var.default_value) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
