"""Tests for inspect utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.inspect_utils import (
    get_signature,
    get_parameters,
    get_return_type,
    get_type_hints_func,
    has_parameter,
    get_parameter_defaults,
    is_async,
    is_generator,
    is_class_method,
    is_static_method,
    get_qualified_name,
    get_source,
    get_file,
    get_line_number,
    get_doc,
    get_attributes,
    get_method_names,
    get_callable_members,
    get_class_hierarchy,
    get_function_info,
    get_object_info,
    format_signature,
    format_parameters,
)


def example_func(a: int, b: str = "default", c: float = 1.0) -> bool:
    """Example function for testing."""
    return True


async def async_example(x: int) -> str:
    """Async function for testing."""
    return str(x)


def generator_example(items: list):
    """Generator function for testing."""
    for item in items:
        yield item


class TestClass:
    """Test class for testing."""

    def method(self):
        """Regular method."""
        pass

    @classmethod
    def class_method(cls):
        """Class method."""
        pass

    @staticmethod
    def static_method():
        """Static method."""
        pass

    def _private(self):
        """Private method."""
        pass


class TestGetSignature:
    """Tests for get_signature function."""

    def test_get_signature(self) -> None:
        """Test getting function signature."""
        sig = get_signature(example_func)
        assert sig is not None

    def test_get_signature_builtin(self) -> None:
        """Test getting builtin function signature."""
        sig = get_signature(len)
        # len has a signature on newer Python versions
        assert sig is not None


class TestGetParameters:
    """Tests for get_parameters function."""

    def test_get_parameters(self) -> None:
        """Test getting function parameters."""
        params = get_parameters(example_func)
        assert len(params) == 3
        assert params[0].name == "a"
        assert params[1].name == "b"
        assert params[2].name == "c"


class TestGetReturnType:
    """Tests for get_return_type function."""

    def test_get_return_type(self) -> None:
        """Test getting return type."""
        result = get_return_type(example_func)
        assert result == bool


class TestGetTypeHintsFunc:
    """Tests for get_type_hints_func function."""

    def test_get_type_hints(self) -> None:
        """Test getting type hints."""
        hints = get_type_hints_func(example_func)
        assert "a" in hints
        assert hints["a"] == int


class TestHasParameter:
    """Tests for has_parameter function."""

    def test_has_parameter_true(self) -> None:
        """Test parameter exists."""
        assert has_parameter(example_func, "a")

    def test_has_parameter_false(self) -> None:
        """Test parameter doesn't exist."""
        assert not has_parameter(example_func, "z")


class TestGetParameterDefaults:
    """Tests for get_parameter_defaults function."""

    def test_get_defaults(self) -> None:
        """Test getting parameter defaults."""
        defaults = get_parameter_defaults(example_func)
        assert "b" in defaults
        assert defaults["b"] == "default"


class TestIsAsync:
    """Tests for is_async function."""

    def test_is_async_true(self) -> None:
        """Test async function detected."""
        assert is_async(async_example)

    def test_is_async_false(self) -> None:
        """Test non-async function."""
        assert not is_async(example_func)


class TestIsGenerator:
    """Tests for is_generator function."""

    def test_is_generator_true(self) -> None:
        """Test generator function detected."""
        assert is_generator(generator_example)

    def test_is_generator_false(self) -> None:
        """Test non-generator function."""
        assert not is_generator(example_func)


class TestIsClassMethod:
    """Tests for is_class_method function."""

    def test_regular_method(self) -> None:
        """Test regular method not detected."""
        assert not is_class_method(TestClass.method)

    def test_static_method(self) -> None:
        """Test static method not detected as classmethod."""
        assert not is_class_method(TestClass.static_method)


class TestIsStaticMethod:
    """Tests for is_static_method function."""

    def test_regular_method(self) -> None:
        """Test regular method not detected."""
        assert not is_static_method(TestClass.method)

    def test_class_method(self) -> None:
        """Test class method not detected as staticmethod."""
        assert not is_static_method(TestClass.class_method)


class TestGetQualifiedName:
    """Tests for get_qualified_name function."""

    def test_function(self) -> None:
        """Test getting function qualified name."""
        name = get_qualified_name(example_func)
        assert "example_func" in name

    def test_class(self) -> None:
        """Test getting class qualified name."""
        name = get_qualified_name(TestClass)
        assert "TestClass" in name


class TestGetSource:
    """Tests for get_source function."""

    def test_get_source(self) -> None:
        """Test getting source code."""
        source = get_source(example_func)
        assert source is not None
        assert "Example function" in source


class TestGetFile:
    """Tests for get_file function."""

    def test_get_file(self) -> None:
        """Test getting file path."""
        path = get_file(example_func)
        assert path is not None
        assert "inspect_utils" in path or ".py" in path


class TestGetLineNumber:
    """Tests for get_line_number function."""

    def test_get_line_number(self) -> None:
        """Test getting line number."""
        lineno = get_line_number(example_func)
        assert lineno is not None
        assert isinstance(lineno, int)


class TestGetDoc:
    """Tests for get_doc function."""

    def test_get_doc(self) -> None:
        """Test getting docstring."""
        doc = get_doc(example_func)
        assert doc is not None
        assert "Example function" in doc


class TestGetAttributes:
    """Tests for get_attributes function."""

    def test_get_attributes(self) -> None:
        """Test getting object attributes."""
        attrs = get_attributes(TestClass)
        assert "method" in attrs

    def test_get_attributes_no_methods(self) -> None:
        """Test getting non-method attributes."""
        attrs = get_attributes(TestClass, include_methods=False)
        assert "method" not in attrs


class TestGetMethodNames:
    """Tests for get_method_names function."""

    def test_get_method_names(self) -> None:
        """Test getting method names."""
        methods = get_method_names(TestClass())
        assert "method" in methods

    def test_excludes_private(self) -> None:
        """Test private methods excluded."""
        methods = get_method_names(TestClass())
        assert "_private" not in methods


class TestGetCallableMembers:
    """Tests for get_callable_members function."""

    def test_get_callable_members(self) -> None:
        """Test getting callable members."""
        members = get_callable_members(TestClass)
        assert len(members) > 0


class TestGetClassHierarchy:
    """Tests for get_class_hierarchy function."""

    def test_get_hierarchy(self) -> None:
        """Test getting class hierarchy."""
        hierarchy = get_class_hierarchy(TestClass)
        assert TestClass in hierarchy
        assert len(hierarchy) >= 1


class TestGetFunctionInfo:
    """Tests for get_function_info function."""

    def test_get_function_info(self) -> None:
        """Test getting function info."""
        info = get_function_info(example_func)
        assert info["name"] == "example_func"
        assert info["is_async"] is False
        assert info["is_generator"] is False
        assert "a" in info["parameters"]

    def test_get_function_info_async(self) -> None:
        """Test getting async function info."""
        info = get_function_info(async_example)
        assert info["is_async"] is True


class TestGetObjectInfo:
    """Tests for get_object_info function."""

    def test_get_object_info(self) -> None:
        """Test getting object info."""
        info = get_object_info(TestClass())
        assert info["type"] == "TestClass"
        assert "method" in info["methods"]


class TestFormatSignature:
    """Tests for format_signature function."""

    def test_format_signature(self) -> None:
        """Test formatting signature."""
        formatted = format_signature(example_func)
        assert "example_func" in formatted
        assert "a" in formatted


class TestFormatParameters:
    """Tests for format_parameters function."""

    def test_format_parameters(self) -> None:
        """Test formatting parameters."""
        formatted = format_parameters(example_func)
        assert "a" in formatted
        assert "b=" in formatted


if __name__ == "__main__":
    pytest.main([__file__, "-v"])