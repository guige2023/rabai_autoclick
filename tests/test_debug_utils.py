"""Tests for debug utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.debug_utils import (
    get_type_name,
    get_type_module,
    get_full_type_name,
    get_object_size,
    get_memory_address,
    get_call_stack,
    get_frame_info,
    format_dict,
    format_list,
    inspect_object,
    log_call,
    log_args,
    trace_calls,
    dump_locals,
    print_memory_stats,
    get_reference_count,
    get_all_attributes,
    has_attribute_chain,
    get_attribute_chain,
    is_debug_mode,
)


class TestGetTypeName:
    """Tests for get_type_name function."""

    def test_get_type_name(self) -> None:
        """Test getting type name."""
        assert get_type_name("hello") == "str"
        assert get_type_name(123) == "int"
        assert get_type_name([1, 2, 3]) == "list"


class TestGetTypeModule:
    """Tests for get_type_module function."""

    def test_get_type_module(self) -> None:
        """Test getting type module."""
        assert get_type_module("hello") == "builtins"
        assert get_type_module(123) == "builtins"


class TestGetFullTypeName:
    """Tests for get_full_type_name function."""

    def test_get_full_type_name(self) -> None:
        """Test getting full type name."""
        assert get_full_type_name("hello") == "builtins.str"
        assert get_full_type_name(123) == "builtins.int"


class TestGetObjectSize:
    """Tests for get_object_size function."""

    def test_get_object_size(self) -> None:
        """Test getting object size."""
        assert get_object_size("hello") > 0
        assert get_object_size([1, 2, 3]) > 0


class TestGetMemoryAddress:
    """Tests for get_memory_address function."""

    def test_get_memory_address(self) -> None:
        """Test getting memory address."""
        addr = get_memory_address("hello")
        assert addr.startswith("0x")


class TestGetCallStack:
    """Tests for get_call_stack function."""

    def test_get_call_stack(self) -> None:
        """Test getting call stack."""
        stack = get_call_stack()
        assert isinstance(stack, list)
        assert len(stack) > 0


class TestGetFrameInfo:
    """Tests for get_frame_info function."""

    def test_get_frame_info(self) -> None:
        """Test getting frame info."""
        info = get_frame_info(0)
        assert "filename" in info
        assert "lineno" in info
        assert "function" in info


class TestFormatDict:
    """Tests for format_dict function."""

    def test_format_dict(self) -> None:
        """Test formatting dictionary."""
        data = {"a": 1, "b": 2}
        result = format_dict(data)
        assert "a: 1" in result
        assert "b: 2" in result

    def test_format_nested_dict(self) -> None:
        """Test formatting nested dictionary."""
        data = {"a": {"b": 1}}
        result = format_dict(data)
        assert "a:" in result
        assert "b: 1" in result


class TestFormatList:
    """Tests for format_list function."""

    def test_format_list(self) -> None:
        """Test formatting list."""
        data = [1, 2, 3]
        result = format_list(data)
        assert "[0]: 1" in result
        assert "[1]: 2" in result


class TestInspectObject:
    """Tests for inspect_object function."""

    def test_inspect_object(self) -> None:
        """Test inspecting object."""
        obj = {"key": "value"}
        result = inspect_object(obj)
        assert "Type:" in result
        assert "Size:" in result
        assert "Address:" in result


class TestLogCall:
    """Tests for log_call decorator."""

    def test_log_call(self) -> None:
        """Test logging calls."""
        @log_call
        def test_func():
            return 42
        result = test_func()
        assert result == 42


class TestLogArgs:
    """Tests for log_args decorator."""

    def test_log_args(self) -> None:
        """Test logging arguments."""
        @log_args
        def test_func(a, b):
            return a + b
        result = test_func(1, 2)
        assert result == 3


class TestTraceCalls:
    """Tests for trace_calls decorator."""

    def test_trace_calls(self) -> None:
        """Test tracing calls."""
        @trace_calls
        def test_func():
            return 42
        result = test_func()
        assert result == 42


class TestDumpLocals:
    """Tests for dump_locals function."""

    def test_dump_locals(self) -> None:
        """Test dumping locals."""
        x = 1
        y = "test"
        result = dump_locals()
        assert isinstance(result, str)


class TestPrintMemoryStats:
    """Tests for print_memory_stats function."""

    def test_print_memory_stats(self) -> None:
        """Test printing memory stats."""
        print_memory_stats()


class TestGetReferenceCount:
    """Tests for get_reference_count function."""

    def test_get_reference_count(self) -> None:
        """Test getting reference count."""
        obj = [1, 2, 3]
        count = get_reference_count(obj)
        assert isinstance(count, int)


class TestGetAllAttributes:
    """Tests for get_all_attributes function."""

    def test_get_all_attributes(self) -> None:
        """Test getting all attributes."""
        attrs = get_all_attributes("hello")
        assert isinstance(attrs, list)
        assert "upper" in attrs


class TestHasAttributeChain:
    """Tests for has_attribute_chain function."""

    def test_has_attribute_chain(self) -> None:
        """Test checking attribute chain."""
        obj = {"nested": {"key": "value"}}
        assert has_attribute_chain(obj, "__class__")
        assert not has_attribute_chain(obj, "nonexistent")


class TestGetAttributeChain:
    """Tests for get_attribute_chain function."""

    def test_get_attribute_chain(self) -> None:
        """Test getting attribute chain."""
        obj = {"nested": {"key": "value"}}
        result = get_attribute_chain(obj, "__class__.__name__")
        assert result == "dict"

    def test_get_attribute_chain_default(self) -> None:
        """Test getting attribute chain with default."""
        obj = {"a": 1}
        result = get_attribute_chain(obj, "nonexistent.path", "default")
        assert result == "default"


class TestIsDebugMode:
    """Tests for is_debug_mode function."""

    def test_is_debug_mode(self) -> None:
        """Test checking debug mode."""
        result = is_debug_mode()
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
