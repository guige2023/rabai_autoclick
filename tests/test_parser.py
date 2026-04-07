"""Tests for parsing utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.parser import (
    StringParser,
    ExpressionParser,
    TemplateParser,
    KeyValueParser,
    INIParser,
)


class TestStringParser:
    """Tests for StringParser."""

    def test_create(self) -> None:
        """Test creating parser."""
        parser = StringParser("hello world")
        assert parser.remaining == "hello world"

    def test_read(self) -> None:
        """Test reading."""
        parser = StringParser("hello")
        assert parser.read(3) == "hel"
        assert parser.pos == 3

    def test_peek(self) -> None:
        """Test peeking."""
        parser = StringParser("hello")
        assert parser.peek(3) == "hel"
        assert parser.pos == 0

    def test_at_end(self) -> None:
        """Test at_end."""
        parser = StringParser("a")
        assert parser.at_end is False
        parser.read()
        assert parser.at_end is True

    def test_read_while(self) -> None:
        """Test read_while."""
        parser = StringParser("123abc")
        result = parser.read_while(str.isdigit)
        assert result == "123"
        assert parser.remaining == "abc"


class TestExpressionParser:
    """Tests for ExpressionParser."""

    def test_create(self) -> None:
        """Test creating parser."""
        parser = ExpressionParser()
        assert parser is not None

    def test_eval_number(self) -> None:
        """Test evaluating number."""
        parser = ExpressionParser()
        assert parser.evaluate("42") == 42

    def test_eval_add(self) -> None:
        """Test evaluating addition."""
        parser = ExpressionParser()
        assert parser.evaluate("1 + 2") == 3

    def test_eval_mult(self) -> None:
        """Test evaluating multiplication."""
        parser = ExpressionParser()
        assert parser.evaluate("2 * 3") == 6

    def test_eval_with_vars(self) -> None:
        """Test evaluating with variables."""
        parser = ExpressionParser()
        assert parser.evaluate("a + b", {"a": 1, "b": 2}) == 3


class TestTemplateParser:
    """Tests for TemplateParser."""

    def test_render(self) -> None:
        """Test rendering template."""
        parser = TemplateParser("Hello, ${name}!")
        result = parser.render({"name": "World"})
        assert result == "Hello, World!"

    def test_render_multiple(self) -> None:
        """Test rendering multiple placeholders."""
        parser = TemplateParser("${a} + ${b} = ${c}")
        result = parser.render({"a": 1, "b": 2, "c": 3})
        assert result == "1 + 2 = 3"

    def test_render_with_filter(self) -> None:
        """Test rendering with filter."""
        parser = TemplateParser("${name|upper}")
        result = parser.render({"name": "hello"})
        assert result == "HELLO"


class TestKeyValueParser:
    """Tests for KeyValueParser."""

    def test_parse(self) -> None:
        """Test parsing."""
        parser = KeyValueParser()
        result = parser.parse("a=1;b=2")
        assert result["a"] == "1"
        assert result["b"] == "2"

    def test_to_string(self) -> None:
        """Test converting to string."""
        parser = KeyValueParser()
        result = parser.to_string({"a": "1", "b": "2"})
        assert "a=1" in result
        assert "b=2" in result


class TestINIParser:
    """Tests for INIParser."""

    def test_parse(self) -> None:
        """Test parsing INI."""
        parser = INIParser()
        text = """
[section1]
key1=value1

[section2]
key2=value2
"""
        sections = parser.parse(text)
        assert "section1" in sections
        assert sections["section1"]["key1"] == "value1"

    def test_get(self) -> None:
        """Test getting value."""
        parser = INIParser()
        parser.parse("[test]\nkey=value")
        assert parser.get("test", "key") == "value"

    def test_get_default(self) -> None:
        """Test getting with default."""
        parser = INIParser()
        parser.parse("")
        assert parser.get("test", "key", "default") == "default"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])