"""Tests for string utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.string_utils import (
    capitalize,
    title_case,
    snake_to_camel,
    camel_to_snake,
    kebab_to_snake,
    snake_to_kebab,
    strip_whitespace,
    normalize_whitespace,
    truncate,
    remove_prefix,
    remove_suffix,
    is_blank,
    is_numeric,
    is_alpha,
    is_alphanumeric,
    count_words,
    reverse_words,
    reverse_string,
    is_palindrome,
    contains,
    replace_all,
    split_lines,
    join_lines,
    indent,
    unindent,
    remove_special_chars,
    normalize_unicode,
    to_ascii,
    word_wrap,
    extract_numbers,
    extract_words,
    filter_chars,
    map_chars,
    pad_left,
    pad_right,
    center_text,
)


class TestCapitalize:
    """Tests for capitalize function."""

    def test_capitalize(self) -> None:
        """Test capitalizing first letter."""
        assert capitalize("hello") == "Hello"
        assert capitalize("HELLO") == "HELLO"
        assert capitalize("") == ""

    def test_capitalize_single_char(self) -> None:
        """Test capitalizing single character."""
        assert capitalize("a") == "A"


class TestTitleCase:
    """Tests for title_case function."""

    def test_title_case(self) -> None:
        """Test converting to title case."""
        assert title_case("hello world") == "Hello World"


class TestSnakeToCamel:
    """Tests for snake_to_camel function."""

    def test_snake_to_camel(self) -> None:
        """Test snake case to camel case."""
        assert snake_to_camel("hello_world") == "helloWorld"
        assert snake_to_camel("hello_world_test") == "helloWorldTest"

    def test_snake_to_camel_single_word(self) -> None:
        """Test single word stays same."""
        assert snake_to_camel("hello") == "hello"


class TestCamelToSnake:
    """Tests for camel_to_snake function."""

    def test_camel_to_snake(self) -> None:
        """Test camel case to snake case."""
        assert camel_to_snake("helloWorld") == "hello_world"
        assert camel_to_snake("HelloWorld") == "hello_world"

    def test_camel_to_snake_single_word(self) -> None:
        """Test single word stays same."""
        assert camel_to_snake("hello") == "hello"


class TestKebabToSnake:
    """Tests for kebab_to_snake function."""

    def test_kebab_to_snake(self) -> None:
        """Test kebab case to snake case."""
        assert kebab_to_snake("hello-world") == "hello_world"


class TestSnakeToKebab:
    """Tests for snake_to_kebab function."""

    def test_snake_to_kebab(self) -> None:
        """Test snake case to kebab case."""
        assert snake_to_kebab("hello_world") == "hello-world"


class TestStripWhitespace:
    """Tests for strip_whitespace function."""

    def test_strip_whitespace(self) -> None:
        """Test stripping all whitespace."""
        assert strip_whitespace("hello world") == "helloworld"
        assert strip_whitespace("a b c") == "abc"


class TestNormalizeWhitespace:
    """Tests for normalize_whitespace function."""

    def test_normalize_whitespace(self) -> None:
        """Test normalizing whitespace."""
        assert normalize_whitespace("hello  world") == "hello world"
        assert normalize_whitespace("a  b   c") == "a b c"


class TestTruncate:
    """Tests for truncate function."""

    def test_truncate(self) -> None:
        """Test truncating string."""
        assert truncate("hello world", 5) == "he..."
        assert truncate("hello world", 8, "..") == "hello .."

    def test_truncate_short_string(self) -> None:
        """Test no truncation needed."""
        assert truncate("hi", 10) == "hi"


class TestRemovePrefix:
    """Tests for remove_prefix function."""

    def test_remove_prefix(self) -> None:
        """Test removing prefix."""
        assert remove_prefix("hello_world", "hello_") == "world"
        assert remove_prefix("hello_world", "xxx") == "hello_world"


class TestRemoveSuffix:
    """Tests for remove_suffix function."""

    def test_remove_suffix(self) -> None:
        """Test removing suffix."""
        assert remove_suffix("hello_world", "_world") == "hello"
        assert remove_suffix("hello_world", "xxx") == "hello_world"


class TestIsBlank:
    """Tests for is_blank function."""

    def test_is_blank(self) -> None:
        """Test checking if blank."""
        assert is_blank("")
        assert is_blank("   ")
        assert not is_blank("hello")


class TestIsNumeric:
    """Tests for is_numeric function."""

    def test_is_numeric(self) -> None:
        """Test checking if numeric."""
        assert is_numeric("123")
        assert is_numeric("0")
        assert not is_numeric("12a")


class TestIsAlpha:
    """Tests for is_alpha function."""

    def test_is_alpha(self) -> None:
        """Test checking if alphabetic."""
        assert is_alpha("hello")
        assert not is_alpha("hello123")


class TestIsAlphanumeric:
    """Tests for is_alphanumeric function."""

    def test_is_alphanumeric(self) -> None:
        """Test checking if alphanumeric."""
        assert is_alphanumeric("hello123")
        assert not is_alphanumeric("hello world")


class TestCountWords:
    """Tests for count_words function."""

    def test_count_words(self) -> None:
        """Test counting words."""
        assert count_words("hello world") == 2
        assert count_words("one two three four") == 4


class TestReverseWords:
    """Tests for reverse_words function."""

    def test_reverse_words(self) -> None:
        """Test reversing words."""
        assert reverse_words("hello world") == "world hello"


class TestReverseString:
    """Tests for reverse_string function."""

    def test_reverse_string(self) -> None:
        """Test reversing string."""
        assert reverse_string("hello") == "olleh"


class TestIsPalindrome:
    """Tests for is_palindrome function."""

    def test_is_palindrome(self) -> None:
        """Test checking palindrome."""
        assert is_palindrome("racecar")
        assert is_palindrome("A man a plan a canal Panama")
        assert not is_palindrome("hello")


class TestContains:
    """Tests for contains function."""

    def test_contains(self) -> None:
        """Test checking if contains substring."""
        assert contains("hello world", "world")
        assert not contains("hello world", "foo")

    def test_contains_case_insensitive(self) -> None:
        """Test case insensitive contains."""
        assert contains("Hello World", "world", case_sensitive=False)
        assert not contains("Hello World", "world", case_sensitive=True)


class TestReplaceAll:
    """Tests for replace_all function."""

    def test_replace_all(self) -> None:
        """Test replacing all occurrences."""
        assert replace_all("hello world world", "world", "python") == "hello python python"


class TestSplitLines:
    """Tests for split_lines function."""

    def test_split_lines(self) -> None:
        """Test splitting into lines."""
        assert split_lines("hello\nworld") == ["hello", "world"]
        assert split_lines("line1\nline2\nline3") == ["line1", "line2", "line3"]


class TestJoinLines:
    """Tests for join_lines function."""

    def test_join_lines(self) -> None:
        """Test joining lines."""
        assert join_lines(["hello", "world"]) == "hello\nworld"


class TestIndent:
    """Tests for indent function."""

    def test_indent(self) -> None:
        """Test indenting text."""
        assert indent("hello\nworld", 4) == "    hello\n    world"


class TestUnindent:
    """Tests for unindent function."""

    def test_unindent(self) -> None:
        """Test unindenting text."""
        assert unindent("    hello\n    world") == "hello\nworld"


class TestRemoveSpecialChars:
    """Tests for remove_special_chars function."""

    def test_remove_special_chars(self) -> None:
        """Test removing special characters."""
        assert remove_special_chars("hello@world!") == "helloworld"
        assert remove_special_chars("a1!b2@", keep="!@") == "a1!b2@"


class TestNormalizeUnicode:
    """Tests for normalize_unicode function."""

    def test_normalize_unicode(self) -> None:
        """Test normalizing unicode."""
        result = normalize_unicode("café")
        assert "é" in result or "e" in result


class TestToAscii:
    """Tests for to_ascii function."""

    def test_to_ascii(self) -> None:
        """Test converting to ASCII."""
        assert to_ascii("café") == "cafe"


class TestWordWrap:
    """Tests for word_wrap function."""

    def test_word_wrap(self) -> None:
        """Test word wrapping."""
        result = word_wrap("hello world", 5)
        assert len(result) >= 2


class TestExtractNumbers:
    """Tests for extract_numbers function."""

    def test_extract_numbers(self) -> None:
        """Test extracting numbers."""
        assert extract_numbers("abc 123 def 456") == [123.0, 456.0]
        assert extract_numbers("no numbers") == []


class TestExtractWords:
    """Tests for extract_words function."""

    def test_extract_words(self) -> None:
        """Test extracting words."""
        assert extract_words("hello world 123") == ["hello", "world"]


class TestFilterChars:
    """Tests for filter_chars function."""

    def test_filter_chars(self) -> None:
        """Test filtering characters."""
        assert filter_chars("hello123", str.isalpha) == "hello"


class TestMapChars:
    """Tests for map_chars function."""

    def test_map_chars(self) -> None:
        """Test mapping characters."""
        assert map_chars("hello", str.upper) == "HELLO"


class TestPadLeft:
    """Tests for pad_left function."""

    def test_pad_left(self) -> None:
        """Test padding left."""
        assert pad_left("hello", 10) == "     hello"
        assert pad_left("hello", 10, "0") == "00000hello"


class TestPadRight:
    """Tests for pad_right function."""

    def test_pad_right(self) -> None:
        """Test padding right."""
        assert pad_right("hello", 10) == "hello     "
        assert pad_right("hello", 10, "0") == "hello00000"


class TestCenterText:
    """Tests for center_text function."""

    def test_center_text(self) -> None:
        """Test centering text."""
        result = center_text("hi", 10)
        assert len(result) == 10
        assert result.startswith(" ")
        assert result.endswith(" ")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
