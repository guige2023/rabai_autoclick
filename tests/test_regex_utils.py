"""Tests for regex utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.regex_utils import (
    compile,
    match,
    search,
    find_all,
    find_iter,
    replace,
    split,
    test,
    extract,
    extract_all,
    extract_dict,
    is_match,
    get_pattern,
    escape,
    unescape,
    build_alternation,
    build_sequence,
    quantifier,
    lookahead,
    lookbehind,
    capture,
    group,
    word_boundary,
    digit,
    non_digit,
    word,
    non_word,
    whitespace,
    non_whitespace,
    any_char,
    line_start,
    line_end,
    string_start,
    string_end,
    PATTERNS,
)


class TestCompile:
    """Tests for compile function."""

    def test_compile_valid(self) -> None:
        """Test compiling valid pattern."""
        result = compile(r"\d+")
        assert result is not None

    def test_compile_invalid(self) -> None:
        """Test compiling invalid pattern."""
        result = compile("[")
        assert result is None


class TestMatch:
    """Tests for match function."""

    def test_match_success(self) -> None:
        """Test matching at start."""
        result = match(r"\d+", "123abc")
        assert result is not None
        assert result.group() == "123"

    def test_match_failure(self) -> None:
        """Test no match at start."""
        result = match(r"\d+", "abc123")
        assert result is None


class TestSearch:
    """Tests for search function."""

    def test_search_success(self) -> None:
        """Test searching finds match."""
        result = search(r"\d+", "abc123")
        assert result is not None
        assert result.group() == "123"

    def test_search_failure(self) -> None:
        """Test search finds nothing."""
        result = search(r"\d+", "abc")
        assert result is None


class TestFindAll:
    """Tests for find_all function."""

    def test_find_all(self) -> None:
        """Test finding all matches."""
        result = find_all(r"\d+", "a1b22c333")
        assert result == ["1", "22", "333"]

    def test_find_all_no_match(self) -> None:
        """Test finding with no matches."""
        result = find_all(r"\d+", "abc")
        assert result == []


class TestFindIter:
    """Tests for find_iter function."""

    def test_find_iter(self) -> None:
        """Test finding all matches with objects."""
        result = find_iter(r"\d+", "a1b22c333")
        groups = [m.group() for m in result]
        assert groups == ["1", "22", "333"]


class TestReplace:
    """Tests for replace function."""

    def test_replace_all(self) -> None:
        """Test replacing all matches."""
        result = replace(r"\d+", "a1b22c333", "#")
        assert result == "a#b#c#"

    def test_replace_count(self) -> None:
        """Test replacing limited matches."""
        result = replace(r"\d+", "a1b22c333", "#", count=1)
        assert result == "a#b22c333"

    def test_replace_invalid(self) -> None:
        """Test replace with invalid pattern."""
        result = replace("[", "abc", "#")
        assert result == "abc"


class TestSplit:
    """Tests for split function."""

    def test_split(self) -> None:
        """Test splitting by pattern."""
        result = split(r"\s+", "a b  c")
        assert result == ["a", "b", "c"]

    def test_split_maxsplit(self) -> None:
        """Test splitting with maxsplit."""
        result = split(r"\s+", "a b c d", maxsplit=1)
        assert result == ["a", "b c d"]


class TestTest:
    """Tests for test function."""

    def test_test_true(self) -> None:
        """Test test returns True."""
        assert test(r"^\d+$", "123")

    def test_test_false(self) -> None:
        """Test test returns False."""
        assert not test(r"^\d+$", "abc")


class TestExtract:
    """Tests for extract function."""

    def test_extract(self) -> None:
        """Test extracting match."""
        result = extract(r"(\d+)-(\d+)", "abc-123-456", group=1)
        assert result == "123"

    def test_extract_full_match(self) -> None:
        """Test extracting full match."""
        result = extract(r"(\d+)-(\d+)", "abc-123-456", group=0)
        assert result == "123-456"

    def test_extract_no_match(self) -> None:
        """Test extracting when no match."""
        result = extract(r"\d+", "abc")
        assert result is None


class TestExtractAll:
    """Tests for extract_all function."""

    def test_extract_all(self) -> None:
        """Test extracting all matches."""
        result = extract_all(r"<(\w+)>", "<a><b><c>", group=1)
        assert result == ["a", "b", "c"]


class TestExtractDict:
    """Tests for extract_dict function."""

    def test_extract_dict(self) -> None:
        """Test extracting named groups."""
        result = extract_dict(r"(?P<year>\d{4})-(?P<month>\d{2})", "2024-01", ["year", "month"])
        assert result == {"year": "2024", "month": "01"}

    def test_extract_dict_no_match(self) -> None:
        """Test extracting with no match."""
        result = extract_dict(r"(\d+)", "abc", ["group"])
        assert result is None


class TestIsMatch:
    """Tests for is_match function."""

    def test_is_match_email(self) -> None:
        """Test matching email pattern."""
        assert is_match("email", "test@example.com")

    def test_is_match_email_invalid(self) -> None:
        """Test non-matching email."""
        assert not is_match("email", "not-an-email")

    def test_is_match_unknown_pattern(self) -> None:
        """Test unknown pattern name."""
        assert not is_match("nonexistent", "text")


class TestGetPattern:
    """Tests for get_pattern function."""

    def test_get_pattern_exists(self) -> None:
        """Test getting existing pattern."""
        result = get_pattern("email")
        assert result is not None
        assert "^" in result

    def test_get_pattern_missing(self) -> None:
        """Test getting missing pattern."""
        result = get_pattern("nonexistent")
        assert result is None


class TestEscape:
    """Tests for escape function."""

    def test_escape(self) -> None:
        """Test escaping special chars."""
        result = escape("a.b*c?")
        assert result == r"a\.b\*c\?"


class TestUnescape:
    """Tests for unescape function."""

    def test_unescape(self) -> None:
        """Test unescaping."""
        result = unescape(r"a\.b\*c\?")
        assert result == "a.b*c?"


class TestBuildAlternation:
    """Tests for build_alternation function."""

    def test_build_alternation(self) -> None:
        """Test building alternation."""
        result = build_alternation(["foo", "bar", "baz"])
        assert "foo" in result
        assert "bar" in result
        assert "baz" in result


class TestBuildSequence:
    """Tests for build_sequence function."""

    def test_build_sequence(self) -> None:
        """Test building sequence."""
        result = build_sequence(r"\d", "-")
        assert result == r"\d(?:-\d)*"

    def test_build_sequence_no_sep(self) -> None:
        """Test building sequence without separator."""
        result = build_sequence(r"\d")
        assert result == r"\d+"


class TestQuantifier:
    """Tests for quantifier function."""

    def test_optional(self) -> None:
        """Test optional quantifier."""
        result = quantifier("a", 0, 1)
        assert result == "(?:a)?"

    def test_zero_or_more(self) -> None:
        """Test zero or more quantifier."""
        result = quantifier("a", 0)
        assert result == "(?:a)*"

    def test_one_or_more(self) -> None:
        """Test one or more quantifier."""
        result = quantifier("a", 1)
        assert result == "(?:a)+"

    def test_exact_count(self) -> None:
        """Test exact count quantifier."""
        result = quantifier("a", 3, 3)
        assert result == "(?:a){3}"

    def test_range(self) -> None:
        """Test range quantifier."""
        result = quantifier("a", 2, 5)
        assert result == "(?:a){2,5}"

    def test_lazy(self) -> None:
        """Test lazy quantifier."""
        result = quantifier("a", 0, 1, greedy=False)
        assert result == "(?:a)??"

    def test_unbounded(self) -> None:
        """Test unbounded quantifier."""
        result = quantifier("a", 2)
        assert result == "(?:a){2,}"


class TestLookahead:
    """Tests for lookahead function."""

    def test_positive_lookahead(self) -> None:
        """Test positive lookahead."""
        result = lookahead(r"\d")
        assert "?=" in result

    def test_negative_lookahead(self) -> None:
        """Test negative lookahead."""
        result = lookahead(r"\d", ahead=False)
        assert "?!" in result


class TestLookbehind:
    """Tests for lookbehind function."""

    def test_positive_lookbehind(self) -> None:
        """Test positive lookbehind."""
        result = lookbehind(r"\d")
        assert "?<=" in result

    def test_negative_lookbehind(self) -> None:
        """Test negative lookbehind."""
        result = lookbehind(r"\d", behind=False)
        assert "?<!" in result


class TestCapture:
    """Tests for capture function."""

    def test_capture(self) -> None:
        """Test capture group."""
        result = capture("a")
        assert result == "(a)"

    def test_capture_named(self) -> None:
        """Test named capture group."""
        result = capture("a", name="letter")
        assert "?P<letter>" in result


class TestGroup:
    """Tests for group function."""

    def test_group(self) -> None:
        """Test non-capture group."""
        result = group("a")
        assert result == "(?:a)"


class TestCharacterClasses:
    """Tests for character class functions."""

    def test_word_boundary(self) -> None:
        """Test word boundary."""
        assert word_boundary() == r"\b"

    def test_digit(self) -> None:
        """Test digit class."""
        assert digit() == r"\d"

    def test_non_digit(self) -> None:
        """Test non-digit class."""
        assert non_digit() == r"\D"

    def test_word(self) -> None:
        """Test word class."""
        assert word() == r"\w"

    def test_non_word(self) -> None:
        """Test non-word class."""
        assert non_word() == r"\W"

    def test_whitespace(self) -> None:
        """Test whitespace class."""
        assert whitespace() == r"\s"

    def test_non_whitespace(self) -> None:
        """Test non-whitespace class."""
        assert non_whitespace() == r"\S"

    def test_any_char(self) -> None:
        """Test any char."""
        assert any_char() == "."


class TestAnchors:
    """Tests for anchor functions."""

    def test_line_start(self) -> None:
        """Test line start."""
        assert line_start() == "^"

    def test_line_end(self) -> None:
        """Test line end."""
        assert line_end() == "$"

    def test_string_start(self) -> None:
        """Test string start."""
        assert string_start() == r"\A"

    def test_string_end(self) -> None:
        """Test string end."""
        assert string_end() == r"\Z"


class TestPatterns:
    """Tests for PATTERNS dict."""

    def test_patterns_exist(self) -> None:
        """Test common patterns exist."""
        assert "email" in PATTERNS
        assert "url" in PATTERNS
        assert "ipv4" in PATTERNS

    def test_patterns_valid(self) -> None:
        """Test patterns compile correctly."""
        for name, pattern in PATTERNS.items():
            result = compile(pattern)
            assert result is not None, f"Pattern {name} failed to compile"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])