"""Tests for text utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.text_utils import (
    clean_whitespace,
    remove_control_chars,
    truncate,
    similarity,
    jaro_similarity,
    levenshtein_distance,
    extract_numbers,
    extract_urls,
    extract_emails,
    slugify,
    md5_hash,
    sha256_hash,
    fuzzy_match,
    camel_to_snake,
    snake_to_camel,
    remove_emoji,
    count_words,
)


class TestCleanWhitespace:
    """Tests for clean_whitespace."""

    def test_collapse_spaces(self) -> None:
        """Test collapsing multiple spaces."""
        result = clean_whitespace("hello    world")
        assert "    " not in result

    def test_collapse_newlines(self) -> None:
        """Test collapsing multiple newlines."""
        result = clean_whitespace("hello\n\n\nworld")
        assert result.count("\n\n") <= 1


class TestRemoveControlChars:
    """Tests for remove_control_chars."""

    def test_remove_control(self) -> None:
        """Test removing control characters."""
        result = remove_control_chars("hello\x00world")
        assert "\x00" not in result


class TestTruncate:
    """Tests for truncate."""

    def test_truncate_long(self) -> None:
        """Test truncating long text."""
        result = truncate("a" * 100, 20)
        assert len(result) == 20

    def test_no_truncate_short(self) -> None:
        """Test no truncation for short text."""
        result = truncate("hello", 10)
        assert result == "hello"


class TestSimilarity:
    """Tests for similarity functions."""

    def test_identical_strings(self) -> None:
        """Test similarity of identical strings."""
        assert similarity("hello", "hello") == 1.0

    def test_completely_different(self) -> None:
        """Test similarity of different strings."""
        assert similarity("abc", "xyz") < 0.5


class TestJaroSimilarity:
    """Tests for jaro_similarity."""

    def test_identical(self) -> None:
        """Test identical strings."""
        assert jaro_similarity("hello", "hello") == 1.0

    def test_empty_strings(self) -> None:
        """Test empty strings."""
        assert jaro_similarity("", "") == 1.0
        assert jaro_similarity("a", "") == 0.0


class TestLevenshteinDistance:
    """Tests for levenshtein_distance."""

    def test_identical(self) -> None:
        """Test distance for identical strings."""
        assert levenshtein_distance("hello", "hello") == 0

    def test_one_char_diff(self) -> None:
        """Test distance for one character difference."""
        assert levenshtein_distance("hello", "hallo") == 1


class TestExtractNumbers:
    """Tests for extract_numbers."""

    def test_extract_integers(self) -> None:
        """Test extracting integers."""
        result = extract_numbers("test 123 and 456")
        assert 123 in result
        assert 456 in result

    def test_extract_floats(self) -> None:
        """Test extracting floats."""
        result = extract_numbers("price 12.99")
        assert 12.99 in result


class TestExtractUrls:
    """Tests for extract_urls."""

    def test_extract_url(self) -> None:
        """Test extracting URLs."""
        result = extract_urls("visit https://example.com today")
        assert "https://example.com" in result


class TestExtractEmails:
    """Tests for extract_emails."""

    def test_extract_email(self) -> None:
        """Test extracting emails."""
        result = extract_emails("contact: test@example.com")
        assert "test@example.com" in result


class TestSlugify:
    """Tests for slugify."""

    def test_basic_slugify(self) -> None:
        """Test basic slugification."""
        result = slugify("Hello World")
        assert result == "hello-world"

    def test_special_chars(self) -> None:
        """Test handling special characters."""
        result = slugify("Hello! World?")
        assert "!" not in result


class TestHashFunctions:
    """Tests for hash functions."""

    def test_md5_hash(self) -> None:
        """Test MD5 hashing."""
        h1 = md5_hash("hello")
        h2 = md5_hash("hello")
        assert h1 == h2

    def test_sha256_hash(self) -> None:
        """Test SHA256 hashing."""
        h1 = sha256_hash("hello")
        h2 = sha256_hash("hello")
        assert h1 == h2


class TestFuzzyMatch:
    """Tests for fuzzy_match."""

    def test_exact_match(self) -> None:
        """Test exact match."""
        results = fuzzy_match("hello", ["hello", "world"])
        assert results[0][0] == "hello"

    def test_fuzzy_match(self) -> None:
        """Test fuzzy matching."""
        results = fuzzy_match("helo", ["hello", "world"])
        assert results[0][0] == "hello"


class TestCaseConversion:
    """Tests for case conversion functions."""

    def test_camel_to_snake(self) -> None:
        """Test camelCase to snake_case."""
        assert camel_to_snake("camelCase") == "camel_case"

    def test_snake_to_camel(self) -> None:
        """Test snake_case to camelCase."""
        assert snake_to_camel("snake_case") == "snakecase"


class TestRemoveEmoji:
    """Tests for remove_emoji."""

    def test_remove_emoji(self) -> None:
        """Test emoji removal."""
        result = remove_emoji("hello👋world")
        assert "👋" not in result


class TestCountWords:
    """Tests for count_words."""

    def test_count_words(self) -> None:
        """Test word counting."""
        assert count_words("hello world") == 2
        assert count_words("hello    world") == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])