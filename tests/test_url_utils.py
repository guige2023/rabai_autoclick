"""Tests for URL utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.url_utils import (
    URL,
    build_url,
    get_query_params,
    add_query_params,
    remove_query_params,
    is_valid_url,
    is_absolute_url,
    join_url,
    normalize_url,
    extract_domain,
    is_same_domain,
)


class TestURL:
    """Tests for URL class."""

    def test_parse(self) -> None:
        """Test parsing URL."""
        url = URL.parse("https://example.com/path?key=value#frag")
        assert url.scheme == "https"
        assert url.host == "example.com"
        assert url.path == "/path"
        assert url.query == {"key": "value"}
        assert url.fragment == "frag"

    def test_str(self) -> None:
        """Test URL string representation."""
        url = URL("https", "example.com", 443, "/path", {"key": "value"}, "")
        assert "https" in str(url)
        assert "example.com" in str(url)

    def test_netloc_with_port(self) -> None:
        """Test netloc with port."""
        url = URL("http", "example.com", 8080, "/", {}, "")
        assert url.netloc == "example.com:8080"


class TestBuildUrl:
    """Tests for build_url."""

    def test_basic_build(self) -> None:
        """Test basic URL building."""
        url = build_url(scheme="https", host="example.com")
        assert url == "https://example.com"

    def test_with_port(self) -> None:
        """Test building URL with port."""
        url = build_url(scheme="http", host="example.com", port=3000)
        assert "3000" in url

    def test_with_query(self) -> None:
        """Test building URL with query params."""
        url = build_url(host="example.com", query={"key": "value"})
        assert "key=value" in url


class TestGetQueryParams:
    """Tests for get_query_params."""

    def test_extract_params(self) -> None:
        """Test extracting query params."""
        params = get_query_params("https://example.com?a=1&b=2")
        assert params["a"] == "1"
        assert params["b"] == "2"


class TestAddQueryParams:
    """Tests for add_query_params."""

    def test_add_params(self) -> None:
        """Test adding query params."""
        url = add_query_params("https://example.com?existing=1", {"new": "2"})
        params = get_query_params(url)
        assert params["existing"] == "1"
        assert params["new"] == "2"


class TestRemoveQueryParams:
    """Tests for remove_query_params."""

    def test_remove_params(self) -> None:
        """Test removing query params."""
        url = "https://example.com?a=1&b=2"
        result = remove_query_params(url, "a")
        params = get_query_params(result)
        assert "a" not in params
        assert "b" in params


class TestIsValidUrl:
    """Tests for is_valid_url."""

    def test_valid_url(self) -> None:
        """Test valid URL."""
        assert is_valid_url("https://example.com")
        assert is_valid_url("http://example.com:8080")

    def test_invalid_url(self) -> None:
        """Test invalid URL."""
        assert not is_valid_url("not a url")
        assert not is_valid_url("")


class TestIsAbsoluteUrl:
    """Tests for is_absolute_url."""

    def test_absolute(self) -> None:
        """Test absolute URL."""
        assert is_absolute_url("https://example.com")
        assert is_absolute_url("http://example.com/path")

    def test_relative(self) -> None:
        """Test relative URL."""
        assert not is_absolute_url("/path")
        assert not is_absolute_url("relative/path")


class TestJoinUrl:
    """Tests for join_url."""

    def test_join(self) -> None:
        """Test joining URLs."""
        assert join_url("https://example.com", "path") == "https://example.com/path"
        assert join_url("https://example.com/", "/path") == "https://example.com/path"


class TestNormalizeUrl:
    """Tests for normalize_url."""

    def test_remove_default_port(self) -> None:
        """Test removing default ports."""
        url = normalize_url("https://example.com/")
        assert ":443" not in url


class TestExtractDomain:
    """Tests for extract_domain."""

    def test_extract(self) -> None:
        """Test extracting domain."""
        assert extract_domain("https://example.com/path") == "example.com"


class TestIsSameDomain:
    """Tests for is_same_domain."""

    def test_same_domain(self) -> None:
        """Test same domain."""
        assert is_same_domain("https://example.com", "https://example.com/path")

    def test_different_domain(self) -> None:
        """Test different domain."""
        assert not is_same_domain("https://example.com", "https://other.com")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])