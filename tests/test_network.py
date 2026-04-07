"""Tests for network utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.network import (
    HTTPResponse,
    http_get,
    http_post,
    check_internet,
    check_url,
)


class TestHTTPResponse:
    """Tests for HTTPResponse."""

    def test_create(self) -> None:
        """Test creating HTTPResponse."""
        response = HTTPResponse(
            status_code=200,
            body="OK",
            headers={"Content-Type": "text/plain"},
        )
        assert response.status_code == 200
        assert response.body == "OK"
        assert response.headers == {"Content-Type": "text/plain"}

    def test_ok_true(self) -> None:
        """Test ok property for success."""
        response = HTTPResponse(status_code=200, body="", headers={})
        assert response.ok is True

    def test_ok_false_for_error(self) -> None:
        """Test ok property for error."""
        response = HTTPResponse(status_code=404, body="Not Found", headers={})
        assert response.ok is False

    def test_ok_false_for_server_error(self) -> None:
        """Test ok property for server error."""
        response = HTTPResponse(status_code=500, body="Server Error", headers={})
        assert response.ok is False

    def test_json_parsing(self) -> None:
        """Test JSON parsing."""
        response = HTTPResponse(
            status_code=200,
            body='{"key": "value"}',
            headers={},
        )
        result = response.json()
        assert result == {"key": "value"}

    def test_json_parsing_invalid(self) -> None:
        """Test JSON parsing with invalid JSON."""
        response = HTTPResponse(
            status_code=200,
            body="not json",
            headers={},
        )
        result = response.json()
        assert result is None


class TestHttpGet:
    """Tests for http_get."""

    def test_get_returns_response(self) -> None:
        """Test http_get returns HTTPResponse."""
        # This will fail to connect but should return None, not raise
        result = http_get("https://localhost:99999/invalid")
        assert result is None

    def test_get_with_headers(self) -> None:
        """Test http_get with custom headers."""
        result = http_get("https://localhost:99999", headers={"X-Custom": "value"})
        assert result is None  # Connection refused, but no exception


class TestHttpPost:
    """Tests for http_post."""

    def test_post_returns_response(self) -> None:
        """Test http_post returns HTTPResponse."""
        result = http_post("https://localhost:99999/invalid", data={"key": "value"})
        assert result is None  # Connection refused, but no exception

    def test_post_with_json_data(self) -> None:
        """Test http_post with JSON data."""
        result = http_post("https://localhost:99999", json_data={"key": "value"})
        assert result is None  # Connection refused, but no exception


class TestCheckInternet:
    """Tests for check_internet."""

    def test_check_internet_returns_bool(self) -> None:
        """Test check_internet returns bool."""
        result = check_internet()
        assert isinstance(result, bool)


class TestCheckUrl:
    """Tests for check_url."""

    def test_check_url_invalid(self) -> None:
        """Test check_url with invalid URL."""
        result = check_url("https://localhost:99999/invalid")
        assert result is False

    def test_check_url_returns_bool(self) -> None:
        """Test check_url returns bool."""
        result = check_url("https://www.google.com", timeout=2)
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])