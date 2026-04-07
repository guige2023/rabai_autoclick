"""Tests for security utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.security import (
    generate_token,
    generate_password,
    hash_password,
    verify_password,
    hash_sha256,
    hash_sha512,
    secure_compare,
    mask_sensitive,
    generate_api_key,
    RateLimiter,
    sanitize_filename,
)


class TestGenerateToken:
    """Tests for generate_token."""

    def test_generate_token_default(self) -> None:
        """Test generating token with defaults."""
        token = generate_token()
        assert isinstance(token, str)
        assert len(token) == 64  # 32 bytes = 64 hex chars

    def test_generate_token_custom_length(self) -> None:
        """Test generating token with custom length."""
        token = generate_token(16)
        assert len(token) == 32  # 16 bytes = 32 hex chars

    def test_generate_token_unique(self) -> None:
        """Test tokens are unique."""
        tokens = [generate_token() for _ in range(100)]
        assert len(set(tokens)) == 100


class TestGeneratePassword:
    """Tests for generate_password."""

    def test_generate_password_default(self) -> None:
        """Test generating password with defaults."""
        password = generate_password()
        assert isinstance(password, str)
        assert len(password) == 16

    def test_generate_password_custom_length(self) -> None:
        """Test generating password with custom length."""
        password = generate_password(length=24)
        assert len(password) == 24

    def test_password_contains_chars(self) -> None:
        """Test password contains expected character types."""
        password = generate_password(length=100)
        has_lower = any(c.islower() for c in password)
        has_upper = any(c.isupper() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(not c.isalnum() for c in password)
        assert has_lower and has_upper and has_digit and has_special


class TestHashPassword:
    """Tests for hash_password."""

    def test_hash_password(self) -> None:
        """Test hashing password."""
        password = "test_password"
        hashed, salt = hash_password(password)
        assert isinstance(hashed, str)
        assert isinstance(salt, str)
        assert len(hashed) == 64  # SHA256 hex
        assert len(salt) == 32  # 16 bytes hex

    def test_hash_password_with_salt(self) -> None:
        """Test hashing with provided salt."""
        password = "test_password"
        salt = "fixed_salt_123"
        hashed, returned_salt = hash_password(password, salt=salt)
        assert returned_salt == salt

    def test_verify_password(self) -> None:
        """Test verifying password."""
        password = "test_password"
        hashed, salt = hash_password(password)
        assert verify_password(password, hashed, salt) is True

    def test_verify_password_wrong(self) -> None:
        """Test verifying wrong password."""
        password = "test_password"
        hashed, salt = hash_password(password)
        assert verify_password("wrong_password", hashed, salt) is False


class TestHashSha256:
    """Tests for hash_sha256."""

    def test_hash_sha256(self) -> None:
        """Test SHA256 hashing."""
        result = hash_sha256("hello")
        assert isinstance(result, str)
        assert len(result) == 64

    def test_hash_sha256_deterministic(self) -> None:
        """Test SHA256 is deterministic."""
        result1 = hash_sha256("hello")
        result2 = hash_sha256("hello")
        assert result1 == result2

    def test_hash_sha256_different_inputs(self) -> None:
        """Test different inputs produce different hashes."""
        result1 = hash_sha256("hello")
        result2 = hash_sha256("world")
        assert result1 != result2


class TestHashSha512:
    """Tests for hash_sha512."""

    def test_hash_sha512(self) -> None:
        """Test SHA512 hashing."""
        result = hash_sha512("hello")
        assert isinstance(result, str)
        assert len(result) == 128


class TestSecureCompare:
    """Tests for secure_compare."""

    def test_secure_compare_equal(self) -> None:
        """Test comparing equal strings."""
        assert secure_compare("hello", "hello") is True

    def test_secure_compare_not_equal(self) -> None:
        """Test comparing different strings."""
        assert secure_compare("hello", "world") is False

    def test_secure_compare_different_length(self) -> None:
        """Test comparing different length strings."""
        assert secure_compare("short", "much_longer_string") is False


class TestMaskSensitive:
    """Tests for mask_sensitive."""

    def test_mask_sensitive_default(self) -> None:
        """Test masking with defaults."""
        result = mask_sensitive("secret123")
        assert result == "****3123"
        assert "*" in result

    def test_mask_sensitive_short(self) -> None:
        """Test masking short string."""
        result = mask_sensitive("abc")
        assert result == "***"

    def test_mask_sensitive_custom_visible(self) -> None:
        """Test masking with custom visible chars."""
        result = mask_sensitive("secret123", visible_chars=6)
        assert result == "*****3123"


class TestGenerateApiKey:
    """Tests for generate_api_key."""

    def test_generate_api_key_format(self) -> None:
        """Test API key format."""
        key = generate_api_key()
        assert key.startswith("rbai_")
        parts = key.split("_")
        assert len(parts) == 2

    def test_generate_api_key_unique(self) -> None:
        """Test API keys are unique."""
        keys = [generate_api_key() for _ in range(100)]
        assert len(set(keys)) == 100


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_create(self) -> None:
        """Test creating rate limiter."""
        limiter = RateLimiter(max_calls=10, window_seconds=60)
        assert limiter.max_calls == 10
        assert limiter.window_seconds == 60

    def test_allows_under_limit(self) -> None:
        """Test allows calls under limit."""
        limiter = RateLimiter(max_calls=5, window_seconds=60)
        for _ in range(5):
            assert limiter.is_allowed("key1") is True

    def test_denies_over_limit(self) -> None:
        """Test denies calls over limit."""
        limiter = RateLimiter(max_calls=3, window_seconds=60)
        for _ in range(3):
            limiter.is_allowed("key1")
        assert limiter.is_allowed("key1") is False

    def test_different_keys(self) -> None:
        """Test different keys have separate limits."""
        limiter = RateLimiter(max_calls=2, window_seconds=60)
        limiter.is_allowed("key1")
        limiter.is_allowed("key1")
        assert limiter.is_allowed("key1") is False
        assert limiter.is_allowed("key2") is True

    def test_window_expiry(self) -> None:
        """Test calls expire after window."""
        limiter = RateLimiter(max_calls=2, window_seconds=1)
        limiter.is_allowed("key1")
        limiter.is_allowed("key1")
        time.sleep(1.1)
        assert limiter.is_allowed("key1") is True


class TestSanitizeFilename:
    """Tests for sanitize_filename."""

    def test_sanitize_normal(self) -> None:
        """Test sanitizing normal filename."""
        result = sanitize_filename("normal_file.txt")
        assert result == "normal_file.txt"

    def test_sanitize_removes_invalid_chars(self) -> None:
        """Test removing invalid characters."""
        result = sanitize_filename('file<>:"|?*.txt')
        assert "<" not in result
        assert ">" not in result
        assert ":" not in result

    def test_sanitize_trims_whitespace(self) -> None:
        """Test trimming whitespace and dots."""
        result = sanitize_filename("  file.txt  ")
        assert result == "file.txt"
        result = sanitize_filename("..file.txt..")
        assert result == "file.txt"

    def test_sanitize_truncates_long(self) -> None:
        """Test truncating long filenames."""
        long_name = "a" * 300 + ".txt"
        result = sanitize_filename(long_name)
        assert len(result) <= 255


if __name__ == "__main__":
    pytest.main([__file__, "-v"])