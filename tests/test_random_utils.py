"""Tests for random utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.random_utils import (
    random_int,
    random_float,
    random_bool,
    random_choice,
    random_sample,
    random_shuffle,
    random_string,
    random_alphanumeric,
    random_alpha,
    random_numeric,
    random_uuid,
    random_hex,
    random_bytes,
    random_hash,
    weighted_choice,
    random_date_year,
    random_ip_v4,
    random_port,
    random_bool_string,
    random_from_set,
    random_color_rgb,
    random_color_hex,
    random_email,
    random_url,
    random_phone,
    random_credit_card,
    random_paragraph,
    random_sentence,
    random_name,
    random_coordinates,
    random_password,
    random_token,
    random_json_friendly,
)


class TestRandomInt:
    """Tests for random_int function."""

    def test_random_int(self) -> None:
        """Test random integer generation."""
        result = random_int(0, 100)
        assert isinstance(result, int)
        assert 0 <= result <= 100


class TestRandomFloat:
    """Tests for random_float function."""

    def test_random_float(self) -> None:
        """Test random float generation."""
        result = random_float(0.0, 100.0)
        assert isinstance(result, float)
        assert 0.0 <= result <= 100.0


class TestRandomBool:
    """Tests for random_bool function."""

    def test_random_bool(self) -> None:
        """Test random boolean generation."""
        result = random_bool(0.5)
        assert isinstance(result, bool)


class TestRandomChoice:
    """Tests for random_choice function."""

    def test_random_choice(self) -> None:
        """Test random choice."""
        items = [1, 2, 3]
        result = random_choice(items)
        assert result in items

    def test_random_choice_empty(self) -> None:
        """Test random choice with empty list."""
        result = random_choice([])
        assert result is None


class TestRandomSample:
    """Tests for random_sample function."""

    def test_random_sample(self) -> None:
        """Test random sampling."""
        items = [1, 2, 3, 4, 5]
        result = random_sample(items, 3)
        assert len(result) == 3
        assert all(x in items for x in result)


class TestRandomShuffle:
    """Tests for random_shuffle function."""

    def test_random_shuffle(self) -> None:
        """Test random shuffling."""
        items = [1, 2, 3, 4, 5]
        result = random_shuffle(items)
        assert len(result) == 5
        assert set(result) == set(items)


class TestRandomString:
    """Tests for random_string function."""

    def test_random_string(self) -> None:
        """Test random string generation."""
        result = random_string(10)
        assert len(result) == 10


class TestRandomAlphanumeric:
    """Tests for random_alphanumeric function."""

    def test_random_alphanumeric(self) -> None:
        """Test random alphanumeric generation."""
        result = random_alphanumeric(10)
        assert len(result) == 10
        assert result.isalnum()


class TestRandomAlpha:
    """Tests for random_alpha function."""

    def test_random_alpha(self) -> None:
        """Test random alphabetic generation."""
        result = random_alpha(10)
        assert len(result) == 10
        assert result.isalpha()


class TestRandomNumeric:
    """Tests for random_numeric function."""

    def test_random_numeric(self) -> None:
        """Test random numeric generation."""
        result = random_numeric(10)
        assert len(result) == 10
        assert result.isdigit()


class TestRandomUuid:
    """Tests for random_uuid function."""

    def test_random_uuid(self) -> None:
        """Test random UUID generation."""
        result = random_uuid()
        assert len(result) == 36
        assert '-' in result


class TestRandomHex:
    """Tests for random_hex function."""

    def test_random_hex(self) -> None:
        """Test random hex generation."""
        result = random_hex(10)
        assert len(result) == 10


class TestRandomBytes:
    """Tests for random_bytes function."""

    def test_random_bytes(self) -> None:
        """Test random bytes generation."""
        result = random_bytes(32)
        assert len(result) == 32
        assert isinstance(result, bytes)


class TestRandomHash:
    """Tests for random_hash function."""

    def test_random_hash(self) -> None:
        """Test random hash generation."""
        result = random_hash("test")
        assert len(result) == 64


class TestWeightedChoice:
    """Tests for weighted_choice function."""

    def test_weighted_choice(self) -> None:
        """Test weighted choice."""
        items = [1, 2, 3]
        weights = [0.5, 0.3, 0.2]
        result = weighted_choice(items, weights)
        assert result in items

    def test_weighted_choice_empty(self) -> None:
        """Test weighted choice with empty list."""
        result = weighted_choice([], [])
        assert result is None


class TestRandomDateYear:
    """Tests for random_date_year function."""

    def test_random_date_year(self) -> None:
        """Test random date generation."""
        result = random_date_year(2024)
        assert len(result) == 10
        assert result.startswith("2024-")


class TestRandomIpV4:
    """Tests for random_ip_v4 function."""

    def test_random_ip_v4(self) -> None:
        """Test random IPv4 generation."""
        result = random_ip_v4()
        parts = result.split('.')
        assert len(parts) == 4
        assert all(0 <= int(p) <= 255 for p in parts)


class TestRandomPort:
    """Tests for random_port function."""

    def test_random_port(self) -> None:
        """Test random port generation."""
        result = random_port()
        assert 0 <= result <= 65535


class TestRandomBoolString:
    """Tests for random_bool_string function."""

    def test_random_bool_string(self) -> None:
        """Test random boolean string."""
        result = random_bool_string("yes", "no")
        assert result in ("yes", "no")


class TestRandomFromSet:
    """Tests for random_from_set function."""

    def test_random_from_set(self) -> None:
        """Test random from set."""
        result = random_from_set(1, 2, 3)
        assert result in (1, 2, 3)


class TestRandomColorRgb:
    """Tests for random_color_rgb function."""

    def test_random_color_rgb(self) -> None:
        """Test random RGB color."""
        result = random_color_rgb()
        assert len(result) == 3
        assert all(0 <= c <= 255 for c in result)


class TestRandomColorHex:
    """Tests for random_color_hex function."""

    def test_random_color_hex(self) -> None:
        """Test random hex color."""
        result = random_color_hex()
        assert result.startswith('#')
        assert len(result) == 7


class TestRandomEmail:
    """Tests for random_email function."""

    def test_random_email(self) -> None:
        """Test random email generation."""
        result = random_email()
        assert '@' in result


class TestRandomUrl:
    """Tests for random_url function."""

    def test_random_url(self) -> None:
        """Test random URL generation."""
        result = random_url()
        assert '://' in result


class TestRandomPhone:
    """Tests for random_phone function."""

    def test_random_phone(self) -> None:
        """Test random phone generation."""
        result = random_phone()
        assert '+' in result or any(c.isdigit() for c in result)


class TestRandomCreditCard:
    """Tests for random_credit_card function."""

    def test_random_credit_card(self) -> None:
        """Test random credit card generation."""
        result = random_credit_card()
        assert len(result) == 16
        assert result.isdigit()


class TestRandomParagraph:
    """Tests for random_paragraph function."""

    def test_random_paragraph(self) -> None:
        """Test random paragraph generation."""
        result = random_paragraph(10)
        assert len(result.split()) >= 5


class TestRandomSentence:
    """Tests for random_sentence function."""

    def test_random_sentence(self) -> None:
        """Test random sentence generation."""
        result = random_sentence()
        assert result.endswith('.')


class TestRandomName:
    """Tests for random_name function."""

    def test_random_name(self) -> None:
        """Test random name generation."""
        result = random_name()
        assert ' ' in result


class TestRandomCoordinates:
    """Tests for random_coordinates function."""

    def test_random_coordinates(self) -> None:
        """Test random coordinates."""
        result = random_coordinates()
        assert len(result) == 2


class TestRandomPassword:
    """Tests for random_password function."""

    def test_random_password(self) -> None:
        """Test random password generation."""
        result = random_password(16)
        assert len(result) == 16


class TestRandomToken:
    """Tests for random_token function."""

    def test_random_token(self) -> None:
        """Test random token generation."""
        result = random_token()
        assert len(result) == 32


class TestRandomJsonFriendly:
    """Tests for random_json_friendly function."""

    def test_random_json_friendly(self) -> None:
        """Test random JSON-friendly value."""
        result = random_json_friendly()
        assert isinstance(result, (int, float, str, bool))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
