"""Tests for UUID utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.uuid_utils import (
    generate,
    generate_hex,
    generate_int,
    generate_time,
    generate_safe,
    parse,
    is_valid,
    to_hex,
    to_int,
    to_bytes,
    from_hex,
    from_int,
    from_bytes,
    get_version,
    get_variant,
    is_time_based,
    is_random,
    is_name_based,
    make_time_based,
    make_random,
    make_md5,
    make_sha1,
    make_namespace_dns,
    make_namespace_url,
    make_namespace_oid,
    make_namespace_x500,
    nil,
    is_nil,
    min_uuid,
    max_uuid,
    compare,
    sort_uuids,
    deduplicate_uuids,
)


class TestGenerate:
    """Tests for generate functions."""

    def test_generate(self) -> None:
        """Test generating UUID."""
        result = generate()
        assert len(result) == 36
        assert is_valid(result)

    def test_generate_hex(self) -> None:
        """Test generating hex UUID."""
        result = generate_hex()
        assert len(result) == 32
        assert "-" not in result

    def test_generate_int(self) -> None:
        """Test generating int UUID."""
        result = generate_int()
        assert isinstance(result, int)

    def test_generate_time(self) -> None:
        """Test generating time UUID."""
        result = generate_time()
        assert len(result) == 36
        assert is_time_based(result)

    def test_generate_safe(self) -> None:
        """Test generating safe UUID."""
        result = generate_safe()
        assert is_random(result)


class TestParse:
    """Tests for parse function."""

    def test_parse_valid(self) -> None:
        """Test parsing valid UUID."""
        result = parse("550e8400-e29b-41d4-a716-446655440000")
        assert result is not None

    def test_parse_invalid(self) -> None:
        """Test parsing invalid UUID."""
        result = parse("not-a-uuid")
        assert result is None


class TestIsValid:
    """Tests for is_valid function."""

    def test_is_valid_true(self) -> None:
        """Test valid UUID."""
        assert is_valid("550e8400-e29b-41d4-a716-446655440000")

    def test_is_valid_false(self) -> None:
        """Test invalid UUID."""
        assert not is_valid("not-a-uuid")

    def test_is_valid_empty(self) -> None:
        """Test empty string."""
        assert not is_valid("")


class TestToHex:
    """Tests for to_hex function."""

    def test_to_hex(self) -> None:
        """Test converting to hex."""
        result = to_hex("550e8400-e29b-41d4-a716-446655440000")
        assert result == "550e8400e29b41d4a716446655440000"


class TestToInt:
    """Tests for to_int function."""

    def test_to_int(self) -> None:
        """Test converting to int."""
        result = to_int("550e8400-e29b-41d4-a716-446655440000")
        assert isinstance(result, int)
        assert result > 0


class TestToBytes:
    """Tests for to_bytes function."""

    def test_to_bytes(self) -> None:
        """Test converting to bytes."""
        result = to_bytes("550e8400-e29b-41d4-a716-446655440000")
        assert isinstance(result, bytes)
        assert len(result) == 16


class TestFromHex:
    """Tests for from_hex function."""

    def test_from_hex(self) -> None:
        """Test creating from hex."""
        result = from_hex("550e8400e29b41d4a716446655440000")
        assert result is not None
        assert is_valid(result)


class TestFromInt:
    """Tests for from_int function."""

    def test_from_int(self) -> None:
        """Test creating from int."""
        original = to_int("550e8400-e29b-41d4-a716-446655440000")
        result = from_int(original)
        assert result is not None
        assert is_valid(result)


class TestFromBytes:
    """Tests for from_bytes function."""

    def test_from_bytes(self) -> None:
        """Test creating from bytes."""
        original = to_bytes("550e8400-e29b-41d4-a716-446655440000")
        result = from_bytes(original)
        assert result is not None
        assert is_valid(result)


class TestGetVersion:
    """Tests for get_version function."""

    def test_get_version_v4(self) -> None:
        """Test getting v4 UUID version."""
        result = get_version(generate())
        assert result == 4

    def test_get_version_v1(self) -> None:
        """Test getting v1 UUID version."""
        result = get_version(generate_time())
        assert result == 1


class TestGetVariant:
    """Tests for get_variant function."""

    def test_get_variant(self) -> None:
        """Test getting variant."""
        result = get_variant(generate())
        # Variant returns RFC 4122 string in Python 3.14+
        assert result is not None


class TestIsTimeBased:
    """Tests for is_time_based function."""

    def test_is_time_based_true(self) -> None:
        """Test time-based UUID."""
        assert is_time_based(generate_time())

    def test_is_time_based_false(self) -> None:
        """Test non-time-based UUID."""
        assert not is_time_based(generate())


class TestIsRandom:
    """Tests for is_random function."""

    def test_is_random_true(self) -> None:
        """Test random UUID."""
        assert is_random(generate())

    def test_is_random_false(self) -> None:
        """Test non-random UUID."""
        assert not is_random(generate_time())


class TestIsNameBased:
    """Tests for is_name_based function."""

    def test_is_name_based_md5(self) -> None:
        """Test MD5 name-based UUID."""
        result = make_md5(make_namespace_dns(), "example.com")
        assert is_name_based(result)

    def test_is_name_based_sha1(self) -> None:
        """Test SHA1 name-based UUID."""
        result = make_sha1(make_namespace_dns(), "example.com")
        assert is_name_based(result)


class TestMakeTimeBased:
    """Tests for make_time_based function."""

    def test_make_time_based(self) -> None:
        """Test creating time-based UUID."""
        result = make_time_based()
        assert is_time_based(result)


class TestMakeRandom:
    """Tests for make_random function."""

    def test_make_random(self) -> None:
        """Test creating random UUID."""
        result = make_random()
        assert is_random(result)


class TestMakeMd5:
    """Tests for make_md5 function."""

    def test_make_md5(self) -> None:
        """Test creating MD5 UUID."""
        result = make_md5(make_namespace_dns(), "example.com")
        assert is_name_based(result)
        assert get_version(result) == 3


class TestMakeSha1:
    """Tests for make_sha1 function."""

    def test_make_sha1(self) -> None:
        """Test creating SHA1 UUID."""
        result = make_sha1(make_namespace_dns(), "example.com")
        assert is_name_based(result)
        assert get_version(result) == 5


class TestNamespaces:
    """Tests for namespace functions."""

    def test_namespace_dns(self) -> None:
        """Test DNS namespace."""
        result = make_namespace_dns()
        assert is_valid(result)
        assert result == "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

    def test_namespace_url(self) -> None:
        """Test URL namespace."""
        result = make_namespace_url()
        assert is_valid(result)

    def test_namespace_oid(self) -> None:
        """Test OID namespace."""
        result = make_namespace_oid()
        assert is_valid(result)

    def test_namespace_x500(self) -> None:
        """Test X500 namespace."""
        result = make_namespace_x500()
        assert is_valid(result)


class TestNil:
    """Tests for nil UUID functions."""

    def test_nil(self) -> None:
        """Test nil UUID."""
        result = nil()
        assert is_valid(result)
        assert is_nil(result)

    def test_is_nil_true(self) -> None:
        """Test is_nil true."""
        assert is_nil(nil())

    def test_is_nil_false(self) -> None:
        """Test is_nil false."""
        assert not is_nil(generate())


class TestMinMax:
    """Tests for min/max UUID functions."""

    def test_min_uuid(self) -> None:
        """Test min UUID."""
        result = min_uuid()
        assert is_valid(result)
        assert result == "00000000-0000-0000-0000-000000000000"

    def test_max_uuid(self) -> None:
        """Test max UUID."""
        result = max_uuid()
        assert is_valid(result)


class TestCompare:
    """Tests for compare function."""

    def test_compare_equal(self) -> None:
        """Test comparing equal UUIDs."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        assert compare(uuid_str, uuid_str) == 0

    def test_compare_less(self) -> None:
        """Test comparing less."""
        result = compare(min_uuid(), max_uuid())
        assert result == -1

    def test_compare_greater(self) -> None:
        """Test comparing greater."""
        result = compare(max_uuid(), min_uuid())
        assert result == 1


class TestSortUuids:
    """Tests for sort_uuids function."""

    def test_sort_uuids(self) -> None:
        """Test sorting UUIDs."""
        uuids = [max_uuid(), min_uuid(), generate()]
        result = sort_uuids(uuids)
        assert result[0] == min_uuid()


class TestDeduplicateUuids:
    """Tests for deduplicate_uuids function."""

    def test_deduplicate(self) -> None:
        """Test deduplicating UUIDs."""
        u1 = generate()
        uuids = [u1, u1, min_uuid()]
        result = deduplicate_uuids(uuids)
        assert len(result) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])