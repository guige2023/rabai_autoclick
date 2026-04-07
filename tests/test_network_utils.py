"""Tests for network utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.network_utils import (
    is_valid_ipv4,
    is_valid_ipv6,
    is_valid_port,
    parse_url,
    build_url,
    parse_query_string,
    build_query_string,
    url_encode,
    url_decode,
    get_hostname,
    get_fqdn,
    resolve_hostname,
    reverse_dns_lookup,
    is_reachable,
    get_local_ip,
    is_private_ip,
    is_loopback_ip,
    parse_http_headers,
    format_http_headers,
    encode_json,
    decode_json,
    is_valid_url,
    is_ssl_port,
    get_content_type,
    join_url_parts,
    normalize_url_path,
    ip_to_int,
    int_to_ip,
    cidr_to_netmask,
    netmask_to_cidr,
)


class TestIsValidIPv4:
    """Tests for is_valid_ipv4 function."""

    def test_valid_ipv4(self) -> None:
        """Test valid IPv4 addresses."""
        assert is_valid_ipv4("192.168.1.1")
        assert is_valid_ipv4("10.0.0.1")
        assert is_valid_ipv4("127.0.0.1")

    def test_invalid_ipv4(self) -> None:
        """Test invalid IPv4 addresses."""
        assert not is_valid_ipv4("256.1.1.1")
        assert not is_valid_ipv4("1.2.3")
        assert not is_valid_ipv4("not.an.ip")


class TestIsValidIPv6:
    """Tests for is_valid_ipv6 function."""

    def test_valid_ipv6(self) -> None:
        """Test valid IPv6 addresses."""
        assert is_valid_ipv6("::1")
        assert is_valid_ipv6("2001:db8::1")

    def test_invalid_ipv6(self) -> None:
        """Test invalid IPv6 addresses."""
        assert not is_valid_ipv6("192.168.1.1")
        assert not is_valid_ipv6("not:an:ip:v6")


class TestIsValidPort:
    """Tests for is_valid_port function."""

    def test_valid_port(self) -> None:
        """Test valid ports."""
        assert is_valid_port(80)
        assert is_valid_port(443)
        assert is_valid_port(8080)
        assert is_valid_port(0)
        assert is_valid_port(65535)

    def test_invalid_port(self) -> None:
        """Test invalid ports."""
        assert not is_valid_port(-1)
        assert not is_valid_port(65536)


class TestParseUrl:
    """Tests for parse_url function."""

    def test_parse_url(self) -> None:
        """Test parsing URL."""
        result = parse_url("https://example.com:8080/path?query=1#frag")
        assert result["scheme"] == "https"
        assert result["hostname"] == "example.com"
        assert result["port"] == 8080
        assert result["path"] == "/path"
        assert result["query"] == "query=1"


class TestBuildUrl:
    """Tests for build_url function."""

    def test_build_url(self) -> None:
        """Test building URL."""
        result = build_url("https", "example.com", "/path", 8080)
        assert result == "https://example.com:8080/path"

    def test_build_url_no_port(self) -> None:
        """Test building URL without port."""
        result = build_url("http", "example.com", "/path")
        assert result == "http://example.com/path"


class TestParseQueryString:
    """Tests for parse_query_string function."""

    def test_parse_query_string(self) -> None:
        """Test parsing query string."""
        result = parse_query_string("a=1&b=2")
        assert result == {"a": "1", "b": "2"}


class TestBuildQueryString:
    """Tests for build_query_string function."""

    def test_build_query_string(self) -> None:
        """Test building query string."""
        result = build_query_string({"a": "1", "b": "2"})
        assert "a=1" in result
        assert "b=2" in result


class TestUrlEncode:
    """Tests for url_encode function."""

    def test_url_encode(self) -> None:
        """Test URL encoding."""
        result = url_encode("hello world")
        assert result == "hello%20world"


class TestUrlDecode:
    """Tests for url_decode function."""

    def test_url_decode(self) -> None:
        """Test URL decoding."""
        result = url_decode("hello%20world")
        assert result == "hello world"


class TestGetHostname:
    """Tests for get_hostname function."""

    def test_get_hostname(self) -> None:
        """Test getting hostname."""
        result = get_hostname()
        assert isinstance(result, str)
        assert len(result) > 0


class TestGetFqdn:
    """Tests for get_fqdn function."""

    def test_get_fqdn(self) -> None:
        """Test getting FQDN."""
        result = get_fqdn()
        assert isinstance(result, str)


class TestResolveHostname:
    """Tests for resolve_hostname function."""

    def test_resolve_hostname(self) -> None:
        """Test resolving hostname."""
        result = resolve_hostname("localhost")
        assert result is not None
        assert is_valid_ipv4(result)


class TestReverseDnsLookup:
    """Tests for reverse_dns_lookup function."""

    def test_reverse_dns_lookup(self) -> None:
        """Test reverse DNS lookup."""
        result = reverse_dns_lookup("127.0.0.1")
        assert result is not None


class TestIsReachable:
    """Tests for is_reachable function."""

    def test_is_reachable_localhost(self) -> None:
        """Test checking localhost reachability."""
        result = is_reachable("127.0.0.1", 80, timeout=0.5)
        assert isinstance(result, bool)


class TestGetLocalIp:
    """Tests for get_local_ip function."""

    def test_get_local_ip(self) -> None:
        """Test getting local IP."""
        result = get_local_ip()
        assert is_valid_ipv4(result)


class TestIsPrivateIp:
    """Tests for is_private_ip function."""

    def test_private_ip(self) -> None:
        """Test private IP detection."""
        assert is_private_ip("192.168.1.1")
        assert is_private_ip("10.0.0.1")
        assert is_private_ip("172.16.0.1")

    def test_public_ip(self) -> None:
        """Test public IP detection."""
        assert not is_private_ip("8.8.8.8")


class TestIsLoopbackIp:
    """Tests for is_loopback_ip function."""

    def test_loopback_ip(self) -> None:
        """Test loopback IP detection."""
        assert is_loopback_ip("127.0.0.1")
        assert is_loopback_ip("127.1.2.3")


class TestParseHttpHeaders:
    """Tests for parse_http_headers function."""

    def test_parse_http_headers(self) -> None:
        """Test parsing HTTP headers."""
        text = "Content-Type: text/html\nServer: nginx"
        result = parse_http_headers(text)
        assert result["Content-Type"] == "text/html"
        assert result["Server"] == "nginx"


class TestFormatHttpHeaders:
    """Tests for format_http_headers function."""

    def test_format_http_headers(self) -> None:
        """Test formatting HTTP headers."""
        headers = {"Content-Type": "text/html", "Server": "nginx"}
        result = format_http_headers(headers)
        assert "Content-Type: text/html" in result


class TestEncodeJson:
    """Tests for encode_json function."""

    def test_encode_json(self) -> None:
        """Test JSON encoding."""
        result = encode_json({"key": "value"})
        assert result == '{"key": "value"}'


class TestDecodeJson:
    """Tests for decode_json function."""

    def test_decode_json(self) -> None:
        """Test JSON decoding."""
        result = decode_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_decode_json_invalid(self) -> None:
        """Test JSON decoding invalid."""
        result = decode_json("not json")
        assert result is None


class TestIsValidUrl:
    """Tests for is_valid_url function."""

    def test_valid_url(self) -> None:
        """Test valid URL detection."""
        assert is_valid_url("https://example.com")
        assert is_valid_url("http://example.com:8080/path")

    def test_invalid_url(self) -> None:
        """Test invalid URL detection."""
        assert not is_valid_url("not a url")
        assert not is_valid_url("example.com")


class TestIsSslPort:
    """Tests for is_ssl_port function."""

    def test_ssl_port(self) -> None:
        """Test SSL port detection."""
        assert is_ssl_port(443)
        assert is_ssl_port(8443)

    def test_non_ssl_port(self) -> None:
        """Test non-SSL port detection."""
        assert not is_ssl_port(80)
        assert not is_ssl_port(8080)


class TestGetContentType:
    """Tests for get_content_type function."""

    def test_get_content_type(self) -> None:
        """Test getting content type."""
        assert get_content_type("file.html") == "text/html"
        assert get_content_type("image.png") == "image/png"
        assert get_content_type("script.js") == "application/javascript"


class TestJoinUrlParts:
    """Tests for join_url_parts function."""

    def test_join_url_parts(self) -> None:
        """Test joining URL parts."""
        result = join_url_parts(["path", "to", "file"])
        assert result == "path/to/file"


class TestNormalizeUrlPath:
    """Tests for normalize_url_path function."""

    def test_normalize_url_path(self) -> None:
        """Test normalizing URL path."""
        assert normalize_url_path("/a/b/../c") == "/a/c"
        assert normalize_url_path("/a/./b") == "/a/b"


class TestIpToInt:
    """Tests for ip_to_int function."""

    def test_ip_to_int(self) -> None:
        """Test converting IP to int."""
        assert ip_to_int("192.168.1.1") == 3232235777
        assert ip_to_int("127.0.0.1") == 2130706433


class TestIntToIp:
    """Tests for int_to_ip function."""

    def test_int_to_ip(self) -> None:
        """Test converting int to IP."""
        assert int_to_ip(3232235777) == "192.168.1.1"
        assert int_to_ip(2130706433) == "127.0.0.1"


class TestCidrToNetmask:
    """Tests for cidr_to_netmask function."""

    def test_cidr_to_netmask(self) -> None:
        """Test converting CIDR to netmask."""
        assert cidr_to_netmask(24) == "255.255.255.0"
        assert cidr_to_netmask(16) == "255.255.0.0"


class TestNetmaskToCidr:
    """Tests for netmask_to_cidr function."""

    def test_netmask_to_cidr(self) -> None:
        """Test converting netmask to CIDR."""
        assert netmask_to_cidr("255.255.255.0") == 24
        assert netmask_to_cidr("255.255.0.0") == 16


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
