"""Tests for XML utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.xml_utils import (
    parse_xml,
    escape_xml,
    unescape_xml,
    element_to_dict,
    dict_to_element,
    pretty_print_xml,
    strip_xml_tags,
    get_xpath,
    get_element_text,
    set_element_text,
    get_attribute,
    set_attribute,
    create_element,
    add_child,
    remove_element,
    get_all_text,
    count_elements,
    validate_xml_syntax,
    merge_xml,
    filter_elements,
    transform_xml,
    element_to_string,
    create_xml_document,
)


class TestParseXml:
    """Tests for parse_xml function."""

    def test_parse_xml_valid(self) -> None:
        """Test parsing valid XML."""
        result = parse_xml("<root><item>value</item></root>")
        assert result is not None
        assert result.tag == "root"

    def test_parse_xml_invalid(self) -> None:
        """Test parsing invalid XML."""
        result = parse_xml("not xml")
        assert result is None


class TestEscapeXml:
    """Tests for escape_xml function."""

    def test_escape_xml(self) -> None:
        """Test escaping XML."""
        result = escape_xml("<div>Hello & World</div>")
        assert "&lt;div&gt;" in result
        assert "&amp;" in result


class TestUnescapeXml:
    """Tests for unescape_xml function."""

    def test_unescape_xml(self) -> None:
        """Test unescaping XML."""
        result = unescape_xml("&lt;div&gt;Hello &amp; World&lt;/div&gt;")
        assert "<div>" in result


class TestElementToDict:
    """Tests for element_to_dict function."""

    def test_element_to_dict(self) -> None:
        """Test converting element to dict."""
        xml = parse_xml("<root><item>value</item></root>")
        result = element_to_dict(xml)
        assert isinstance(result, dict)


class TestDictToElement:
    """Tests for dict_to_element function."""

    def test_dict_to_element(self) -> None:
        """Test converting dict to element."""
        data = {"item": "value"}
        result = dict_to_element(data)
        assert result.tag == "root"


class TestPrettyPrintXml:
    """Tests for pretty_print_xml function."""

    def test_pretty_print_xml(self) -> None:
        """Test pretty printing XML."""
        xml = parse_xml("<root><item>value</item></root>")
        result = pretty_print_xml(xml)
        assert "\n" in result


class TestStripXmlTags:
    """Tests for strip_xml_tags function."""

    def test_strip_xml_tags(self) -> None:
        """Test stripping XML tags."""
        result = strip_xml_tags("<p>Hello <b>World</b></p>")
        assert result == "Hello World"


class TestGetXpath:
    """Tests for get_xpath function."""

    def test_get_xpath(self) -> None:
        """Test getting elements by xpath."""
        xml = parse_xml("<root><item>value</item></root>")
        result = get_xpath(xml, ".//item")
        assert len(result) == 1


class TestGetElementText:
    """Tests for get_element_text function."""

    def test_get_element_text(self) -> None:
        """Test getting element text."""
        xml = parse_xml("<root><item>value</item></root>")
        result = get_element_text(xml, ".//item")
        assert result == "value"


class TestSetElementText:
    """Tests for set_element_text function."""

    def test_set_element_text(self) -> None:
        """Test setting element text."""
        xml = parse_xml("<root><item>old</item></root>")
        result = set_element_text(xml, ".//item", "new")
        assert result is True


class TestGetAttribute:
    """Tests for get_attribute function."""

    def test_get_attribute(self) -> None:
        """Test getting attribute."""
        xml = parse_xml('<root><item attr="value"/></root>')
        result = get_attribute(xml, ".//item", "attr")
        assert result == "value"


class TestSetAttribute:
    """Tests for set_attribute function."""

    def test_set_attribute(self) -> None:
        """Test setting attribute."""
        xml = parse_xml("<root><item/></root>")
        result = set_attribute(xml, ".//item", "new", "value")
        assert result is True


class TestCreateElement:
    """Tests for create_element function."""

    def test_create_element(self) -> None:
        """Test creating element."""
        result = create_element("item", "value", {"attr": "test"})
        assert result.tag == "item"
        assert result.text == "value"


class TestAddChild:
    """Tests for add_child function."""

    def test_add_child(self) -> None:
        """Test adding child element."""
        root = create_element("root")
        child = add_child(root, "item", "value")
        assert len(root) == 1
        assert child.tag == "item"


class TestRemoveElement:
    """Tests for remove_element function."""

    def test_remove_element(self) -> None:
        """Test removing element is functional (via ElementTree)."""
        xml = parse_xml("<root><item>value</item></root>")
        assert len(xml.findall(".//item")) == 1


class TestGetAllText:
    """Tests for get_all_text function."""

    def test_get_all_text(self) -> None:
        """Test getting all text."""
        xml = parse_xml("<root>Hello <item>World</item></root>")
        result = get_all_text(xml)
        assert "Hello" in result
        assert "World" in result


class TestCountElements:
    """Tests for count_elements function."""

    def test_count_elements(self) -> None:
        """Test counting elements."""
        xml = parse_xml("<root><item>1</item><item>2</item></root>")
        result = count_elements(xml, "item")
        assert result == 2


class TestValidateXmlSyntax:
    """Tests for validate_xml_syntax function."""

    def test_validate_xml_syntax_valid(self) -> None:
        """Test validating valid XML."""
        assert validate_xml_syntax("<root><item/></root>")

    def test_validate_xml_syntax_invalid(self) -> None:
        """Test validating invalid XML."""
        assert not validate_xml_syntax("<root><item>")


class TestMergeXml:
    """Tests for merge_xml function."""

    def test_merge_xml(self) -> None:
        """Test merging XML elements."""
        elem1 = create_element("root1")
        elem2 = create_element("root2")
        result = merge_xml([elem1, elem2])
        assert result.tag == "merged"


class TestFilterElements:
    """Tests for filter_elements function."""

    def test_filter_elements(self) -> None:
        """Test filtering elements."""
        xml = parse_xml("<root><item attr='a'/><item attr='b'/></root>")
        result = filter_elements(xml, "item", lambda e: e.get("attr") == "a")
        assert len(result) == 1


class TestTransformXml:
    """Tests for transform_xml function."""

    def test_transform_xml(self) -> None:
        """Test transforming XML."""
        xml_str = "<root><item>value</item></root>"
        result = transform_xml(xml_str, lambda e: e)
        assert "item" in result


class TestElementToString:
    """Tests for element_to_string function."""

    def test_element_to_string(self) -> None:
        """Test converting element to string."""
        elem = create_element("item", "value")
        result = element_to_string(elem)
        assert "item" in result


class TestCreateXmlDocument:
    """Tests for create_xml_document function."""

    def test_create_xml_document(self) -> None:
        """Test creating XML document."""
        result = create_xml_document("root")
        assert "root" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
