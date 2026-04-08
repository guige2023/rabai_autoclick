"""
XML parsing, manipulation, and serialization utilities.

Provides lightweight XML parsing, element construction,
namespace handling, and XPath queries.
"""

from __future__ import annotations

import re
from typing import Any
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element


def parse_xml(xml_string: str) -> Element:
    """
    Parse XML string into Element tree.

    Args:
        xml_string: Raw XML content

    Returns:
        Root Element

    Raises:
        ET.ParseError: If XML is malformed
    """
    return ET.fromstring(xml_string)


def parse_xml_safe(xml_string: str, default: Element | None = None) -> Element | None:
    """
    Parse XML with graceful fallback.

    Args:
        xml_string: Raw XML content
        default: Default element if parsing fails

    Returns:
        Root Element or default on failure
    """
    try:
        return ET.fromstring(xml_string)
    except ET.ParseError:
        return default


def element_to_dict(element: Element) -> dict[str, Any]:
    """
    Convert XML element to dictionary.

    Args:
        element: XML element

    Returns:
        Dictionary representation
    """
    result: dict[str, Any] = {"@tag": element.tag}

    if element.attrib:
        result["@attributes"] = dict(element.attrib)

    if element.text and element.text.strip():
        result["#text"] = element.text.strip()

    for child in element:
        child_data = element_to_dict(child)
        tag = child.tag
        if tag in result and "@children" not in result:
            if not isinstance(result.get("@children"), list):
                result["@children"] = [result.pop(tag)]
            result["@children"].append(child_data)
        elif tag in result:
            result["@children"].append(child_data)
        else:
            result[tag] = child_data

    return result


def dict_to_element(data: dict[str, Any], root_tag: str = "root") -> Element:
    """
    Convert dictionary back to XML element.

    Args:
        data: Dictionary representation
        root_tag: Root element tag name

    Returns:
        XML Element
    """
    attrib = data.get("@attributes", {})
    root = ET.Element(root_tag, **{k: str(v) for k, v in attrib.items()})

    for key, value in data.items():
        if key in ("@tag", "@attributes", "@children"):
            continue
        if isinstance(value, dict):
            root.append(dict_to_element(value, key))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    root.append(dict_to_element(item, key))
        else:
            child = ET.SubElement(root, key)
            child.text = str(value)

    return root


def element_to_string(element: Element, pretty: bool = False) -> str:
    """
    Serialize element to XML string.

    Args:
        element: XML element
        pretty: Add indentation for readability

    Returns:
        XML string
    """
    if pretty:
        indent_xml(element)
    return ET.tostring(element, encoding="unicode")


def indent_xml(element: Element, level: int = 0) -> None:
    """
    In-place XML indentation (mutates element).

    Args:
        element: XML element to indent
        level: Current indentation level
    """
    indent = "\n" + "  " * level
    if len(element):
        if not element.text or not element.text.strip():
            element.text = ""
        element.text += indent + "  "
        for child in element:
            indent_xml(child, level + 1)
            if not child.tail or not child.tail.strip():
                child.tail = indent + "  "
        child.tail = indent if element.text else ""


def xpath_query(element: Element, query: str) -> list[Any]:
    """
    Execute XPath query on element.

    Args:
        element: XML element
        query: XPath expression

    Returns:
        List of matched elements/texts
    """
    return element.findall(query)


def find_all_text(element: Element, tag: str) -> list[str]:
    """
    Find all text content for a given tag.

    Args:
        element: Root element
        tag: Tag name to search

    Returns:
        List of text contents
    """
    return [e.text or "" for e in element.iter(tag) if e.text]


def strip_namespaces(xml_string: str) -> str:
    """
    Remove all namespace declarations from XML.

    Args:
        xml_string: Raw XML string

    Returns:
        XML without namespaces
    """
    result = re.sub(r'xmlns[^"]*"[^"]*"', "", xml_string)
    result = re.sub(r'xsi:[a-z]+="[^"]*"', "", result)
    result = re.sub(r"<([a-z0-9_]+):", "<", result)
    result = re.sub(r"</([a-z0-9_]+):", "</", result)
    return result


def create_element(
    tag: str,
    text: str | None = None,
    attrib: dict[str, str] | None = None,
    children: list[Element] | None = None,
) -> Element:
    """
    Programmatically create an XML element.

    Args:
        tag: Element tag name
        text: Text content
        attrib: Attribute dictionary
        children: Child elements

    Returns:
        New XML Element
    """
    elem = ET.Element(tag, **(attrib or {}))
    if text:
        elem.text = text
    if children:
        for child in children:
            elem.append(child)
    return elem


def build_soap_envelope(body_xml: str) -> str:
    """
    Wrap content in SOAP envelope.

    Args:
        body_xml: XML body content

    Returns:
        Complete SOAP envelope string
    """
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    {body_xml}
  </soap:Body>
</soap:Envelope>"""


def extract_text_content(element: Element) -> str:
    """
    Extract all text content recursively.

    Args:
        element: XML element

    Returns:
        Concatenated text content
    """
    parts = []
    if element.text:
        parts.append(element.text)
    for child in element:
        parts.append(extract_text_content(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()
