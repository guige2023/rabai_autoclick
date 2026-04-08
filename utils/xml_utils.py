"""XML utilities for RabAI AutoClick.

Provides:
- XML parsing and manipulation
- Element creation
- XPath queries
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import (
    Any,
    Dict,
    List,
    Optional,
)


def parse_xml(xml_string: str) -> Optional[ET.Element]:
    """Parse an XML string.

    Args:
        xml_string: XML content as string.

    Returns:
        Root element or None on error.
    """
    try:
        return ET.fromstring(xml_string)
    except ET.ParseError:
        return None


def parse_xml_file(path: str) -> Optional[ET.Element]:
    """Parse an XML file.

    Args:
        path: Path to XML file.

    Returns:
        Root element or None on error.
    """
    try:
        return ET.parse(path).getroot()
    except (ET.ParseError, FileNotFoundError):
        return None


def to_string(element: ET.Element) -> str:
    """Convert element to XML string.

    Args:
        element: XML element.

    Returns:
        XML string.
    """
    return ET.tostring(element, encoding="unicode")


def find_all(element: ET.Element, xpath: str) -> List[ET.Element]:
    """Find all elements matching xpath.

    Args:
        element: Root element.
        xpath: XPath expression.

    Returns:
        List of matching elements.
    """
    return element.findall(xpath)


def find_first(element: ET.Element, xpath: str) -> Optional[ET.Element]:
    """Find first element matching xpath.

    Args:
        element: Root element.
        xpath: XPath expression.

    Returns:
        First matching element or None.
    """
    return element.find(xpath)


def get_text(element: ET.Element, path: Optional[str] = None) -> Optional[str]:
    """Get text content.

    Args:
        element: Element or subelement.
        path: Optional subelement path.

    Returns:
        Text content or None.
    """
    target = element if path is None else element.find(path)
    return target.text if target is not None else None


def set_text(element: ET.Element, text: str) -> None:
    """Set text content of element.

    Args:
        element: Element to modify.
        text: Text to set.
    """
    element.text = text


def get_attr(element: ET.Element, name: str) -> Optional[str]:
    """Get an attribute value.

    Args:
        element: Element.
        name: Attribute name.

    Returns:
        Attribute value or None.
    """
    return element.attrib.get(name)


def set_attr(element: ET.Element, name: str, value: str) -> None:
    """Set an attribute value.

    Args:
        element: Element to modify.
        name: Attribute name.
        value: Attribute value.
    """
    element.set(name, value)


def create_element(
    tag: str,
    text: Optional[str] = None,
    attrib: Optional[Dict[str, str]] = None,
) -> ET.Element:
    """Create a new XML element.

    Args:
        tag: Element tag name.
        text: Optional text content.
        attrib: Optional attributes dict.

    Returns:
        New element.
    """
    el = ET.Element(tag, attrib or {})
    if text is not None:
        el.text = text
    return el


def add_child(
    parent: ET.Element,
    tag: str,
    text: Optional[str] = None,
    attrib: Optional[Dict[str, str]] = None,
) -> ET.Element:
    """Add a child element to parent.

    Args:
        parent: Parent element.
        tag: Child tag name.
        text: Optional text content.
        attrib: Optional attributes dict.

    Returns:
        New child element.
    """
    child = create_element(tag, text, attrib)
    parent.append(child)
    return child


__all__ = [
    "parse_xml",
    "parse_xml_file",
    "to_string",
    "find_all",
    "find_first",
    "get_text",
    "set_text",
    "get_attr",
    "set_attr",
    "create_element",
    "add_child",
]
