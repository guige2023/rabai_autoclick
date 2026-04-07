"""XML utilities for RabAI AutoClick.

Provides:
- XML parsing and manipulation
- XML building utilities
- XML validation helpers
"""

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Union


def parse_xml(xml_string: str) -> Optional[ET.Element]:
    """Parse XML string to Element tree.

    Args:
        xml_string: XML string to parse.

    Returns:
        Root element or None if parsing fails.
    """
    try:
        return ET.fromstring(xml_string)
    except ET.ParseError:
        return None


def escape_xml(text: str) -> str:
    """Escape XML special characters.

    Args:
        text: Text to escape.

    Returns:
        Escaped text.
    """
    xml_escape_table = {
        "&": "&amp;",
        '"': "&quot;",
        "'": "&apos;",
        ">": "&gt;",
        "<": "&lt;",
    }
    return "".join(xml_escape_table.get(c, c) for c in text)


def unescape_xml(text: str) -> str:
    """Unescape XML entities.

    Args:
        text: XML text to unescape.

    Returns:
        Unescaped text.
    """
    xml_unescape_table = {
        "&amp;": "&",
        "&quot;": '"',
        "&apos;": "'",
        "&gt;": ">",
        "&lt;": "<",
    }
    result = text
    for entity, char in xml_unescape_table.items():
        result = result.replace(entity, char)
    return result


def element_to_dict(element: ET.Element) -> Dict[str, Any]:
    """Convert XML element to dictionary.

    Args:
        element: XML element.

    Returns:
        Dictionary representation.
    """
    result: Dict[str, Any] = {}
    if element.attrib:
        result["@attributes"] = dict(element.attrib)
    if element.text and element.text.strip():
        if len(element) == 0:
            return element.text.strip()
        result["#text"] = element.text.strip()
    for child in element:
        child_data = element_to_dict(child)
        if child.tag in result:
            if not isinstance(result[child.tag], list):
                result[child.tag] = [result[child.tag]]
            result[child.tag].append(child_data)
        else:
            result[child.tag] = child_data
    return result


def dict_to_element(data: Dict[str, Any], root_tag: str = "root") -> ET.Element:
    """Convert dictionary to XML element.

    Args:
        data: Dictionary to convert.
        root_tag: Root element tag name.

    Returns:
        XML element.
    """
    root = ET.Element(root_tag)
    for key, value in data.items():
        if key.startswith("@"):
            continue
        child = ET.SubElement(root, key)
        if isinstance(value, dict):
            child.extend(dict_to_element(value, key).findall(key))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    child.extend(dict_to_element(item, key).findall(key))
                else:
                    child.text = str(item)
        else:
            child.text = str(value)
    return root


def pretty_print_xml(element: ET.Element, indent: str = "  ") -> str:
    """Pretty print XML element.

    Args:
        element: XML element.
        indent: Indentation string.

    Returns:
        Formatted XML string.
    """
    def format_element(el: ET.Element, level: int) -> str:
        result = [indent * level + f"<{el.tag}"]
        if el.attrib:
            for key, value in el.attrib.items():
                result[0] += f' {key}="{value}"'
        if len(el) == 0 and not el.text:
            result[0] += " />"
        else:
            result[0] += ">"
            if el.text and el.text.strip():
                result.append(indent * (level + 1) + el.text.strip())
            for child in el:
                result.extend(format_element(child, level + 1))
            result.append(indent * level + f"</{el.tag}>")
        return result

    lines = format_element(element, 0)
    return "\n".join(lines)


def strip_xml_tags(xml_string: str) -> str:
    """Remove XML tags from string.

    Args:
        xml_string: XML string.

    Returns:
        Text without XML tags.
    """
    return re.sub(r'<[^>]+>', '', xml_string)


def get_xpath(element: ET.Element, path: str) -> List[ET.Element]:
    """Get elements by XPath.

    Args:
        element: Root element.
        path: XPath expression.

    Returns:
        List of matching elements.
    """
    try:
        return element.findall(path)
    except SyntaxError:
        return []


def get_element_text(element: ET.Element, path: str = None) -> Optional[str]:
    """Get element text by path.

    Args:
        element: Root element.
        path: Optional path to child element.

    Returns:
        Element text or None.
    """
    if path:
        target = element.find(path)
        if target is not None:
            return target.text
        return None
    return element.text


def set_element_text(element: ET.Element, path: str, text: str) -> bool:
    """Set element text by path.

    Args:
        element: Root element.
        path: Path to element.
        text: Text to set.

    Returns:
        True if successful.
    """
    target = element.find(path)
    if target is not None:
        target.text = text
        return True
    return False


def get_attribute(element: ET.Element, path: str, attr: str) -> Optional[str]:
    """Get element attribute by path.

    Args:
        element: Root element.
        path: Path to element.
        attr: Attribute name.

    Returns:
        Attribute value or None.
    """
    target = element.find(path)
    if target is not None:
        return target.get(attr)
    return None


def set_attribute(element: ET.Element, path: str, attr: str, value: str) -> bool:
    """Set element attribute by path.

    Args:
        element: Root element.
        path: Path to element.
        attr: Attribute name.
        value: Attribute value.

    Returns:
        True if successful.
    """
    target = element.find(path)
    if target is not None:
        target.set(attr, value)
        return True
    return False


def create_element(tag: str, text: str = None, attributes: Dict[str, str] = None) -> ET.Element:
    """Create XML element.

    Args:
        tag: Element tag.
        text: Optional text content.
        attributes: Optional attributes.

    Returns:
        New XML element.
    """
    element = ET.Element(tag)
    if text:
        element.text = text
    if attributes:
        for key, value in attributes.items():
            element.set(key, value)
    return element


def add_child(parent: ET.Element, tag: str, text: str = None, attributes: Dict[str, str] = None) -> ET.Element:
    """Add child element to parent.

    Args:
        parent: Parent element.
        tag: Child tag.
        text: Optional text content.
        attributes: Optional attributes.

    Returns:
        New child element.
    """
    child = create_element(tag, text, attributes)
    parent.append(child)
    return child


def remove_element(element: ET.Element, path: str) -> bool:
    """Remove element by path.

    Args:
        element: Root element.
        path: Path to element to remove.

    Returns:
        True if removed.
    """
    parent = element.find("..")
    if parent is not None:
        target = element.find(path)
        if target is not None:
            parent.remove(target)
            return True
    return False


def get_all_text(element: ET.Element) -> str:
    """Get all text content including nested elements.

    Args:
        element: XML element.

    Returns:
        All text content.
    """
    texts = []
    if element.text:
        texts.append(element.text)
    for child in element:
        texts.append(get_all_text(child))
        if child.tail:
            texts.append(child.tail)
    return "".join(texts)


def count_elements(element: ET.Element, tag: str = None) -> int:
    """Count elements by tag.

    Args:
        element: Root element.
        tag: Optional tag to count (None for all).

    Returns:
        Number of elements.
    """
    if tag is None:
        return len(list(element.iter()))
    return len(element.findall(f".//{tag}"))


def validate_xml_syntax(xml_string: str) -> bool:
    """Validate XML syntax.

    Args:
        xml_string: XML string to validate.

    Returns:
        True if valid XML syntax.
    """
    try:
        ET.fromstring(xml_string)
        return True
    except ET.ParseError:
        return False


def merge_xml(elements: List[ET.Element], root_tag: str = "merged") -> ET.Element:
    """Merge multiple XML elements.

    Args:
        elements: List of elements to merge.
        root_tag: Tag for merged root.

    Returns:
        Merged root element.
    """
    root = ET.Element(root_tag)
    for element in elements:
        root.append(element)
    return root


def filter_elements(element: ET.Element, tag: str, predicate: callable) -> List[ET.Element]:
    """Filter elements by predicate.

    Args:
        element: Root element.
        tag: Element tag to filter.
        predicate: Function that returns True to keep element.

    Returns:
        List of matching elements.
    """
    results = []
    for el in element.findall(f".//{tag}"):
        if predicate(el):
            results.append(el)
    return results


def transform_xml(xml_string: str, transformer: callable) -> str:
    """Transform XML string using function.

    Args:
        xml_string: XML string.
        transformer: Function that takes element and returns element.

    Returns:
        Transformed XML string.
    """
    root = parse_xml(xml_string)
    if root is None:
        return xml_string
    transformed = transformer(root)
    return ET.tostring(transformed, encoding='unicode')


def element_to_string(element: ET.Element) -> str:
    """Convert element to string.

    Args:
        element: XML element.

    Returns:
        XML string.
    """
    return ET.tostring(element, encoding='unicode')


def create_xml_document(root_tag: str, declaration: bool = True) -> str:
    """Create new XML document.

    Args:
        root_tag: Root element tag.
        declaration: Include XML declaration.

    Returns:
        XML document string.
    """
    root = ET.Element(root_tag)
    if declaration:
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + element_to_string(root)
    return element_to_string(root)
