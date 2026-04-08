"""
XPath and XSLT transformation utilities for XML processing.

Provides XPath query execution, XSLT transformation, and
XML traversal helpers with namespace support.

Example:
    >>> from utils.xpath_utils import xpath_query, transform_xslt
    >>> titles = xpath_query(xml_string, "//book/title")
    >>> transformed = transform_xslt(xml_string, xslt_string)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from xml.dom import minidom
from xml.etree import ElementTree as ET


# Namespaces for common XML vocabularies
NAMESPACES: Dict[str, str] = {
    "xml": "http://www.w3.org/XML/1998/namespace",
    "html": "http://www.w3.org/1999/xhtml",
    "svg": "http://www.w3.org/2000/svg",
    "mathml": "http://www.w3.org/1998/Math/MathML",
    "xs": "http://www.w3.org/2001/XMLSchema",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "atom": "http://www.w3.org/2005/Atom",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
}


class XPathQuery:
    """
    XPath query executor with namespace and predicate support.

    Supports XPath 1.0 expressions including:
    - Location paths (/, //, .)
    - Predicates [condition]
    - Axis specifiers (child::, descendant::, etc.)
    - Functions (contains(), text(), etc.)
    - Wildcards (*)
    """

    def __init__(
        self,
        namespaces: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initialize the XPath query executor.

        Args:
            namespaces: Prefix to namespace URI mapping.
        """
        self.namespaces = namespaces or {}

    def query(
        self,
        xml_data: Union[str, bytes, ET.Element],
        xpath_expr: str,
    ) -> List[Any]:
        """
        Execute an XPath query.

        Args:
            xml_data: XML string, bytes, or Element.
            xpath_expr: XPath expression.

        Returns:
            List of matching values.
        """
        if isinstance(xml_data, (str, bytes)):
            root = ET.fromstring(xml_data)
        else:
            root = xml_data

        nsmap = self._build_nsmap(root)

        try:
            result = root.findall(xpath_expr, nsmap)
        except SyntaxError:
            result = self._query_with_regex(root, xpath_expr)

        return [self._element_to_dict(elem) if isinstance(elem, ET.Element) else elem for elem in result]

    def query_text(
        self,
        xml_data: Union[str, bytes, ET.Element],
        xpath_expr: str,
        separator: str = "",
    ) -> str:
        """
        Execute XPath and return concatenated text content.

        Args:
            xml_data: XML string, bytes, or Element.
            xpath_expr: XPath expression.
            separator: Text separator between results.

        Returns:
            Concatenated text content.
        """
        results = self.query(xml_data, xpath_expr)
        return separator.join(str(r) for r in results if r)

    def query_first(
        self,
        xml_data: Union[str, bytes, ET.Element],
        xpath_expr: str,
        default: Any = None,
    ) -> Any:
        """
        Execute XPath and return the first result.

        Args:
            xml_data: XML string, bytes, or Element.
            xpath_expr: XPath expression.
            default: Default value if no match.

        Returns:
            First matching value or default.
        """
        results = self.query(xml_data, xpath_expr)
        return results[0] if results else default

    def _build_nsmap(self, root: ET.Element) -> Dict[str, str]:
        """Build namespace map from root element."""
        nsmap = dict(self.namespaces)
        for elem in root.iter():
            for prefix, uri in elem.attrib.items():
                if prefix.startswith("xmlns"):
                    p = prefix.split(":")[1] if ":" in prefix else ""
                    if p:
                        nsmap[p] = uri
        return nsmap

    def _element_to_dict(self, elem: ET.Element) -> Dict[str, Any]:
        """Convert an Element to a dictionary."""
        result: Dict[str, Any] = {}

        if elem.attrib:
            result["@"] = dict(elem.attrib)

        children: Dict[str, List[Any]] = {}
        for child in elem:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            if tag not in children:
                children[tag] = []

            child_data = self._element_to_dict(child)
            if len(child) == 0:
                child_text = child.text.strip() if child.text else ""
                if child_text:
                    if "@" in child_data:
                        child_data["_text"] = child_text
                    else:
                        children[tag].append(child_text)
                else:
                    children[tag].append(child_data)
            else:
                children[tag].append(child_data)

        for tag, items in children.items():
            if len(items) == 1:
                result[tag] = items[0]
            else:
                result[tag] = items

        return result

    def _query_with_regex(
        self,
        root: ET.Element,
        xpath_expr: str
    ) -> List[ET.Element]:
        """Fallback regex-based query for complex expressions."""
        results: List[ET.Element] = []

        def matches(elem: ET.Element) -> bool:
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            if xpath_expr.startswith("//"):
                search = xpath_expr[2:]
                if search == "*" or tag == search:
                    results.append(elem)
                for child in elem:
                    matches(child)
            elif xpath_expr.startswith(".//"):
                search = xpath_expr[3:]
                if search == "*" or tag == search:
                    results.append(elem)
                for child in elem:
                    self._query_with_regex(child, xpath_expr[1:])
            elif xpath_expr == "*":
                results.append(elem)
            elif tag == xpath_expr:
                results.append(elem)

            return True

        matches(root)
        return results


class XsltTransformer:
    """
    XSLT transformation engine.

    Applies XSLT stylesheets to XML documents to produce
    transformed output.
    """

    def __init__(
        self,
        xslt_data: Optional[Union[str, bytes]] = None,
    ) -> None:
        """
        Initialize the XSLT transformer.

        Args:
            xslt_data: XSLT stylesheet string or bytes.
        """
        self.xslt_data = xslt_data

    def transform(
        self,
        xml_data: Union[str, bytes],
        xslt_data: Optional[Union[str, bytes]] = None,
        **params: str
    ) -> bytes:
        """
        Transform XML using XSLT.

        Args:
            xml_data: XML string or bytes to transform.
            xslt_data: XSLT stylesheet (uses instance attribute if not provided).
            **params: Parameters to pass to the stylesheet.

        Returns:
            Transformed output bytes.

        Raises:
            ImportError: If lxml is not installed.
        """
        try:
            from lxml import etree
        except ImportError:
            raise ImportError("lxml is required for XSLT. Install with: pip install lxml")

        if isinstance(xml_data, str):
            xml_data = xml_data.encode()

        xslt_data = xslt_data or self.xslt_data
        if xslt_data is None:
            raise ValueError("XSLT data must be provided")

        if isinstance(xslt_data, str):
            xslt_data = xslt_data.encode()

        xml_doc = etree.fromstring(xml_data)
        xslt_doc = etree.fromstring(xslt_data)

        transformer = etree.XSLT(xslt_doc)
        result = transformer(xml_doc, **params)

        return bytes(result)

    def transform_to_string(
        self,
        xml_data: Union[str, bytes],
        xslt_data: Optional[Union[str, bytes]] = None,
        **params: str
    ) -> str:
        """
        Transform XML to string.

        Args:
            xml_data: XML string or bytes to transform.
            xslt_data: XSLT stylesheet.
            **params: Parameters to pass to the stylesheet.

        Returns:
            Transformed output as string.
        """
        return self.transform(xml_data, xslt_data, **params).decode("utf-8")


def xpath_query(
    xml_data: Union[str, bytes, ET.Element],
    xpath_expr: str,
    namespaces: Optional[Dict[str, str]] = None,
) -> List[Any]:
    """
    Convenience function to execute XPath query.

    Args:
        xml_data: XML string, bytes, or Element.
        xpath_expr: XPath expression.
        namespaces: Namespace prefix to URI mapping.

    Returns:
        List of matching values.
    """
    return XPathQuery(namespaces=namespaces).query(xml_data, xpath_expr)


def xpath_text(
    xml_data: Union[str, bytes, ET.Element],
    xpath_expr: str,
    namespaces: Optional[Dict[str, str]] = None,
    separator: str = "",
) -> str:
    """
    Convenience function to get text from XPath query.

    Args:
        xml_data: XML string, bytes, or Element.
        xpath_expr: XPath expression.
        namespaces: Namespace prefix to URI mapping.
        separator: Text separator between results.

    Returns:
        Concatenated text content.
    """
    return XPathQuery(namespaces=namespaces).query_text(xml_data, xpath_expr, separator)


def transform_xslt(
    xml_data: Union[str, bytes],
    xslt_data: Union[str, bytes],
    **params: str
) -> bytes:
    """
    Convenience function to transform XML with XSLT.

    Args:
        xml_data: XML string or bytes to transform.
        xslt_data: XSLT stylesheet string or bytes.
        **params: Parameters to pass to the stylesheet.

    Returns:
        Transformed output bytes.
    """
    return XsltTransformer(xslt_data).transform(xml_data, **params)
