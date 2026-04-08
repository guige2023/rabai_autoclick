"""XML processing and parsing utilities.

Handles XML parsing, validation, transformation,
namespace handling, and element manipulation.
"""

from typing import Any, Optional, Iterator
import logging
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO, StringIO

try:
    import xml.etree.ElementTree as ET
except ImportError:
    ET = None

try:
    from lxml import etree
    from lxml.builder import E
    HAS_LXML = True
except ImportError:
    HAS_LXML = False
    etree = None

logger = logging.getLogger(__name__)


@dataclass
class XMLConfig:
    """Configuration for XML processing."""
    encoding: str = "utf-8"
    indent: str = "  "
    namespace_separator: str = ":"
    strip_whitespace: bool = True


@dataclass
class XMLElement:
    """Wrapper for XML element with convenient access."""
    tag: str
    text: Optional[str] = None
    tail: Optional[str] = None
    attributes: dict = field(default_factory=dict)
    children: list = field(default_factory=list)
    namespaces: dict = field(default_factory=dict)


class XMLParseError(Exception):
    """Raised on XML parsing errors."""
    pass


class XMLValidationError(Exception):
    """Raised on XML validation errors."""
    pass


class XMLAction:
    """XML processing utilities."""

    def __init__(self, config: Optional[XMLConfig] = None):
        """Initialize XML processor with configuration.

        Args:
            config: XMLConfig with processing options
        """
        if ET is None:
            raise ImportError("xml.etree.ElementTree is required (stdlib)")

        self.config = config or XMLConfig()
        self._namespaces: dict[str, str] = {}

    def parse_string(self, xml_string: str) -> XMLElement:
        """Parse XML from string.

        Args:
            xml_string: XML content as string

        Returns:
            XMLElement root object

        Raises:
            XMLParseError: On parse failure
        """
        try:
            if self.config.strip_whitespace:
                xml_string = xml_string.strip()

            root = ET.fromstring(xml_string)
            return self._element_to_dict(root)

        except ET.ParseError as e:
            raise XMLParseError(f"Parse failed: {e}")

    def parse_bytes(self, xml_bytes: bytes) -> XMLElement:
        """Parse XML from bytes.

        Args:
            xml_bytes: XML content as bytes

        Returns:
            XMLElement root object
        """
        try:
            root = ET.fromstring(xml_bytes)
            return self._element_to_dict(root)

        except ET.ParseError as e:
            raise XMLParseError(f"Parse failed: {e}")

    def parse_file(self, file_path: str) -> XMLElement:
        """Parse XML from file.

        Args:
            file_path: Path to XML file

        Returns:
            XMLElement root object
        """
        try:
            tree = ET.parse(file_path)
            return self._element_to_dict(tree.getroot())

        except ET.ParseError as e:
            raise XMLParseError(f"Parse failed: {e}")

    def to_string(self, element: XMLElement, root_name: Optional[str] = None) -> str:
        """Convert XMLElement to XML string.

        Args:
            element: XMLElement to serialize
            root_name: Optional root element name override

        Returns:
            XML string
        """
        root = self._dict_to_element(element, root_name)
        return ET.tostring(root, encoding=self.config.encoding, xml_declaration=True).decode(self.config.encoding)

    def validate_xsd(self, xml_string: str, xsd_string: str) -> bool:
        """Validate XML against XSD schema.

        Args:
            xml_string: XML content
            xsd_string: XSD schema

        Returns:
            True if valid

        Raises:
            XMLValidationError: If invalid
        """
        if not HAS_LXML:
            logger.warning("lxml not available, skipping XSD validation")
            return True

        try:
            schema_doc = etree.fromstring(xsd_string.encode(self.config.encoding))
            schema = etree.XMLSchema(schema_doc)

            xml_doc = etree.fromstring(xml_string.encode(self.config.encoding))

            if not schema.validate(xml_doc):
                errors = "\n".join(str(e) for e in schema.error_log)
                raise XMLValidationError(f"Validation failed:\n{errors}")

            return True

        except etree.XMLSyntaxError as e:
            raise XMLValidationError(f"XSD syntax error: {e}")

    def transform_xslt(self, xml_string: str, xslt_string: str) -> str:
        """Transform XML using XSLT.

        Args:
            xml_string: XML content
            xslt_string: XSLT stylesheet

        Returns:
            Transformed XML string
        """
        if not HAS_LXML:
            raise ImportError("lxml required for XSLT transformation: pip install lxml")

        try:
            xslt_doc = etree.fromstring(xslt_string.encode(self.config.encoding))
            transform = etree.XSLT(xslt_doc)

            xml_doc = etree.fromstring(xml_string.encode(self.config.encoding))
            result = transform(xml_doc)

            return str(result)

        except etree.XMLSyntaxError as e:
            raise XMLParseError(f"XSLT parse failed: {e}")

    def xpath_query(self, xml_string: str, xpath: str,
                   namespaces: Optional[dict] = None) -> list:
        """Execute XPath query on XML.

        Args:
            xml_string: XML content
            xpath: XPath expression
            namespaces: Optional namespace prefix map

        Returns:
            List of matching elements/texts/values
        """
        try:
            root = ET.fromstring(xml_string)
            ns = namespaces or {}

            results = root.findall(xpath, ns)

            output = []
            for elem in results:
                if isinstance(elem, str):
                    output.append(elem)
                elif elem is None:
                    pass
                else:
                    output.append(self._element_to_dict(elem))

            return output

        except ET.ParseError as e:
            raise XMLParseError(f"Parse failed: {e}")

    def find_elements(self, element: XMLElement,
                     tag: str,
                     recursive: bool = True) -> list[XMLElement]:
        """Find child elements by tag name.

        Args:
            element: Parent XMLElement
            tag: Tag name to find
            recursive: Search recursively

        Returns:
            List of matching XMLElement objects
        """
        results = []

        for child in element.children:
            if child.tag == tag:
                results.append(child)

            if recursive and child.children:
                results.extend(self.find_elements(child, tag, recursive=True))

        return results

    def add_element(self, parent: XMLElement,
                   new_element: XMLElement,
                   index: Optional[int] = None) -> None:
        """Add child element to parent.

        Args:
            parent: Parent XMLElement
            new_element: Element to add
            index: Optional position to insert at
        """
        if index is None or index >= len(parent.children):
            parent.children.append(new_element)
        else:
            parent.children.insert(index, new_element)

    def remove_element(self, parent: XMLElement,
                      element: XMLElement) -> bool:
        """Remove child element from parent.

        Args:
            parent: Parent XMLElement
            element: Element to remove

        Returns:
            True if removed
        """
        try:
            parent.children.remove(element)
            return True
        except ValueError:
            return False

    def set_attribute(self, element: XMLElement,
                      name: str, value: Any) -> None:
        """Set element attribute.

        Args:
            element: XMLElement
            name: Attribute name
            value: Attribute value
        """
        element.attributes[name] = str(value)

    def get_attribute(self, element: XMLElement,
                     name: str, default: Any = None) -> Any:
        """Get element attribute.

        Args:
            element: XMLElement
            name: Attribute name
            default: Default if not found

        Returns:
            Attribute value or default
        """
        return element.attributes.get(name, default)

    def remove_attribute(self, element: XMLElement, name: str) -> bool:
        """Remove element attribute.

        Args:
            element: XMLElement
            name: Attribute name

        Returns:
            True if removed
        """
        if name in element.attributes:
            del element.attributes[name]
            return True
        return False

    def set_text(self, element: XMLElement, text: str) -> None:
        """Set element text content.

        Args:
            element: XMLElement
            text: Text content
        """
        element.text = text

    def get_text(self, element: XMLElement) -> str:
        """Get element text content.

        Args:
            element: XMLElement

        Returns:
            Text content or empty string
        """
        return element.text or ""

    def flatten(self, element: XMLElement) -> dict:
        """Flatten element to nested dict.

        Args:
            element: XMLElement to flatten

        Returns:
            Nested dict representation
        """
        result: dict[str, Any] = {
            "@tag": element.tag,
            "@attributes": element.attributes
        }

        if element.text:
            result["#text"] = element.text

        if element.children:
            children_by_tag: dict[str, Any] = {}

            for child in element.children:
                child_data = self.flatten(child)

                for key, value in child_data.items():
                    if key in children_by_tag:
                        existing = children_by_tag[key]
                        if isinstance(existing, list):
                            existing.append(value)
                        else:
                            children_by_tag[key] = [existing, value]
                    else:
                        children_by_tag[key] = value

            result.update(children_by_tag)

        return result

    def _element_to_dict(self, elem: ET.Element) -> XMLElement:
        """Convert ET.Element to XMLElement."""
        children = []

        for child in elem:
            children.append(self._element_to_dict(child))

        return XMLElement(
            tag=elem.tag,
            text=elem.text,
            tail=elem.tail,
            attributes=elem.attrib,
            children=children,
            namespaces=self._namespaces
        )

    def _dict_to_element(self, d: XMLElement,
                        root_name: Optional[str] = None) -> ET.Element:
        """Convert XMLElement to ET.Element."""
        tag = root_name or d.tag
        elem = ET.Element(tag, attrib=d.attributes)

        if d.text:
            elem.text = d.text

        for child in d.children:
            child_elem = self._dict_to_element(child, None)
            elem.append(child_elem)

        return elem
