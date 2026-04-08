"""XML building and manipulation utilities.

Provides XML element construction and manipulation
for generating XML data in automation workflows.
"""

from typing import Any, Dict, List, Optional, Union


class XmlElement:
    """Represents an XML element.

    Example:
        root = XmlElement("config")
        root.set_attr("version", "1.0")
        child = root.add_child("item", text="Hello")
        print(root.to_xml())
    """

    def __init__(
        self,
        tag: str,
        text: Optional[str] = None,
        attrs: Optional[Dict[str, str]] = None,
    ) -> None:
        self.tag = tag
        self.text = text
        self.attrs: Dict[str, str] = attrs or {}
        self.children: List[XmlElement] = []

    def set_attr(self, key: str, value: str) -> "XmlElement":
        """Set an attribute.

        Args:
            key: Attribute name.
            value: Attribute value.

        Returns:
            Self for chaining.
        """
        self.attrs[key] = value
        return self

    def add_child(
        self,
        tag: str,
        text: Optional[str] = None,
        attrs: Optional[Dict[str, str]] = None,
    ) -> "XmlElement":
        """Add a child element.

        Args:
            tag: Child tag name.
            text: Optional text content.
            attrs: Optional attributes.

        Returns:
            New child element.
        """
        child = XmlElement(tag, text, attrs)
        self.children.append(child)
        return child

    def add_text(self, text: str) -> "XmlElement":
        """Add text content.

        Args:
            text: Text to add.

        Returns:
            Self for chaining.
        """
        if self.children:
            last = self.children[-1]
            if isinstance(last, str):
                self.children[-1] = last + text
                return self
        self.children.append(text)
        return self

    def to_xml(self, indent: int = 0, indent_str: str = "  ") -> str:
        """Convert element to XML string.

        Args:
            indent: Current indentation level.
            indent_str: Indentation string.

        Returns:
            XML string.
        """
        prefix = indent_str * indent
        attrs_str = "".join(f' {k}="{v}"' for k, v in self.attrs.items())

        if not self.children and self.text is None:
            return f"{prefix}<{self.tag}{attrs_str} />"

        if not self.children and self.text is not None:
            return f"{prefix}<{self.tag}{attrs_str}>{self._escape(self.text)}</{self.tag}>"

        lines = [f"{prefix}<{self.tag}{attrs_str}>"]
        if self.text is not None:
            lines.append(f"{indent_str * (indent + 1)}{self._escape(self.text)}")
        for child in self.children:
            if isinstance(child, str):
                lines.append(f"{indent_str * (indent + 1)}{self._escape(child)}")
            else:
                lines.append(child.to_xml(indent + 1, indent_str))
        lines.append(f"{prefix}</{self.tag}>")

        return "\n".join(lines)

    @staticmethod
    def _escape(s: str) -> str:
        return (s.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
                 .replace('"', "&quot;"))

    def __str__(self) -> str:
        return self.to_xml()


def xml_element(
    tag: str,
    text: Optional[str] = None,
    **attrs: str,
) -> XmlElement:
    """Create an XML element.

    Args:
        tag: Element tag.
        text: Optional text content.
        **attrs: Element attributes.

    Returns:
        New XmlElement.
    """
    return XmlElement(tag, text, attrs)


def xml_escape(s: str) -> str:
    """Escape XML special characters.

    Args:
        s: String to escape.

    Returns:
        Escaped string.
    """
    return XmlElement._escape(s)


def parse_xml_tag(tag_str: str) -> tuple:
    """Parse XML tag string into tag name and attributes.

    Args:
        tag_str: Tag string like '<tag attr="value">'.

    Returns:
        Tuple of (tag, attrs_dict).
    """
    import re
    match = re.match(r"<(\w+)(.*)>", tag_str)
    if not match:
        return "", {}

    tag = match.group(1)
    attrs_str = match.group(2)
    attrs: Dict[str, str] = {}

    for attr_match in re.finditer(r'(\w+)="([^"]*)"', attrs_str):
        attrs[attr_match.group(1)] = attr_match.group(2)

    return tag, attrs
