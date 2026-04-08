"""XML Parser action module for RabAI AutoClick.

Provides XML parsing, traversal, transformation, and validation
operations with support for namespaces and XPath queries.
"""

import sys
import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class XMLConfig:
    """XML processing configuration."""
    encoding: str = "utf-8"
    indent: str = "  "
    namespaces: Dict[str, str] = None
    preserve_whitespace: bool = False


class XMLAction(BaseAction):
    """Action for XML operations.
    
    Features:
        - Parse XML from string or file
        - Serialize Python objects to XML
        - Validate XML syntax
        - XPath query support
        - Element traversal
        - Namespace handling
        - XML transformation (XSLT-like)
        - Attribute manipulation
        - Pretty printing
    """
    
    def __init__(self, config: Optional[XMLConfig] = None):
        """Initialize XML action.
        
        Args:
            config: XML configuration.
        """
        super().__init__()
        self.config = config or XMLConfig()
        if self.config.namespaces is None:
            self.config.namespaces = {}
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute XML operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (parse, serialize, validate,
                           xpath, find, transform, pretty)
                - content: XML content string
                - file_path: Path to XML file
                - data: Python object for serialization
                - xpath: XPath expression
                - root: Root element name for serialization
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "parse":
                return self._parse(params)
            elif operation == "serialize":
                return self._serialize(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "xpath":
                return self._xpath(params)
            elif operation == "find":
                return self._find(params)
            elif operation == "transform":
                return self._transform(params)
            elif operation == "pretty":
                return self._pretty(params)
            elif operation == "get_attributes":
                return self._get_attributes(params)
            elif operation == "set_attributes":
                return self._set_attributes(params)
            elif operation == "add_element":
                return self._add_element(params)
            elif operation == "remove_element":
                return self._remove_element(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"XML operation failed: {str(e)}")
    
    def _parse(self, params: Dict[str, Any]) -> ActionResult:
        """Parse XML content or file into element tree."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
            except ET.ParseError as e:
                return ActionResult(success=False, message=f"XML parse error: {str(e)}")
        elif content:
            try:
                root = ET.fromstring(content)
            except ET.ParseError as e:
                return ActionResult(success=False, message=f"XML parse error: {str(e)}")
        else:
            return ActionResult(success=False, message="content or file_path required")
        
        return ActionResult(
            success=True,
            message="XML parsed successfully",
            data={
                "root_tag": root.tag,
                "xml_str": ET.tostring(root, encoding=self.config.encoding),
                "namespace": root.tag.split("}")[0] + "}" if "}" in root.tag else ""
            }
        )
    
    def _serialize(self, params: Dict[str, Any]) -> ActionResult:
        """Serialize Python object/dict to XML string."""
        data = params.get("data", {})
        root_tag = params.get("root", "root")
        output_path = params.get("output_path", "")
        
        if not data:
            return ActionResult(success=False, message="data is required for serialization")
        
        try:
            root = self._dict_to_element(data, root_tag)
            xml_str = self._element_to_string(root)
            
            if output_path:
                os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
                with open(output_path, "w", encoding=self.config.encoding) as f:
                    f.write(xml_str)
                return ActionResult(
                    success=True,
                    message=f"XML serialized to {output_path}",
                    data={"path": output_path, "size": len(xml_str)}
                )
            
            return ActionResult(
                success=True,
                message="XML serialized",
                data={"xml": xml_str}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Serialization error: {str(e)}")
    
    def _dict_to_element(self, data: Union[Dict, List], tag: str) -> ET.Element:
        """Convert Python dict/list to XML element."""
        if isinstance(data, dict):
            elem = ET.Element(tag)
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    child = self._dict_to_element(value, key)
                    elem.append(child)
                else:
                    child = ET.SubElement(elem, key)
                    child.text = str(value) if value is not None else ""
            return elem
        elif isinstance(data, list):
            elem = ET.Element(tag)
            for i, item in enumerate(data):
                child = self._dict_to_element(item, f"item{i}")
                elem.append(child)
            return elem
        else:
            elem = ET.Element(tag)
            elem.text = str(data) if data is not None else ""
            return elem
    
    def _element_to_string(self, elem: ET.Element, indent_level: int = 0) -> str:
        """Convert element to formatted XML string."""
        indent = self.config.indent * indent_level
        new_indent = self.config.indent * (indent_level + 1)
        
        attrs = ""
        if elem.attrib:
            attr_parts = [f'{k}="{v}"' for k, v in elem.attrib.items()]
            attrs = " " + " ".join(attr_parts)
        
        if len(elem) == 0 and (elem.text is None or elem.text == ""):
            return f"{indent}<{elem.tag}{attrs}/>"
        
        lines = [f"{indent}<{elem.tag}{attrs}>"]
        
        if elem.text and elem.text.strip():
            lines.append(f"{new_indent}{elem.text}")
        
        for child in elem:
            lines.append(self._element_to_string(child, indent_level + 1))
        
        lines.append(f"{indent}</{elem.tag}>")
        
        return "\n".join(lines)
    
    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate XML syntax."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            try:
                ET.parse(file_path)
                return ActionResult(
                    success=True,
                    message="XML is valid",
                    data={"path": file_path, "valid": True}
                )
            except ET.ParseError as e:
                return ActionResult(
                    success=False,
                    message=f"XML is invalid: {str(e)}",
                    data={"path": file_path, "valid": False, "error": str(e)}
                )
        elif content:
            try:
                ET.fromstring(content)
                return ActionResult(
                    success=True,
                    message="XML is valid",
                    data={"valid": True}
                )
            except ET.ParseError as e:
                return ActionResult(
                    success=False,
                    message=f"XML is invalid: {str(e)}",
                    data={"valid": False, "error": str(e)}
                )
        else:
            return ActionResult(success=False, message="content or file_path required")
    
    def _xpath(self, params: Dict[str, Any]) -> ActionResult:
        """Execute XPath query on XML."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        xpath_expr = params.get("xpath", "")
        namespaces = params.get("namespaces", self.config.namespaces)
        
        if not xpath_expr:
            return ActionResult(success=False, message="xpath expression required")
        
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                tree = ET.parse(file_path)
                root = tree.getroot()
            elif content:
                root = ET.fromstring(content)
            else:
                return ActionResult(success=False, message="content or file_path required")
            
            if namespaces:
                results = root.findall(xpath_expr, namespaces)
            else:
                results = root.findall(xpath_expr)
            
            result_data = []
            for elem in results:
                if isinstance(elem, ET.Element):
                    result_data.append({
                        "tag": elem.tag,
                        "text": elem.text,
                        "attrib": elem.attrib,
                        "xml": ET.tostring(elem, encoding=self.config.encoding)
                    })
                else:
                    result_data.append(str(elem))
            
            return ActionResult(
                success=True,
                message=f"XPath returned {len(results)} results",
                data={"results": result_data, "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XPath error: {str(e)}")
    
    def _find(self, params: Dict[str, Any]) -> ActionResult:
        """Find single element matching path."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        path = params.get("path", "")
        namespaces = params.get("namespaces", self.config.namespaces)
        
        if not path:
            return ActionResult(success=False, message="path required")
        
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                tree = ET.parse(file_path)
                root = tree.getroot()
            elif content:
                root = ET.fromstring(content)
            else:
                return ActionResult(success=False, message="content or file_path required")
            
            if namespaces:
                elem = root.find(path, namespaces)
            else:
                elem = root.find(path)
            
            if elem is None:
                return ActionResult(
                    success=False,
                    message=f"No element found at path: {path}",
                    data={"found": False}
                )
            
            return ActionResult(
                success=True,
                message=f"Found element: {elem.tag}",
                data={
                    "found": True,
                    "tag": elem.tag,
                    "text": elem.text,
                    "attrib": elem.attrib,
                    "xml": ET.tostring(elem, encoding=self.config.encoding)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Find error: {str(e)}")
    
    def _transform(self, params: Dict[str, Any]) -> ActionResult:
        """Transform XML using mapping rules."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        mappings = params.get("mappings", {})  # {"old_tag": "new_tag"}
        operations = params.get("operations", [])
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            with open(file_path, "r", encoding=self.config.encoding) as f:
                content = f.read()
        
        if not content:
            return ActionResult(success=False, message="content or file_path required")
        
        try:
            root = ET.fromstring(content)
            
            if mappings:
                self._apply_mappings(root, mappings)
            
            if operations:
                self._apply_operations(root, operations)
            
            xml_str = ET.tostring(root, encoding=self.config.encoding)
            
            return ActionResult(
                success=True,
                message="Transformation complete",
                data={"xml": xml_str}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {str(e)}")
    
    def _apply_mappings(self, elem: ET.Element, mappings: Dict[str, str]) -> None:
        """Apply tag renaming mappings."""
        for old_tag, new_tag in mappings.items():
            if elem.tag == old_tag:
                elem.tag = new_tag
        for child in elem:
            self._apply_mappings(child, mappings)
    
    def _apply_operations(self, elem: ET.Element, operations: List[Dict]) -> None:
        """Apply transformation operations."""
        for op in operations:
            op_type = op.get("type", "")
            
            if op_type == "rename":
                new_tag = op.get("new_tag", "")
                if elem.tag == op.get("tag", ""):
                    elem.tag = new_tag
            
            elif op_type == "remove_attributes":
                attrs = op.get("attributes", [])
                for attr in attrs:
                    if attr in elem.attrib:
                        del elem.attrib[attr]
            
            elif op_type == "set_text":
                text = op.get("text", "")
                elem.text = text
            
            elif op_type == "filter_elements":
                tags = op.get("tags", [])
                children_to_remove = [c for c in elem if c.tag in tags]
                for child in children_to_remove:
                    elem.remove(child)
            
            for child in elem:
                self._apply_operations(child, operations)
    
    def _pretty(self, params: Dict[str, Any]) -> ActionResult:
        """Pretty print XML with indentation."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        indent_str = params.get("indent", self.config.indent)
        
        if file_path:
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            with open(file_path, "r", encoding=self.config.encoding) as f:
                content = f.read()
        
        if not content:
            return ActionResult(success=False, message="content or file_path required")
        
        try:
            root = ET.fromstring(content)
            self.config.indent = indent_str
            pretty_xml = self._element_to_string(root)
            
            return ActionResult(
                success=True,
                message="XML pretty printed",
                data={"xml": pretty_xml}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pretty print error: {str(e)}")
    
    def _get_attributes(self, params: Dict[str, Any]) -> ActionResult:
        """Get attributes of an element."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        path = params.get("path", "")
        namespaces = params.get("namespaces", self.config.namespaces)
        
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                tree = ET.parse(file_path)
                root = tree.getroot()
            elif content:
                root = ET.fromstring(content)
            else:
                return ActionResult(success=False, message="content or file_path required")
            
            if path:
                if namespaces:
                    elem = root.find(path, namespaces)
                else:
                    elem = root.find(path)
            else:
                elem = root
            
            if elem is None:
                return ActionResult(success=False, message=f"Element not found at: {path}")
            
            return ActionResult(
                success=True,
                message=f"Attributes for {elem.tag}",
                data={"tag": elem.tag, "attributes": elem.attrib}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Get attributes error: {str(e)}")
    
    def _set_attributes(self, params: Dict[str, Any]) -> ActionResult:
        """Set attributes on an element."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        path = params.get("path", "")
        attributes = params.get("attributes", {})
        namespaces = params.get("namespaces", self.config.namespaces)
        
        if not attributes:
            return ActionResult(success=False, message="attributes dict required")
        
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                tree = ET.parse(file_path)
                root = tree.getroot()
            elif content:
                root = ET.fromstring(content)
            else:
                return ActionResult(success=False, message="content or file_path required")
            
            if path:
                if namespaces:
                    elem = root.find(path, namespaces)
                else:
                    elem = root.find(path)
            else:
                elem = root
            
            if elem is None:
                return ActionResult(success=False, message=f"Element not found at: {path}")
            
            for key, value in attributes.items():
                elem.set(key, str(value))
            
            xml_str = ET.tostring(root, encoding=self.config.encoding)
            
            return ActionResult(
                success=True,
                message=f"Set {len(attributes)} attributes",
                data={"xml": xml_str, "attributes": elem.attrib}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Set attributes error: {str(e)}")
    
    def _add_element(self, params: Dict[str, Any]) -> ActionResult:
        """Add new element to XML."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        parent_path = params.get("parent_path", "")
        tag = params.get("tag", "")
        text = params.get("text", "")
        attributes = params.get("attributes", {})
        namespaces = params.get("namespaces", self.config.namespaces)
        
        if not tag:
            return ActionResult(success=False, message="tag is required")
        
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                tree = ET.parse(file_path)
                root = tree.getroot()
            elif content:
                root = ET.fromstring(content)
            else:
                return ActionResult(success=False, message="content or file_path required")
            
            if parent_path:
                if namespaces:
                    parent = root.find(parent_path, namespaces)
                else:
                    parent = root.find(parent_path)
                if parent is None:
                    return ActionResult(success=False, message=f"Parent not found: {parent_path}")
            else:
                parent = root
            
            new_elem = ET.SubElement(parent, tag)
            new_elem.text = text
            for key, value in attributes.items():
                new_elem.set(key, str(value))
            
            xml_str = ET.tostring(root, encoding=self.config.encoding)
            
            return ActionResult(
                success=True,
                message=f"Added element {tag}",
                data={"xml": xml_str}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Add element error: {str(e)}")
    
    def _remove_element(self, params: Dict[str, Any]) -> ActionResult:
        """Remove element from XML."""
        content = params.get("content", "")
        file_path = params.get("file_path", "")
        path = params.get("path", "")
        namespaces = params.get("namespaces", self.config.namespaces)
        
        if not path:
            return ActionResult(success=False, message="path is required")
        
        try:
            if file_path:
                if not os.path.exists(file_path):
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                tree = ET.parse(file_path)
                root = tree.getroot()
            elif content:
                root = ET.fromstring(content)
            else:
                return ActionResult(success=False, message="content or file_path required")
            
            parent_path = "/".join(path.split("/")[:-1])
            child_tag = path.split("/")[-1]
            
            if namespaces:
                parent = root.find(parent_path, namespaces)
            else:
                parent = root.find(parent_path)
            
            if parent is None:
                return ActionResult(success=False, message=f"Parent not found: {parent_path}")
            
            elem_to_remove = None
            for child in parent:
                if child.tag == child_tag:
                    elem_to_remove = child
                    break
            
            if elem_to_remove is None:
                return ActionResult(success=False, message=f"Element not found: {path}")
            
            parent.remove(elem_to_remove)
            xml_str = ET.tostring(root, encoding=self.config.encoding)
            
            return ActionResult(
                success=True,
                message=f"Removed element {child_tag}",
                data={"xml": xml_str}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Remove element error: {str(e)}")
