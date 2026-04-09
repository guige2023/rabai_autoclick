"""Data XML action module for RabAI AutoClick.

Provides XML parsing, generation, and transformation for
handling XML-formatted data and configuration files.
"""

import xml.etree.ElementTree as ET
import json
from typing import Any, Dict, List, Optional, Union

from core.base_action import BaseAction, ActionResult


class XmlParserAction(BaseAction):
    """Parse XML strings into Python dictionaries or ElementTree objects.
    
    Supports namespace handling, attribute extraction, and
    conversion to JSON for easier processing.
    """
    action_type = "xml_parser"
    display_name = "XML解析"
    description = "将XML字符串解析为Python对象"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse XML data.
        
        Args:
            context: Execution context.
            params: Dict with keys: xml_data, namespaces, to_json,
                   preserve_attributes.
        
        Returns:
            ActionResult with parsed XML data.
        """
        xml_data = params.get("xml_data", "")
        namespaces = params.get("namespaces", {})
        to_json = params.get("to_json", False)
        preserve_attributes = params.get("preserve_attributes", True)
        
        if not xml_data:
            return ActionResult(success=False, message="XML data is required")
        
        try:
            root = ET.fromstring(xml_data)
            
            if preserve_attributes:
                result = self._element_to_dict(root, namespaces)
            else:
                result = self._element_to_dict_simple(root)
            
            if to_json:
                result = self._dict_to_json_friendly(result)
            
            return ActionResult(
                success=True,
                message=f"Parsed XML: {root.tag}",
                data={
                    "root": result,
                    "tag": root.tag,
                    "text": root.text.strip() if root.text else None
                }
            )
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML parsing failed: {e}")
    
    def _element_to_dict(self, element: ET.Element, namespaces: Dict) -> Dict:
        result = {}
        
        if element.attrib:
            result["@attributes"] = element.attrib
        
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result["#text"] = element.text.strip()
        
        children = {}
        for child in element:
            tag = child.tag
            if "}" in tag:
                tag = tag.split("}")[1]
            
            child_data = self._element_to_dict(child, namespaces)
            
            if tag in children:
                if not isinstance(children[tag], list):
                    children[tag] = [children[tag]]
                children[tag].append(child_data)
            else:
                children[tag] = child_data
        
        result.update(children)
        
        return result
    
    def _element_to_dict_simple(self, element: ET.Element) -> Any:
        if len(element) == 0:
            return element.text.strip() if element.text else ""
        
        children = {}
        for child in element:
            tag = child.tag
            if "}" in tag:
                tag = tag.split("}")[1]
            
            child_data = self._element_to_dict_simple(child)
            
            if tag in children:
                if not isinstance(children[tag], list):
                    children[tag] = [children[tag]]
                children[tag].append(child_data)
            else:
                children[tag] = child_data
        
        return children
    
    def _dict_to_json_friendly(self, data: Any) -> Any:
        if isinstance(data, dict):
            return {k: self._dict_to_json_friendly(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._dict_to_json_friendly(item) for item in data]
        elif isinstance(data, (str, int, float, bool, type(None))):
            return data
        else:
            return str(data)


class XmlGeneratorAction(BaseAction):
    """Generate XML from Python dictionaries or structured data.
    
    Supports custom root elements, attribute handling, namespaces,
    and pretty-print formatting.
    """
    action_type = "xml_generator"
    display_name = "XML生成"
    description = "从Python对象生成XML字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate XML from data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, root_tag, namespaces,
                   pretty_print, indent.
        
        Returns:
            ActionResult with generated XML string.
        """
        data = params.get("data")
        root_tag = params.get("root_tag", "root")
        namespaces = params.get("namespaces", {})
        pretty_print = params.get("pretty_print", True)
        indent = params.get("indent", "  ")
        
        if data is None:
            return ActionResult(success=False, message="Data is required")
        
        try:
            root = self._dict_to_element(data, root_tag, namespaces)
            
            for prefix, uri in namespaces.items():
                if prefix:
                    ET.register_namespace(prefix, uri)
            
            if pretty_print:
                self._indent_element(root, indent)
            
            xml_bytes = ET.tostring(root, encoding="unicode")
            xml_string = f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_bytes}'
            
            return ActionResult(
                success=True,
                message=f"Generated XML: {len(xml_string)} bytes",
                data={
                    "xml": xml_string,
                    "size": len(xml_string)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML generation failed: {e}")
    
    def _dict_to_element(self, data: Any, tag: str, namespaces: Dict) -> ET.Element:
        if isinstance(data, dict):
            attrib = {}
            children = {}
            
            if "@attributes" in data:
                attrib = data.pop("@attributes")
            
            for key, value in list(data.items()):
                if key.startswith("@"):
                    attrib[key[1:]] = data.pop(key)
            
            element = ET.Element(tag, attrib)
            
            for key, value in data.items():
                if key == "#text":
                    element.text = str(value)
                else:
                    child = self._dict_to_element(value, key, namespaces)
                    element.append(child)
            
            return element
        elif isinstance(data, list):
            element = ET.Element(tag)
            for item in data:
                child = self._dict_to_element(item, "item", namespaces)
                element.append(child)
            return element
        else:
            element = ET.Element(tag)
            element.text = str(data)
            return element
    
    def _indent_element(self, element: ET.Element, indent: str, level: int = 0) -> None:
        i = "\n" + indent * level
        if len(element):
            if not element.text or not element.text.strip():
                element.text = i + indent
            for child in element:
                self._indent_element(child, indent, level + 1)
                if not child.tail or not child.tail.strip():
                    child.tail = i + indent
            if not child.tail or not child.tail.strip():
                child.tail = i


class XmlTransformAction(BaseAction):
    """Transform XML using XPath queries and filtering.
    
    Supports XPath selection, element filtering, attribute
    extraction, and XML-to-JSON conversion.
    """
    action_type = "xml_transform"
    display_name = "XML转换"
    description = "使用XPath查询转换XML"
    VALID_MODES = ["xpath", "filter", "convert"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transform XML data.
        
        Args:
            context: Execution context.
            params: Dict with keys: xml_data, mode, xpath, namespaces,
                   filter_tag, filter_attr.
        
        Returns:
            ActionResult with transformed data.
        """
        xml_data = params.get("xml_data", "")
        mode = params.get("mode", "xpath")
        xpath = params.get("xpath", ".//*")
        namespaces = params.get("namespaces", {})
        filter_tag = params.get("filter_tag")
        filter_attr = params.get("filter_attr")
        to_json = params.get("to_json", True)
        
        if not xml_data:
            return ActionResult(success=False, message="XML data is required")
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            root = ET.fromstring(xml_data)
            
            if mode == "xpath":
                if namespaces:
                    results = root.findall(xpath, namespaces)
                else:
                    results = root.findall(xpath)
                
                parsed = [self._element_to_dict_simple(r) for r in results]
                
                return ActionResult(
                    success=True,
                    message=f"XPath returned {len(parsed)} results",
                    data={
                        "results": parsed,
                        "count": len(parsed)
                    }
                )
            elif mode == "filter":
                filtered = self._filter_elements(root, filter_tag, filter_attr)
                
                return ActionResult(
                    success=True,
                    message=f"Filtered to {len(filtered)} elements",
                    data={
                        "elements": [self._element_to_dict_simple(e) for e in filtered],
                        "count": len(filtered)
                    }
                )
            elif mode == "convert":
                result = self._element_to_dict_simple(root)
                if to_json:
                    import json
                    result = json.loads(json.dumps(result, default=str))
                
                return ActionResult(
                    success=True,
                    message="XML converted to dict",
                    data={"result": result}
                )
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML transform failed: {e}")
    
    def _filter_elements(
        self,
        element: ET.Element,
        filter_tag: Optional[str],
        filter_attr: Optional[Dict]
    ) -> List[ET.Element]:
        results = []
        
        if filter_tag and element.tag == filter_tag:
            if filter_attr:
                if all(element.attrib.get(k) == v for k, v in filter_attr.items()):
                    results.append(element)
            else:
                results.append(element)
        
        for child in element:
            results.extend(self._filter_elements(child, filter_tag, filter_attr))
        
        return results
    
    def _element_to_dict_simple(self, element: ET.Element) -> Any:
        if len(element) == 0:
            return element.text.strip() if element.text else ""
        
        children = {}
        for child in element:
            tag = child.tag
            if "}" in tag:
                tag = tag.split("}")[1]
            
            child_data = self._element_to_dict_simple(child)
            
            if tag in children:
                if not isinstance(children[tag], list):
                    children[tag] = [children[tag]]
                children[tag].append(child_data)
            else:
                children[tag] = child_data
        
        return children
