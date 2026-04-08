"""XML action module for RabAI AutoClick.

Provides XML parsing, generation, transformation, and validation
operations for workflow automation.
"""

import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class XmlParseAction(BaseAction):
    """Parse XML string or file into Python objects.
    
    Converts XML to dict, list, or ElementTree objects with
    namespace handling and attribute extraction.
    """
    action_type = "xml_parse"
    display_name = "解析XML"
    description = "解析XML字符串或文件为Python对象"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse XML data.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - source: XML string or file path (required)
                - source_type: 'file' or 'string' (default auto)
                - output_format: 'dict', 'element', or 'string' (default 'dict')
                - include_attributes: Include XML attributes (default True)
                - include_text: Include text content (default True)
                - namespaces: Dict of namespace prefix -> URI (optional)
                - encoding: File encoding (default utf-8)
        
        Returns:
            ActionResult with parsed XML data.
        """
        source = params.get('source', '')
        if not source:
            return ActionResult(success=False, message="source is required")
        
        source_type = params.get('source_type', 'auto')
        output_format = params.get('output_format', 'dict')
        include_attributes = params.get('include_attributes', True)
        include_text = params.get('include_text', True)
        namespaces = params.get('namespaces', {})
        encoding = params.get('encoding', 'utf-8')
        
        if source_type == 'auto':
            source_type = 'file' if os.path.exists(source) else 'string'
        
        try:
            if source_type == 'file':
                tree = ET.parse(source)
                root = tree.getroot()
            else:
                root = ET.fromstring(source)
            
            if output_format == 'element':
                return ActionResult(
                    success=True,
                    message="Parsed XML element",
                    data={'element': root}
                )
            
            if output_format == 'string':
                rough = ET.tostring(root, encoding='unicode')
                formatted = minidom.parseString(rough).toprettyxml(indent='  ')
                return ActionResult(
                    success=True,
                    message="XML parsed and formatted",
                    data={'xml': formatted}
                )
            
            # Dict format
            def element_to_dict(element: ET.Element) -> Dict[str, Any]:
                """Recursively convert element to dict."""
                result: Dict[str, Any] = {}
                
                # Tag with namespace
                tag = element.tag
                if '}' in tag:
                    ns_uri, local_tag = tag.split('}', 1)
                    result['_tag'] = local_tag
                    result['_namespace'] = ns_uri
                else:
                    result['_tag'] = tag
                
                # Attributes
                if include_attributes and element.attrib:
                    result['_attributes'] = dict(element.attrib)
                
                # Text content
                if include_text:
                    text = element.text
                    if text and text.strip():
                        result['_text'] = text.strip()
                
                # Children
                children: Dict[str, List] = {}
                for child in element:
                    child_dict = element_to_dict(child)
                    child_tag = child_dict.pop('_tag', child.tag)
                    
                    if child_tag not in children:
                        children[child_tag] = []
                    children[child_tag].append(child_dict)
                
                for tag, child_list in children.items():
                    if len(child_list) == 1:
                        result[tag] = child_list[0]
                    else:
                        result[tag] = child_list
                
                return result
            
            result_dict = element_to_dict(root)
            
            return ActionResult(
                success=True,
                message="XML parsed to dict",
                data={'xml': result_dict, 'root_tag': root.tag}
            )
            
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {source}")
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML error: {e}")


class XmlBuildAction(BaseAction):
    """Build XML from Python dict or raw data.
    
    Constructs XML documents from structured data with
    namespace support and pretty formatting.
    """
    action_type = "xml_build"
    display_name = "构建XML"
    description = "从Python字典构建XML文档"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build XML document.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - root_tag: Root element tag name (required)
                - data: Dict or list data for XML content (required)
                - namespaces: Dict of prefix -> URI for namespaces (optional)
                - root_attributes: Dict of attributes for root element (optional)
                - pretty: Pretty print output (default True)
                - encoding: XML encoding declaration (default utf-8)
                - standalone: XML standalone declaration (optional)
        
        Returns:
            ActionResult with generated XML string.
        """
        root_tag = params.get('root_tag', '')
        data = params.get('data', {})
        namespaces = params.get('namespaces', {})
        root_attributes = params.get('root_attributes', {})
        pretty = params.get('pretty', True)
        encoding = params.get('encoding', 'utf-8')
        standalone = params.get('standalone', None)
        
        if not root_tag:
            return ActionResult(success=False, message="root_tag is required")
        
        try:
            # Register namespaces
            for prefix, uri in namespaces.items():
                ET.register_namespace(prefix, uri)
            
            # Build root
            root = ET.Element(root_tag, **root_attributes)
            
            def add_data(parent: ET.Element, content: Any) -> None:
                """Recursively add data to XML element."""
                if isinstance(content, dict):
                    for key, value in content.items():
                        if key.startswith('_'):
                            continue
                        if isinstance(value, dict):
                            child = ET.SubElement(parent, key)
                            add_data(child, value)
                        elif isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict):
                                    child = ET.SubElement(parent, key)
                                    add_data(child, item)
                                else:
                                    child = ET.SubElement(parent, key)
                                    child.text = str(item)
                        else:
                            child = ET.SubElement(parent, key)
                            child.text = str(value) if value is not None else ''
                else:
                    parent.text = str(content) if content is not None else ''
            
            add_data(root, data)
            
            # Generate string
            if pretty:
                rough = ET.tostring(root, encoding='unicode')
                dom = minidom.parseString(rough)
                result = dom.toprettyxml(indent='  ', encoding=encoding)
                if isinstance(result, bytes):
                    result = result.decode(encoding)
            else:
                result = ET.tostring(root, encoding=encoding)
                if isinstance(result, bytes):
                    result = result.decode(encoding)
            
            return ActionResult(
                success=True,
                message=f"Built XML with root <{root_tag}>",
                data={'xml': result.strip(), 'root': root_tag}
            )
            
        except Exception as e:
            return ActionResult(success=False, message=f"XML build error: {e}")


class XmlXPathAction(BaseAction):
    """Query XML using XPath expressions.
    
    Extracts specific elements and values from XML documents
    using XPath with namespace support.
    """
    action_type = "xml_xpath"
    display_name = "XPath查询"
    description = "使用XPath表达式查询XML元素"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Query XML with XPath.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - source: XML string or file path (required)
                - xpath: XPath expression (required)
                - namespaces: Dict of prefix -> URI for namespaces (optional)
                - output: 'text', 'all', or 'count' (default 'all')
        
        Returns:
            ActionResult with matched elements.
        """
        source = params.get('source', '')
        xpath = params.get('xpath', '')
        namespaces = params.get('namespaces', {})
        output = params.get('output', 'all')
        
        if not source:
            return ActionResult(success=False, message="source is required")
        if not xpath:
            return ActionResult(success=False, message="xpath is required")
        
        try:
            if os.path.exists(source):
                tree = ET.parse(source)
                root = tree.getroot()
            else:
                root = ET.fromstring(source)
            
            # Apply namespaces
            if namespaces:
                ns_map = {'ns': uri for uri in namespaces.values()}
                xpath_with_ns = xpath
                for prefix, uri in namespaces.items():
                    xpath_with_ns = xpath.replace(f"{prefix}:", f"{{{uri}}}")
            else:
                ns_map = None
            
            results = root.findall(xpath, namespaces=ns_map or {})
            
            if output == 'count':
                return ActionResult(
                    success=True,
                    message=f"Found {len(results)} matches",
                    data={'count': len(results)}
                )
            
            if output == 'text':
                texts = [elem.text.strip() if elem.text and elem.text.strip() else '' for elem in results]
                return ActionResult(
                    success=True,
                    message=f"Extracted {len(texts)} text values",
                    data={'texts': texts}
                )
            
            # 'all' - return element info
            items = []
            for elem in results:
                items.append({
                    'tag': elem.tag,
                    'text': elem.text.strip() if elem.text else '',
                    'attributes': dict(elem.attrib),
                    'tail': elem.tail.strip() if elem.tail else ''
                })
            
            return ActionResult(
                success=True,
                message=f"Found {len(items)} matches",
                data={'matches': items, 'count': len(items)}
            )
            
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"XPath error: {e}")


class XmlValidateAction(BaseAction):
    """Validate XML against XSD schema or DTD.
    
    Verifies XML structure and content against schema definitions
    with detailed error reporting.
    """
    action_type = "xml_validate"
    display_name = "验证XML"
    description = "使用XSD或DTD验证XML结构"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate XML against schema.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys:
                - xml_source: XML string or file path (required)
                - schema_source: XSD file path or schema string (required)
                - schema_type: 'xsd' or 'dtd' (default 'xsd')
        
        Returns:
            ActionResult with validation status.
        """
        xml_source = params.get('xml_source', '')
        schema_source = params.get('schema_source', '')
        schema_type = params.get('schema_type', 'xsd').lower()
        
        if not xml_source:
            return ActionResult(success=False, message="xml_source is required")
        if not schema_source:
            return ActionResult(success=False, message="schema_source is required")
        
        try:
            import xmlschema
            
            # Load schema
            if os.path.exists(schema_source):
                schema = xmlschema.XMLSchema(schema_source)
            else:
                schema = xmlschema.XMLSchema.fromstring(schema_source.encode())
            
            # Load XML
            if os.path.exists(xml_source):
                if schema_type == 'dtd':
                    tree = ET.parse(xml_source)
                    is_valid = schema.is_valid(tree.getroot())
                else:
                    is_valid = schema.is_valid(xml_source)
                xml_tree = ET.parse(xml_source)
                xml_root = xml_tree.getroot()
            else:
                xml_root = ET.fromstring(xml_source)
                is_valid = schema.is_valid(xml_root)
            
            if is_valid:
                return ActionResult(
                    success=True,
                    message="XML is valid",
                    data={'valid': True}
                )
            else:
                errors = schema.validate(xml_root, lazy=True)
                error_msgs = [str(e) for e in schema.validation_errors]
                return ActionResult(
                    success=False,
                    message="XML validation failed",
                    data={'valid': False, 'errors': error_msgs}
                )
                
        except ImportError:
            return ActionResult(
                success=False,
                message="xmlschema library required for validation: pip install xmlschema"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Validation error: {e}",
                data={'error': str(e)}
            )
