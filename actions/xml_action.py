"""XML action module for RabAI AutoClick.

Provides XML parsing, manipulation, and generation with
XPath queries, element operations, and schema validation.
"""

import sys
import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Union, Callable
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class XMLAction(BaseAction):
    """Parse, manipulate, and generate XML.
    
    Supports parsing, XPath queries, element manipulation,
    XML generation, and namespace handling.
    """
    action_type = "xml"
    display_name = "XML处理"
    description = "XML解析、查询和生成，支持XPath"

    NAMESPACES = {
        'xml': 'http://www.w3.org/XML/1998/namespace',
    }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform XML operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (parse/validate/xpath/create/transform)
                - xml_string: str, XML content (for parse/validate/xpath)
                - xpath: str, XPath expression
                - namespaces: dict, namespace prefix mappings
                - root_tag: str (for create)
                - elements: list of element dicts (for create)
                - save_to_var: str
        
        Returns:
            ActionResult with XML operation result.
        """
        operation = params.get('operation', 'parse')
        xml_string = params.get('xml_string', '')
        save_to_var = params.get('save_to_var', None)

        if operation == 'parse':
            return self._parse_xml(xml_string, params, save_to_var)
        elif operation == 'validate':
            return self._validate_xml(xml_string, params)
        elif operation == 'xpath':
            return self._xpath_query(xml_string, params, save_to_var)
        elif operation == 'create':
            return self._create_xml(params, save_to_var)
        elif operation == 'transform':
            return self._transform_xml(xml_string, params, save_to_var)
        elif operation == 'query':
            return self._query_elements(xml_string, params, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _parse_xml(
        self, xml_string: str, params: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Parse XML string into dict/tree."""
        if not xml_string:
            return ActionResult(success=False, message="xml_string is required")

        namespaces = params.get('namespaces', {})
        parse_as_dict = params.get('parse_as_dict', True)

        try:
            # Register namespaces
            for prefix, uri in namespaces.items():
                ET.register_namespace(prefix, uri)

            root = ET.fromstring(xml_string)

            if parse_as_dict:
                result = self._element_to_dict(root)
            else:
                result = root

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result

            return ActionResult(
                success=True,
                message=f"Parsed XML with root tag: {root.tag}",
                data=result
            )
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML error: {e}")

    def _validate_xml(self, xml_string: str, params: Dict) -> ActionResult:
        """Validate XML syntax."""
        if not xml_string:
            return ActionResult(success=False, message="xml_string is required")

        try:
            root = ET.fromstring(xml_string)
            return ActionResult(
                success=True,
                message=f"XML is valid (root: {root.tag})",
                data={'valid': True, 'root_tag': root.tag}
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML is invalid: {e}",
                data={'valid': False, 'error': str(e)}
            )

    def _xpath_query(
        self, xml_string: str, params: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Execute XPath query on XML."""
        if not xml_string:
            return ActionResult(success=False, message="xml_string is required")

        xpath = params.get('xpath', '')
        namespaces = params.get('namespaces', {})

        if not xpath:
            return ActionResult(success=False, message="xpath is required")

        try:
            root = ET.fromstring(xml_string)

            # Add namespaces to root for XPath
            for prefix, uri in namespaces.items():
                root.set(f'xmlns:{prefix}', uri)

            results = root.findall(xpath, namespaces)

            result_list = []
            for elem in results:
                if isinstance(elem, ET.Element):
                    result_list.append(self._element_to_dict(elem))
                else:
                    result_list.append(str(elem))

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result_list

            return ActionResult(
                success=True,
                message=f"XPath found {len(result_list)} result(s)",
                data=result_list
            )
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"XPath error: {e}")

    def _create_xml(self, params: Dict, save_to_var: Optional[str]) -> ActionResult:
        """Create XML from structure."""
        root_tag = params.get('root_tag', 'root')
        elements = params.get('elements', [])
        namespaces = params.get('namespaces', {})
        indent = params.get('indent', True)

        try:
            # Register namespaces
            for prefix, uri in namespaces.items():
                ET.register_namespace(prefix, uri)

            root = ET.Element(root_tag)

            for elem_spec in elements:
                self._add_element_to_tree(root, elem_spec)

            # Serialize
            if indent:
                self._indent_element(root)
            xml_string = ET.tostring(root, encoding='unicode')

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = xml_string

            return ActionResult(
                success=True,
                message=f"Created XML with root: {root_tag}",
                data=xml_string
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML creation error: {e}")

    def _transform_xml(
        self, xml_string: str, params: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Transform XML using XSLT-like operations."""
        if not xml_string:
            return ActionResult(success=False, message="xml_string is required")

        transform_type = params.get('transform_type', 'prettify')
        indent_size = params.get('indent_size', 2)

        try:
            root = ET.fromstring(xml_string)

            if transform_type == 'prettify':
                self._indent_element(root, level=0, indent_size=indent_size)
                xml_out = ET.tostring(root, encoding='unicode')
            elif transform_type == 'remove_empty':
                self._remove_empty_elements(root)
                xml_out = ET.tostring(root, encoding='unicode')
            elif transform_type == 'flatten':
                xml_out = self._flatten_to_string(root)
            else:
                xml_out = ET.tostring(root, encoding='unicode')

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = xml_out

            return ActionResult(
                success=True,
                message=f"Transformed XML: {transform_type}",
                data=xml_out
            )
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {e}")

    def _query_elements(
        self, xml_string: str, params: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Query elements by tag, attribute, or value."""
        if not xml_string:
            return ActionResult(success=False, message="xml_string is required")

        tag = params.get('tag', None)
        attr_key = params.get('attr_key', None)
        attr_value = params.get('attr_value', None)
        value_contains = params.get('value_contains', None)

        try:
            root = ET.fromstring(xml_string)
            results = []

            if tag:
                for elem in root.iter(tag):
                    if attr_key:
                        if elem.get(attr_key) != attr_value:
                            continue
                    if value_contains:
                        if value_contains not in (elem.text or ''):
                            continue
                    results.append(self._element_to_dict(elem))

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = results

            return ActionResult(
                success=True,
                message=f"Found {len(results)} element(s)",
                data=results
            )
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {e}")

    def _element_to_dict(self, elem: ET.Element) -> Dict[str, Any]:
        """Convert ElementTree element to dict."""
        result = {'_tag': elem.tag}
        if elem.attrib:
            result['_attributes'] = elem.attrib
        if elem.text and elem.text.strip():
            if len(elem) == 0:
                return {elem.tag: elem.text.strip()}
            result['_text'] = elem.text.strip()
        for child in elem:
            child_data = self._element_to_dict(child)
            child_tag = list(child_data.keys())[0]
            child_val = child_data[child_tag]
            if child_tag in result:
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_val)
            else:
                result[child_tag] = child_val
        return result

    def _add_element_to_tree(self, parent: ET.Element, spec: Dict) -> None:
        """Add element to ElementTree from spec dict."""
        tag = spec.get('tag', 'element')
        text = spec.get('text', '')
        attrib = spec.get('attributes', {})
        children = spec.get('children', [])

        elem = ET.SubElement(parent, tag, **attrib)
        if text:
            elem.text = str(text)
        for child_spec in children:
            self._add_element_to_tree(elem, child_spec)

    def _indent_element(
        self, elem: ET.Element, level: int = 0, indent_size: int = 2
    ) -> None:
        """Pretty-print indent element."""
        indent = "\n" + " " * level * indent_size
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = indent + " " * indent_size
            for child in elem:
                self._indent_element(child, level + 1, indent_size)
            if not child.tail or not child.tail.strip():
                child.tail = indent

    def _remove_empty_elements(self, elem: ET.Element) -> None:
        """Remove elements with no content."""
        to_remove = []
        for child in elem:
            self._remove_empty_elements(child)
            if len(child) == 0 and (not child.text or not child.text.strip()):
                to_remove.append(child)
        for child in to_remove:
            elem.remove(child)

    def _flatten_to_string(self, elem: ET.Element) -> str:
        """Flatten element to string representation."""
        parts = []
        parts.append(f"<{elem.tag}")
        if elem.attrib:
            for k, v in elem.attrib.items():
                parts.append(f' {k}="{v}"')
        if elem.text and elem.text.strip():
            parts.append(f">{elem.text.strip()}</{elem.tag}>")
        else:
            parts.append(">")
            for child in elem:
                parts.append(self._flatten_to_string(child))
            parts.append(f"</{elem.tag}>")
        return "".join(parts)

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'xml_string': '',
            'xpath': '',
            'namespaces': {},
            'root_tag': 'root',
            'elements': [],
            'parse_as_dict': True,
            'transform_type': 'prettify',
            'indent_size': 2,
            'tag': None,
            'attr_key': None,
            'attr_value': None,
            'value_contains': None,
            'save_to_var': None,
        }
