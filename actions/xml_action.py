"""XML parsing and manipulation action module for RabAI AutoClick.

Provides XML operations:
- XmlParseAction: Parse XML string/file
- XmlToDictAction: Convert XML to dict
- XmlFromDictAction: Create XML from dict
- XmlXPathAction: Query XML with XPath
- XmlValidateAction: Validate XML against schema
"""

from __future__ import annotations

import sys
import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XmlParseAction(BaseAction):
    """Parse XML string or file."""
    action_type = "xml_parse"
    display_name = "XML解析"
    description = "解析XML"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML parse."""
        xml_str = params.get('xml', '')
        xml_file = params.get('xml_file', None)
        output_var = params.get('output_var', 'xml_tree')

        if not xml_str and not xml_file:
            return ActionResult(success=False, message="xml or xml_file is required")

        try:
            resolved_str = context.resolve_value(xml_str) if context else xml_str
            resolved_file = context.resolve_value(xml_file) if context else xml_file

            if resolved_file:
                tree = ET.parse(resolved_file)
                root = tree.getroot()
            else:
                root = ET.fromstring(resolved_str)

            result = {'tag': root.tag, 'text': root.text, 'attributes': root.attrib}
            if context:
                context.set(output_var, root)
            return ActionResult(success=True, message=f"Parsed XML: <{root.tag}>", data=result)
        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'xml': '', 'xml_file': None, 'output_var': 'xml_tree'}


class XmlToDictAction(BaseAction):
    """Convert XML to dictionary."""
    action_type = "xml_to_dict"
    display_name = "XML转字典"
    description = "将XML转换为字典"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML to dict."""
        xml_str = params.get('xml', '')
        xml_file = params.get('xml_file', None)
        output_var = params.get('output_var', 'xml_dict')

        if not xml_str and not xml_file:
            return ActionResult(success=False, message="xml or xml_file is required")

        try:
            import xmltodict

            resolved_str = context.resolve_value(xml_str) if context else xml_str
            resolved_file = context.resolve_value(xml_file) if context else xml_file

            if resolved_file:
                with open(resolved_file, 'r') as f:
                    data = xmltodict.parse(f.read())
            else:
                data = xmltodict.parse(resolved_str)

            if context:
                context.set(output_var, data)
            return ActionResult(success=True, message="XML converted to dict", data={'keys': list(data.keys()) if isinstance(data, dict) else type(data).__name__})
        except ImportError:
            return ActionResult(success=False, message="xmltodict not installed. Run: pip install xmltodict")
        except Exception as e:
            return ActionResult(success=False, message=f"XML to dict error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'xml': '', 'xml_file': None, 'output_var': 'xml_dict'}


class XmlFromDictAction(BaseAction):
    """Create XML from dictionary."""
    action_type = "xml_from_dict"
    display_name = "字典转XML"
    description = "将字典转换为XML"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute dict to XML."""
        data = params.get('data', {})
        root_tag = params.get('root_tag', 'root')
        output_var = params.get('output_var', 'xml_string')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            import xmltodict

            resolved_data = context.resolve_value(data) if context else data

            xml_str = xmltodict.unparse(resolved_data, full_document=True)
            if root_tag and not xml_str.startswith('<'):
                xml_str = f'<{root_tag}>{xml_str}</{root_tag}>'

            if context:
                context.set(output_var, xml_str)
            return ActionResult(success=True, message=f"Generated XML ({len(xml_str)} chars)", data={'xml': xml_str})
        except ImportError:
            return ActionResult(success=False, message="xmltodict not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Dict to XML error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'root_tag': 'root', 'output_var': 'xml_string'}


class XmlXPathAction(BaseAction):
    """Query XML with XPath."""
    action_type = "xml_xpath"
    display_name = "XML XPath查询"
    description = "XPath查询XML"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XPath query."""
        xml_tree_var = params.get('xml_tree_var', 'xml_tree')
        xpath = params.get('xpath', '')
        output_var = params.get('output_var', 'xpath_result')

        if not xpath:
            return ActionResult(success=False, message="xpath is required")

        try:
            resolved_xpath = context.resolve_value(xpath) if context else xpath

            tree = context.resolve_value(xml_tree_var) if context else None
            if tree is None:
                tree = context.resolve_value(xml_tree_var)

            if hasattr(tree, 'iter'):
                results = tree.findall(resolved_xpath)
            else:
                return ActionResult(success=False, message=f"{xml_tree_var} is not an XML tree")

            results_list = []
            for elem in results:
                results_list.append({
                    'tag': elem.tag,
                    'text': elem.text,
                    'attrib': elem.attrib,
                })

            result_data = {'results': results_list, 'count': len(results_list)}
            if context:
                context.set(output_var, results_list)
            return ActionResult(success=True, message=f"XPath found {len(results_list)} matches", data=result_data)
        except Exception as e:
            return ActionResult(success=False, message=f"XPath error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['xpath']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'xml_tree_var': 'xml_tree', 'output_var': 'xpath_result'}


class XmlValidateAction(BaseAction):
    """Validate XML against schema."""
    action_type = "xml_validate"
    display_name = "XML验证"
    description = "验证XML"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML validate."""
        xml_str = params.get('xml', '')
        xml_file = params.get('xml_file', None)
        schema_file = params.get('schema_file', None)
        output_var = params.get('output_var', 'xml_valid')

        if not schema_file:
            return ActionResult(success=False, message="schema_file is required")

        try:
            import xml.etree.ElementTree as ET

            resolved_schema = context.resolve_value(schema_file) if context else schema_file

            schema_tree = ET.parse(resolved_schema)
            schema = ET.XMLSchema(schema_tree.getroot())

            resolved_str = context.resolve_value(xml_str) if context else xml_str
            resolved_file = context.resolve_value(xml_file) if context else xml_file

            if resolved_file:
                doc = ET.parse(resolved_file)
            else:
                doc = ET.fromstring(resolved_str)

            schema.assertValid(doc)

            result = {'valid': True}
            if context:
                context.set(output_var, True)
            return ActionResult(success=True, message="XML is valid", data=result)
        except ET.ParseError as e:
            result = {'valid': False, 'error': str(e)}
            if context:
                context.set(output_var, False)
            return ActionResult(success=False, message=f"XML is invalid: {str(e)}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"XML validate error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'xml': '', 'xml_file': None, 'schema_file': None, 'output_var': 'xml_valid'}
