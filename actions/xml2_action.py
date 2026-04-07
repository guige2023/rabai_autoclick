"""Xml2 action module for RabAI AutoClick.

Provides additional XML operations:
- XmlParseAction: Parse XML string
- XmlToJsonAction: Convert XML to JSON
- XmlFromJsonAction: Convert JSON to XML
- XmlGetValueAction: Get value by XPath
- XmlSetValueAction: Set value by XPath
"""

import xml.etree.ElementTree as ET
import json
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XmlParseAction(BaseAction):
    """Parse XML string."""
    action_type = "xml2_parse"
    display_name = "XML解析"
    description = "解析XML字符串"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML parse.

        Args:
            context: Execution context.
            params: Dict with xml_str, output_var.

        Returns:
            ActionResult with parsed XML.
        """
        xml_str = params.get('xml_str', '')
        output_var = params.get('output_var', 'xml_parsed')

        valid, msg = self.validate_type(xml_str, str, 'xml_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(xml_str)
            root = ET.fromstring(resolved)

            def elem_to_dict(elem):
                result = {}
                if elem.attrib:
                    result['@attributes'] = elem.attrib
                if elem.text and elem.text.strip():
                    result['#text'] = elem.text.strip()
                for child in elem:
                    child_dict = elem_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict)
                    else:
                        result[child.tag] = child_dict
                return result

            result = {root.tag: elem_to_dict(root)}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"XML解析完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_parsed'}


class XmlToJsonAction(BaseAction):
    """Convert XML to JSON."""
    action_type = "xml2_to_json"
    display_name = "XML转JSON"
    description = "将XML转换为JSON"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML to JSON.

        Args:
            context: Execution context.
            params: Dict with xml_str, output_var.

        Returns:
            ActionResult with JSON string.
        """
        xml_str = params.get('xml_str', '')
        output_var = params.get('output_var', 'json_result')

        valid, msg = self.validate_type(xml_str, str, 'xml_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(xml_str)
            root = ET.fromstring(resolved)

            def elem_to_dict(elem):
                result = {}
                if elem.attrib:
                    result['@attributes'] = elem.attrib
                if elem.text and elem.text.strip():
                    result['#text'] = elem.text.strip()
                for child in elem:
                    child_dict = elem_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict)
                    else:
                        result[child.tag] = child_dict
                return result

            result = {root.tag: elem_to_dict(root)}
            json_str = json.dumps(result, ensure_ascii=False, indent=2)
            context.set(output_var, json_str)

            return ActionResult(
                success=True,
                message=f"XML转JSON完成",
                data={
                    'result': json_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML转JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_result'}


class XmlFromJsonAction(BaseAction):
    """Convert JSON to XML."""
    action_type = "xml2_from_json"
    display_name = "JSON转XML"
    description = "将JSON转换为XML"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON to XML.

        Args:
            context: Execution context.
            params: Dict with json_str, root_element, output_var.

        Returns:
            ActionResult with XML string.
        """
        json_str = params.get('json_str', '')
        root_element = params.get('root_element', 'root')
        output_var = params.get('output_var', 'xml_result')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_json = context.resolve_value(json_str)
            resolved_root = context.resolve_value(root_element)

            data = json.loads(resolved_json)

            def dict_to_xml(tag, dict_data):
                elem = ET.Element(tag)
                if isinstance(dict_data, dict):
                    for key, val in dict_data.items():
                        if key == '@attributes':
                            for attr_key, attr_val in val.items():
                                elem.set(attr_key, str(attr_val))
                        elif key == '#text':
                            elem.text = str(val)
                        elif isinstance(val, list):
                            for item in val:
                                elem.append(dict_to_xml(key, item))
                        else:
                            elem.append(dict_to_xml(key, val))
                else:
                    elem.text = str(dict_data)
                return elem

            root = dict_to_xml(resolved_root, data)
            result = ET.tostring(root, encoding='unicode')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON转XML完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON转XML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'root_element': 'root', 'output_var': 'xml_result'}


class XmlGetValueAction(BaseAction):
    """Get value by XPath."""
    action_type = "xml2_get_value"
    display_name = "XML获取值"
    description = "通过XPath获取XML中的值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML get value.

        Args:
            context: Execution context.
            params: Dict with xml_str, xpath, output_var.

        Returns:
            ActionResult with value.
        """
        xml_str = params.get('xml_str', '')
        xpath = params.get('xpath', '')
        output_var = params.get('output_var', 'xml_value')

        valid, msg = self.validate_type(xml_str, str, 'xml_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_xml = context.resolve_value(xml_str)
            resolved_xpath = context.resolve_value(xpath)

            root = ET.fromstring(resolved_xml)
            elements = root.findall(resolved_xpath)

            if elements is None:
                return ActionResult(
                    success=False,
                    message=f"XML获取值失败: XPath无效"
                )

            result = []
            for elem in elements:
                if elem.text:
                    result.append(elem.text.strip())
                elif len(elem) > 0:
                    result.append(ET.tostring(elem, encoding='unicode'))

            context.set(output_var, result if len(result) > 1 else (result[0] if result else None))

            return ActionResult(
                success=True,
                message=f"XML获取值完成",
                data={
                    'xpath': resolved_xpath,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML获取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_str', 'xpath']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_value'}


class XmlSetValueAction(BaseAction):
    """Set value by XPath."""
    action_type = "xml2_set_value"
    display_name = "XML设置值"
    description = "通过XPath设置XML中的值"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML set value.

        Args:
            context: Execution context.
            params: Dict with xml_str, xpath, value, output_var.

        Returns:
            ActionResult with updated XML.
        """
        xml_str = params.get('xml_str', '')
        xpath = params.get('xpath', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'xml_updated')

        valid, msg = self.validate_type(xml_str, str, 'xml_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_xml = context.resolve_value(xml_str)
            resolved_xpath = context.resolve_value(xpath)
            resolved_value = context.resolve_value(value)

            root = ET.fromstring(resolved_xml)
            elements = root.findall(resolved_xpath)

            if not elements:
                new_elem = ET.SubElement(root, resolved_xpath.split('/')[-1])
                new_elem.text = resolved_value
            else:
                for elem in elements:
                    elem.text = resolved_value

            result = ET.tostring(root, encoding='unicode')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"XML设置值完成",
                data={
                    'xpath': resolved_xpath,
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML设置值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_str', 'xpath', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_updated'}