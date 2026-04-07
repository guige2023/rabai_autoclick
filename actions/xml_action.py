"""XML action module for RabAI AutoClick.

Provides XML operations:
- XmlParseAction: Parse XML string
- XmlToDictAction: Convert XML to dict
- XmlFromDictAction: Create XML from dict
- XmlGetValueAction: Get value from XML
"""

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XmlParseAction(BaseAction):
    """Parse XML string."""
    action_type = "xml_parse"
    display_name = "解析XML"
    description = "解析XML字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML parsing.

        Args:
            context: Execution context.
            params: Dict with xml_string, output_var.

        Returns:
            ActionResult with parsed XML.
        """
        xml_string = params.get('xml_string', '')
        output_var = params.get('output_var', 'xml_result')

        valid, msg = self.validate_type(xml_string, str, 'xml_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(xml_string)
            root = ET.fromstring(resolved)

            result = {
                'tag': root.tag,
                'text': root.text,
                'attrib': root.attrib,
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"XML解析完成: {root.tag}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析XML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_result'}


class XmlToDictAction(BaseAction):
    """Convert XML to dict."""
    action_type = "xml_to_dict"
    display_name = "XML转字典"
    description = "将XML转换为字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML to dict conversion.

        Args:
            context: Execution context.
            params: Dict with xml_string, output_var.

        Returns:
            ActionResult with dict representation.
        """
        xml_string = params.get('xml_string', '')
        output_var = params.get('output_var', 'xml_dict')

        valid, msg = self.validate_type(xml_string, str, 'xml_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(xml_string)

            def etree_to_dict(node):
                result = {}
                if node.attrib:
                    result['@attributes'] = node.attrib
                if node.text and node.text.strip():
                    result['#text'] = node.text.strip()
                for child in node:
                    child_dict = etree_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict[child.tag])
                    else:
                        result[child.tag] = child_dict[child.tag]
                return {node.tag: result}

            root = ET.fromstring(resolved)
            result = etree_to_dict(root)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="XML转字典完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML转字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_dict'}


class XmlFromDictAction(BaseAction):
    """Create XML from dict."""
    action_type = "xml_from_dict"
    display_name = "字典转XML"
    description = "将字典转换为XML"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict to XML conversion.

        Args:
            context: Execution context.
            params: Dict with data, root_tag, output_var.

        Returns:
            ActionResult with XML string.
        """
        data = params.get('data', {})
        root_tag = params.get('root_tag', 'root')
        output_var = params.get('output_var', 'xml_string')

        valid, msg = self.validate_type(data, dict, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(root_tag, str, 'root_tag')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_root = context.resolve_value(root_tag)

            def dict_to_etree(tag, d):
                element = ET.Element(tag)
                if isinstance(d, dict):
                    if '@attributes' in d:
                        element.attrib.update(d['@attributes'])
                    for key, value in d.items():
                        if key == '@attributes':
                            continue
                        elif key == '#text':
                            element.text = str(value)
                        elif isinstance(value, list):
                            for item in value:
                                element.append(dict_to_etree(key, item))
                        else:
                            element.append(dict_to_etree(key, value))
                else:
                    element.text = str(d)
                return element

            root = dict_to_etree(resolved_root, resolved_data)
            result = ET.tostring(root, encoding='unicode')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="字典转XML完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典转XML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'root_tag']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_string'}


class XmlGetValueAction(BaseAction):
    """Get value from XML."""
    action_type = "xml_get_value"
    display_name = "获取XML值"
    description = "从XML中获取指定路径的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML value extraction.

        Args:
            context: Execution context.
            params: Dict with xml_string, path, output_var.

        Returns:
            ActionResult with extracted value.
        """
        xml_string = params.get('xml_string', '')
        path = params.get('path', '')
        output_var = params.get('output_var', 'xml_value')

        valid, msg = self.validate_type(xml_string, str, 'xml_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_xml = context.resolve_value(xml_string)
            resolved_path = context.resolve_value(path)

            root = ET.fromstring(resolved_xml)

            # Parse path like "root/child/tag"
            parts = resolved_path.split('/')
            current = root
            for part in parts:
                found = False
                for elem in current:
                    if elem.tag == part:
                        current = elem
                        found = True
                        break
                if not found:
                    return ActionResult(
                        success=False,
                        message=f"路径不存在: {resolved_path}"
                    )

            result = current.text if current.text else str(current.attrib)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取XML值: {result}",
                data={
                    'result': result,
                    'path': resolved_path,
                    'output_var': output_var
                }
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取XML值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_string', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_value'}