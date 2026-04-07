"""XML action module for RabAI AutoClick.

Provides XML processing operations:
- XmlParseAction: Parse XML string/file
- XmlToDictAction: Convert XML to dictionary
- XmlFromDictAction: Create XML from dictionary
- XmlValidateAction: Validate XML schema
- XmlXPathAction: Extract data using XPath
- XmlPrettyAction: Pretty print XML
- XmlMinifyAction: Minify XML
"""

import xml.etree.ElementTree as ET
import json
from typing import Any, Dict, List, Optional, Union

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XmlParseAction(BaseAction):
    """Parse XML string/file."""
    action_type = "xml_parse"
    display_name = "解析XML"
    description = "解析XML字符串或文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with content, file_path, output_var.

        Returns:
            ActionResult with parsed XML.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'xml_parsed')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            root = ET.fromstring(resolved_content)

            def etree_to_dict(node):
                result = {}
                if node.text and node.text.strip():
                    if len(node) == 0:
                        return node.text.strip()
                    result['#text'] = node.text.strip()
                for attr, val in node.attrib.items():
                    result[f'@{attr}'] = val
                for child in node:
                    child_dict = etree_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict)
                    else:
                        result[child.tag] = child_dict
                return result

            parsed = etree_to_dict(root)
            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message=f"XML已解析: {root.tag}",
                data={'root': root.tag, 'parsed': parsed, 'output_var': output_var}
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'xml_parsed'}


class XmlToDictAction(BaseAction):
    """Convert XML to dictionary."""
    action_type = "xml_to_dict"
    display_name = "XML转字典"
    description = "将XML转换为字典"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with content, file_path, output_var.

        Returns:
            ActionResult with dictionary.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'xml_dict')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            root = ET.fromstring(resolved_content)

            def etree_to_dict(node):
                result = {}
                if node.text and node.text.strip():
                    if len(node) == 0:
                        return node.text.strip()
                    result['#text'] = node.text.strip()
                for attr, val in node.attrib.items():
                    result[f'@{attr}'] = val
                for child in node:
                    child_dict = etree_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict)
                    else:
                        result[child.tag] = child_dict
                return result

            result_dict = etree_to_dict(root)
            context.set(output_var, result_dict)

            return ActionResult(
                success=True,
                message=f"XML转字典完成",
                data={'dict': result_dict, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML转字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'xml_dict'}


class XmlFromDictAction(BaseAction):
    """Create XML from dictionary."""
    action_type = "xml_from_dict"
    display_name = "字典转XML"
    description = "将字典转换为XML"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with data, root_tag, output_var.

        Returns:
            ActionResult with XML string.
        """
        data = params.get('data', {})
        root_tag = params.get('root_tag', 'root')
        output_var = params.get('output_var', 'xml_output')
        pretty = params.get('pretty', True)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_root = context.resolve_value(root_tag)
            resolved_pretty = context.resolve_value(pretty)

            def dict_to_etree(parent, data_dict):
                if isinstance(data_dict, dict):
                    for key, value in data_dict.items():
                        if key.startswith('@'):
                            parent.set(key[1:], str(value))
                        elif key == '#text':
                            parent.text = str(value)
                        elif isinstance(value, list):
                            for item in value:
                                child = ET.SubElement(parent, key)
                                dict_to_etree(child, item)
                        elif isinstance(value, dict):
                            child = ET.SubElement(parent, key)
                            dict_to_etree(child, value)
                        else:
                            child = ET.SubElement(parent, key)
                            child.text = str(value)
                elif data_dict is not None:
                    parent.text = str(data_dict)

            root = ET.Element(resolved_root)
            dict_to_etree(root, resolved_data)

            if resolved_pretty:
                ET.indent(root, space='  ')

            xml_str = ET.tostring(root, encoding='unicode', xml_declaration=True)

            context.set(output_var, xml_str)

            return ActionResult(
                success=True,
                message=f"字典转XML完成 ({len(xml_str)} 字符)",
                data={'xml': xml_str, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典转XML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'root_tag': 'root', 'output_var': 'xml_output', 'pretty': True}


class XmlXPathAction(BaseAction):
    """Extract data using XPath."""
    action_type = "xml_xpath"
    display_name = "XPath提取"
    description = "使用XPath从XML中提取数据"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XPath.

        Args:
            context: Execution context.
            params: Dict with content, file_path, xpath, output_var.

        Returns:
            ActionResult with extracted data.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        xpath = params.get('xpath', '')
        output_var = params.get('output_var', 'xpath_result')

        valid, msg = self.validate_type(xpath, str, 'xpath')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            resolved_xpath = context.resolve_value(xpath)

            root = ET.fromstring(resolved_content)
            nodes = root.findall(resolved_xpath)

            results = []
            for node in nodes:
                if len(node) == 0:
                    results.append(node.text)
                else:
                    results.append(ET.tostring(node, encoding='unicode'))

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"XPath提取: {len(results)} 个结果",
                data={'count': len(results), 'results': results, 'output_var': output_var}
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XPath提取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xpath']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'xpath_result'}


class XmlPrettyAction(BaseAction):
    """Pretty print XML."""
    action_type = "xml_pretty"
    display_name = "格式化XML"
    description = "格式化美化XML"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pretty.

        Args:
            context: Execution context.
            params: Dict with content, file_path, indent, output_var.

        Returns:
            ActionResult with formatted XML.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        indent = params.get('indent', '  ')
        output_var = params.get('output_var', 'xml_pretty')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            resolved_indent = context.resolve_value(indent)

            root = ET.fromstring(resolved_content)
            ET.indent(root, space=resolved_indent)
            formatted = ET.tostring(root, encoding='unicode')

            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"XML已格式化 ({len(formatted)} 字符)",
                data={'xml': formatted, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'indent': '  ', 'output_var': 'xml_pretty'}


class XmlMinifyAction(BaseAction):
    """Minify XML."""
    action_type = "xml_minify"
    display_name = "压缩XML"
    description = "压缩XML去除空白"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minify.

        Args:
            context: Execution context.
            params: Dict with content, file_path, output_var.

        Returns:
            ActionResult with minified XML.
        """
        content = params.get('content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'xml_minified')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_content = ''
            if content:
                resolved_content = context.resolve_value(content)
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                if not os.path.exists(resolved_path):
                    return ActionResult(
                        success=False,
                        message=f"文件不存在: {resolved_path}"
                    )
                with open(resolved_path, 'r', encoding='utf-8') as f:
                    resolved_content = f.read()
            else:
                return ActionResult(
                    success=False,
                    message="必须提供content或file_path"
                )

            root = ET.fromstring(resolved_content)
            minified = ET.tostring(root, encoding='unicode')

            context.set(output_var, minified)

            return ActionResult(
                success=True,
                message=f"XML已压缩 ({len(minified)} 字符)",
                data={'xml': minified, 'size': len(minified), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'content': '', 'file_path': '', 'output_var': 'xml_minified'}
