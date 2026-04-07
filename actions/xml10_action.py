"""XML10 action module for RabAI AutoClick.

Provides additional XML operations:
- XMLParseAction: Parse XML string
- XMLToStringAction: Convert XML to string
- XMLGetAction: Get XML element
- XMLSetAction: Set XML element
- XMLFindAction: Find XML elements
- XMLCreateAction: Create XML element
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XMLParseAction(BaseAction):
    """Parse XML string."""
    action_type = "xml10_parse"
    display_name = "解析XML"
    description = "解析XML字符串"
    version = "10.0"

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
        output_var = params.get('output_var', 'parsed_xml')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(xml_str)

            if isinstance(resolved, str):
                result = ET.fromstring(resolved)
            else:
                result = resolved

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析XML: {result.tag}",
                data={
                    'root_tag': result.tag,
                    'output_var': output_var
                }
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析XML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_xml'}


class XMLToStringAction(BaseAction):
    """Convert XML to string."""
    action_type = "xml10_tostring"
    display_name = "XML转字符串"
    description = "将XML转换为字符串"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML to string.

        Args:
            context: Execution context.
            params: Dict with element, output_var.

        Returns:
            ActionResult with XML string.
        """
        element = params.get('element', None)
        output_var = params.get('output_var', 'xml_string')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(element) if element is not None else None

            if resolved is None:
                return ActionResult(
                    success=False,
                    message=f"XML元素为空"
                )

            if isinstance(resolved, str):
                result = resolved
            else:
                result = ET.tostring(resolved, encoding='unicode')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"XML转字符串: {len(result)}字符",
                data={
                    'xml_string': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_string'}


class XMLGetAction(BaseAction):
    """Get XML element."""
    action_type = "xml10_get"
    display_name = "获取XML元素"
    description = "获取XML元素"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML get.

        Args:
            context: Execution context.
            params: Dict with element, path, attr, output_var.

        Returns:
            ActionResult with XML element.
        """
        element = params.get('element', None)
        path = params.get('path', '')
        attr = params.get('attr', None)
        output_var = params.get('output_var', 'xml_element')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(element) if element is not None else None
            resolved_path = context.resolve_value(path) if path else ''
            resolved_attr = context.resolve_value(attr) if attr else None

            if resolved is None:
                return ActionResult(
                    success=False,
                    message=f"XML元素为空"
                )

            if isinstance(resolved, str):
                resolved = ET.fromstring(resolved)

            if resolved_path:
                target = resolved.find(resolved_path)
            else:
                target = resolved

            if target is None:
                return ActionResult(
                    success=False,
                    message=f"未找到元素: {resolved_path}"
                )

            if resolved_attr:
                result = target.attrib.get(resolved_attr)
            else:
                result = target.text

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取XML元素: {target.tag}",
                data={
                    'tag': target.tag,
                    'text': target.text,
                    'attrib': target.attrib,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ET.ParseError as e:
            return ActionResult(
                success=False,
                message=f"XML解析失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取XML元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '', 'attr': None, 'output_var': 'xml_element'}


class XMLSetAction(BaseAction):
    """Set XML element."""
    action_type = "xml10_set"
    display_name = "设置XML元素"
    description = "设置XML元素"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML set.

        Args:
            context: Execution context.
            params: Dict with element, path, text, attr, output_var.

        Returns:
            ActionResult with modified XML.
        """
        element = params.get('element', None)
        path = params.get('path', '')
        text = params.get('text', None)
        attr = params.get('attr', None)
        output_var = params.get('output_var', 'xml_element')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(element) if element is not None else None
            resolved_path = context.resolve_value(path) if path else ''
            resolved_text = context.resolve_value(text) if text is not None else None
            resolved_attr = context.resolve_value(attr) if attr is not None else None

            if resolved is None:
                return ActionResult(
                    success=False,
                    message=f"XML元素为空"
                )

            if isinstance(resolved, str):
                resolved = ET.fromstring(resolved)

            if resolved_path:
                target = resolved.find(resolved_path)
            else:
                target = resolved

            if target is None:
                return ActionResult(
                    success=False,
                    message=f"未找到元素: {resolved_path}"
                )

            if resolved_text is not None:
                target.text = str(resolved_text)

            if resolved_attr is not None and isinstance(resolved_attr, dict):
                target.attrib.update(resolved_attr)

            context.set(output_var, resolved)

            return ActionResult(
                success=True,
                message=f"设置XML元素: {target.tag}",
                data={
                    'tag': target.tag,
                    'text': target.text,
                    'attrib': target.attrib,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置XML元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '', 'text': None, 'attr': None, 'output_var': 'xml_element'}


class XMLFindAction(BaseAction):
    """Find XML elements."""
    action_type = "xml10_find"
    display_name = "查找XML元素"
    description = "查找XML元素"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML find.

        Args:
            context: Execution context.
            params: Dict with element, path, output_var.

        Returns:
            ActionResult with found elements.
        """
        element = params.get('element', None)
        path = params.get('path', '')
        output_var = params.get('output_var', 'xml_elements')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(element) if element is not None else None
            resolved_path = context.resolve_value(path) if path else '*'

            if resolved is None:
                return ActionResult(
                    success=False,
                    message=f"XML元素为空"
                )

            if isinstance(resolved, str):
                resolved = ET.fromstring(resolved)

            results = resolved.findall(resolved_path)
            result = [{'tag': r.tag, 'text': r.text, 'attrib': r.attrib} for r in results]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查找XML元素: {len(result)}个",
                data={
                    'path': resolved_path,
                    'elements': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找XML元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['element', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_elements'}


class XMLCreateAction(BaseAction):
    """Create XML element."""
    action_type = "xml10_create"
    display_name = "创建XML元素"
    description = "创建XML元素"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML create.

        Args:
            context: Execution context.
            params: Dict with tag, text, attrib, output_var.

        Returns:
            ActionResult with created element.
        """
        tag = params.get('tag', 'element')
        text = params.get('text', '')
        attrib = params.get('attrib', {})
        output_var = params.get('output_var', 'xml_element')

        try:
            import xml.etree.ElementTree as ET

            resolved_tag = context.resolve_value(tag) if tag else 'element'
            resolved_text = context.resolve_value(text) if text else ''
            resolved_attrib = context.resolve_value(attrib) if attrib else {}

            if not isinstance(resolved_attrib, dict):
                resolved_attrib = {}

            result = ET.Element(resolved_tag, **resolved_attrib)
            result.text = str(resolved_text) if resolved_text else None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建XML元素: {resolved_tag}",
                data={
                    'tag': result.tag,
                    'text': result.text,
                    'attrib': result.attrib,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建XML元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tag']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'text': '', 'attrib': {}, 'output_var': 'xml_element'}