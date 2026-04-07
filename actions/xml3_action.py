"""XML3 action module for RabAI AutoClick.

Provides additional XML operations:
- XMLParseAction: Parse XML string
- XMLToDictAction: Convert XML to dictionary
- XMLFromDictAction: Create XML from dictionary
- XMLValidateAction: Validate XML
- XMLPrettyPrintAction: Pretty print XML
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XMLParseAction(BaseAction):
    """Parse XML string."""
    action_type = "xml3_parse"
    display_name = "解析XML"
    description = "解析XML字符串"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML parse.

        Args:
            context: Execution context.
            params: Dict with xml_string, output_var.

        Returns:
            ActionResult with parsed XML.
        """
        xml_string = params.get('xml_string', '')
        output_var = params.get('output_var', 'parsed_xml')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(xml_string)

            root = ET.fromstring(resolved)

            context.set(output_var, ET.tostring(root, encoding='unicode'))

            return ActionResult(
                success=True,
                message=f"XML解析成功",
                data={
                    'parsed': ET.tostring(root, encoding='unicode'),
                    'root_tag': root.tag,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_xml'}


class XMLToDictAction(BaseAction):
    """Convert XML to dictionary."""
    action_type = "xml3_to_dict"
    display_name = "XML转字典"
    description = "将XML转换为字典"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML to dict.

        Args:
            context: Execution context.
            params: Dict with xml_string, output_var.

        Returns:
            ActionResult with dictionary.
        """
        xml_string = params.get('xml_string', '')
        output_var = params.get('output_var', 'xml_dict')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(xml_string)

            root = ET.fromstring(resolved)

            def etree_to_dict(t):
                d = {t.tag: {} if t.attrib else None}
                children = list(t)
                if children:
                    dd = {}
                    for child in children:
                        d_child = etree_to_dict(child)
                        tag = list(d_child.keys())[0]
                        if tag in dd:
                            if not isinstance(dd[tag], list):
                                dd[tag] = [dd[tag]]
                            dd[tag].append(d_child[tag])
                        else:
                            dd[tag] = d_child[tag]
                    d = {t.tag: dd}
                if t.attrib:
                    d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
                if t.text and t.text.strip():
                    d[t.tag]['#text'] = t.text
                return d

            result = etree_to_dict(root)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"XML转字典成功",
                data={
                    'xml_dict': result,
                    'output_var': output_var
                }
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


class XMLFromDictAction(BaseAction):
    """Create XML from dictionary."""
    action_type = "xml3_from_dict"
    display_name = "字典转XML"
    description = "将字典转换为XML"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict to XML.

        Args:
            context: Execution context.
            params: Dict with dict, root_tag, output_var.

        Returns:
            ActionResult with XML string.
        """
        input_dict = params.get('dict', {})
        root_tag = params.get('root_tag', 'root')
        output_var = params.get('output_var', 'xml_string')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(input_dict)
            resolved_root = context.resolve_value(root_tag) if root_tag else 'root'

            def dict_to_etree(d, tag):
                elem = ET.Element(tag)
                if isinstance(d, dict):
                    for k, v in d.items():
                        if k.startswith('@'):
                            elem.set(k[1:], v)
                        elif k == '#text':
                            elem.text = str(v)
                        else:
                            child = dict_to_etree(v, k)
                            elem.append(child)
                else:
                    elem.text = str(d)
                return elem

            root = dict_to_etree(resolved, resolved_root)
            result = ET.tostring(root, encoding='unicode')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典转XML成功",
                data={
                    'xml_string': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典转XML失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict', 'root_tag']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_string'}


class XMLValidateAction(BaseAction):
    """Validate XML."""
    action_type = "xml3_validate"
    display_name = "验证XML"
    description = "验证XML格式"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML validate.

        Args:
            context: Execution context.
            params: Dict with xml_string, output_var.

        Returns:
            ActionResult with validation result.
        """
        xml_string = params.get('xml_string', '')
        output_var = params.get('output_var', 'xml_valid')

        try:
            import xml.etree.ElementTree as ET

            resolved = context.resolve_value(xml_string)

            ET.fromstring(resolved)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"XML验证: 有效",
                data={
                    'valid': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            context.set(output_var, False)
            return ActionResult(
                success=True,
                message=f"XML验证: 无效 - {str(e)}",
                data={
                    'valid': False,
                    'error': str(e),
                    'output_var': output_var
                }
            )

    def get_required_params(self) -> List[str]:
        return ['xml_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'xml_valid'}


class XMLPrettyPrintAction(BaseAction):
    """Pretty print XML."""
    action_type = "xml3_pretty"
    display_name = "格式化XML"
    description = "格式化输出XML"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XML pretty print.

        Args:
            context: Execution context.
            params: Dict with xml_string, indent, output_var.

        Returns:
            ActionResult with formatted XML.
        """
        xml_string = params.get('xml_string', '')
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'pretty_xml')

        try:
            import xml.etree.ElementTree as ET
            import xml.dom.minidom

            resolved = context.resolve_value(xml_string)
            resolved_indent = int(context.resolve_value(indent)) if indent else 2

            dom = xml.dom.minidom.parseString(resolved)
            result = dom.toprettyxml(indent=' ' * resolved_indent)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"XML格式化成功",
                data={
                    'pretty_xml': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"XML格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['xml_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'pretty_xml'}