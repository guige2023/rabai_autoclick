"""XML action module for RabAI AutoClick.

Provides XML operations:
- XMLParseAction: Parse XML string or file
- XMLDumpAction: Convert dict/object to XML string
- XMLValidateAction: Validate XML against schema
- XMLXPathAction: Query XML using XPath
- XMLFindAction: Find elements by tag name
- XMLAttrAction: Get/set element attributes
"""

from typing import Any, Dict, List, Optional, Union
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class XMLParseAction(BaseAction):
    """Parse XML from string or file."""
    action_type = "xml_parse"
    display_name = "XML解析"
    description = "解析XML字符串或文件为树结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML parse operation.

        Args:
            context: Execution context.
            params: Dict with xml_content or file_path, output_var.

        Returns:
            ActionResult with parsed XML tree.
        """
        xml_content = params.get('xml_content', '')
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'xml_tree')

        try:
            if file_path:
                resolved_path = context.resolve_value(file_path)
                tree = ET.parse(resolved_path)
                root = tree.getroot()
            elif xml_content:
                resolved_content = context.resolve_value(xml_content)
                root = ET.fromstring(resolved_content)
            else:
                return ActionResult(success=False, message="xml_content or file_path required")

            def etree_to_dict(node: ET.Element) -> Dict[str, Any]:
                result: Dict[str, Any] = {}
                if node.attrib:
                    result['@attrs'] = dict(node.attrib)
                if node.text and node.text.strip():
                    result['#text'] = node.text.strip()
                for child in node:
                    child_data = etree_to_dict(child)
                    tag = child.tag
                    if tag in result:
                        if not isinstance(result[tag], list):
                            result[tag] = [result[tag]]
                        result[tag].append(child_data)
                    else:
                        result[tag] = child_data
                return {node.tag: result}

            xml_dict = etree_to_dict(root)
            context.set(output_var, xml_dict)
            context.set(f'{output_var}_root', root)
            return ActionResult(success=True, data=xml_dict,
                               message=f"Parsed XML: root tag = {root.tag}")

        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {str(e)}")
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_path}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML error: {str(e)}")


class XMLDumpAction(BaseAction):
    """Convert dict to XML string."""
    action_type = "xml_dump"
    display_name = "XML输出"
    description = "将字典数据转换为XML字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML dump operation.

        Args:
            context: Execution context.
            params: Dict with data, root_tag, pretty_print, output_var.

        Returns:
            ActionResult with XML string.
        """
        data = params.get('data', {})
        root_tag = params.get('root_tag', 'root')
        pretty_print = params.get('pretty_print', True)
        output_var = params.get('output_var', 'xml_string')

        try:
            resolved_data = context.resolve_value(data)
            resolved_root_tag = context.resolve_value(root_tag)
            resolved_pretty = context.resolve_value(pretty_print)

            def dict_to_etree(parent: str, d: Any) -> ET.Element:
                if isinstance(d, dict):
                    elem = ET.Element(parent)
                    for key, val in d.items():
                        if key == '@attrs':
                            for attr_name, attr_val in val.items():
                                elem.set(attr_name, str(attr_val))
                        elif key == '#text':
                            elem.text = str(val)
                        else:
                            if isinstance(val, list):
                                for item in val:
                                    child = dict_to_etree(key, item)
                                    elem.append(child)
                            else:
                                child = dict_to_etree(key, val)
                                elem.append(child)
                    return elem
                else:
                    elem = ET.Element(parent)
                    elem.text = str(d)
                    return elem

            if isinstance(resolved_data, dict):
                root = dict_to_etree(resolved_root_tag, resolved_data)
            else:
                root = dict_to_etree(resolved_root_tag, {'item': resolved_data})

            tree = ET.ElementTree(root)
            xml_str = ET.tostring(root, encoding='unicode')

            if resolved_pretty:
                dom = minidom.parseString(xml_str)
                xml_str = dom.toprettyxml(indent='  ')

            context.set(output_var, xml_str)
            return ActionResult(success=True, data=xml_str,
                               message=f"Dumped XML: {len(xml_str)} chars")

        except Exception as e:
            return ActionResult(success=False, message=f"XML dump error: {str(e)}")


class XMLValidateAction(BaseAction):
    """Validate XML against XSD schema."""
    action_type = "xml_validate"
    display_name = "XML验证"
    description = "用XSD模式验证XML文档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML validate operation.

        Args:
            context: Execution context.
            params: Dict with xml_content or file_path, schema_path, output_var.

        Returns:
            ActionResult with validation status.
        """
        xml_content = params.get('xml_content', '')
        file_path = params.get('file_path', '')
        schema_path = params.get('schema_path', '')
        output_var = params.get('output_var', 'xml_valid')

        try:
            import xmlschema
        except ImportError:
            return ActionResult(success=False,
                              message="xmlschema library required: pip install xmlschema")

        try:
            if file_path:
                resolved_path = context.resolve_value(file_path)
                xml_doc = ET.parse(resolved_path)
            elif xml_content:
                resolved_content = context.resolve_value(xml_content)
                xml_doc = ET.fromstring(resolved_content)
            else:
                return ActionResult(success=False, message="xml_content or file_path required")

            resolved_schema = context.resolve_value(schema_path)

            schema = xmlschema.XMLSchema11(resolved_schema)
            is_valid = schema.is_valid(xml_doc)

            context.set(output_var, is_valid)
            if is_valid:
                return ActionResult(success=True, data=True, message="XML is valid")
            else:
                errors = list(schema.iter_errors(xml_doc))
                error_msgs = [str(e) for e in errors[:5]]
                return ActionResult(success=False, data=False,
                                   message=f"XML invalid: {error_msgs[0] if error_msgs else 'unknown error'}")

        except ImportError:
            return ActionResult(success=False, message="xmlschema not installed")
        except FileNotFoundError:
            return ActionResult(success=False, message=f"File not found: {resolved_schema}")
        except Exception as e:
            return ActionResult(success=False, message=f"XML validate error: {str(e)}")


class XMLXPathAction(BaseAction):
    """Query XML using XPath."""
    action_type = "xml_xpath"
    display_name = "XML查询"
    description = "使用XPath查询XML元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML XPath query.

        Args:
            context: Execution context.
            params: Dict with xml_tree or xml_content, xpath, output_var.

        Returns:
            ActionResult with matched elements.
        """
        xml_tree = params.get('xml_tree', None)
        xml_content = params.get('xml_content', '')
        file_path = params.get('file_path', '')
        xpath = params.get('xpath', '')
        output_var = params.get('output_var', 'xpath_result')

        if not xpath:
            return ActionResult(success=False, message="xpath query is required")

        try:
            resolved_xpath = context.resolve_value(xpath)

            if xml_tree is not None:
                root = context.resolve_value(xml_tree)
                if isinstance(root, ET.Element):
                    elem = root
                else:
                    return ActionResult(success=False, message="xml_tree must be an Element")
            elif file_path:
                resolved_path = context.resolve_value(file_path)
                tree = ET.parse(resolved_path)
                elem = tree.getroot()
            elif xml_content:
                resolved_content = context.resolve_value(xml_content)
                elem = ET.fromstring(resolved_content)
            else:
                return ActionResult(success=False, message="xml_tree, xml_content, or file_path required")

            results = elem.findall(resolved_xpath)

            def element_to_dict(e: ET.Element) -> Dict[str, Any]:
                result: Dict[str, Any] = {'@tag': e.tag}
                if e.attrib:
                    result['@attrs'] = dict(e.attrib)
                if e.text and e.text.strip():
                    result['#text'] = e.text.strip()
                children = [element_to_dict(c) for c in e]
                if children:
                    result['children'] = children
                return result

            results_dict = [element_to_dict(r) for r in results]

            context.set(output_var, results_dict)
            return ActionResult(success=True, data=results_dict,
                               message=f"XPath found {len(results)} elements")

        except ET.ParseError as e:
            return ActionResult(success=False, message=f"XML parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"XPath error: {str(e)}")


class XMLFindAction(BaseAction):
    """Find XML elements by tag name."""
    action_type = "xml_find"
    display_name = "XML查找"
    description = "按标签名查找XML元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML find operation.

        Args:
            context: Execution context.
            params: Dict with xml_tree, tag, all_matches, output_var.

        Returns:
            ActionResult with found elements.
        """
        xml_tree = params.get('xml_tree', None)
        tag = params.get('tag', '')
        all_matches = params.get('all_matches', True)
        output_var = params.get('output_var', 'xml_find_result')

        if not tag:
            return ActionResult(success=False, message="tag name is required")

        try:
            resolved_tag = context.resolve_value(tag)
            resolved_all = context.resolve_value(all_matches)

            if xml_tree is not None:
                root = context.resolve_value(xml_tree)
                if not isinstance(root, ET.Element):
                    return ActionResult(success=False, message="xml_tree must be an Element")
            else:
                return ActionResult(success=False, message="xml_tree is required")

            if resolved_all:
                results = root.findall(f'.//{resolved_tag}')
            else:
                result = root.find(f'.//{resolved_tag}')
                results = [result] if result is not None else []

            def elem_to_dict(e: ET.Element) -> Dict[str, Any]:
                res: Dict[str, Any] = {'@tag': e.tag}
                if e.attrib:
                    res['@attrs'] = dict(e.attrib)
                if e.text and e.text.strip():
                    res['#text'] = e.text.strip()
                return res

            results_dict = [elem_to_dict(r) for r in results]
            context.set(output_var, results_dict)
            return ActionResult(success=True, data=results_dict,
                               message=f"Found {len(results)} elements with tag '{resolved_tag}'")

        except Exception as e:
            return ActionResult(success=False, message=f"XML find error: {str(e)}")


class XMLAttrAction(BaseAction):
    """Get or set XML element attributes."""
    action_type = "xml_attr"
    display_name = "XML属性"
    description = "获取或设置XML元素属性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML attribute operation.

        Args:
            context: Execution context.
            params: Dict with xml_tree, element_path, attr_name, attr_value, output_var.

        Returns:
            ActionResult with attribute value or update status.
        """
        xml_tree = params.get('xml_tree', None)
        element_path = params.get('element_path', '')
        attr_name = params.get('attr_name', '')
        attr_value = params.get('attr_value', None)
        output_var = params.get('output_var', 'xml_attr_result')

        if not attr_name:
            return ActionResult(success=False, message="attr_name is required")

        try:
            resolved_attr = context.resolve_value(attr_name)

            if xml_tree is not None:
                root = context.resolve_value(xml_tree)
                if not isinstance(root, ET.Element):
                    return ActionResult(success=False, message="xml_tree must be an Element")
            else:
                return ActionResult(success=False, message="xml_tree is required")

            if element_path:
                resolved_path = context.resolve_value(element_path)
                elem = root.find(resolved_path)
                if elem is None:
                    return ActionResult(success=False, message=f"Element not found: {resolved_path}")
            else:
                elem = root

            if attr_value is not None:
                resolved_val = context.resolve_value(attr_value)
                elem.set(resolved_attr, str(resolved_val))
                context.set(output_var, True)
                return ActionResult(success=True, data=True,
                                   message=f"Set attribute {resolved_attr} = {resolved_val}")
            else:
                value = elem.get(resolved_attr, '')
                context.set(output_var, value)
                return ActionResult(success=True, data=value,
                                   message=f"Got attribute {resolved_attr} = {value}")

        except Exception as e:
            return ActionResult(success=False, message=f"XML attr error: {str(e)}")
