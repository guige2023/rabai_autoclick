"""API Converter Action Module. Converts API data between formats."""
import sys, os, json
import xml.etree.ElementTree as ET
from typing import Any
from dataclasses import dataclass
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

class APIConverterAction(BaseAction):
    action_type = "api_converter"; display_name = "API格式转换"
    description = "转换API数据格式"
    def __init__(self) -> None: super().__init__()
    def _json_to_xml(self, data: Any, root_tag: str = "root") -> str:
        root = ET.Element(root_tag)
        def add(parent, key, value):
            if isinstance(value, dict):
                elem = ET.SubElement(parent, str(key))
                for k, v in value.items(): add(elem, k, v)
            elif isinstance(value, list):
                for item in value: add(parent, key, item)
            else:
                elem = ET.SubElement(parent, str(key))
                elem.text = str(value) if value is not None else ""
        if isinstance(data, dict):
            for k, v in data.items(): add(root, k, v)
        else: root.text = str(data)
        return ET.tostring(root, encoding="unicode")
    def execute(self, context: Any, params: dict) -> ActionResult:
        data = params.get("data",""); from_fmt = params.get("from_format","json").lower()
        to_fmt = params.get("to_format","xml").lower(); root_tag = params.get("root_tag","root")
        if from_fmt == to_fmt: return ActionResult(success=True, message="Same format")
        try:
            if from_fmt == "json": parsed = json.loads(data) if isinstance(data, str) else data
            else: parsed = data
            if to_fmt == "xml": output = self._json_to_xml(parsed, root_tag)
            elif to_fmt == "json": output = json.dumps(parsed, indent=2, ensure_ascii=False)
            else: return ActionResult(success=False, message=f"Unknown target: {to_fmt}")
            return ActionResult(success=True, message=f"Converted {from_fmt}→{to_fmt}: {len(str(output))} chars", data={"converted": output})
        except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
