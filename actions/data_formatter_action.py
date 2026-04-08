"""Data Formatter Action Module. Formats data as JSON/CSV/XML/YAML/table."""
import sys, os, json, csv, io
from typing import Any
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

class DataFormatterAction(BaseAction):
    action_type = "data_formatter"; display_name = "数据格式化"
    description = "格式化数据"
    def __init__(self) -> None: super().__init__()
    def _to_csv(self, data: list, headers: list = None) -> str:
        if not data: return ""
        if not headers: headers = list(data[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader(); writer.writerows(data)
        return output.getvalue()
    def _to_xml(self, data: Any, root: str = "root") -> str:
        import xml.etree.ElementTree as ET
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
            root_elem = ET.Element(root)
            for k, v in data.items(): add(root_elem, k, v)
        else: root_elem = ET.Element(root); root_elem.text = str(data)
        return ET.tostring(root_elem, encoding="unicode")
    def execute(self, context: Any, params: dict) -> ActionResult:
        data = params.get("data"); output_fmt = params.get("format","json").lower()
        if data is None: return ActionResult(success=False, message="No data provided")
        if isinstance(data, str):
            try: data = json.loads(data)
            except: pass
        try:
            if output_fmt == "json": result = json.dumps(data, indent=params.get("indent",2), ensure_ascii=False)
            elif output_fmt == "csv":
                if not isinstance(data, list): return ActionResult(success=False, message="CSV requires list")
                result = self._to_csv(data, params.get("headers"))
            elif output_fmt == "xml": result = self._to_xml(data, params.get("root_tag","root"))
            elif output_fmt == "yaml":
                def to_yaml(d, indent=0):
                    prefix = "  " * indent
                    if isinstance(d, dict):
                        return "\n".join([f"{prefix}{k}: {to_yaml(v, indent+1) if isinstance(v,(dict,list)) else v}" for k,v in d.items()])
                    elif isinstance(d, list):
                        return "\n".join([f"{prefix}- {to_yaml(item, indent+1) if isinstance(item,(dict,list)) else item}" for item in d])
                    return str(d)
                result = to_yaml(data)
            else: result = str(data)
            return ActionResult(success=True, message=f"Formatted as {output_fmt}: {len(result)} chars",
                              data={"format": output_fmt, "output": result})
        except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
