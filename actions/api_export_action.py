"""API Export Action.

Exports API responses to various formats (JSON, CSV, XML).
"""
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
import csv
import io
import json
import xml.etree.ElementTree as ET


@dataclass
class ExportConfig:
    format: str = "json"
    indent: int = 2
    include_metadata: bool = True


class APIExportAction:
    """Exports API data to multiple formats."""

    def __init__(self, config: Optional[ExportConfig] = None) -> None:
        self.config = config or ExportConfig()

    def export_json(
        self,
        data: Union[Dict, List],
        pretty: bool = True,
    ) -> str:
        indent = self.config.indent if pretty else None
        return json.dumps(data, indent=indent, default=str)

    def export_csv(
        self,
        data: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
    ) -> str:
        if not data:
            return ""
        cols = columns or list(data[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    def export_xml(
        self,
        data: Union[Dict, List],
        root_name: str = "root",
        item_name: str = "item",
    ) -> str:
        root = ET.Element(root_name)
        if isinstance(data, list):
            for item in data:
                child = ET.SubElement(root, item_name)
                self._dict_to_xml(child, item)
        else:
            self._dict_to_xml(root, data)
        return ET.tostring(root, encoding="unicode")

    def _dict_to_xml(self, parent: ET.Element, data: Dict) -> None:
        for key, value in data.items():
            child = ET.SubElement(parent, str(key))
            if isinstance(value, dict):
                self._dict_to_xml(child, value)
            elif isinstance(value, list):
                for item in value:
                    sub = ET.SubElement(child, "item")
                    if isinstance(item, dict):
                        self._dict_to_xml(sub, item)
                    else:
                        sub.text = str(item)
            else:
                child.text = str(value) if value is not None else ""

    def export_to_file(
        self,
        data: Union[Dict, List],
        path: str,
        format: Optional[str] = None,
    ) -> None:
        fmt = format or self.config.format
        if fmt == "json":
            content = self.export_json(data)
        elif fmt == "csv":
            content = self.export_csv(data)
        elif fmt == "xml":
            content = self.export_xml(data)
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
