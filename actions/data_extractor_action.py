"""Data Extractor Action Module. Extracts structured data from unstructured sources."""
import sys, os, re, json, csv
from typing import Any
from dataclasses import dataclass
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

class DataExtractorAction(BaseAction):
    action_type = "data_extractor"; display_name = "数据提取"
    description = "提取结构化数据"
    def __init__(self) -> None: super().__init__()
    def execute(self, context: Any, params: dict) -> ActionResult:
        source = params.get("source",""); source_type = params.get("source_type","text")
        mode = params.get("mode","regex")
        if source_type == "file" and os.path.isfile(str(source)):
            with open(source, "r", encoding="utf-8", errors="replace") as f: source = f.read()
        if mode == "regex":
            pattern = params.get("pattern",""); field_names = params.get("field_names",[])
            group_idx = params.get("group_index")
            if not pattern: return ActionResult(success=False, message="Pattern required")
            try:
                regex = re.compile(pattern, re.MULTILINE | re.DOTALL); records = []
                for match in regex.finditer(source):
                    if group_idx is not None:
                        records.append({field_names[0] if field_names else "match": match.group(group_idx)})
                    elif field_names:
                        groups = match.groupdict()
                        records.append(groups)
                    else:
                        groups = match.groups()
                        records.append({f"field_{i}": g for i, g in enumerate(groups, 1)})
                return ActionResult(success=True, message=f"Extracted {len(records)} records",
                                  data={"records": records, "count": len(records)})
            except Exception as e: return ActionResult(success=False, message=f"Regex error: {e}")
        if mode == "json":
            try:
                data = json.loads(source) if isinstance(source, str) else source
                return ActionResult(success=True, message="Parsed JSON", data={"data": data})
            except Exception as e: return ActionResult(success=False, message=f"JSON error: {e}")
        if mode == "csv":
            delimiter = params.get("delimiter",",")
            try:
                records = list(csv.DictReader(source.splitlines(), delimiter=delimiter))
                return ActionResult(success=True, message=f"Extracted {len(records)} CSV records",
                                  data={"records": records, "count": len(records)})
            except Exception as e: return ActionResult(success=False, message=f"CSV error: {e}")
        if mode == "delimited":
            delimiter = params.get("delimiter","|")
            field_names = params.get("field_names",[])
            lines = source.strip().split("\n"); records = []
            for line in lines:
                if not line.strip(): continue
                parts = line.split(delimiter)
                if field_names: records.append({fn: parts[j] if j < len(parts) else "" for j, fn in enumerate(field_names)})
                else: records.append({f"col_{j}": parts[j] if j < len(parts) else "" for j in range(len(parts))})
            return ActionResult(success=True, message=f"Extracted {len(records)} records",
                              data={"records": records, "count": len(records)})
        return ActionResult(success=False, message=f"Unknown mode: {mode}")
