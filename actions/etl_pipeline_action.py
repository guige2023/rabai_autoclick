"""ETL pipeline action module for RabAI AutoClick.

Provides ETL (Extract, Transform, Load) pipeline operations:
- ExtractAction: Extract data from various sources
- TransformAction: Apply transformations to data
- LoadAction: Load data into destination
- EtlPipelineAction: Orchestrate full ETL pipeline
"""

import json
import csv
import io
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ExtractAction(BaseAction):
    """Extract data from various sources."""
    action_type = "etl_extract"
    display_name = "ETL数据提取"
    description = "从各种数据源提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source_type = params.get("source_type", "json")
            source = params.get("source", "")
            encoding = params.get("encoding", "utf-8")

            if source_type == "json":
                if source.startswith("http"):
                    import urllib.request
                    with urllib.request.urlopen(source, timeout=30) as resp:
                        content = resp.read().decode(encoding)
                        data = json.loads(content)
                else:
                    with open(source, "r", encoding=encoding) as f:
                        data = json.load(f)
                items = data if isinstance(data, list) else [data]

            elif source_type == "csv":
                with open(source, "r", encoding=encoding, newline="") as f:
                    reader = csv.DictReader(f)
                    items = list(reader)

            elif source_type == "jsonl":
                items = []
                with open(source, "r", encoding=encoding) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            items.append(json.loads(line))

            elif source_type == "xml":
                import xml.etree.ElementTree as ET
                tree = ET.parse(source)
                root = tree.getroot()
                items = [{child.tag: child.text for child in elem} for elem in root]

            elif source_type == "api":
                import urllib.request
                method = params.get("method", "GET")
                headers = params.get("headers", {})
                req = urllib.request.Request(source, method=method)
                for k, v in headers.items():
                    req.add_header(k, v)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    content = resp.read().decode(encoding)
                    data = json.loads(content)
                    items = data if isinstance(data, list) else [data]

            else:
                return ActionResult(success=False, message=f"Unknown source_type: {source_type}")

            return ActionResult(
                success=True,
                message=f"Extracted {len(items)} records from {source_type}",
                data={"items": items, "count": len(items), "source_type": source_type},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Extract error: {e}")


class TransformAction(BaseAction):
    """Apply transformations to data."""
    action_type = "etl_transform"
    display_name = "ETL数据转换"
    description = "对数据应用转换规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            transforms = params.get("transforms", [])

            if not isinstance(data, list):
                data = [data]

            if not transforms:
                return ActionResult(success=True, message="No transforms applied", data={"data": data})

            for transform in transforms:
                t_type = transform.get("type", "passthrough")
                field = transform.get("field")
                config = transform.get("config", {})

                if t_type == "rename":
                    old_name = config.get("old_name")
                    new_name = config.get("new_name")
                    for item in data:
                        if isinstance(item, dict) and old_name in item:
                            item[new_name] = item.pop(old_name)

                elif t_type == "cast":
                    target_type = config.get("target_type")
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            try:
                                if target_type == "int":
                                    item[field] = int(item[field])
                                elif target_type == "float":
                                    item[field] = float(item[field])
                                elif target_type == "str":
                                    item[field] = str(item[field])
                                elif target_type == "bool":
                                    item[field] = bool(item[field])
                            except (ValueError, TypeError):
                                pass

                elif t_type == "map_values":
                    mapping = config.get("mapping", {})
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            item[field] = mapping.get(item[field], item[field])

                elif t_type == "fillna":
                    fill_value = config.get("value", "")
                    for item in data:
                        if isinstance(item, dict) and field in item and (item[field] is None or item[field] == ""):
                            item[field] = fill_value

                elif t_type == "trim":
                    for item in data:
                        if isinstance(item, dict) and field in item and isinstance(item[field], str):
                            item[field] = item[field].strip()

                elif t_type == "lowercase":
                    for item in data:
                        if isinstance(item, dict) and field in item and isinstance(item[field], str):
                            item[field] = item[field].lower()

                elif t_type == "uppercase":
                    for item in data:
                        if isinstance(item, dict) and field in item and isinstance(item[field], str):
                            item[field] = item[field].upper()

                elif t_type == "split":
                    delimiter = config.get("delimiter", ",")
                    new_fields = config.get("new_fields", [])
                    for item in data:
                        if isinstance(item, dict) and field in item:
                            parts = str(item[field]).split(delimiter)
                            for i, nf in enumerate(new_fields):
                                item[nf] = parts[i] if i < len(parts) else None

                elif t_type == "merge":
                    source_fields = config.get("source_fields", [])
                    target_field = config.get("target_field", "merged")
                    separator = config.get("separator", " ")
                    for item in data:
                        if isinstance(item, dict):
                            parts = [str(item.get(f, "")) for f in source_fields]
                            item[target_field] = separator.join(p for p in parts if p)

                elif t_type == "derive":
                    target_field = config.get("target_field")
                    expression = config.get("expression")
                    for item in data:
                        if isinstance(item, dict) and expression:
                            try:
                                item[target_field] = eval(expression, {"item": item, "__builtins__": {}})
                            except Exception:
                                item[target_field] = None

            return ActionResult(
                success=True,
                message=f"Transformed {len(data)} records with {len(transforms)} transforms",
                data={"data": data, "count": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {e}")


class LoadAction(BaseAction):
    """Load data into destination."""
    action_type = "etl_load"
    display_name = "ETL数据加载"
    description = "将数据加载到目标位置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            dest_type = params.get("dest_type", "json")
            dest = params.get("dest", "")
            encoding = params.get("encoding", "utf-8")
            mode = params.get("mode", "overwrite")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to load")

            if dest_type == "json":
                if dest.startswith("http"):
                    import urllib.request
                    body = json.dumps(data).encode(encoding)
                    req = urllib.request.Request(dest, data=body, method="POST")
                    req.add_header("Content-Type", "application/json")
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        result = {"status": resp.status}
                else:
                    with open(dest, "w", encoding=encoding) as f:
                        if mode == "append":
                            existing = []
                            if os.path.exists(dest):
                                with open(dest, "r", encoding=encoding) as ef:
                                    existing = json.load(ef)
                            data = existing + data
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    result = {"path": dest}

            elif dest_type == "csv":
                if not data:
                    return ActionResult(success=False, message="No data to load")
                keys = list(data[0].keys()) if isinstance(data[0], dict) else []
                with open(dest, "w", encoding=encoding, newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=keys)
                    writer.writeheader()
                    writer.writerows(data)
                result = {"path": dest, "rows": len(data)}

            elif dest_type == "jsonl":
                with open(dest, "a" if mode == "append" else "w", encoding=encoding) as f:
                    for item in data:
                        f.write(json.dumps(item, ensure_ascii=False) + "\n")
                result = {"path": dest, "rows": len(data)}

            elif dest_type == "stdout":
                for item in data:
                    print(json.dumps(item, ensure_ascii=False))
                result = {"rows": len(data)}

            return ActionResult(
                success=True,
                message=f"Loaded {len(data)} records to {dest_type}",
                data={"result": result, "loaded_count": len(data), "dest_type": dest_type},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Load error: {e}")


class EtlPipelineAction(BaseAction):
    """Orchestrate full ETL pipeline."""
    action_type = "etl_pipeline"
    display_name = "ETL流水线"
    description = "编排完整的ETL流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stages = params.get("stages", [])
            data = params.get("initial_data", [])

            if not stages:
                return ActionResult(success=False, message="stages is required")

            stage_results = []
            current_data = data
            start_time = datetime.now()

            for i, stage in enumerate(stages):
                stage_name = stage.get("name", f"stage_{i}")
                stage_type = stage.get("type")
                stage_config = stage.get("config", {})
                stage_start = datetime.now()

                stage_config["data"] = current_data

                if stage_type == "extract":
                    result = ExtractAction().execute(context, stage_config)
                elif stage_type == "transform":
                    result = TransformAction().execute(context, stage_config)
                elif stage_type == "load":
                    result = LoadAction().execute(context, stage_config)
                elif stage_type == "filter":
                    from actions.stream_processing_action import StreamFilterAction
                    result = StreamFilterAction().execute(context, stage_config)
                else:
                    result = ActionResult(success=False, message=f"Unknown stage type: {stage_type}")

                stage_duration = (datetime.now() - stage_start).total_seconds() * 1000
                stage_results.append({
                    "name": stage_name,
                    "type": stage_type,
                    "success": result.success,
                    "duration_ms": int(stage_duration),
                    "output_count": len(result.data.get("data", result.data.get("items", []))) if result.success else 0,
                })

                if result.success:
                    current_data = result.data.get("data", result.data.get("items", current_data))
                else:
                    return ActionResult(
                        success=False,
                        message=f"ETL pipeline failed at stage '{stage_name}': {result.message}",
                        data={"stage_results": stage_results},
                    )

            total_duration = (datetime.now() - start_time).total_seconds() * 1000

            return ActionResult(
                success=True,
                message=f"ETL pipeline completed: {len(stages)} stages in {int(total_duration)}ms",
                data={
                    "data": current_data,
                    "record_count": len(current_data),
                    "stage_results": stage_results,
                    "total_duration_ms": int(total_duration),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EtlPipeline error: {e}")
