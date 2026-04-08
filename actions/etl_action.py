"""ETL pipeline action module for RabAI AutoClick.

Provides ETL (Extract-Transform-Load) operations:
- ETLExtractAction: Extract data from various sources
- ETLTransformAction: Transform extracted data
- ETLLoadAction: Load data to destination
- ETLPipelineAction: Orchestrate full ETL pipeline
"""

import json
import csv
import io
from typing import Any, Dict, List, Optional, Callable


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ETLExtractAction(BaseAction):
    """Extract data from various sources."""
    action_type = "etl_extract"
    display_name = "ETL提取"
    description = "从各种数据源提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source_type = params.get("source_type", "json")
            source = params.get("source", "")
            query = params.get("query", {})
            filters = params.get("filters", [])

            extracted = []

            if source_type == "json":
                if isinstance(source, str):
                    try:
                        data = json.loads(source)
                    except json.JSONDecodeError:
                        return ActionResult(success=False, message="Invalid JSON source")
                else:
                    data = source

                if isinstance(data, list):
                    extracted = data
                elif isinstance(data, dict):
                    extracted = [data]

            elif source_type == "csv":
                if isinstance(source, str):
                    reader = csv.DictReader(io.StringIO(source))
                    extracted = list(reader)

            elif source_type == "api":
                url = query.get("url", "")
                method = query.get("method", "GET")
                headers = query.get("headers", {})
                if url:
                    import urllib.request
                    req = urllib.request.Request(url, headers=headers, method=method)
                    with urllib.request.urlopen(req, timeout=30) as response:
                        result = json.loads(response.read().decode("utf-8"))
                        extracted = result if isinstance(result, list) else [result]

            for f in filters:
                col = f.get("column", "")
                op = f.get("operator", "==")
                val = f.get("value", None)
                if col and extracted:
                    extracted = [r for r in extracted if str(r.get(col, "")) == str(val)]

            return ActionResult(
                success=True,
                message=f"Extracted {len(extracted)} records from {source_type}",
                data={"records": extracted, "count": len(extracted), "source_type": source_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"ETL extract failed: {str(e)}")


class ETLTransformAction(BaseAction):
    """Transform ETL data."""
    action_type = "etl_transform"
    display_name = "ETL转换"
    description = "转换ETL数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            transforms = params.get("transforms", [])
            derived_columns = params.get("derived_columns", [])
            normalize = params.get("normalize", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            records = list(data)

            for t in transforms:
                t_type = t.get("type", "")
                if t_type == "rename":
                    old_name = t.get("old", "")
                    new_name = t.get("new", "")
                    for r in records:
                        if old_name in r:
                            r[new_name] = r.pop(old_name)

                elif t_type == "type_cast":
                    column = t.get("column", "")
                    target_type = t.get("target_type", "string")
                    for r in records:
                        if column in r:
                            try:
                                if target_type == "int":
                                    r[column] = int(r[column])
                                elif target_type == "float":
                                    r[column] = float(r[column])
                                elif target_type == "string":
                                    r[column] = str(r[column])
                            except (ValueError, TypeError):
                                pass

                elif t_type == "filter":
                    column = t.get("column", "")
                    op = t.get("operator", "==")
                    value = t.get("value", None)
                    for r in records[:]:
                        col_val = str(r.get(column, ""))
                        val_str = str(value)
                        if op == "==":
                            keep = col_val == val_str
                        elif op == "!=":
                            keep = col_val != val_str
                        elif op == "contains":
                            keep = val_str in col_val
                        else:
                            keep = True
                        if not keep:
                            records.remove(r)

                elif t_type == "deduplicate":
                    seen = set()
                    for r in records[:]:
                        key = json.dumps(r, sort_keys=True)
                        if key in seen:
                            records.remove(r)
                        else:
                            seen.add(key)

            for dc in derived_columns:
                col_name = dc.get("name", "")
                expression = dc.get("expression", "")
                for r in records:
                    try:
                        r[col_name] = eval(expression, {"__builtins__": {}}, r)
                    except Exception:
                        r[col_name] = None

            return ActionResult(
                success=True,
                message=f"Transformed {len(records)} records",
                data={"records": records, "count": len(records)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"ETL transform failed: {str(e)}")


class ETLLoadAction(BaseAction):
    """Load data to destination."""
    action_type = "etl_load"
    display_name = "ETL加载"
    description = "将数据加载到目标位置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            dest_type = params.get("dest_type", "json")
            dest = params.get("dest", "")
            mode = params.get("mode", "append")

            if not data:
                return ActionResult(success=False, message="data is required")

            loaded_count = len(data)

            if dest_type == "json":
                result = {"records": data, "count": loaded_count, "mode": mode}

            elif dest_type == "csv":
                output = io.StringIO()
                if data:
                    writer = csv.DictWriter(output, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                result = {"csv_data": output.getvalue(), "count": loaded_count}

            elif dest_type == "api":
                url = dest
                headers = params.get("headers", {"Content-Type": "application/json"})
                import urllib.request
                payload = json.dumps({"records": data, "mode": mode}).encode()
                req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=60) as response:
                    result = {"response": response.read().decode(), "count": loaded_count}

            else:
                return ActionResult(success=False, message=f"Unknown dest_type: {dest_type}")

            return ActionResult(
                success=True,
                message=f"Loaded {loaded_count} records to {dest_type}",
                data=result
            )

        except Exception as e:
            return ActionResult(success=False, message=f"ETL load failed: {str(e)}")


class ETLPipelineAction(BaseAction):
    """Orchestrate a complete ETL pipeline."""
    action_type = "etl_pipeline"
    display_name = "ETL流水线"
    description = "编排完整的ETL流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            extract_params = params.get("extract", {})
            transform_params = params.get("transform", {})
            load_params = params.get("load", {})
            steps = params.get("steps", [])
            on_error = params.get("on_error", "stop")

            extracted_data = None
            transformed_data = None

            if steps:
                for step in steps:
                    step_type = step.get("type", "")
                    step_params = step.get("params", {})

                    if step_type == "extract":
                        extract_action = ETLExtractAction()
                        result = extract_action.execute(context, {**extract_params, **step_params})
                        if not result.success:
                            if on_error == "stop":
                                return result
                            continue
                        extracted_data = result.data.get("records", [])

                    elif step_type == "transform":
                        transform_action = ETLTransformAction()
                        result = transform_action.execute(context, {
                            **transform_params,
                            "data": extracted_data or [],
                            **step_params
                        })
                        if not result.success:
                            if on_error == "stop":
                                return result
                            continue
                        transformed_data = result.data.get("records", [])

                    elif step_type == "load":
                        load_action = ETLLoadAction()
                        result = load_action.execute(context, {
                            **load_params,
                            "data": transformed_data or extracted_data or [],
                            **step_params
                        })
                        if not result.success:
                            if on_error == "stop":
                                return result
                            continue

            return ActionResult(
                success=True,
                message="ETL pipeline completed",
                data={
                    "extracted": len(extracted_data) if extracted_data else 0,
                    "transformed": len(transformed_data) if transformed_data else 0,
                    "loaded": len(transformed_data) if transformed_data else (len(extracted_data) if extracted_data else 0)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"ETL pipeline failed: {str(e)}")
