"""Data extractor action module for RabAI AutoClick.

Provides data extraction operations:
- DataExtractorAction: Extract data from sources
- FieldExtractorAction: Extract specific fields
- PatternExtractorAction: Extract data by patterns
- JSONPathExtractorAction: Extract using JSONPath
- StreamExtractorAction: Extract from streams
"""

import re
import json
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataExtractorAction(BaseAction):
    """Extract data from sources."""
    action_type = "data_extractor"
    display_name = "数据提取"
    description = "从来源提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            extraction_type = params.get("extraction_type", "jsonpath")
            query = params.get("query", "")
            default_value = params.get("default", None)

            if not source:
                return ActionResult(success=False, message="source is required")

            if extraction_type == "jsonpath":
                extracted = self._extract_jsonpath(source, query)
            elif extraction_type == "xpath":
                extracted = self._extract_xpath(source, query)
            elif extraction_type == "regex":
                extracted = self._extract_regex(source, query)
            elif extraction_type == "field":
                extracted = self._extract_field(source, query)
            else:
                extracted = source

            if extracted is None:
                extracted = default_value

            return ActionResult(
                success=True,
                data={
                    "extraction_type": extraction_type,
                    "query": query,
                    "extracted": extracted,
                    "is_default": extracted == default_value and extracted is not None,
                    "extracted_at": datetime.now().isoformat()
                },
                message=f"Extracted data using {extraction_type}: {str(extracted)[:50]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data extractor error: {str(e)}")

    def _extract_jsonpath(self, source: str, query: str) -> Any:
        try:
            data = json.loads(source) if isinstance(source, str) else source
            parts = query.replace("$.", "").split(".")
            current = data
            for part in parts:
                if part and part in current:
                    current = current[part]
                else:
                    return None
            return current
        except:
            return None

    def _extract_xpath(self, source: str, query: str) -> Any:
        return source

    def _extract_regex(self, source: str, pattern: str) -> Any:
        try:
            match = re.search(pattern, source)
            return match.group(0) if match else None
        except:
            return None

    def _extract_field(self, source: Any, field: str) -> Any:
        if isinstance(source, dict):
            return source.get(field)
        return None


class FieldExtractorAction(BaseAction):
    """Extract specific fields from data."""
    action_type = "field_extractor"
    display_name = "字段提取"
    description = "提取特定字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            fields = params.get("fields", [])
            include_missing = params.get("include_missing", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            extracted = {}
            missing_fields = []

            for field in fields:
                if isinstance(data, dict):
                    if field in data:
                        extracted[field] = data[field]
                    else:
                        missing_fields.append(field)
                        if include_missing:
                            extracted[field] = None

            return ActionResult(
                success=len(missing_fields) == 0,
                data={
                    "extracted": extracted,
                    "missing_fields": missing_fields,
                    "extracted_count": len(extracted),
                    "missing_count": len(missing_fields)
                },
                message=f"Extracted {len(extracted)} fields, {len(missing_fields)} missing"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Field extractor error: {str(e)}")


class PatternExtractorAction(BaseAction):
    """Extract data by patterns."""
    action_type = "pattern_extractor"
    display_name = "模式提取"
    description = "按模式提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            patterns = params.get("patterns", [])
            flags = params.get("flags", 0)
            return_all = params.get("return_all", True)

            if not text:
                return ActionResult(success=False, message="text is required")

            if not patterns:
                return ActionResult(success=False, message="patterns is required")

            results = {}
            for pattern in patterns:
                try:
                    compiled = re.compile(pattern, flags)
                    matches = compiled.findall(text)
                    if return_all:
                        results[pattern] = matches
                    else:
                        results[pattern] = matches[0] if matches else None
                except Exception as e:
                    results[pattern] = None

            all_matches = sum(len(v) if isinstance(v, list) else (1 if v else 0) for v in results.values())

            return ActionResult(
                success=True,
                data={
                    "patterns_used": len(patterns),
                    "total_matches": all_matches,
                    "results": results
                },
                message=f"Pattern extraction: {all_matches} matches from {len(patterns)} patterns"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pattern extractor error: {str(e)}")


class JSONPathExtractorAction(BaseAction):
    """Extract using JSONPath."""
    action_type = "jsonpath_extractor"
    display_name = "JSONPath提取"
    description = "使用JSONPath提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            json_data = params.get("json", {})
            jsonpath_expr = params.get("jsonpath", "$")
            default_value = params.get("default", None)

            if not json_data:
                return ActionResult(success=False, message="json is required")

            if isinstance(json_data, str):
                try:
                    json_data = json.loads(json_data)
                except:
                    return ActionResult(success=False, message="Invalid JSON string")

            result = self._evaluate_jsonpath(json_data, jsonpath_expr)

            if result is None:
                result = default_value

            return ActionResult(
                success=True,
                data={
                    "jsonpath": jsonpath_expr,
                    "result": result,
                    "is_default": result == default_value and result is not None,
                    "result_type": type(result).__name__
                },
                message=f"JSONPath '{jsonpath_expr}': {str(result)[:50]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSONPath extractor error: {str(e)}")

    def _evaluate_jsonpath(self, data: Any, path: str) -> Any:
        try:
            parts = path.replace("$.", "").split(".")
            current = data
            for part in parts:
                if not part:
                    continue
                if isinstance(current, dict):
                    current = current.get(part)
                elif isinstance(current, list):
                    try:
                        idx = int(part)
                        current = current[idx] if idx < len(current) else None
                    except:
                        current = None
                else:
                    return None
            return current
        except:
            return None


class StreamExtractorAction(BaseAction):
    """Extract from data streams."""
    action_type = "stream_extractor"
    display_name = "流提取"
    description = "从数据流提取"

    def __init__(self):
        super().__init__()
        self._stream_state = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "extract")
            stream_id = params.get("stream_id", "default")
            chunk = params.get("chunk", None)
            extraction_query = params.get("query", "")

            if operation == "start":
                self._stream_state[stream_id] = {
                    "chunks": [],
                    "extracted": [],
                    "started_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "stream_id": stream_id,
                        "started": True
                    },
                    message=f"Stream extractor '{stream_id}' started"
                )

            elif operation == "add_chunk":
                if stream_id not in self._stream_state:
                    self._stream_state[stream_id] = {"chunks": [], "extracted": [], "started_at": datetime.now().isoformat()}

                self._stream_state[stream_id]["chunks"].append({
                    "data": chunk,
                    "added_at": datetime.now().isoformat()
                })

                return ActionResult(
                    success=True,
                    data={
                        "stream_id": stream_id,
                        "chunk_count": len(self._stream_state[stream_id]["chunks"])
                    },
                    message=f"Chunk added to stream '{stream_id}'"
                )

            elif operation == "extract":
                if stream_id not in self._stream_state:
                    return ActionResult(success=False, message=f"Stream '{stream_id}' not found")

                chunks = self._stream_state[stream_id]["chunks"]
                extracted = [chunk.get("data") for chunk in chunks]

                return ActionResult(
                    success=True,
                    data={
                        "stream_id": stream_id,
                        "extracted_count": len(extracted),
                        "extracted": extracted
                    },
                    message=f"Extracted {len(extracted)} items from stream '{stream_id}'"
                )

            elif operation == "close":
                if stream_id in self._stream_state:
                    del self._stream_state[stream_id]
                return ActionResult(
                    success=True,
                    data={"stream_id": stream_id, "closed": True},
                    message=f"Stream extractor '{stream_id}' closed"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Stream extractor error: {str(e)}")
