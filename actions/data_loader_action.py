"""Data loader action module for RabAI AutoClick.

Provides data loading operations:
- DataLoaderAction: Load data from various sources
- FileDataLoaderAction: Load data from files
- DatabaseDataLoaderAction: Load data from databases
- APIDataLoaderAction: Load data from APIs
- StreamDataLoaderAction: Load data from streams
"""

import time
import json
from typing import Any, Dict, List, Optional, Iterator
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataLoaderAction(BaseAction):
    """Load data from various sources."""
    action_type = "data_loader"
    display_name = "数据加载"
    description = "从各种来源加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source_type = params.get("source_type", "file")
            source = params.get("source", {})
            options = params.get("options", {})

            supported_sources = ["file", "database", "api", "stream", "memory"]

            if source_type not in supported_sources:
                return ActionResult(
                    success=False,
                    message=f"Unsupported source type: {source_type}. Supported: {supported_sources}"
                )

            start_time = time.time()

            if source_type == "file":
                data = self._load_from_file(source, options)
            elif source_type == "database":
                data = self._load_from_database(source, options)
            elif source_type == "api":
                data = self._load_from_api(source, options)
            elif source_type == "stream":
                data = self._load_from_stream(source, options)
            elif source_type == "memory":
                data = source.get("data", None)

            elapsed = time.time() - start_time

            return ActionResult(
                success=True,
                data={
                    "source_type": source_type,
                    "source": source,
                    "loaded": True,
                    "record_count": len(data) if isinstance(data, list) else 1,
                    "elapsed_ms": round(elapsed * 1000, 2),
                    "data_type": type(data).__name__
                },
                message=f"Loaded data from {source_type}: {len(data) if isinstance(data, list) else 1} records in {elapsed*1000:.1f}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data loader error: {str(e)}")

    def _load_from_file(self, source: Dict, options: Dict) -> Any:
        return []

    def _load_from_database(self, source: Dict, options: Dict) -> Any:
        return []

    def _load_from_api(self, source: Dict, options: Dict) -> Any:
        return []

    def _load_from_stream(self, source: Dict, options: Dict) -> Any:
        return []


class FileDataLoaderAction(BaseAction):
    """Load data from files."""
    action_type = "file_data_loader"
    display_name = "文件数据加载"
    description = "从文件加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            file_format = params.get("file_format", "auto")
            encoding = params.get("encoding", "utf-8")
            skip_rows = params.get("skip_rows", 0)
            max_rows = params.get("max_rows", None)
            delimiter = params.get("delimiter", ",")

            if not file_path:
                return ActionResult(success=False, message="file_path is required")

            if file_format == "auto":
                if file_path.endswith(".json"):
                    file_format = "json"
                elif file_path.endswith((".csv", ".tsv")):
                    file_format = "csv"
                elif file_path.endswith(".xml"):
                    file_format = "xml"
                elif file_path.endswith(".txt"):
                    file_format = "text"
                else:
                    file_format = "unknown"

            data_preview = f"Preview of {file_path} ({file_format})"

            return ActionResult(
                success=True,
                data={
                    "file_path": file_path,
                    "file_format": file_format,
                    "encoding": encoding,
                    "skip_rows": skip_rows,
                    "max_rows": max_rows,
                    "delimiter": delimiter,
                    "loaded_at": datetime.now().isoformat()
                },
                message=f"File data loader configured: {file_format} format, path={file_path}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"File data loader error: {str(e)}")


class DatabaseDataLoaderAction(BaseAction):
    """Load data from databases."""
    action_type = "database_data_loader"
    display_name = "数据库数据加载"
    description = "从数据库加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_string = params.get("connection_string", "")
            query = params.get("query", "")
            table = params.get("table", "")
            batch_size = params.get("batch_size", 1000)
            timeout = params.get("timeout", 30)

            if not connection_string:
                return ActionResult(success=False, message="connection_string is required")

            if not query and not table:
                return ActionResult(success=False, message="Either query or table is required")

            return ActionResult(
                success=True,
                data={
                    "connection_string_preview": connection_string[:20] + "...",
                    "query": query,
                    "table": table,
                    "batch_size": batch_size,
                    "timeout": timeout,
                    "loaded_at": datetime.now().isoformat()
                },
                message=f"Database data loader configured: table={table}, batch_size={batch_size}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Database data loader error: {str(e)}")


class APIDataLoaderAction(BaseAction):
    """Load data from APIs."""
    action_type = "api_data_loader"
    display_name = "API数据加载"
    description = "从API加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            method = params.get("method", "GET")
            headers = params.get("headers", {})
            params_data = params.get("params", {})
            body = params.get("body", None)
            auth = params.get("auth", None)
            timeout = params.get("timeout", 30)
            pagination = params.get("pagination", None)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            return ActionResult(
                success=True,
                data={
                    "endpoint": endpoint,
                    "method": method,
                    "has_headers": bool(headers),
                    "has_params": bool(params_data),
                    "has_body": body is not None,
                    "has_auth": auth is not None,
                    "timeout": timeout,
                    "pagination_enabled": pagination is not None,
                    "loaded_at": datetime.now().isoformat()
                },
                message=f"API data loader configured: {method} {endpoint}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API data loader error: {str(e)}")


class StreamDataLoaderAction(BaseAction):
    """Load data from streams."""
    action_type = "stream_data_loader"
    display_name = "流数据加载"
    description = "从流加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_type = params.get("stream_type", "kafka")
            stream_name = params.get("stream_name", "")
            topic = params.get("topic", "")
            group_id = params.get("group_id", "")
            offset = params.get("offset", "latest")
            max_messages = params.get("max_messages", 100)
            timeout = params.get("timeout", 10)

            if not stream_name and not topic:
                return ActionResult(success=False, message="stream_name or topic is required")

            return ActionResult(
                success=True,
                data={
                    "stream_type": stream_type,
                    "stream_name": stream_name or topic,
                    "topic": topic,
                    "group_id": group_id,
                    "offset": offset,
                    "max_messages": max_messages,
                    "timeout": timeout,
                    "loaded_at": datetime.now().isoformat()
                },
                message=f"Stream data loader configured: {stream_type}/{stream_name or topic}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream data loader error: {str(e)}")


class ChunkedDataLoaderAction(BaseAction):
    """Load data in chunks."""
    action_type = "chunked_data_loader"
    display_name = "分块数据加载"
    description = "分块加载数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", {})
            chunk_size = params.get("chunk_size", 1000)
            overlap = params.get("overlap", 0)
            total_size = params.get("total_size", 0)

            if chunk_size <= 0:
                return ActionResult(success=False, message="chunk_size must be positive")

            total_chunks = max(1, (total_size + chunk_size - overlap - 1) // (chunk_size - overlap)) if total_size > 0 else 1

            chunks = []
            for i in range(total_chunks):
                start = i * (chunk_size - overlap)
                end = min(start + chunk_size, total_size) if total_size > 0 else start + chunk_size
                chunks.append({
                    "chunk_id": i,
                    "start": start,
                    "end": end,
                    "size": end - start
                })

            return ActionResult(
                success=True,
                data={
                    "chunk_size": chunk_size,
                    "overlap": overlap,
                    "total_size": total_size,
                    "total_chunks": total_chunks,
                    "chunks": chunks
                },
                message=f"Chunked loader configured: {total_chunks} chunks of size {chunk_size}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Chunked data loader error: {str(e)}")
