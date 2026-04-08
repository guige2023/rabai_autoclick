"""API logger action module for RabAI AutoClick.

Provides structured logging operations:
- LoggerLogAction: Write log entry
- LoggerBatchAction: Batch write logs
- LoggerQueryAction: Query logs
- LoggerRotateAction: Rotate logs
- LoggerStatsAction: Get log statistics
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LoggerLogAction(BaseAction):
    """Write a log entry."""
    action_type = "logger_log"
    display_name = "写入日志"
    description = "写入结构化日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            level = params.get("level", "INFO")
            message = params.get("message", "")
            metadata = params.get("metadata", {})
            source = params.get("source", "default")

            if not message:
                return ActionResult(success=False, message="message is required")

            log_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "log_entries"):
                context.log_entries = []
            context.log_entries.append({
                "log_id": log_id,
                "level": level,
                "message": message,
                "metadata": metadata,
                "source": source,
                "timestamp": time.time(),
            })

            return ActionResult(
                success=True,
                data={"log_id": log_id, "level": level, "source": source},
                message=f"[{level}] {message[:50]}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Logger log failed: {e}")


class LoggerBatchAction(BaseAction):
    """Batch write log entries."""
    action_type = "logger_batch"
    display_name = "批量日志"
    description = "批量写入日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            entries = params.get("entries", [])
            if not entries:
                return ActionResult(success=False, message="entries list is required")

            if not hasattr(context, "log_entries"):
                context.log_entries = []

            logged_ids = []
            for entry in entries:
                log_id = str(uuid.uuid4())[:8]
                context.log_entries.append({
                    "log_id": log_id,
                    "level": entry.get("level", "INFO"),
                    "message": entry.get("message", ""),
                    "metadata": entry.get("metadata", {}),
                    "source": entry.get("source", "default"),
                    "timestamp": time.time(),
                })
                logged_ids.append(log_id)

            return ActionResult(
                success=True,
                data={"logged_count": len(logged_ids), "log_ids": logged_ids[:5]},
                message=f"Batch logged {len(logged_ids)} entries",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Logger batch failed: {e}")


class LoggerQueryAction(BaseAction):
    """Query log entries."""
    action_type = "logger_query"
    display_name = "查询日志"
    description = "查询日志条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            level = params.get("level", "")
            source = params.get("source", "")
            limit = params.get("limit", 100)

            entries = getattr(context, "log_entries", [])
            filtered = entries

            if level:
                filtered = [e for e in filtered if e["level"] == level]
            if source:
                filtered = [e for e in filtered if e.get("source") == source]

            results = filtered[-limit:]

            return ActionResult(
                success=True,
                data={"results": results, "count": len(results), "total_entries": len(entries)},
                message=f"Query: {len(results)} log entries",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Logger query failed: {e}")


class LoggerRotateAction(BaseAction):
    """Rotate log files."""
    action_type = "logger_rotate"
    display_name = "日志轮转"
    description = "轮转日志文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_entries = params.get("max_entries", 10000)
            strategy = params.get("strategy", "oldest")

            entries = getattr(context, "log_entries", [])
            original_count = len(entries)

            if len(entries) > max_entries:
                if strategy == "oldest":
                    context.log_entries = entries[-max_entries:]
                elif strategy == "newest":
                    context.log_entries = entries[:max_entries]

            rotated_count = original_count - len(context.log_entries)

            return ActionResult(
                success=True,
                data={"rotated_count": rotated_count, "remaining": len(context.log_entries), "strategy": strategy},
                message=f"Rotated {rotated_count} log entries ({strategy})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Logger rotate failed: {e}")


class LoggerStatsAction(BaseAction):
    """Get log statistics."""
    action_type = "logger_stats"
    display_name = "日志统计"
    description = "获取日志统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            entries = getattr(context, "log_entries", [])

            level_counts = {}
            source_counts = {}
            for e in entries:
                level_counts[e["level"]] = level_counts.get(e["level"], 0) + 1
                source = e.get("source", "unknown")
                source_counts[source] = source_counts.get(source, 0) + 1

            return ActionResult(
                success=True,
                data={"total_entries": len(entries), "level_counts": level_counts, "source_counts": source_counts},
                message=f"Log stats: {len(entries)} entries across {len(source_counts)} sources",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Logger stats failed: {e}")
