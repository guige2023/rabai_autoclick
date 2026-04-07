"""Logging action module for RabAI AutoClick.

Provides logging operations:
- LogMessageAction: Log a message
- LogFormatAction: Format log messages
- LogLevelFilterAction: Filter by log level
- LogAggregateAction: Aggregate logs
- LogSearchAction: Search logs
"""

import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogMessageAction(BaseAction):
    """Log a message."""
    action_type = "log_message"
    display_name = "记录日志"
    description = "记录日志消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            message = params.get("message", "")
            level = params.get("level", "info")
            source = params.get("source", "action")
            metadata = params.get("metadata", {})

            if not message:
                return ActionResult(success=False, message="message is required")

            timestamp = datetime.now().isoformat()
            log_entry = {
                "timestamp": timestamp,
                "level": level.upper(),
                "source": source,
                "message": message,
                "metadata": metadata
            }

            return ActionResult(
                success=True,
                message=f"[{level.upper()}] {message}",
                data={"log_entry": log_entry}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Log error: {str(e)}")


class LogFormatAction(BaseAction):
    """Format log messages."""
    action_type = "log_format"
    display_name = "格式化日志"
    description = "格式化日志消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            format_template = params.get("template", "{timestamp} [{level}] {message}")
            output_format = params.get("output_format", "text")

            if not logs:
                return ActionResult(success=False, message="No logs to format")

            formatted = []

            for log in logs:
                if isinstance(log, str):
                    log = {"message": log, "level": "INFO", "timestamp": datetime.now().isoformat()}

                message = format_template
                for key, value in log.items():
                    placeholder = "{" + key + "}"
                    if placeholder in message:
                        message = message.replace(placeholder, str(value))

                formatted.append(message)

            if output_format == "json":
                result = json.dumps(formatted, ensure_ascii=False)
            elif output_format == "csv":
                import csv
                import io
                output = io.StringIO()
                writer = csv.writer(output)
                for log in logs:
                    if isinstance(log, dict):
                        writer.writerow([log.get("timestamp", ""), log.get("level", ""), log.get("message", "")])
                result = output.getvalue()
            else:
                result = "\n".join(formatted)

            return ActionResult(
                success=True,
                message=f"Formatted {len(logs)} log entries",
                data={"formatted": result, "count": len(formatted)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Format error: {str(e)}")


class LogLevelFilterAction(BaseAction):
    """Filter logs by level."""
    action_type = "log_level_filter"
    display_name = "日志级别过滤"
    description = "按级别过滤日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            min_level = params.get("min_level", "DEBUG")
            max_level = params.get("max_level", "CRITICAL")
            levels = params.get("levels", [])

            if not logs:
                return ActionResult(success=False, message="No logs to filter")

            level_priority = {
                "DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50
            }

            min_prio = level_priority.get(min_level.upper(), 0)
            max_prio = level_priority.get(max_level.upper(), 100)

            filtered = []

            for log in logs:
                if isinstance(log, str):
                    log = {"message": log, "level": "INFO"}

                log_level = log.get("level", "INFO").upper()
                log_prio = level_priority.get(log_level, 20)

                if levels:
                    if log_level in [l.upper() for l in levels]:
                        filtered.append(log)
                else:
                    if min_prio <= log_prio <= max_prio:
                        filtered.append(log)

            removed = len(logs) - len(filtered)

            return ActionResult(
                success=True,
                message=f"Filtered {removed} logs, {len(filtered)} remaining",
                data={"filtered": filtered, "count": len(filtered), "removed": removed}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")


class LogAggregateAction(BaseAction):
    """Aggregate logs."""
    action_type = "log_aggregate"
    display_name = "日志聚合"
    description = "聚合日志数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            group_by = params.get("group_by", "level")
            aggregate = params.get("aggregate", "count")

            if not logs:
                return ActionResult(success=False, message="No logs to aggregate")

            groups = {}

            for log in logs:
                if isinstance(log, str):
                    log = {"message": log, "level": "INFO"}

                if group_by == "level":
                    key = log.get("level", "UNKNOWN")
                elif group_by == "source":
                    key = log.get("source", "unknown")
                elif group_by == "hour":
                    ts = log.get("timestamp", "")
                    if ts:
                        key = ts[:13] + ":00"
                    else:
                        key = "unknown"
                else:
                    key = "all"

                if key not in groups:
                    groups[key] = []
                groups[key].append(log)

            results = []
            for key, group_logs in groups.items():
                result = {"group": key, "count": len(group_logs)}

                if aggregate == "count":
                    result["value"] = len(group_logs)
                elif aggregate == "messages":
                    result["messages"] = [l.get("message", "") for l in group_logs[:10]]
                elif aggregate == "latest":
                    result["latest"] = group_logs[-1] if group_logs else None

                results.append(result)

            results.sort(key=lambda x: x["count"], reverse=True)

            return ActionResult(
                success=True,
                message=f"Aggregated into {len(results)} groups",
                data={"groups": results, "group_count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate error: {str(e)}")


class LogSearchAction(BaseAction):
    """Search logs."""
    action_type = "log_search"
    display_name = "搜索日志"
    description = "搜索日志内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            pattern = params.get("pattern", "")
            regex = params.get("regex", False)
            case_sensitive = params.get("case_sensitive", False)
            field = params.get("field", "message")

            if not logs:
                return ActionResult(success=False, message="No logs to search")

            if not pattern:
                return ActionResult(success=False, message="pattern is required")

            if regex:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    compiled = re.compile(pattern, flags)
                except re.error as e:
                    return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
            else:
                if not case_sensitive:
                    pattern = pattern.lower()

            matches = []

            for log in logs:
                if isinstance(log, str):
                    log = {"message": log}

                search_text = log.get(field, "")
                if not case_sensitive and isinstance(search_text, str):
                    search_text = search_text.lower()

                if regex:
                    if compiled.search(str(log.get(field, ""))):
                        matches.append(log)
                else:
                    if pattern in str(search_text):
                        matches.append(log)

            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={"matches": matches, "count": len(matches), "pattern": pattern}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Search error: {str(e)}")
