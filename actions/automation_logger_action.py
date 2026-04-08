"""Automation logger action module for RabAI AutoClick.

Provides automation logging:
- AutomationLoggerAction: Log automation events
- LogFormatterAction: Format log entries
- LogAggregatorAction: Aggregate log entries
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationLoggerAction(BaseAction):
    """Log automation events."""
    action_type = "automation_logger"
    display_name = "自动化日志"
    description = "记录自动化事件"

    def __init__(self):
        super().__init__()
        self._logs = deque(maxlen=10000)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            level = params.get("level", "INFO")
            message = params.get("message", "")
            automation_id = params.get("automation_id", "default")
            metadata = params.get("metadata", {})

            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "level": level,
                "message": message,
                "automation_id": automation_id,
                "metadata": metadata
            }

            self._logs.append(log_entry)

            return ActionResult(
                success=True,
                data={
                    "logged": True,
                    "level": level,
                    "total_logs": len(self._logs)
                },
                message=f"[{level}] {message}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Automation logger error: {str(e)}")


class LogFormatterAction(BaseAction):
    """Format log entries."""
    action_type = "log_formatter"
    display_name = "日志格式化"
    description = "格式化日志条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            format_type = params.get("format", "json")
            log_entry = params.get("log_entry", {})

            if format_type == "json":
                formatted = str(log_entry)
            elif format_type == "text":
                formatted = f"[{log_entry.get('timestamp', '')}] {log_entry.get('level', 'INFO')}: {log_entry.get('message', '')}"
            else:
                formatted = str(log_entry)

            return ActionResult(
                success=True,
                data={"formatted": formatted, "format": format_type},
                message=f"Log formatted as {format_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log formatter error: {str(e)}")


class LogAggregatorAction(BaseAction):
    """Aggregate log entries."""
    action_type = "log_aggregator"
    display_name = "日志聚合"
    description = "聚合日志条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            group_by = params.get("group_by", "level")

            aggregated = {}
            for log in logs:
                key = log.get(group_by, "unknown")
                if key not in aggregated:
                    aggregated[key] = []
                aggregated[key].append(log)

            return ActionResult(
                success=True,
                data={
                    "groups": {k: len(v) for k, v in aggregated.items()},
                    "total_logs": len(logs)
                },
                message=f"Logs aggregated by {group_by}: {len(aggregated)} groups"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log aggregator error: {str(e)}")
