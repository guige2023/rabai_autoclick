"""Log parsing action module for RabAI AutoClick.

Provides log operations:
- LogParseAction: Parse log files
- LogFilterAction: Filter log entries
- LogSearchAction: Search logs
- LogStatsAction: Log statistics
- LogAlertAction: Alert on log patterns
- LogRotateAction: Rotate log files
- LogExportAction: Export parsed logs
- LogMonitorAction: Monitor logs in real-time
"""

import gzip
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogParserAction(BaseAction):
    """Parse log files."""
    action_type = "log_parse"
    display_name = "日志解析"
    description = "解析日志文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            format_type = params.get("format", "auto")
            limit = params.get("limit", 1000)
            
            if not file_path:
                return ActionResult(success=False, message="file_path is required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            entries = []
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f):
                    if i >= limit:
                        break
                    entries.append(self._parse_line(line, format_type))
            
            return ActionResult(
                success=True,
                message=f"Parsed {len(entries)} log entries",
                data={"entries": entries[:100], "count": len(entries)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log parse failed: {str(e)}")

    def _parse_line(self, line: str, format_type: str) -> Dict[str, Any]:
        """Parse a single log line."""
        entry = {"raw": line.strip(), "timestamp": None, "level": None, "message": line.strip()}
        
        if format_type == "auto":
            patterns = [
                (r"(\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2})", "timestamp"),
                (r"(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL)", "level"),
                (r"\[(.*?)\]", "component"),
            ]
            for pattern, field in patterns:
                match = re.search(pattern, line)
                if match:
                    if field == "timestamp":
                        entry["timestamp"] = match.group(1)
                    elif field == "level":
                        entry["level"] = match.group(1)
                    else:
                        entry[field] = match.group(1)
        
        return entry


class LogFilterAction(BaseAction):
    """Filter log entries."""
    action_type = "log_filter"
    display_name = "日志过滤"
    description = "过滤日志条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            entries = params.get("entries", [])
            level = params.get("level", "")
            pattern = params.get("pattern", "")
            start_time = params.get("start_time", "")
            end_time = params.get("end_time", "")
            
            filtered = entries
            
            if level:
                filtered = [e for e in filtered if e.get("level", "").upper() == level.upper()]
            
            if pattern:
                try:
                    regex = re.compile(pattern)
                    filtered = [e for e in filtered if regex.search(e.get("raw", ""))]
                except re.error:
                    filtered = [e for e in filtered if pattern.lower() in e.get("raw", "").lower()]
            
            return ActionResult(
                success=True,
                message=f"Filtered to {len(filtered)} entries",
                data={"filtered": filtered[:100], "count": len(filtered), "original_count": len(entries)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log filter failed: {str(e)}")


class LogSearchAction(BaseAction):
    """Search logs for patterns."""
    action_type = "log_search"
    display_name = "日志搜索"
    description = "搜索日志内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            query = params.get("query", "")
            use_regex = params.get("regex", False)
            limit = params.get("limit", 100)
            
            if not file_path or not query:
                return ActionResult(success=False, message="file_path and query required")
            
            matches = []
            pattern = re.compile(query) if use_regex else re.compile(re.escape(query), re.IGNORECASE)
            
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f):
                    if pattern.search(line):
                        matches.append({"line_number": i + 1, "content": line.strip()})
                        if len(matches) >= limit:
                            break
            
            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={"matches": matches, "count": len(matches), "query": query}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log search failed: {str(e)}")


class LogStatsAction(BaseAction):
    """Get log statistics."""
    action_type = "log_stats"
    display_name = "日志统计"
    description = "获取日志统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            entries = params.get("entries", [])
            
            if not file_path and not entries:
                return ActionResult(success=False, message="file_path or entries required")
            
            if file_path and not entries:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    entries = [line.strip() for line in f]
            
            level_counts = Counter()
            timestamp_pattern = r"(\d{4}-\d{2}-\d{2})"
            date_counts = Counter()
            
            for entry in entries:
                level_match = re.search(r"(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL)", entry, re.IGNORECASE)
                if level_match:
                    level_counts[level_match.group(1).upper()] += 1
                
                date_match = re.search(timestamp_pattern, entry)
                if date_match:
                    date_counts[date_match.group(1)] += 1
            
            return ActionResult(
                success=True,
                message=f"Log stats: {len(entries)} entries",
                data={
                    "total_entries": len(entries),
                    "level_counts": dict(level_counts),
                    "date_counts": dict(date_counts.most_common(10)),
                    "error_rate": level_counts.get("ERROR", 0) / len(entries) if entries else 0
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log stats failed: {str(e)}")


class LogAlertAction(BaseAction):
    """Alert on log patterns."""
    action_type = "log_alert"
    display_name = "日志告警"
    description = "日志模式告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            entries = params.get("entries", [])
            patterns = params.get("patterns", [])
            threshold = params.get("threshold", 5)
            window = params.get("window", 60)
            
            if not patterns:
                return ActionResult(success=False, message="patterns required")
            
            alerts = []
            for pattern_def in patterns:
                pattern = pattern_def.get("pattern", "")
                severity = pattern_def.get("severity", "warning")
                regex = re.compile(pattern, re.IGNORECASE)
                
                matches = [e for e in entries if regex.search(e.get("raw", ""))]
                
                if len(matches) >= threshold:
                    alerts.append({
                        "pattern": pattern,
                        "severity": severity,
                        "count": len(matches),
                        "triggered": True
                    })
            
            return ActionResult(
                success=True,
                message=f"Alerts triggered: {len(alerts)}",
                data={"alerts": alerts, "total_entries": len(entries)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log alert failed: {str(e)}")


class LogRotateAction(BaseAction):
    """Rotate log files."""
    action_type = "log_rotate"
    display_name = "日志轮转"
    description = "轮转日志文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            max_size = params.get("max_size_mb", 100)
            keep = params.get("keep", 5)
            
            if not file_path:
                return ActionResult(success=False, message="file_path required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            rotated = []
            
            if size_mb > max_size:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                rotated_path = f"{file_path}.{timestamp}"
                
                import shutil
                shutil.move(file_path, rotated_path)
                rotated.append(rotated_path)
                
                with open(file_path, "w") as f:
                    pass
            
            old_backups = sorted([f for f in os.listdir(os.path.dirname(file_path) or ".") 
                                  if f.startswith(os.path.basename(file_path) + ".")],
                                reverse=True)
            
            for old in old_backups[keep:]:
                old_path = os.path.join(os.path.dirname(file_path) or ".", old)
                if os.path.isfile(old_path):
                    os.remove(old_path)
            
            return ActionResult(
                success=True,
                message=f"Log rotated: {len(rotated)} files",
                data={"rotated": rotated, "size_mb": size_mb}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log rotate failed: {str(e)}")


class LogExportAction(BaseAction):
    """Export parsed logs."""
    action_type = "log_export"
    display_name = "日志导出"
    description = "导出解析的日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            entries = params.get("entries", [])
            format = params.get("format", "json")
            output_path = params.get("output_path", "/tmp/logs_export.json")
            
            if not entries:
                return ActionResult(success=False, message="entries required")
            
            if format == "json":
                with open(output_path, "w") as f:
                    json.dump(entries, f, indent=2)
            elif format == "csv":
                import csv
                with open(output_path, "w", newline="") as f:
                    if entries:
                        writer = csv.DictWriter(f, fieldnames=entries[0].keys())
                        writer.writeheader()
                        writer.writerows(entries)
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format}")
            
            return ActionResult(
                success=True,
                message=f"Exported {len(entries)} entries to {output_path}",
                data={"output_path": output_path, "count": len(entries), "format": format}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log export failed: {str(e)}")


class LogMonitorAction(BaseAction):
    """Monitor logs in real-time."""
    action_type = "log_monitor"
    display_name = "日志监控"
    description = "实时监控日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            file_path = params.get("file_path", "")
            patterns = params.get("patterns", [])
            duration = params.get("duration", 60)
            
            if not file_path:
                return ActionResult(success=False, message="file_path required")
            
            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")
            
            file_size = os.path.getsize(file_path)
            start_time = time.time()
            matches = []
            regexes = [re.compile(p, re.IGNORECASE) for p in patterns] if patterns else None
            
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                f.seek(file_size)
                
                while time.time() - start_time < duration:
                    line = f.readline()
                    if not line:
                        time.sleep(0.5)
                        continue
                    
                    if regexes:
                        for regex in regexes:
                            if regex.search(line):
                                matches.append(line.strip())
                                break
                    else:
                        matches.append(line.strip())
            
            return ActionResult(
                success=True,
                message=f"Monitored for {duration}s, {len(matches)} matches",
                data={"matches": matches[:100], "count": len(matches), "duration": duration}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log monitor failed: {str(e)}")
