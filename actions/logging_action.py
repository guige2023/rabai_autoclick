"""
Logging utilities - structured logging, log parsing, log levels, formatting.
"""
from typing import Any, Dict, List, Optional
import logging
import json
import re
import time
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


def _parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    patterns = [
        (r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^\s]*)\s+\[(?P<level>\w+)\]\s+(?P<message>.+)$", "standard"),
        (r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^\s]*)\s+(?P<level>\w+)\s+(?P<logger>\S+)\s+(?P<message>.+)$", "detailed"),
        (r"^\[(?P<level>\w+)\]\s+(?P<timestamp>[^\]]+)\]\s+(?P<message>.+)$", "bracket"),
        (r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+(?P<message>.+)$", "simple"),
    ]
    for pattern, fmt in patterns:
        match = re.match(pattern, line.strip())
        if match:
            return {"format": fmt, **match.groupdict(), "raw": line}
    return None


def _json_log(level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> str:
    log_obj = {"timestamp": datetime.utcnow().isoformat() + "Z", "level": level.upper(), "message": message}
    if extra:
        log_obj.update(extra)
    return json.dumps(log_obj)


class LoggingAction(BaseAction):
    """Logging operations.

    Provides structured logging, log parsing, level filtering, format conversion.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "parse")
        text = params.get("text", "")
        level = params.get("level", "INFO")
        message = params.get("message", "")

        try:
            if operation == "parse":
                if not text:
                    return {"success": False, "error": "text required"}
                lines = text.strip().split("\n")
                parsed = []
                for line in lines:
                    if line.strip():
                        p = _parse_log_line(line)
                        if p:
                            parsed.append(p)
                return {"success": True, "logs": parsed, "count": len(parsed), "raw_lines": len(lines)}

            elif operation == "filter_level":
                if not text:
                    return {"success": False, "error": "text required"}
                lines = text.strip().split("\n")
                min_level = getattr(LogLevel, level.upper(), LogLevel.INFO).value
                filtered = []
                for line in lines:
                    parsed = _parse_log_line(line)
                    if parsed:
                        log_level = getattr(LogLevel, parsed.get("level", "INFO").upper(), LogLevel.INFO).value
                        if log_level >= min_level:
                            filtered.append(parsed)
                return {"success": True, "logs": filtered, "count": len(filtered), "level": level}

            elif operation == "json_log":
                extra = params.get("extra", {})
                result = _json_log(level, message, extra)
                return {"success": True, "log": result}

            elif operation == "log_stats":
                if not text:
                    return {"success": False, "error": "text required"}
                lines = text.strip().split("\n")
                counts: Dict[str, int] = {}
                for line in lines:
                    parsed = _parse_log_line(line)
                    if parsed:
                        lvl = parsed.get("level", "UNKNOWN").upper()
                        counts[lvl] = counts.get(lvl, 0) + 1
                total = sum(counts.values())
                return {"success": True, "counts": counts, "total": total}

            elif operation == "extract_errors":
                if not text:
                    return {"success": False, "error": "text required"}
                lines = text.strip().split("\n")
                errors = []
                for line in lines:
                    if "ERROR" in line or "CRITICAL" in line or "exception" in line.lower():
                        parsed = _parse_log_line(line)
                        errors.append(parsed if parsed else {"raw": line})
                return {"success": True, "errors": errors, "count": len(errors)}

            elif operation == "format_text":
                fmt = params.get("format", "json")
                timestamp = datetime.now().isoformat()
                if fmt == "json":
                    result = json.dumps({"timestamp": timestamp, "level": level.upper(), "message": message})
                elif fmt == "simple":
                    result = f"[{level.upper()}] {message}"
                elif fmt == "standard":
                    result = f"{timestamp} [{level.upper()}] {message}"
                elif fmt == "detailed":
                    result = f"{timestamp} {level.upper()} root {message}"
                else:
                    result = message
                return {"success": True, "formatted": result}

            elif operation == "group_by_level":
                if not text:
                    return {"success": False, "error": "text required"}
                lines = text.strip().split("\n")
                by_level: Dict[str, List[Dict[str, Any]]] = {}
                for line in lines:
                    parsed = _parse_log_line(line)
                    if parsed:
                        lvl = parsed.get("level", "UNKNOWN").upper()
                        if lvl not in by_level:
                            by_level[lvl] = []
                        by_level[lvl].append(parsed)
                return {"success": True, "by_level": by_level}

            elif operation == "tail":
                if not text:
                    return {"success": False, "error": "text required"}
                lines = text.strip().split("\n")
                n = int(params.get("n", 10))
                tail_lines = lines[-n:]
                parsed = []
                for line in tail_lines:
                    p = _parse_log_line(line)
                    parsed.append(p if p else {"raw": line})
                return {"success": True, "logs": parsed, "count": len(parsed)}

            elif operation == "grep":
                if not text:
                    return {"success": False, "error": "text required"}
                pattern = params.get("pattern", "")
                if not pattern:
                    return {"success": False, "error": "pattern required"}
                regex = re.compile(pattern)
                lines = text.strip().split("\n")
                matched = []
                for line in lines:
                    if regex.search(line):
                        parsed = _parse_log_line(line)
                        matched.append(parsed if parsed else {"raw": line})
                return {"success": True, "matches": matched, "count": len(matched)}

            elif operation == "anonymize":
                if not text:
                    return {"success": False, "error": "text required"}
                import re
                email_pattern = r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b"
                ip_pattern = r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"
                result = re.sub(email_pattern, "[EMAIL]", text)
                result = re.sub(ip_pattern, "[IP]", result)
                return {"success": True, "anonymized": result}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"LoggingAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for logging operations."""
    return LoggingAction().execute(context, params)
