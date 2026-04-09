"""
API Request Logger Action Module

Structured request/response logging with sanitization,
search capabilities, and analytics.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    """Log severity levels."""
    
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class LogEntry:
    """Structured log entry for API requests."""
    
    id: str
    timestamp: float
    level: LogLevel
    request_id: str
    method: str
    path: str
    status_code: Optional[int] = None
    duration_ms: float = 0
    request_body_size: int = 0
    response_body_size: int = 0
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    request_body: Optional[str] = None
    response_body: Optional[str] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LogConfig:
    """Configuration for request logging."""
    
    enabled: bool = True
    log_body: bool = False
    log_headers: bool = False
    max_body_size: int = 10000
    sanitize_fields: List[str] = field(default_factory=lambda: [
        "password", "token", "secret", "api_key", "authorization", "cookie"
    ])
    retention_days: int = 7
    log_level: LogLevel = LogLevel.INFO


class Sanitizer:
    """Sanitizes sensitive data from logs."""
    
    def __init__(self, fields_to_sanitize: List[str]):
        self.fields_to_sanitize = fields_to_sanitize
    
    def sanitize(self, data: Dict) -> Dict:
        """Remove sensitive fields from data."""
        result = dict(data)
        
        for field in self.fields_to_sanitize:
            if field in result:
                result[field] = "[REDACTED]"
            
            for key in list(result.keys()):
                if field.lower() in key.lower():
                    result[key] = "[REDACTED]"
        
        return result
    
    def sanitize_headers(self, headers: Dict) -> Dict:
        """Sanitize headers."""
        return self.sanitize(headers)


class LogStorage:
    """Storage backend for log entries."""
    
    def __init__(self, max_entries: int = 100000):
        self.max_entries = max_entries
        self._entries: List[LogEntry] = []
        self._by_request_id: Dict[str, LogEntry] = {}
        self._by_path: Dict[str, List[LogEntry]] = defaultdict(list)
        self._lock = asyncio.Lock()
    
    async def add(self, entry: LogEntry) -> None:
        """Add a log entry."""
        async with self._lock:
            self._entries.append(entry)
            self._by_request_id[entry.request_id] = entry
            self._by_path[entry.path].append(entry)
            
            if len(self._entries) > self.max_entries:
                removed = self._entries.pop(0)
                if removed.request_id in self._by_request_id:
                    del self._by_request_id[removed.request_id]
    
    async def get_by_request_id(self, request_id: str) -> Optional[LogEntry]:
        """Get log entry by request ID."""
        return self._by_request_id.get(request_id)
    
    async def get_by_path(self, path: str, limit: int = 100) -> List[LogEntry]:
        """Get log entries by path."""
        return self._by_path.get(path, [])[-limit:]
    
    async def search(
        self,
        query: Optional[str] = None,
        method: Optional[str] = None,
        path_pattern: Optional[str] = None,
        status_code: Optional[int] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 100
    ) -> List[LogEntry]:
        """Search log entries."""
        results = self._entries
        
        if query:
            query_lower = query.lower()
            results = [
                e for e in results
                if query_lower in e.path.lower() or
                   (e.error and query_lower in e.error.lower())
            ]
        
        if method:
            results = [e for e in results if e.method == method]
        
        if path_pattern:
            import re
            pattern = re.compile(path_pattern)
            results = [e for e in results if pattern.search(e.path)]
        
        if status_code:
            results = [e for e in results if e.status_code == status_code]
        
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]
        
        return results[-limit:]
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get log statistics."""
        if not self._entries:
            return {
                "total_entries": 0,
                "by_method": {},
                "by_status": {},
                "avg_duration_ms": 0
            }
        
        by_method = defaultdict(int)
        by_status = defaultdict(int)
        total_duration = 0
        
        for entry in self._entries:
            by_method[entry.method] += 1
            if entry.status_code:
                by_status[entry.status_code] += 1
            total_duration += entry.duration_ms
        
        return {
            "total_entries": len(self._entries),
            "by_method": dict(by_method),
            "by_status": dict(by_status),
            "avg_duration_ms": total_duration / len(self._entries),
            "oldest_entry": self._entries[0].timestamp if self._entries else None,
            "newest_entry": self._entries[-1].timestamp if self._entries else None
        }


class APIRequestLoggerAction:
    """
    Main API request logger action handler.
    
    Provides structured logging of API requests and responses
    with sanitization, search, and analytics.
    """
    
    def __init__(self, config: Optional[LogConfig] = None):
        self.config = config or LogConfig()
        self.sanitizer = Sanitizer(self.config.sanitize_fields)
        self.storage = LogStorage()
        self._middleware: List[Callable] = []
    
    async def log_request(
        self,
        method: str,
        path: str,
        request_id: Optional[str] = None,
        headers: Optional[Dict] = None,
        body: Optional[Any] = None,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """Log an incoming request."""
        request_id = request_id or str(uuid.uuid4())
        
        headers = headers or {}
        
        request_body = None
        body_size = 0
        
        if self.config.log_body and body:
            request_body = self._serialize_body(body)
            body_size = len(request_body)
        
        if self.config.log_headers:
            headers = self.sanitizer.sanitize_headers(headers)
        
        entry = LogEntry(
            id=str(uuid.uuid4()),
            timestamp=time.time(),
            level=LogLevel.INFO,
            request_id=request_id,
            method=method,
            path=path,
            request_body_size=body_size,
            client_ip=client_ip,
            user_agent=user_agent,
            headers=headers if self.config.log_headers else {},
            request_body=request_body,
            metadata=metadata or {}
        )
        
        await self.storage.add(entry)
        
        for mw in self._middleware:
            await mw("request", entry)
        
        return request_id
    
    async def log_response(
        self,
        request_id: str,
        status_code: int,
        response_body: Optional[Any] = None,
        duration_ms: float = 0,
        error: Optional[str] = None
    ) -> None:
        """Log a response for a request."""
        entry = await self.storage.get_by_request_id(request_id)
        
        if not entry:
            return
        
        response_body_size = 0
        response_body_str = None
        
        if self.config.log_body and response_body:
            response_body_str = self._serialize_body(response_body)
            response_body_size = len(response_body_str)
        
        entry.status_code = status_code
        entry.duration_ms = duration_ms
        entry.response_body_size = response_body_size
        entry.response_body = response_body_str
        entry.error = error
        
        if status_code >= 500:
            entry.level = LogLevel.ERROR
        elif status_code >= 400:
            entry.level = LogLevel.WARNING
        
        for mw in self._middleware:
            await mw("response", entry)
    
    def _serialize_body(self, body: Any) -> str:
        """Serialize request/response body for logging."""
        if isinstance(body, str):
            body = body[:self.config.max_body_size]
        elif isinstance(body, (dict, list)):
            try:
                body = json.dumps(body)[:self.config.max_body_size]
            except Exception:
                body = str(body)[:self.config.max_body_size]
        else:
            body = str(body)[:self.config.max_body_size]
        
        return body
    
    async def search_logs(
        self,
        query: Optional[str] = None,
        method: Optional[str] = None,
        path_pattern: Optional[str] = None,
        status_code: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Search log entries."""
        start_ts = start_time.timestamp() if start_time else None
        end_ts = end_time.timestamp() if end_time else None
        
        entries = await self.storage.search(
            query=query,
            method=method,
            path_pattern=path_pattern,
            status_code=status_code,
            start_time=start_ts,
            end_time=end_ts,
            limit=limit
        )
        
        return [
            {
                "id": e.id,
                "request_id": e.request_id,
                "timestamp": datetime.fromtimestamp(e.timestamp).isoformat(),
                "method": e.method,
                "path": e.path,
                "status_code": e.status_code,
                "duration_ms": e.duration_ms,
                "client_ip": e.client_ip,
                "error": e.error
            }
            for e in entries
        ]
    
    async def get_request_trace(self, request_id: str) -> Optional[Dict]:
        """Get full trace of a request."""
        entry = await self.storage.get_by_request_id(request_id)
        
        if not entry:
            return None
        
        return {
            "id": entry.id,
            "request_id": entry.request_id,
            "timestamp": datetime.fromtimestamp(entry.timestamp).isoformat(),
            "method": entry.method,
            "path": entry.path,
            "status_code": entry.status_code,
            "duration_ms": entry.duration_ms,
            "client_ip": entry.client_ip,
            "headers": entry.headers,
            "request_body": entry.request_body,
            "response_body": entry.response_body,
            "error": entry.error,
            "metadata": entry.metadata
        }
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get logging statistics."""
        stats = await self.storage.get_stats()
        
        recent_entries = await self.storage.search(limit=1000)
        
        error_count = sum(1 for e in recent_entries if e.status_code and e.status_code >= 400)
        success_count = sum(1 for e in recent_entries if e.status_code and e.status_code < 400)
        
        stats["recent_errors"] = error_count
        stats["recent_success"] = success_count
        
        return stats
    
    def add_middleware(self, middleware: Callable) -> None:
        """Add logging middleware."""
        self._middleware.append(middleware)
