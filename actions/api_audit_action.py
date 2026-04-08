"""API Audit Action.

Records API calls for compliance auditing and security review.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import hashlib
import json


@dataclass
class AuditEntry:
    entry_id: str
    timestamp: float
    method: str
    endpoint: str
    request_headers: Dict[str, str]
    request_body_hash: Optional[str]
    response_status: int
    user_id: Optional[str]
    ip_address: Optional[str]
    duration_ms: float
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "method": self.method,
            "endpoint": self.endpoint,
            "response_status": self.response_status,
            "user_id": self.user_id,
            "ip_address": self.ip_address,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "metadata": self.metadata,
        }


class APIAuditAction:
    """Audits API calls for compliance and security."""

    def __init__(self, max_entries: int = 100000) -> None:
        self.max_entries = max_entries
        self.entries: List[AuditEntry] = []
        self._counter = 0

    def record(
        self,
        method: str,
        endpoint: str,
        request_headers: Optional[Dict[str, str]] = None,
        request_body: Optional[bytes] = None,
        response_status: int = 200,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        duration_ms: float = 0.0,
        error: Optional[str] = None,
        **metadata,
    ) -> AuditEntry:
        body_hash = None
        if request_body:
            body_hash = hashlib.sha256(request_body).hexdigest()
        entry = AuditEntry(
            entry_id=f"audit_{int(time.time()*1000)}_{self._counter}",
            timestamp=time.time(),
            method=method,
            endpoint=endpoint,
            request_headers=request_headers or {},
            request_body_hash=body_hash,
            response_status=response_status,
            user_id=user_id,
            ip_address=ip_address,
            duration_ms=duration_ms,
            error=error,
            metadata=metadata,
        )
        self._counter += 1
        self.entries.append(entry)
        if len(self.entries) > self.max_entries:
            self.entries.pop(0)
        return entry

    def query(
        self,
        method: Optional[str] = None,
        endpoint: Optional[str] = None,
        user_id: Optional[str] = None,
        since: Optional[float] = None,
        until: Optional[float] = None,
        limit: int = 100,
    ) -> List[AuditEntry]:
        result = self.entries
        if method:
            result = [e for e in result if e.method == method]
        if endpoint:
            result = [e for e in result if endpoint in e.endpoint]
        if user_id:
            result = [e for e in result if e.user_id == user_id]
        if since:
            result = [e for e in result if e.timestamp >= since]
        if until:
            result = [e for e in result if e.timestamp <= until]
        return result[-limit:]

    def export_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump([e.to_dict() for e in self.entries], f, indent=2)
