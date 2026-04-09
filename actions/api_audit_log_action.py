"""API Audit Log Action Module.

Records detailed audit trails for API requests including timing,
request/response payloads, user context, and compliance metadata.
"""

import time
import json
import uuid
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class AuditEntry:
    entry_id: str
    timestamp: float
    request_id: str
    user_id: Optional[str]
    api_endpoint: str
    method: str
    request_headers: Dict[str, str]
    request_body_size: int
    response_status: int
    response_body_size: int
    latency_ms: float
    success: bool
    error_message: Optional[str]
    tags: List[str]
    metadata: Dict[str, Any]


class APIAuditLogAction:
    """Records comprehensive audit logs for API operations."""

    def __init__(
        self,
        log_dir: str = "/tmp/api_audit_logs",
        max_entries: int = 100000,
        retention_days: int = 90,
        compress_old: bool = True,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.max_entries = max_entries
        self.retention_days = retention_days
        self.compress_old = compress_old
        self._entries: List[AuditEntry] = []
        self._index: Dict[str, int] = {}
        self._load_recent()

    def log_request(
        self,
        request_id: Optional[str],
        user_id: Optional[str],
        endpoint: str,
        method: str,
        request_headers: Dict[str, str],
        request_body_size: int,
        response_status: int,
        response_body_size: int,
        latency_ms: float,
        success: bool,
        error_message: Optional[str] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        entry_id = str(uuid.uuid4())[:16]
        request_id = request_id or str(uuid.uuid4())
        entry = AuditEntry(
            entry_id=entry_id,
            timestamp=time.time(),
            request_id=request_id,
            user_id=user_id,
            api_endpoint=endpoint,
            method=method,
            request_headers=self._sanitize_headers(request_headers),
            request_body_size=request_body_size,
            response_status=response_status,
            response_body_size=response_body_size,
            latency_ms=latency_ms,
            success=success,
            error_message=error_message,
            tags=tags or [],
            metadata=metadata or {},
        )
        self._entries.append(entry)
        self._index[entry_id] = len(self._entries) - 1
        if len(self._entries) > self.max_entries:
            self._prune_old()
        return entry_id

    def query(
        self,
        request_id: Optional[str] = None,
        user_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        success: Optional[bool] = None,
        tags: Optional[List[str]] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        results = []
        for entry in self._entries:
            if request_id and entry.request_id != request_id:
                continue
            if user_id and entry.user_id != user_id:
                continue
            if endpoint and entry.api_endpoint != endpoint:
                continue
            if method and entry.method != method:
                continue
            if success is not None and entry.success != success:
                continue
            if tags and not any(t in entry.tags for t in tags):
                continue
            if start_time and entry.timestamp < start_time:
                continue
            if end_time and entry.timestamp > end_time:
                continue
            results.append(self._entry_to_dict(entry))
            if len(results) >= limit:
                break
        return results

    def get_failed_requests(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        return self.query(success=False, start_time=start_time, end_time=end_time, limit=limit)

    def get_user_activity(
        self,
        user_id: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return self.query(user_id=user_id, limit=limit)

    def get_endpoint_stats(
        self,
        endpoint: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> Dict[str, Any]:
        entries = self.query(endpoint=endpoint, start_time=start_time, end_time=end_time, limit=self.max_entries)
        if not entries:
            return {"count": 0}
        total_latency = sum(e["latency_ms"] for e in entries)
        failed = sum(1 for e in entries if not e["success"])
        return {
            "count": len(entries),
            "success_count": len(entries) - failed,
            "failure_count": failed,
            "failure_rate": failed / len(entries),
            "avg_latency_ms": total_latency / len(entries),
            "min_latency_ms": min(e["latency_ms"] for e in entries),
            "max_latency_ms": max(e["latency_ms"] for e in entries),
        }

    def export_to_file(self, filepath: str, format: str = "jsonl") -> int:
        count = 0
        with open(filepath, "w") as f:
            for entry in self._entries:
                if format == "jsonl":
                    f.write(json.dumps(self._entry_to_dict(entry)) + "\n")
                else:
                    f.write(json.dumps(self._entry_to_dict(entry), indent=2))
                count += 1
        logger.info(f"Exported {count} audit entries to {filepath}")
        return count

    def _entry_to_dict(self, entry: AuditEntry) -> Dict[str, Any]:
        return {
            "entry_id": entry.entry_id,
            "timestamp": entry.timestamp,
            "datetime": datetime.fromtimestamp(entry.timestamp).isoformat(),
            "request_id": entry.request_id,
            "user_id": entry.user_id,
            "api_endpoint": entry.api_endpoint,
            "method": entry.method,
            "request_body_size": entry.request_body_size,
            "response_status": entry.response_status,
            "response_body_size": entry.response_body_size,
            "latency_ms": entry.latency_ms,
            "success": entry.success,
            "error_message": entry.error_message,
            "tags": entry.tags,
            "metadata": entry.metadata,
        }

    def _sanitize_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        sensitive = {"authorization", "cookie", "x-api-key", "x-auth-token"}
        return {
            k: "***REDACTED***" if k.lower() in sensitive else v
            for k, v in headers.items()
        }

    def _load_recent(self) -> None:
        index_path = self.log_dir / "audit_index.json"
        if index_path.exists():
            try:
                with open(index_path) as f:
                    raw = json.load(f)
                    self._entries = [
                        AuditEntry(**e) for e in raw.get("entries", [])
                    ]
                    self._index = {e.entry_id: i for i, e in enumerate(self._entries)}
            except Exception as e:
                logger.warning(f"Failed to load audit index: {e}")

    def _save_index(self) -> None:
        index_path = self.log_dir / "audit_index.json"
        with open(index_path, "w") as f:
            json.dump(
                {"entries": [self._entry_to_dict(e) for e in self._entries[-10000:]]},
                f,
                indent=2,
            )

    def _prune_old(self) -> int:
        cutoff = time.time() - (self.retention_days * 86400)
        original_count = len(self._entries)
        self._entries = [e for e in self._entries if e.timestamp > cutoff]
        self._index = {e.entry_id: i for i, e in enumerate(self._entries)}
        pruned = original_count - len(self._entries)
        if pruned > 0:
            self._save_index()
            logger.info(f"Pruned {pruned} old audit entries")
        return pruned
