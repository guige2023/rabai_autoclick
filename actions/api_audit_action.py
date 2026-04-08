"""API Audit Action Module. Audits API access and maintains audit trails."""
import sys, os, time, json, hashlib, uuid
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class AuditEntry:
    entry_id: str; timestamp: str; action: str; resource: str; user: str
    ip_address: Optional[str]; result: str; duration_ms: float
    metadata: dict = field(default_factory=dict)
    hash: Optional[str] = None

class APIAuditAction(BaseAction):
    action_type = "api_audit"; display_name = "API审计"
    description = "记录API访问审计日志"
    def __init__(self) -> None:
        super().__init__(); self._audit_log = []; self._log_file = None
    def _hash_entry(self, entry: AuditEntry) -> str:
        content = json.dumps({"entry_id": entry.entry_id, "timestamp": entry.timestamp,
                             "action": entry.action, "resource": entry.resource,
                             "user": entry.user, "result": entry.result}, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()
    def execute(self, context: Any, params: dict) -> ActionResult:
        action_name = params.get("action", "api_call")
        resource = params.get("resource", "")
        user = params.get("user", "anonymous")
        ip_address = params.get("ip_address")
        result = params.get("result", "success")
        duration_ms = params.get("duration_ms", 0.0)
        metadata = params.get("metadata", {})
        log_file = params.get("log_file")
        url = params.get("url")
        start_time = time.time()
        if url:
            method = params.get("method", "GET").upper()
            headers = params.get("headers", {})
            body = params.get("body")
            try:
                import urllib.request
                serialized = json.dumps(body).encode() if body else None
                req = urllib.request.Request(url, data=serialized, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=30) as response:
                    status = response.status; _ = response.read()
                    result = "success" if 200 <= status < 400 else f"http_{status}"
            except Exception as e: result = f"error: {e}"
            duration_ms = (time.time() - start_time) * 1000
        timestamp = datetime.utcnow().isoformat() + "Z"
        entry_id = uuid.uuid4().hex
        entry = AuditEntry(entry_id=entry_id, timestamp=timestamp, action=action_name,
                           resource=resource or url or "unknown", user=user,
                           ip_address=ip_address, result=result,
                           duration_ms=duration_ms, metadata=metadata)
        entry.hash = self._hash_entry(entry)
        self._audit_log.append(entry)
        if log_file:
            try:
                with open(log_file, "a") as f:
                    f.write(json.dumps(vars(entry), default=str) + "\n")
            except Exception as e:
                return ActionResult(success=False, message=f"Logged but write failed: {e}", data=vars(entry))
        return ActionResult(success=True, message=f"Audit: {action_name} by {user} -> {result}",
                          data={"entry": vars(entry), "total_entries": len(self._audit_log)})
