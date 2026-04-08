"""API Observer Action Module.

Observes API traffic, logs requests/responses, and emits events for monitoring.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import json


@dataclass
class APIEvent:
    timestamp: float
    method: str
    url: str
    status_code: Optional[int]
    duration_ms: float
    request_size: int
    response_size: int
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "method": self.method,
            "url": self.url,
            "status_code": self.status_code,
            "duration_ms": self.duration_ms,
            "request_size": self.request_size,
            "response_size": self.response_size,
            "error": self.error,
            "metadata": self.metadata,
        }


class APIObserverAction:
    """Observes API calls and emits structured events."""

    def __init__(
        self,
        filters: Optional[List[Callable[[APIEvent], bool]]] = None,
        handlers: Optional[List[Callable[[APIEvent], None]]] = None,
        max_events: int = 10000,
    ) -> None:
        self.filters = filters or []
        self.handlers = handlers or []
        self.events: List[APIEvent] = []
        self.max_events = max_events
        self._enabled = True

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def observe(
        self,
        method: str,
        url: str,
        status_code: Optional[int],
        duration_ms: float,
        request_size: int = 0,
        response_size: int = 0,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> APIEvent:
        event = APIEvent(
            timestamp=time.time(),
            method=method.upper(),
            url=url,
            status_code=status_code,
            duration_ms=duration_ms,
            request_size=request_size,
            response_size=response_size,
            error=error,
            metadata=metadata or {},
        )
        if self._enabled:
            if all(f(event) for f in self.filters):
                self.events.append(event)
                if len(self.events) > self.max_events:
                    self.events.pop(0)
                for handler in self.handlers:
                    try:
                        handler(event)
                    except Exception:
                        pass
        return event

    def get_events(
        self,
        method: Optional[str] = None,
        status_code: Optional[int] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[APIEvent]:
        results = self.events
        if method:
            results = [e for e in results if e.method == method.upper()]
        if status_code is not None:
            results = [e for e in results if e.status_code == status_code]
        if since is not None:
            results = [e for e in results if e.timestamp >= since]
        return results[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        if not self.events:
            return {"total": 0, "error_rate": 0.0, "avg_duration_ms": 0.0}
        errors = sum(1 for e in self.events if e.error or (e.status_code and e.status_code >= 400))
        durations = [e.duration_ms for e in self.events if e.duration_ms > 0]
        return {
            "total": len(self.events),
            "error_rate": errors / len(self.events),
            "avg_duration_ms": sum(durations) / len(durations) if durations else 0.0,
            "methods": list(set(e.method for e in self.events)),
        }

    def export_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump([e.to_dict() for e in self.events], f, indent=2)
