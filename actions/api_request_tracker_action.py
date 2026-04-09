"""
API Request Tracking and Audit Module.

Tracks all API requests with full request/response logging,
correlation IDs, and audit trails for compliance and debugging.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class RequestStatus(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    SUCCESS = auto()
    CLIENT_ERROR = auto()
    SERVER_ERROR = auto()
    TIMEOUT = auto()
    CANCELLED = auto()


@dataclass
class TrackedRequest:
    request_id: str
    correlation_id: Optional[str]
    method: str
    url: str
    headers: Tuple[Tuple[str, str], ...]
    body: Optional[bytes]
    timestamp: float = field(default_factory=time.time)
    status: RequestStatus = RequestStatus.PENDING
    response_status: Optional[int] = None
    response_body: Optional[bytes] = None
    response_headers: Optional[Tuple[Tuple[str, str], ...]] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    client_ip: str = ""
    user_agent: str = ""


@dataclass
class AuditEntry:
    timestamp: float
    request_id: str
    event_type: str
    actor: str
    resource: str
    action: str
    result: str
    metadata: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)


class RequestIdGenerator:
    """Generates unique request and correlation IDs."""

    @staticmethod
    def generate_request_id() -> str:
        return f"req_{int(time.time() * 1000)}_{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"

    @staticmethod
    def generate_correlation_id() -> str:
        return f"corr_{hashlib.sha256(str(time.time()).encode()).hexdigest()[:16]}"

    @staticmethod
    def generate_trace_id() -> str:
        return hashlib.sha256(str(time.time()).encode()).hexdigest()[:32]


class APIRequestTracker:
    """
    Tracks API requests with full audit logging.
    """

    def __init__(self, retention_seconds: int = 86400):
        self.retention_seconds = retention_seconds
        self._requests: Dict[str, TrackedRequest] = {}
        self._correlation_index: Dict[str, List[str]] = defaultdict(list)
        self._audit_log: List[AuditEntry] = []
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running: bool = False

    def start(self) -> None:
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Request tracker started")

    async def stop(self) -> None:
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Request tracker stopped")

    async def _cleanup_loop(self) -> None:
        while self._running:
            await asyncio.sleep(3600)
            self._cleanup_old_requests()

    def _cleanup_old_requests(self) -> None:
        cutoff = time.time() - self.retention_seconds
        to_delete = [rid for rid, req in self._requests.items() if req.timestamp < cutoff]
        for rid in to_delete:
            del self._requests[rid]
        if to_delete:
            logger.debug("Cleaned up %d old requests", len(to_delete))

    def begin_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        correlation_id: Optional[str] = None,
        client_ip: str = "",
        user_agent: str = "",
    ) -> str:
        request_id = RequestIdGenerator.generate_request_id()
        if not correlation_id:
            correlation_id = RequestIdGenerator.generate_correlation_id()

        tracked = TrackedRequest(
            request_id=request_id,
            correlation_id=correlation_id,
            method=method.upper(),
            url=url,
            headers=tuple((k, v) for k, v in (headers or {}).items()),
            body=body,
            client_ip=client_ip,
            user_agent=user_agent,
        )

        self._requests[request_id] = tracked
        self._correlation_index[correlation_id].append(request_id)
        tracked.status = RequestStatus.IN_PROGRESS
        tracked.started_at = time.time()

        self._add_audit_entry(
            request_id,
            "request_started",
            client_ip or "system",
            url,
            method,
            "pending",
        )

        logger.debug("Started tracking request %s [%s]", request_id, correlation_id)
        return request_id

    def complete_request(
        self,
        request_id: str,
        status_code: int,
        response_body: Optional[bytes] = None,
        response_headers: Optional[Dict[str, str]] = None,
        error: Optional[str] = None,
    ) -> None:
        tracked = self._requests.get(request_id)
        if not tracked:
            logger.warning("Request %s not found for completion", request_id)
            return

        tracked.completed_at = time.time()
        tracked.latency_ms = (tracked.completed_at - (tracked.started_at or 0)) * 1000
        tracked.response_status = status_code
        tracked.response_body = response_body
        tracked.response_headers = tuple((k, v) for k, v in (response_headers or {}).items())

        if 200 <= status_code < 300:
            tracked.status = RequestStatus.SUCCESS
        elif 400 <= status_code < 500:
            tracked.status = RequestStatus.CLIENT_ERROR
        elif 500 <= status_code < 600:
            tracked.status = RequestStatus.SERVER_ERROR
        else:
            tracked.status = RequestStatus.SUCCESS

        if error:
            tracked.error = error
            tracked.status = RequestStatus.SERVER_ERROR

        self._add_audit_entry(
            request_id,
            "request_completed",
            tracked.client_ip,
            tracked.url,
            tracked.method,
            tracked.status.name,
            metadata=[("status_code", str(status_code)), ("latency_ms", str(tracked.latency_ms))],
        )

        logger.debug(
            "Completed request %s: %s (%d, %.1fms)",
            request_id,
            tracked.status.name,
            status_code,
            tracked.latency_ms,
        )

    def cancel_request(self, request_id: str, reason: str = "") -> None:
        tracked = self._requests.get(request_id)
        if tracked:
            tracked.status = RequestStatus.CANCELLED
            tracked.completed_at = time.time()
            tracked.error = reason
            self._add_audit_entry(
                request_id,
                "request_cancelled",
                tracked.client_ip,
                tracked.url,
                tracked.method,
                "cancelled",
                metadata=[("reason", reason)],
            )

    def _add_audit_entry(
        self,
        request_id: str,
        event_type: str,
        actor: str,
        resource: str,
        action: str,
        result: str,
        metadata: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        entry = AuditEntry(
            timestamp=time.time(),
            request_id=request_id,
            event_type=event_type,
            actor=actor,
            resource=resource,
            action=action,
            result=result,
            metadata=tuple(metadata or []),
        )
        self._audit_log.append(entry)

    def get_request(self, request_id: str) -> Optional[TrackedRequest]:
        return self._requests.get(request_id)

    def get_requests_by_correlation(
        self, correlation_id: str
    ) -> List[TrackedRequest]:
        request_ids = self._correlation_index.get(correlation_id, [])
        return [self._requests[rid] for rid in request_ids if rid in self._requests]

    def get_requests_by_status(
        self, status: RequestStatus, limit: int = 100
    ) -> List[TrackedRequest]:
        results = [
            req for req in self._requests.values() if req.status == status
        ]
        return sorted(results, key=lambda r: r.timestamp, reverse=True)[:limit]

    def get_failed_requests(
        self, since: Optional[float] = None, limit: int = 100
    ) -> List[TrackedRequest]:
        failed_statuses = {RequestStatus.CLIENT_ERROR, RequestStatus.SERVER_ERROR, RequestStatus.TIMEOUT}
        results = [
            req for req in self._requests.values()
            if req.status in failed_statuses
            and (since is None or req.timestamp >= since)
        ]
        return sorted(results, key=lambda r: r.timestamp, reverse=True)[:limit]

    def get_slow_requests(
        self, threshold_ms: float = 1000, limit: int = 100
    ) -> List[TrackedRequest]:
        results = [
            req for req in self._requests.values()
            if req.latency_ms is not None and req.latency_ms >= threshold_ms
        ]
        return sorted(results, key=lambda r: r.latency_ms or 0, reverse=True)[:limit]

    def get_audit_log(
        self,
        request_id: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        entries = self._audit_log
        if request_id:
            entries = [e for e in entries if e.request_id == request_id]
        if since:
            entries = [e for e in entries if e.timestamp >= since]
        entries = sorted(entries, key=lambda e: e.timestamp, reverse=True)[:limit]
        return [
            {
                "timestamp": e.timestamp,
                "request_id": e.request_id,
                "event_type": e.event_type,
                "actor": e.actor,
                "resource": e.resource,
                "action": e.action,
                "result": e.result,
                "metadata": dict(e.metadata),
            }
            for e in entries
        ]

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._requests)
        by_status = defaultdict(int)
        total_latency = 0.0
        counted = 0

        for req in self._requests.values():
            by_status[req.status.name] += 1
            if req.latency_ms is not None:
                total_latency += req.latency_ms
                counted += 1

        return {
            "total_requests": total,
            "by_status": dict(by_status),
            "avg_latency_ms": total_latency / max(counted, 1),
            "retention_seconds": self.retention_seconds,
            "audit_entries": len(self._audit_log),
        }
