"""API Replay Action Module.

Records API requests/responses and supports replay with optional mutations
for regression testing, load testing, and debugging scenarios.
"""

import json
import time
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class RecordedRequest:
    timestamp: float
    request_id: str
    url: str
    method: str
    headers: Dict[str, str]
    body: Optional[bytes]
    response_status: int
    response_body: Optional[bytes]
    response_headers: Dict[str, str]
    latency_ms: float


@dataclass
class ReplayOptions:
    """Options for request replay."""
    speed_multiplier: float = 1.0
    mutate_headers: Dict[str, str] = field(default_factory=dict)
    mutate_url_params: Dict[str, str] = field(default_factory=dict)
    skip_validation: bool = False
    repeat_count: int = 1
    fail_on_mismatch: bool = True


class APIReplayAction:
    """Records and replays API interactions for testing and debugging."""

    def __init__(self, storage_path: Optional[str] = None) -> None:
        self.storage_path = Path(storage_path) if storage_path else Path("/tmp/api_replay")
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self._records: Dict[str, List[RecordedRequest]] = {}
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def record(
        self,
        request_id: str,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        response_status: int,
        response_body: Optional[bytes],
        response_headers: Dict[str, str],
        latency_ms: float,
    ) -> str:
        record = RecordedRequest(
            timestamp=time.time(),
            request_id=request_id,
            url=url,
            method=method,
            headers=dict(headers),
            body=body,
            response_status=response_status,
            response_body=response_body,
            response_headers=dict(response_headers),
            latency_ms=latency_ms,
        )
        if request_id not in self._records:
            self._records[request_id] = []
        self._records[request_id].append(record)
        return self._hash_record(record)

    def _hash_record(self, record: RecordedRequest) -> str:
        content = f"{record.url}:{record.method}:{record.body}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def save_session(self, label: Optional[str] = None) -> str:
        filename = f"replay_{label or self._session_id}.json"
        filepath = self.storage_path / filename
        data = {
            "session_id": self._session_id,
            "saved_at": time.time(),
            "records": [
                {
                    "request_id": r.request_id,
                    "url": r.url,
                    "method": r.method,
                    "headers": r.headers,
                    "body": r.body.decode("utf-8") if r.body else None,
                    "response_status": r.response_status,
                    "response_body": r.response_body.decode("utf-8") if r.response_body else None,
                    "response_headers": r.response_headers,
                    "latency_ms": r.latency_ms,
                    "timestamp": r.timestamp,
                }
                for records in self._records.values()
                for r in records
            ],
        }
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(data['records'])} records to {filepath}")
        return str(filepath)

    def load_session(self, filepath: str) -> int:
        with open(filepath) as f:
            data = json.load(f)
        self._records.clear()
        for item in data.get("records", []):
            record = RecordedRequest(
                timestamp=item["timestamp"],
                request_id=item["request_id"],
                url=url,
                method=item["method"],
                headers=item["headers"],
                body=item["body"].encode("utf-8") if item.get("body") else None,
                response_status=item["response_status"],
                response_body=item["response_body"].encode("utf-8") if item.get("response_body") else None,
                response_headers=item["response_headers"],
                latency_ms=item["latency_ms"],
            )
            if record.request_id not in self._records:
                self._records[record.request_id] = []
            self._records[record.request_id].append(record)
        return len(self._records)

    def replay(
        self,
        request_id: str,
        options: Optional[ReplayOptions] = None,
    ) -> List[Dict[str, Any]]:
        opts = options or ReplayOptions()
        results = []
        records = self._records.get(request_id, [])
        if not records:
            logger.warning(f"No records found for request_id: {request_id}")
            return results
        for i in range(opts.repeat_count):
            for record in records:
                mutated_url = self._mutate_url(record.url, opts.mutate_url_params)
                mutated_headers = {**record.headers, **opts.mutate_headers}
                delay = record.latency_ms / 1000.0 / opts.speed_multiplier
                if delay > 0 and i > 0:
                    time.sleep(delay)
                results.append(
                    {
                        "iteration": i,
                        "url": mutated_url,
                        "method": record.method,
                        "headers": mutated_headers,
                        "body": record.body,
                        "expected_status": record.response_status,
                        "expected_body": record.response_body,
                        "latency_ms": record.latency_ms / opts.speed_multiplier,
                    }
                )
        return results

    def _mutate_url(self, url: str, params: Dict[str, str]) -> str:
        if not params:
            return url
        from urllib.parse import urlparse, parse_qs, urlencode
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        qs.update(params)
        return parsed._replace(query=urlencode(qs, doseq=True)).geturl()

    def compare(
        self,
        request_id: str,
        live_response: Dict[str, Any],
    ) -> Dict[str, Any]:
        records = self._records.get(request_id, [])
        if not records:
            return {"matched": False, "reason": "no_record"}
        baseline = records[-1]
        mismatches = []
        if live_response.get("status") != baseline.response_status:
            mismatches.append(
                f"status: expected {baseline.response_status}, got {live_response.get('status')}"
            )
        live_body = live_response.get("body", b"")
        if live_body != baseline.response_body:
            mismatches.append("body mismatch")
        return {
            "matched": len(mismatches) == 0,
            "mismatches": mismatches,
            "baseline_status": baseline.response_status,
            "live_status": live_response.get("status"),
        }

    def get_record_count(self) -> int:
        return sum(len(r) for r in self._records.values())

    def list_request_ids(self) -> List[str]:
        return list(self._records.keys())
