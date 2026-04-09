"""API Request Bin Action Module.

Provides request bin capabilities for capturing and inspecting
API requests including headers, body, timing, and replay.

Example:
    >>> from actions.api.api_request_bin_action import RequestBin, BinEntry
    >>> bin = RequestBin(bin_id="test123")
    >>> bin.capture(request_data)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import hashlib
import json
import threading
import time
import uuid


class RequestMethod(Enum):
    """HTTP request methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class RequestData:
    """Captured request data.
    
    Attributes:
        method: HTTP method
        url: Request URL
        headers: Request headers
        query_params: Query parameters
        body: Request body
        timestamp: Request timestamp
        client_ip: Client IP address
    """
    method: str
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    client_ip: Optional[str] = None


@dataclass
class ResponseData:
    """Captured response data.
    
    Attributes:
        status_code: HTTP status code
        headers: Response headers
        body: Response body
        duration_ms: Response duration in ms
    """
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class BinEntry:
    """Request bin entry.
    
    Attributes:
        entry_id: Unique entry identifier
        request: Captured request data
        response: Associated response data
        matched_rules: Rules that matched this request
        notes: Optional notes
    """
    entry_id: str
    request: RequestData
    response: Optional[ResponseData] = None
    matched_rules: List[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class BinRule:
    """Rule for matching requests.
    
    Attributes:
        rule_id: Unique rule identifier
        name: Rule name
        match_url: URL pattern to match
        match_method: HTTP method to match
        match_headers: Headers that must be present
        match_body: Body pattern to match
        action: Action to take when matched
    """
    rule_id: str
    name: str
    match_url: Optional[str] = None
    match_method: Optional[str] = None
    match_headers: Dict[str, str] = field(default_factory=dict)
    match_body: Optional[str] = None
    action: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BinStats:
    """Request bin statistics.
    
    Attributes:
        total_requests: Total requests captured
        total_responses: Total responses captured
        total_size_bytes: Total size in bytes
        methods: Request method breakdown
    """
    total_requests: int = 0
    total_responses: int = 0
    total_size_bytes: int = 0
    methods: Dict[str, int] = field(default_factory=dict)


class RequestBin:
    """Request bin for capturing and inspecting API requests.
    
    Provides request/response capture, matching rules,
    and replay capabilities.
    
    Attributes:
        bin_id: Unique bin identifier
        entries: Captured entries
        rules: Matching rules
        stats: Bin statistics
        _lock: Thread safety lock
        _retention_hours: Entry retention in hours
    """
    
    def __init__(
        self,
        bin_id: Optional[str] = None,
        retention_hours: int = 24,
    ) -> None:
        """Initialize request bin.
        
        Args:
            bin_id: Unique bin ID (auto-generated if None)
            retention_hours: How long to retain entries
        """
        self.bin_id = bin_id or str(uuid.uuid4())[:12]
        self.entries: List[BinEntry] = []
        self.rules: Dict[str, BinRule] = {}
        self.stats = BinStats()
        self._lock = threading.RLock()
        self._retention_hours = retention_hours
        self._callbacks: List[Callable[[BinEntry], None]] = []
    
    def capture_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
        body: Optional[str] = None,
        client_ip: Optional[str] = None,
    ) -> BinEntry:
        """Capture an incoming request.
        
        Args:
            method: HTTP method
            url: Request URL
            headers: Request headers
            query_params: Query parameters
            body: Request body
            client_ip: Client IP address
            
        Returns:
            Created bin entry
        """
        request = RequestData(
            method=method.upper(),
            url=url,
            headers=headers or {},
            query_params=query_params or {},
            body=body,
            client_ip=client_ip,
        )
        
        entry_id = self._generate_entry_id(request)
        entry = BinEntry(entry_id=entry_id, request=request)
        
        # Match rules
        matched_rules = self._match_rules(request)
        entry.matched_rules = matched_rules
        
        with self._lock:
            self.entries.append(entry)
            self._cleanup_old_entries()
            self.stats.total_requests += 1
            self.stats.total_size_bytes += len(body or "")
            
            method_key = request.method
            self.stats.methods[method_key] = self.stats.methods.get(method_key, 0) + 1
        
        # Notify callbacks
        for callback in self._callbacks:
            try:
                callback(entry)
            except Exception:
                pass
        
        return entry
    
    def capture_response(
        self,
        entry_id: str,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[str] = None,
        duration_ms: float = 0.0,
    ) -> Optional[BinEntry]:
        """Capture a response for an entry.
        
        Args:
            entry_id: Entry ID to update
            status_code: HTTP status code
            headers: Response headers
            body: Response body
            duration_ms: Response duration
            
        Returns:
            Updated bin entry or None
        """
        response = ResponseData(
            status_code=status_code,
            headers=headers or {},
            body=body,
            duration_ms=duration_ms,
        )
        
        with self._lock:
            for entry in self.entries:
                if entry.entry_id == entry_id:
                    entry.response = response
                    self.stats.total_responses += 1
                    self.stats.total_size_bytes += len(body or "")
                    return entry
            return None
    
    def add_rule(self, rule: BinRule) -> str:
        """Add a matching rule.
        
        Args:
            rule: Rule to add
            
        Returns:
            Rule ID
        """
        with self._lock:
            self.rules[rule.rule_id] = rule
        return rule.rule_id
    
    def remove_rule(self, rule_id: str) -> bool:
        """Remove a rule.
        
        Args:
            rule_id: Rule ID to remove
            
        Returns:
            True if rule was removed
        """
        with self._lock:
            if rule_id in self.rules:
                del self.rules[rule_id]
                return True
            return False
    
    def register_callback(self, callback: Callable[[BinEntry], None]) -> None:
        """Register callback for new entries.
        
        Args:
            callback: Callback function
        """
        self._callbacks.append(callback)
    
    def get_entries(
        self,
        limit: int = 100,
        method: Optional[str] = None,
        url_pattern: Optional[str] = None,
    ) -> List[BinEntry]:
        """Get bin entries.
        
        Args:
            limit: Maximum entries to return
            method: Filter by HTTP method
            url_pattern: Filter by URL pattern
            
        Returns:
            List of bin entries
        """
        with self._lock:
            entries = list(self.entries)
        
        if method:
            entries = [e for e in entries if e.request.method.upper() == method.upper()]
        
        if url_pattern:
            import re
            pattern = re.compile(url_pattern)
            entries = [e for e in entries if pattern.search(e.request.url)]
        
        return entries[-limit:]
    
    def get_entry(self, entry_id: str) -> Optional[BinEntry]:
        """Get a specific entry.
        
        Args:
            entry_id: Entry ID
            
        Returns:
            Bin entry or None
        """
        with self._lock:
            for entry in self.entries:
                if entry.entry_id == entry_id:
                    return entry
            return None
    
    def replay(self, entry_id: str) -> Optional[BinEntry]:
        """Replay a captured request.
        
        Args:
            entry_id: Entry ID to replay
            
        Returns:
            New entry with replayed request/response
        """
        entry = self.get_entry(entry_id)
        if not entry:
            return None
        
        return self.capture_request(
            method=entry.request.method,
            url=entry.request.url,
            headers=dict(entry.request.headers),
            query_params=dict(entry.request.query_params),
            body=entry.request.body,
            client_ip=entry.request.client_ip,
        )
    
    def add_note(self, entry_id: str, note: str) -> bool:
        """Add a note to an entry.
        
        Args:
            entry_id: Entry ID
            note: Note to add
            
        Returns:
            True if note was added
        """
        with self._lock:
            for entry in self.entries:
                if entry.entry_id == entry_id:
                    entry.notes = note
                    return True
            return False
    
    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self.entries.clear()
            self.stats = BinStats()
    
    def export(self, format: str = "json") -> str:
        """Export bin data.
        
        Args:
            format: Export format (json/http Archive)
            
        Returns:
            Exported data
        """
        with self._lock:
            data = {
                "bin_id": self.bin_id,
                "entries": [
                    {
                        "entry_id": e.entry_id,
                        "request": {
                            "method": e.request.method,
                            "url": e.request.url,
                            "headers": e.request.headers,
                            "query_params": e.request.query_params,
                            "body": e.request.body,
                            "timestamp": e.request.timestamp.isoformat(),
                            "client_ip": e.request.client_ip,
                        },
                        "response": {
                            "status_code": e.response.status_code if e.response else None,
                            "headers": e.response.headers if e.response else None,
                            "body": e.response.body if e.response else None,
                            "duration_ms": e.response.duration_ms if e.response else None,
                        } if e.response else None,
                        "notes": e.notes,
                    }
                    for e in self.entries
                ],
                "stats": {
                    "total_requests": self.stats.total_requests,
                    "total_responses": self.stats.total_responses,
                    "total_size_bytes": self.stats.total_size_bytes,
                    "methods": self.stats.methods,
                },
            }
        
        if format == "json":
            return json.dumps(data, indent=2)
        return str(data)
    
    def _generate_entry_id(self, request: RequestData) -> str:
        """Generate unique entry ID.
        
        Args:
            request: Request data
            
        Returns:
            Entry ID
        """
        unique_str = f"{request.url}:{request.method}:{time.time()}"
        return hashlib.md5(unique_str.encode()).hexdigest()[:12]
    
    def _match_rules(self, request: RequestData) -> List[str]:
        """Match request against rules.
        
        Args:
            request: Request to match
            
        Returns:
            List of matching rule IDs
        """
        matched: List[str] = []
        
        for rule in self.rules.values():
            if rule.match_method and rule.match_method != request.method:
                continue
            if rule.match_url and rule.match_url not in request.url:
                continue
            matched.append(rule.rule_id)
        
        return matched
    
    def _cleanup_old_entries(self) -> None:
        """Remove entries older than retention period."""
        cutoff = datetime.now() - timedelta(hours=self._retention_hours)
        self.entries = [e for e in self.entries if e.request.timestamp > cutoff]


class RequestBinManager:
    """Manager for multiple request bins."""
    
    def __init__(self) -> None:
        """Initialize bin manager."""
        self._bins: Dict[str, RequestBin] = {}
        self._lock = threading.RLock()
    
    def create_bin(self, bin_id: Optional[str] = None) -> RequestBin:
        """Create a new request bin.
        
        Args:
            bin_id: Optional bin ID
            
        Returns:
            Created request bin
        """
        bin_obj = RequestBin(bin_id=bin_id)
        with self._lock:
            self._bins[bin_obj.bin_id] = bin_obj
        return bin_obj
    
    def get_bin(self, bin_id: str) -> Optional[RequestBin]:
        """Get a bin by ID.
        
        Args:
            bin_id: Bin ID
            
        Returns:
            Request bin or None
        """
        with self._lock:
            return self._bins.get(bin_id)
    
    def delete_bin(self, bin_id: str) -> bool:
        """Delete a bin.
        
        Args:
            bin_id: Bin ID
            
        Returns:
            True if deleted
        """
        with self._lock:
            if bin_id in self._bins:
                del self._bins[bin_id]
                return True
            return False
    
    def list_bins(self) -> List[str]:
        """List all bin IDs.
        
        Returns:
            List of bin IDs
        """
        with self._lock:
            return list(self._bins.keys())
