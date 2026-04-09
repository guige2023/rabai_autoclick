"""API Sandbox Action Module.

Provides API sandbox capabilities for safe testing and development,
including request recording, replay, and mock response generation.

Example:
    >>> from actions.api.api_sandbox_action import APISandbox
    >>> sandbox = APISandbox()
    >>> sandbox.record_request(request, response)
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading


class SandboxMode(Enum):
    """Sandbox operating modes."""
    RECORD = "record"
    REPLAY = "replay"
    MOCK = "mock"
    SHADOW = "shadow"
    LIVE = "live"


class MatchStrategy(Enum):
    """Request matching strategies."""
    EXACT = "exact"
    URL_PATTERN = "url_pattern"
    BODY_JSON = "body_json"
    FUZZY = "fuzzy"


@dataclass
class RecordedRequest:
    """A recorded API request.
    
    Attributes:
        request_id: Unique request identifier
        timestamp: When request was made
        method: HTTP method
        url: Full URL
        headers: Request headers
        body: Request body
        query_params: Query parameters
        duration: Request duration in ms
    """
    request_id: str
    timestamp: datetime
    method: str
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    query_params: Dict[str, str] = field(default_factory=dict)
    duration: float = 0.0


@dataclass
class RecordedResponse:
    """A recorded API response.
    
    Attributes:
        request_id: Matching request ID
        status_code: HTTP status code
        headers: Response headers
        body: Response body
        timestamp: When response was received
    """
    request_id: str
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class MockResponse:
    """Mock response configuration.
    
    Attributes:
        matcher: How to match requests
        match_value: Value to match against
        status_code: Response status code
        body: Response body
        headers: Response headers
        delay: Artificial delay in ms
    """
    matcher: MatchStrategy = MatchStrategy.EXACT
    match_value: str = ""
    status_code: int = 200
    body: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    delay: float = 0.0


@dataclass
class SandboxInteraction:
    """A complete request/response interaction.
    
    Attributes:
        request: Recorded request
        response: Recorded response
        matched_mock: If this was matched to a mock
    """
    request: RecordedRequest
    response: RecordedResponse
    matched_mock: Optional[MockResponse] = None


@dataclass
class SandboxConfig:
    """Configuration for API sandbox.
    
    Attributes:
        mode: Operating mode
        persistence_path: Path to save/load recordings
        default_delay: Default mock response delay
        enable_logging: Whether to log all interactions
        match_strictness: How strictly to match requests
        passthrough_on_miss: Pass to real API if no match
    """
    mode: SandboxMode = SandboxMode.REPLAY
    persistence_path: Optional[str] = None
    default_delay: float = 0.0
    enable_logging: bool = True
    match_strictness: float = 1.0
    passthrough_on_miss: bool = True


class APISandbox:
    """Handles API sandbox operations for testing.
    
    Provides recording, replay, and mock capabilities
    for safe API testing and development.
    
    Attributes:
        config: Sandbox configuration
    
    Example:
        >>> sandbox = APISandbox()
        >>> sandbox.set_mode(SandboxMode.RECORD)
        >>> response = await sandbox.handle_request(request)
    """
    
    def __init__(self, config: Optional[SandboxConfig] = None):
        """Initialize the API sandbox.
        
        Args:
            config: Sandbox configuration
        """
        self.config = config or SandboxConfig()
        self._interactions: List[SandboxInteraction] = []
        self._mocks: List[MockResponse] = []
        self._request_map: Dict[str, RecordedResponse] = {}
        self._url_index: Dict[str, List[str]] = {}  # URL -> request IDs
        self._lock = threading.RLock()
        self._interaction_counter = 0
        self._passthrough_fn: Optional[Callable] = None
    
    def set_passthrough(self, fn: Callable) -> None:
        """Set the function for passthrough requests.
        
        Args:
            fn: Async function to call for real requests
        """
        self._passthrough_fn = fn
    
    def set_mode(self, mode: SandboxMode) -> None:
        """Set the sandbox operating mode.
        
        Args:
            mode: New operating mode
        """
        self.config.mode = mode
    
    def add_mock(self, mock: MockResponse) -> None:
        """Add a mock response.
        
        Args:
            mock: Mock response configuration
        """
        with self._lock:
            self._mocks.append(mock)
    
    def create_mock(
        self,
        match_url: str,
        match_strategy: MatchStrategy = MatchStrategy.EXACT,
        status_code: int = 200,
        body: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        delay: float = 0.0
    ) -> MockResponse:
        """Create and add a mock response.
        
        Args:
            match_url: URL or pattern to match
            match_strategy: How to match requests
            status_code: Response status code
            body: Response body
            headers: Response headers
            delay: Response delay in ms
        
        Returns:
            Created mock
        """
        mock = MockResponse(
            matcher=match_strategy,
            match_value=match_url,
            status_code=status_code,
            body=body,
            headers=headers or {},
            delay=delay or self.config.default_delay
        )
        self.add_mock(mock)
        return mock
    
    def record_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[str] = None,
        duration: float = 0.0
    ) -> RecordedRequest:
        """Record an API request.
        
        Args:
            method: HTTP method
            url: Full URL
            headers: Request headers
            body: Request body
            duration: Request duration in ms
        
        Returns:
            Recorded request
        """
        self._interaction_counter += 1
        request_id = f"req_{self._interaction_counter}_{int(time.time() * 1000)}"
        
        request = RecordedRequest(
            request_id=request_id,
            timestamp=datetime.now(),
            method=method.upper(),
            url=url,
            headers=headers or {},
            body=body,
            duration=duration
        )
        
        # Parse query params
        if "?" in url:
            path, query = url.split("?", 1)
            for param in query.split("&"):
                if "=" in param:
                    key, value = param.split("=", 1)
                    request.query_params[key] = value
        
        # Index by URL
        base_url = self._normalize_url(url)
        if base_url not in self._url_index:
            self._url_index[base_url] = []
        self._url_index[base_url].append(request_id)
        
        with self._lock:
            self._interactions.append(SandboxInteraction(request=request, response=None))
        
        return request
    
    def record_response(
        self,
        request_id: str,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[str] = None
    ) -> RecordedResponse:
        """Record an API response.
        
        Args:
            request_id: Matching request ID
            status_code: HTTP status code
            headers: Response headers
            body: Response body
        
        Returns:
            Recorded response
        """
        response = RecordedResponse(
            request_id=request_id,
            status_code=status_code,
            headers=headers or {},
            body=body,
            timestamp=datetime.now()
        )
        
        with self._lock:
            self._request_map[request_id] = response
            
            # Update the interaction
            for interaction in reversed(self._interactions):
                if interaction.request.request_id == request_id:
                    interaction.response = response
                    break
        
        return response
    
    def _normalize_url(self, url: str) -> str:
        """Normalize a URL for matching.
        
        Args:
            url: URL to normalize
        
        Returns:
            Normalized URL
        """
        # Remove query params for base URL matching
        if "?" in url:
            url = url.split("?")[0]
        
        # Remove trailing slash
        return url.rstrip("/")
    
    def _match_request(
        self,
        request: RecordedRequest
    ) -> Optional[MockResponse]:
        """Match a request to a mock.
        
        Args:
            request: Request to match
        
        Returns:
            Matching mock or None
        """
        for mock in self._mocks:
            if self._matches(request, mock):
                return mock
        return None
    
    def _matches(self, request: RecordedRequest, mock: MockResponse) -> bool:
        """Check if a request matches a mock.
        
        Args:
            request: Request to check
            mock: Mock to match against
        
        Returns:
            True if matches
        """
        if mock.matcher == MatchStrategy.EXACT:
            return request.url == mock.match_value
        
        elif mock.matcher == MatchStrategy.URL_PATTERN:
            pattern = re.compile(mock.match_value)
            return bool(pattern.match(request.url))
        
        elif mock.matcher == MatchStrategy.BODY_JSON:
            try:
                if not request.body:
                    return False
                req_body = json.loads(request.body)
                mock_body = json.loads(mock.match_value)
                return self._fuzzy_match_dict(req_body, mock_body)
            except json.JSONDecodeError:
                return False
        
        elif mock.matcher == MatchStrategy.FUZZY:
            return self._fuzzy_match(request, mock.match_value)
        
        return False
    
    def _fuzzy_match(self, request: RecordedRequest, pattern: str) -> bool:
        """Perform fuzzy matching on request.
        
        Args:
            request: Request to match
            pattern: Pattern to match
        
        Returns:
            True if fuzzy match
        """
        score = 0.0
        
        # Method match
        if request.method == pattern.split(":")[0] if ":" in pattern else request.method:
            score += 0.2
        
        # URL substring
        if pattern in request.url:
            score += 0.5
        
        # Header match
        pattern_headers = {}
        if "{" in pattern:
            for match in re.finditer(r'(\w+)=([^;]+)', pattern):
                pattern_headers[match.group(1)] = match.group(2)
            
            for key, value in pattern_headers.items():
                if request.headers.get(key) == value:
                    score += 0.15
        
        return score >= self.config.match_strictness
    
    def _fuzzy_match_dict(self, actual: Dict, expected: Dict) -> bool:
        """Fuzzy match two dictionaries.
        
        Args:
            actual: Actual dictionary
            expected: Expected dictionary
        
        Returns:
            True if fuzzy match
        """
        matches = 0
        total = len(expected)
        
        for key, value in expected.items():
            if key in actual:
                if actual[key] == value:
                    matches += 1
                elif isinstance(value, dict) and isinstance(actual[key], dict):
                    if self._fuzzy_match_dict(actual[key], value):
                        matches += 1
        
        return total > 0 and (matches / total) >= self.config.match_strictness
    
    async def handle_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[str] = None
    ) -> Tuple[int, Dict[str, str], Optional[str]]:
        """Handle an API request in sandbox mode.
        
        Args:
            method: HTTP method
            url: Full URL
            headers: Request headers
            body: Request body
        
        Returns:
            Tuple of (status_code, headers, body)
        
        Raises:
            ValueError: If in replay mode with no matching recording
        """
        # Record if in record mode
        if self.config.mode == SandboxMode.RECORD:
            request = self.record_request(method, url, headers, body)
            
            # Try to passthrough
            if self._passthrough_fn and self.config.passthrough_on_miss:
                status, resp_headers, resp_body = await self._passthrough_fn(
                    method, url, headers, body
                )
                self.record_response(request.request_id, status, resp_headers, resp_body)
                return status, resp_headers, resp_body
            
            return 200, {}, None
        
        # Replay mode
        if self.config.mode == SandboxMode.REPLAY:
            return await self._replay_request(method, url, headers, body)
        
        # Mock mode
        if self.config.mode == SandboxMode.MOCK:
            return await self._mock_request(method, url, headers, body)
        
        # Shadow mode - passthrough but also record
        if self.config.mode == SandboxMode.SHADOW:
            request = self.record_request(method, url, headers, body)
            
            if self._passthrough_fn:
                status, resp_headers, resp_body = await self._passthrough_fn(
                    method, url, headers, body
                )
                self.record_response(request.request_id, status, resp_headers, resp_body)
                return status, resp_headers, resp_body
            
            raise ValueError("Shadow mode requires passthrough function")
        
        # Live mode - passthrough only
        if self.config.mode == SandboxMode.LIVE:
            if self._passthrough_fn:
                return await self._passthrough_fn(method, url, headers, body)
            raise ValueError("Live mode requires passthrough function")
        
        raise ValueError(f"Unknown sandbox mode: {self.config.mode}")
    
    async def _replay_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]],
        body: Optional[str]
    ) -> Tuple[int, Dict[str, str], Optional[str]]:
        """Replay a recorded request.
        
        Args:
            method: HTTP method
            url: Full URL
            headers: Request headers
            body: Request body
        
        Returns:
            Tuple of (status_code, headers, body)
        """
        # Find matching recorded request
        request_id = self._find_matching_request(method, url, body)
        
        if not request_id:
            if self.config.passthrough_on_miss and self._passthrough_fn:
                return await self._passthrough_fn(method, url, headers, body)
            raise ValueError(f"No recording found for {method} {url}")
        
        response = self._request_map.get(request_id)
        
        if not response:
            raise ValueError(f"No response found for request {request_id}")
        
        return response.status_code, response.headers, response.body
    
    async def _mock_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]],
        body: Optional[str]
    ) -> Tuple[int, Dict[str, str], Optional[str]]:
        """Handle a request using mocks.
        
        Args:
            method: HTTP method
            url: Full URL
            headers: Request headers
            body: Request body
        
        Returns:
            Tuple of (status_code, headers, body)
        """
        request = RecordedRequest(
            request_id="mock",
            timestamp=datetime.now(),
            method=method,
            url=url,
            headers=headers or {},
            body=body
        )
        
        mock = self._match_request(request)
        
        if not mock:
            if self.config.passthrough_on_miss and self._passthrough_fn:
                return await self._passthrough_fn(method, url, headers, body)
            raise ValueError(f"No mock found for {method} {url}")
        
        # Apply delay
        if mock.delay > 0:
            import asyncio
            await asyncio.sleep(mock.delay / 1000.0)
        
        return mock.status_code, mock.headers, mock.body
    
    def _find_matching_request(
        self,
        method: str,
        url: str,
        body: Optional[str]
    ) -> Optional[str]:
        """Find a matching recorded request.
        
        Args:
            method: HTTP method
            url: Full URL
            body: Request body
        
        Returns:
            Matching request ID or None
        """
        base_url = self._normalize_url(url)
        request_ids = self._url_index.get(base_url, [])
        
        for request_id in request_ids:
            # Find the interaction
            for interaction in self._interactions:
                if interaction.request.request_id == request_id:
                    if interaction.request.method == method.upper():
                        # Check body if specified
                        if body is None or body == interaction.request.body:
                            return request_id
        
        return None
    
    def get_interactions(
        self,
        url_pattern: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[SandboxInteraction]:
        """Get recorded interactions.
        
        Args:
            url_pattern: Optional URL filter
            limit: Maximum results
        
        Returns:
            List of interactions
        """
        with self._lock:
            interactions = list(self._interactions)
        
        if url_pattern:
            pattern = re.compile(url_pattern)
            interactions = [i for i in interactions if pattern.search(i.request.url)]
        
        if limit:
            interactions = interactions[-limit:]
        
        return interactions
    
    def export_recordings(self) -> List[Dict[str, Any]]:
        """Export all recordings.
        
        Returns:
            List of request/response pairs
        """
        with self._lock:
            return [
                {
                    "request": {
                        "timestamp": i.request.timestamp.isoformat(),
                        "method": i.request.method,
                        "url": i.request.url,
                        "headers": i.request.headers,
                        "body": i.request.body
                    },
                    "response": {
                        "status_code": i.response.status_code,
                        "headers": i.response.headers,
                        "body": i.response.body,
                        "timestamp": i.response.timestamp.isoformat()
                    } if i.response else None
                }
                for i in self._interactions
                if i.response
            ]
    
    def import_recordings(self, recordings: List[Dict[str, Any]]) -> int:
        """Import recordings.
        
        Args:
            recordings: List of request/response pairs
        
        Returns:
            Number of recordings imported
        """
        count = 0
        
        for recording in recordings:
            request_data = recording.get("request", {})
            response_data = recording.get("response", {})
            
            if not request_data:
                continue
            
            request = self.record_request(
                method=request_data.get("method", "GET"),
                url=request_data.get("url", ""),
                headers=request_data.get("headers"),
                body=request_data.get("body")
            )
            
            if response_data:
                self.record_response(
                    request_id=request.request_id,
                    status_code=response_data.get("status_code", 200),
                    headers=response_data.get("headers"),
                    body=response_data.get("body")
                )
            
            count += 1
        
        return count
    
    def clear(self) -> int:
        """Clear all recordings and mocks.
        
        Returns:
            Number of interactions cleared
        """
        with self._lock:
            count = len(self._interactions)
            self._interactions.clear()
            self._request_map.clear()
            self._url_index.clear()
            self._mocks.clear()
            return count
