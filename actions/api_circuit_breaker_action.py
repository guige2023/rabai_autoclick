"""
API Circuit Breaker Action Module.

Implements the circuit breaker pattern for API calls to prevent
cascade failures, with configurable states, thresholds, and recovery.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FailureReason(Enum):
    """Reasons for circuit breaker trip."""
    TIMEOUT = "timeout"
    HTTP_ERROR = "http_error"
    CONNECTION_ERROR = "connection_error"
    THRESHOLD_EXCEEDED = "threshold_exceeded"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3
    excluded_status_codes: List[int] = field(default_factory=lambda: [400, 401, 403, 404])
    track_consecutive: bool = True


@dataclass
class CircuitBreakerStats:
    """Statistics for a circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: float = 0.0
    last_failure_reason: Optional[str] = None
    state_change_times: List[Dict[str, float]] = field(default_factory=list)
    opened_at: Optional[float] = None
    closed_at: Optional[float] = None


class CircuitBreaker:
    """Circuit breaker for a single endpoint."""
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._lock = threading.RLock()
        self._stats = CircuitBreakerStats()
        self._last_state_change = time.time()
        self._half_open_calls = 0
    
    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_state_change >= self.config.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state
    
    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        old_state = self._state
        self._state = new_state
        self._last_state_change = time.time()
        self._stats.state_change_times.append({
            "from": old_state.value,
            "to": new_state.value,
            "timestamp": self._last_state_change
        })
        if new_state == CircuitState.OPEN:
            self._stats.opened_at = self._last_state_change
        elif new_state == CircuitState.CLOSED:
            self._stats.closed_at = self._last_state_change
        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
    
    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._stats.total_calls += 1
            self._stats.successful_calls += 1
            self._stats.consecutive_failures = 0
            self._stats.consecutive_successes += 1
            
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._stats.consecutive_successes >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    self._stats.consecutive_successes = 0
    
    def record_failure(self, reason: FailureReason = FailureReason.HTTP_ERROR) -> None:
        """Record a failed call."""
        with self._lock:
            self._stats.total_calls += 1
            self._stats.failed_calls += 1
            self._stats.consecutive_failures += 1
            self._stats.consecutive_successes = 0
            self._stats.last_failure_time = time.time()
            self._stats.last_failure_reason = reason.value
            
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._state == CircuitState.CLOSED:
                if self._stats.consecutive_failures >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
    
    def allow_request(self) -> bool:
        """Check if a request is allowed."""
        with self._lock:
            state = self.state
            if state == CircuitState.CLOSED:
                return True
            elif state == CircuitState.HALF_OPEN:
                if self._half_open_calls < self.config.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False
            else:
                self._stats.rejected_calls += 1
                return False
    
    def get_stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics."""
        with self._lock:
            return CircuitBreakerStats(
                total_calls=self._stats.total_calls,
                successful_calls=self._stats.successful_calls,
                failed_calls=self._stats.failed_calls,
                rejected_calls=self._stats.rejected_calls,
                consecutive_failures=self._stats.consecutive_failures,
                consecutive_successes=self._stats.consecutive_successes,
                last_failure_time=self._stats.last_failure_time,
                last_failure_reason=self._stats.last_failure_reason,
                state_change_times=self._stats.state_change_times.copy(),
                opened_at=self._stats.opened_at,
                closed_at=self._stats.closed_at
            )


class ApiCircuitBreakerAction(BaseAction):
    """API circuit breaker action.
    
    Protects API calls with circuit breaker pattern to prevent
    cascade failures and enable graceful degradation.
    """
    action_type = "api_circuit_breaker"
    display_name = "API断路器"
    description = "防止级联故障的API断路器"
    
    def __init__(self):
        super().__init__()
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        self._default_config = CircuitBreakerConfig()
    
    def get_breaker(self, endpoint: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get or create a circuit breaker for an endpoint."""
        with self._lock:
            if endpoint not in self._breakers:
                self._breakers[endpoint] = CircuitBreaker(endpoint, config or self._default_config)
            return self._breakers[endpoint]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an API request with circuit breaker protection.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - url: The API endpoint URL
                - method: HTTP method (default GET)
                - headers: Request headers
                - body: Request body
                - timeout: Request timeout in seconds
                - circuit_breaker: Optional circuit breaker config
                - breaker_name: Optional custom breaker name
                
        Returns:
            ActionResult with execution results.
        """
        start_time = time.time()
        
        url = params.get("url", "")
        method = params.get("method", "GET")
        headers = params.get("headers", {})
        body = params.get("body")
        timeout = params.get("timeout", 30.0)
        breaker_name = params.get("breaker_name", url)
        
        if not url:
            return ActionResult(
                success=False,
                message="Missing required parameter: url",
                duration=time.time() - start_time
            )
        
        breaker = self.get_breaker(breaker_name)
        
        if not breaker.allow_request():
            stats = breaker.get_stats()
            return ActionResult(
                success=False,
                message=f"Circuit breaker OPEN for {breaker_name}",
                data={
                    "circuit_state": breaker.state.value,
                    "rejected": True,
                    "stats": {
                        "total_calls": stats.total_calls,
                        "failed_calls": stats.failed_calls,
                        "consecutive_failures": stats.consecutive_failures
                    }
                },
                duration=time.time() - start_time
            )
        
        try:
            result = self._execute_http_request(url, method, headers, body, timeout)
            stats = breaker.get_stats()
            
            if result["success"]:
                breaker.record_success()
                return ActionResult(
                    success=True,
                    message=f"Request succeeded via {breaker_name}",
                    data={
                        "circuit_state": breaker.state.value,
                        "status_code": result.get("status_code"),
                        "latency_ms": result.get("latency_ms"),
                        "stats": {
                            "total_calls": stats.total_calls,
                            "successful_calls": stats.successful_calls
                        }
                    },
                    duration=time.time() - start_time
                )
            else:
                reason = FailureReason.HTTP_ERROR if result.get("status_code") else FailureReason.CONNECTION_ERROR
                breaker.record_failure(reason)
                stats = breaker.get_stats()
                return ActionResult(
                    success=False,
                    message=f"Request failed: {result.get('error')}",
                    data={
                        "circuit_state": breaker.state.value,
                        "status_code": result.get("status_code"),
                        "stats": {
                            "total_calls": stats.total_calls,
                            "failed_calls": stats.failed_calls,
                            "consecutive_failures": stats.consecutive_failures
                        }
                    },
                    duration=time.time() - start_time
                )
                
        except Exception as e:
            breaker.record_failure(FailureReason.CONNECTION_ERROR)
            return ActionResult(
                success=False,
                message=f"Circuit breaker error: {str(e)}",
                data={"circuit_state": breaker.state.value},
                duration=time.time() - start_time
            )
    
    def _execute_http_request(
        self, url: str, method: str, headers: Dict, body: Any, timeout: float
    ) -> Dict[str, Any]:
        """Execute an HTTP request and return result dict."""
        req_start = time.time()
        try:
            body_data = None
            if body is not None:
                if isinstance(body, dict):
                    body_data = json.dumps(body).encode("utf-8")
                else:
                    body_data = str(body).encode("utf-8")
            
            req_headers = dict(headers)
            if body_data and "Content-Type" not in req_headers:
                req_headers["Content-Type"] = "application/json"
            
            req = Request(url, data=body_data, headers=req_headers, method=method.upper())
            
            with urlopen(req, timeout=timeout) as response:
                response_body = response.read()
                try:
                    parsed_body = json.loads(response_body)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    parsed_body = response_body.decode("utf-8", errors="replace")
                
                return {
                    "success": True,
                    "status_code": response.status,
                    "body": parsed_body,
                    "latency_ms": (time.time() - req_start) * 1000
                }
                
        except HTTPError as e:
            return {
                "success": False,
                "status_code": e.code,
                "error": f"HTTP {e.code}: {str(e)}",
                "latency_ms": (time.time() - req_start) * 1000
            }
        except URLError as e:
            return {
                "success": False,
                "status_code": None,
                "error": f"Connection error: {str(e)}",
                "latency_ms": (time.time() - req_start) * 1000
            }
        except Exception as e:
            return {
                "success": False,
                "status_code": None,
                "error": str(e),
                "latency_ms": (time.time() - req_start) * 1000
            }
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all circuit breakers."""
        with self._lock:
            return {
                name: breaker.get_stats().__dict__
                for name, breaker in self._breakers.items()
            }
    
    def reset_breaker(self, name: str) -> bool:
        """Manually reset a circuit breaker to closed state."""
        with self._lock:
            if name in self._breakers:
                self._breakers[name]._transition_to(CircuitState.CLOSED)
                self._breakers[name]._stats.consecutive_failures = 0
                self._breakers[name]._stats.consecutive_successes = 0
                return True
            return False
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate circuit breaker parameters."""
        if "url" not in params:
            return False, "Missing required parameter: url"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["url"]
