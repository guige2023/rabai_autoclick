"""API Fallback Action Module.

Implements cascading fallback chains for API requests with health
checking, circuit breaking, and automatic failover between endpoints.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class EndpointState(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class Endpoint:
    name: str
    url: str
    weight: int = 1
    timeout: float = 10.0
    state: EndpointState = EndpointState.HEALTHY
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_check: float = 0.0
    last_success: float = 0.0
    last_failure: float = 0.0


@dataclass
class FallbackConfig:
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    health_check_interval: float = 30.0
    fallback_delay: float = 0.5


class APIFallbackAction:
    """Cascading fallback chain for API requests with circuit breaking."""

    def __init__(self, config: Optional[FallbackConfig] = None) -> None:
        self._config = config or FallbackConfig()
        self._endpoints: Dict[str, Endpoint] = {}
        self._primary: Optional[str] = None

    def register_endpoint(
        self,
        name: str,
        url: str,
        weight: int = 1,
        timeout: float = 10.0,
        is_primary: bool = False,
    ) -> None:
        endpoint = Endpoint(
            name=name,
            url=url,
            weight=weight,
            timeout=timeout,
            state=EndpointState.HEALTHY,
        )
        self._endpoints[name] = endpoint
        if is_primary or self._primary is None:
            self._primary = name

    def remove_endpoint(self, name: str) -> bool:
        if name in self._endpoints:
            del self._endpoints[name]
            if self._primary == name:
                self._primary = next(iter(self._endpoints), None)
            return True
        return False

    def execute(
        self,
        request_builder: Callable[[str], Tuple[str, Dict, Any]],
        health_checker: Optional[Callable[[str], bool]] = None,
    ) -> Tuple[bool, Any, str]:
        ordered = self._get_ordered_endpoints()
        last_error = ""
        for name in ordered:
            ep = self._endpoints[name]
            if ep.state == EndpointState.CIRCUIT_OPEN:
                if self._should_try_circuit(ep):
                    ep.state = EndpointState.DEGRADED
                else:
                    continue
            try:
                url, headers, body = request_builder(ep.url)
                start = time.time()
                success = self._do_request(url, headers, body, ep.timeout)
                latency = time.time() - start
                if success:
                    self._record_success(ep)
                    return True, {"url": ep.url, "latency": latency}, name
                else:
                    last_error = f"Request failed for {name}"
                    self._record_failure(ep)
            except Exception as e:
                last_error = str(e)
                self._record_failure(ep)
        return False, {"error": last_error}, ""

    def _do_request(
        self,
        url: str,
        headers: Dict,
        body: Any,
        timeout: float,
    ) -> bool:
        import urllib.request
        import json
        try:
            data = json.dumps(body).encode("utf-8") if body else None
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return 200 <= resp.status < 300
        except Exception:
            return False

    def _get_ordered_endpoints(self) -> List[str]:
        def sort_key(name: str) -> Tuple[int, int, float]:
            ep = self._endpoints[name]
            state_order = {
                EndpointState.HEALTHY: 0,
                EndpointState.DEGRADED: 1,
                EndpointState.UNHEALTHY: 2,
                EndpointState.CIRCUIT_OPEN: 3,
            }
            return (state_order[ep.state], -ep.consecutive_successes, -ep.weight)
        return sorted(self._endpoints.keys(), key=sort_key)

    def _record_success(self, ep: Endpoint) -> None:
        ep.consecutive_successes += 1
        ep.consecutive_failures = 0
        ep.last_success = time.time()
        if ep.consecutive_successes >= 3 and ep.state != EndpointState.HEALTHY:
            ep.state = EndpointState.HEALTHY
            logger.info(f"Endpoint {ep.name} recovered")

    def _record_failure(self, ep: Endpoint) -> None:
        ep.consecutive_failures += 1
        ep.consecutive_successes = 0
        ep.last_failure = time.time()
        if ep.consecutive_failures >= self._config.circuit_breaker_threshold:
            ep.state = EndpointState.CIRCUIT_OPEN
            logger.warning(f"Circuit opened for {ep.name}")

    def _should_try_circuit(self, ep: Endpoint) -> bool:
        if ep.last_failure > 0:
            return time.time() - ep.last_failure >= self._config.circuit_breaker_timeout
        return True

    def get_endpoint_states(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: {
                "state": ep.state.value,
                "consecutive_failures": ep.consecutive_failures,
                "consecutive_successes": ep.consecutive_successes,
                "last_success": ep.last_success,
                "last_failure": ep.last_failure,
            }
            for name, ep in self._endpoints.items()
        }

    def reset_circuit(self, name: str) -> bool:
        ep = self._endpoints.get(name)
        if not ep:
            return False
        ep.state = EndpointState.HEALTHY
        ep.consecutive_failures = 0
        ep.consecutive_successes = 0
        return True
