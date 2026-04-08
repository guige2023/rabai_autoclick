"""API Gateway action module for RabAI AutoClick.

Provides API gateway operations:
- ApiGatewayRouteAction: Route requests to backend services
- ApiGatewayAuthAction: Centralized authentication middleware
- ApiGatewayRateLimitAction: Global rate limiting
- ApiGatewayTransformAction: Request/response transformation
- ApiGatewayCircuitBreakerAction: Circuit breaker pattern
"""

import time
import hashlib
import json
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ApiGatewayRouteAction(BaseAction):
    """Route requests to appropriate backend services."""
    action_type = "api_gateway_route"
    display_name = "API网关路由"
    description = "路由请求到后端服务"

    def __init__(self):
        super().__init__()
        self._routes: Dict[str, Dict] = {}
        self._default_route: Optional[Dict] = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "route")
            path = params.get("path", "")
            method = params.get("method", "GET").upper()

            if operation == "add_route":
                route_path = params.get("route_path", "")
                backend_url = params.get("backend_url", "")
                backend_method = params.get("backend_method", "GET")
                strip_prefix = params.get("strip_prefix", False)
                add_prefix = params.get("add_prefix", "")
                timeout = params.get("timeout", 30)
                weight = params.get("weight", 1)

                if not route_path or not backend_url:
                    return ActionResult(success=False, message="route_path and backend_url required")

                self._routes[route_path] = {
                    "backend_url": backend_url,
                    "backend_method": backend_method,
                    "strip_prefix": strip_prefix,
                    "add_prefix": add_prefix,
                    "timeout": timeout,
                    "weight": weight,
                    "active": True,
                    "total_requests": 0,
                    "failed_requests": 0
                }

                return ActionResult(
                    success=True,
                    data={"route": route_path, "backend": backend_url},
                    message=f"Route '{route_path}' added"
                )

            elif operation == "route":
                if not path:
                    return ActionResult(success=False, message="path is required")

                matched_route = self._find_route(path)

                if matched_route is None:
                    if self._default_route:
                        matched_route = self._default_route
                    else:
                        return ActionResult(
                            success=False,
                            message=f"No route matched for '{path}'",
                            data={"path": path, "available_routes": list(self._routes.keys())}
                        )

                request_body = params.get("body", None)
                request_headers = params.get("headers", {})
                query_params = params.get("query_params", {})

                target_path = path
                if matched_route["strip_prefix"]:
                    route_key = self._find_route_key(path)
                    if route_key and path.startswith(route_key):
                        target_path = path[len(route_key):]
                if matched_route["add_prefix"]:
                    target_path = matched_route["add_prefix"] + target_path

                backend_url = matched_route["backend_url"]
                if not backend_url.endswith("/") and not target_path.startswith("/"):
                    backend_url += "/"
                elif backend_url.endswith("/") and target_path.startswith("/"):
                    target_path = target_path[1:]

                final_url = backend_url + target_path
                if query_params:
                    separator = "&" if "?" in final_url else "?"
                    final_url += separator + urllib.parse.urlencode(query_params)

                req = urllib.request.Request(final_url, method=matched_route["backend_method"])
                for key, value in request_headers.items():
                    req.add_header(key, value)

                if request_body:
                    if isinstance(request_body, dict):
                        body_data = json.dumps(request_body).encode("utf-8")
                    else:
                        body_data = str(request_body).encode("utf-8")
                    req.data = body_data
                    req.add_header("Content-Type", "application/json")

                matched_route["total_requests"] += 1

                try:
                    with urllib.request.urlopen(req, timeout=matched_route["timeout"]) as response:
                        content = response.read()
                        try:
                            data = json.loads(content.decode("utf-8"))
                        except:
                            data = content.decode("utf-8", errors="replace")

                        return ActionResult(
                            success=True,
                            data={
                                "status_code": response.status,
                                "backend_response": data,
                                "route": matched_route["backend_url"],
                                "target_path": target_path
                            },
                            message=f"Routed to {matched_route['backend_url']}"
                        )

                except urllib.error.HTTPError as e:
                    matched_route["failed_requests"] += 1
                    return ActionResult(
                        success=False,
                        message=f"Backend error: {e.code}",
                        data={"status_code": e.code, "backend": matched_route["backend_url"]}
                    )
                except Exception as e:
                    matched_route["failed_requests"] += 1
                    return ActionResult(
                        success=False,
                        message=f"Routing error: {str(e)}",
                        data={"backend": matched_route["backend_url"]}
                    )

            elif operation == "list_routes":
                return ActionResult(
                    success=True,
                    data={"routes": self._routes, "default": self._default_route},
                    message=f"{len(self._routes)} routes configured"
                )

            elif operation == "remove_route":
                route_path = params.get("route_path", "")
                if route_path in self._routes:
                    del self._routes[route_path]
                    return ActionResult(success=True, message=f"Route '{route_path}' removed")
                return ActionResult(success=False, message=f"Route '{route_path}' not found")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Gateway route error: {str(e)}")

    def _find_route(self, path: str) -> Optional[Dict]:
        """Find the most specific matching route."""
        best_match = None
        best_len = 0

        for route_path, route_data in self._routes.items():
            if not route_data["active"]:
                continue
            if path.startswith(route_path) and len(route_path) > best_len:
                best_match = route_data
                best_len = len(route_path)

        return best_match

    def _find_route_key(self, path: str) -> Optional[str]:
        """Find the route key that matches the path."""
        best_match = None
        best_len = 0

        for route_path in self._routes:
            if path.startswith(route_path) and len(route_path) > best_len:
                best_match = route_path
                best_len = len(route_path)

        return best_match


class ApiGatewayAuthAction(BaseAction):
    """Centralized authentication for API gateway."""
    action_type = "api_gateway_auth"
    display_name = "API网关认证"
    description = "API网关集中认证"

    def __init__(self):
        super().__init__()
        self._valid_tokens: Dict[str, Dict] = {}
        self._revoked_tokens: set = set()
        self._api_keys: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            auth_header = params.get("auth_header", "")
            api_key = params.get("api_key", "")
            token = params.get("token", "")
            client_id = params.get("client_id", "")
            client_secret = params.get("client_secret", "")

            if operation == "add_api_key":
                if not api_key or not client_id:
                    return ActionResult(success=False, message="api_key and client_id required")

                self._api_keys[api_key] = {
                    "client_id": client_id,
                    "active": True,
                    "rate_limit": params.get("rate_limit", 1000),
                    "scopes": params.get("scopes", []),
                    "created_at": time.time()
                }

                return ActionResult(
                    success=True,
                    data={"client_id": client_id, "api_key": api_key[:8] + "***"},
                    message=f"API key added for client '{client_id}'"
                )

            elif operation == "revoke_token":
                if not token:
                    return ActionResult(success=False, message="token required")

                self._revoked_tokens.add(self._hash_token(token))
                return ActionResult(success=True, message="Token revoked")

            elif operation == "validate":
                auth_type = params.get("auth_type", "header")

                if auth_type == "header" and auth_header:
                    if auth_header.startswith("Bearer "):
                        token = auth_header[7:]
                    elif auth_header.startswith("Basic "):
                        try:
                            import base64
                            decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                            client_id, client_secret = decoded.split(":", 1)
                        except:
                            return ActionResult(success=False, message="Invalid Basic auth format")
                    else:
                        api_key = auth_header

                if api_key:
                    if api_key in self._revoked_tokens:
                        return ActionResult(success=False, message="API key revoked", data={"revoked": True})

                    key_data = self._api_keys.get(api_key)
                    if not key_data:
                        return ActionResult(success=False, message="Invalid API key", data={"valid": False})

                    if not key_data["active"]:
                        return ActionResult(success=False, message="API key inactive", data={"active": False})

                    return ActionResult(
                        success=True,
                        data={
                            "valid": True,
                            "client_id": key_data["client_id"],
                            "scopes": key_data["scopes"],
                            "rate_limit": key_data["rate_limit"]
                        },
                        message="Authentication successful"
                    )

                elif token:
                    token_hash = self._hash_token(token)
                    if token_hash in self._revoked_tokens:
                        return ActionResult(success=False, message="Token revoked")

                    token_data = self._valid_tokens.get(token_hash)
                    if not token_data:
                        return ActionResult(success=False, message="Invalid token")

                    if token_data.get("expires_at", float("inf")) < time.time():
                        return ActionResult(success=False, message="Token expired")

                    return ActionResult(
                        success=True,
                        data={"valid": True, "client_id": token_data.get("client_id"), "scopes": token_data.get("scopes", [])},
                        message="Token valid"
                    )

                else:
                    return ActionResult(success=False, message="No credentials provided")

            elif operation == "issue_token":
                if not client_id or not client_secret:
                    return ActionResult(success=False, message="client_id and client_secret required")

                expected_secret = params.get("expected_secret", client_id)
                if client_secret != expected_secret:
                    return ActionResult(success=False, message="Invalid client credentials")

                token = self._generate_token(client_id)
                expires_in = params.get("expires_in", 3600)

                self._valid_tokens[self._hash_token(token)] = {
                    "client_id": client_id,
                    "scopes": params.get("scopes", []),
                    "issued_at": time.time(),
                    "expires_at": time.time() + expires_in
                }

                return ActionResult(
                    success=True,
                    data={"access_token": token, "token_type": "Bearer", "expires_in": expires_in},
                    message="Token issued"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Gateway auth error: {str(e)}")

    def _hash_token(self, token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def _generate_token(self, client_id: str) -> str:
        raw = f"{client_id}:{time.time()}:{os.urandom(16).hex()}"
        return hashlib.sha256(raw.encode()).hexdigest()


class ApiGatewayRateLimitAction(BaseAction):
    """Global rate limiting for API gateway."""
    action_type = "api_gateway_ratelimit"
    display_name = "API网关限流"
    description = "API网关全局限流"

    def __init__(self):
        super().__init__()
        self._limits: Dict[str, Dict] = {}
        self._requests: Dict[str, List[float]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            client_id = params.get("client_id", "")
            api_key = params.get("api_key", "")
            identifier = client_id or api_key or params.get("ip", "unknown")

            if operation == "set_limit":
                if not identifier:
                    return ActionResult(success=False, message="identifier required")

                limit = params.get("limit", 100)
                window = params.get("window_seconds", 60)
                self._limits[identifier] = {"limit": limit, "window": window, "burst": params.get("burst", 0)}
                return ActionResult(success=True, message=f"Limit set for '{identifier}': {limit}/{window}s")

            elif operation == "check":
                limit_config = self._limits.get(identifier, {"limit": 100, "window": 60, "burst": 0})
                limit = limit_config["limit"]
                window = limit_config["window"]
                burst = limit_config["burst"]

                if identifier not in self._requests:
                    self._requests[identifier] = []

                now = time.time()
                cutoff = now - window
                self._requests[identifier] = [t for t in self._requests[identifier] if t > cutoff]

                current_count = len(self._requests[identifier])
                effective_limit = limit + burst

                if current_count >= effective_limit:
                    retry_after = window - (now % window)
                    return ActionResult(
                        success=False,
                        message="Rate limit exceeded",
                        data={
                            "allowed": False,
                            "current": current_count,
                            "limit": limit,
                            "burst": burst,
                            "retry_after": int(retry_after)
                        }
                    )

                self._requests[identifier].append(now)

                return ActionResult(
                    success=True,
                    message="Request allowed",
                    data={
                        "allowed": True,
                        "current": current_count + 1,
                        "limit": limit,
                        "remaining": effective_limit - current_count - 1
                    }
                )

            elif operation == "reset":
                if identifier in self._requests:
                    self._requests[identifier] = []
                return ActionResult(success=True, message=f"Reset limit for '{identifier}'")

            elif operation == "get_status":
                limit_config = self._limits.get(identifier, {"limit": 100, "window": 60})
                cutoff = time.time() - limit_config["window"]
                recent = [t for t in self._requests.get(identifier, []) if t > cutoff]
                return ActionResult(
                    success=True,
                    data={
                        "identifier": identifier,
                        "current_usage": len(recent),
                        "limit": limit_config["limit"],
                        "window_seconds": limit_config["window"],
                        "reset_at": int(time.time() + limit_config["window"])
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Gateway rate limit error: {str(e)}")


class ApiGatewayTransformAction(BaseAction):
    """Request/response transformation for API gateway."""
    action_type = "api_gateway_transform"
    display_name = "API网关转换"
    description = "API网关请求响应转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "transform")
            transform_type = params.get("transform_type", "request")
            data = params.get("data", {})
            template = params.get("template", {})
            mappings = params.get("mappings", {})

            if operation == "transform":
                if transform_type == "request":
                    transformed = self._transform_request(data, template, mappings)
                elif transform_type == "response":
                    transformed = self._transform_response(data, template, mappings)
                elif transform_type == "headers":
                    transformed = self._transform_headers(data, mappings)
                else:
                    return ActionResult(success=False, message=f"Unknown transform_type: {transform_type}")

                return ActionResult(
                    success=True,
                    data={"transformed": transformed, "type": transform_type},
                    message=f"{transform_type} transformation completed"
                )

            elif operation == "add_mapping":
                source_field = params.get("source_field", "")
                target_field = params.get("target_field", "")
                transform_func = params.get("transform_func", "passthrough")

                if not source_field or not target_field:
                    return ActionResult(success=False, message="source_field and target_field required")

                return ActionResult(
                    success=True,
                    data={"mapping": {"source": source_field, "target": target_field, "func": transform_func}},
                    message="Mapping added"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Gateway transform error: {str(e)}")

    def _transform_request(self, data: Dict, template: Dict, mappings: Dict) -> Dict:
        result = {}
        for target_field, source_field in mappings.items():
            if isinstance(source_field, str) and "." in source_field:
                parts = source_field.split(".")
                value = data
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        value = None
                        break
                result[target_field] = value
            else:
                result[target_field] = data.get(source_field, template.get(target_field))
        return result

    def _transform_response(self, data: Dict, template: Dict, mappings: Dict) -> Dict:
        result = {}
        for source_field, target_field in mappings.items():
            value = data.get(source_field, template.get(source_field))
            if isinstance(target_field, str) and "." in target_field:
                parts = target_field.split(".")
                container = result
                for part in parts[:-1]:
                    if part not in container:
                        container[part] = {}
                    container = container[part]
                container[parts[-1]] = value
            else:
                result[target_field] = value
        return result

    def _transform_headers(self, data: Dict, mappings: Dict) -> Dict:
        result = {}
        for key, value in data.items():
            mapped_key = mappings.get(key, key)
            result[mapped_key] = value
        return result


class ApiGatewayCircuitBreakerAction(BaseAction):
    """Circuit breaker pattern for API gateway backends."""
    action_type = "api_gateway_circuit_breaker"
    display_name = "API网关断路器"
    description = "API网关后端断路器"

    def __init__(self):
        super().__init__()
        self._circuits: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")
            service_name = params.get("service_name", "default")
            failure_threshold = params.get("failure_threshold", 5)
            timeout_seconds = params.get("timeout_seconds", 60)
            half_open_max_calls = params.get("half_open_max_calls", 3)

            if operation == "setup":
                self._circuits[service_name] = {
                    "state": CircuitState.CLOSED,
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure_time": None,
                    "timeout_seconds": timeout_seconds,
                    "failure_threshold": failure_threshold,
                    "half_open_max_calls": half_open_max_calls,
                    "half_open_calls": 0
                }
                return ActionResult(success=True, message=f"Circuit '{service_name}' setup")

            elif operation == "call":
                if service_name not in self._circuits:
                    self._circuits[service_name] = {
                        "state": CircuitState.CLOSED,
                        "failure_count": 0,
                        "success_count": 0,
                        "last_failure_time": None,
                        "timeout_seconds": timeout_seconds,
                        "failure_threshold": failure_threshold,
                        "half_open_max_calls": half_open_max_calls,
                        "half_open_calls": 0
                    }

                circuit = self._circuits[service_name]

                if circuit["state"] == CircuitState.OPEN:
                    if circuit["last_failure_time"]:
                        elapsed = time.time() - circuit["last_failure_time"]
                        if elapsed >= circuit["timeout_seconds"]:
                            circuit["state"] = CircuitState.HALF_OPEN
                            circuit["half_open_calls"] = 0
                            return ActionResult(
                                success=False,
                                message="Circuit half-open, retry allowed",
                                data={"state": CircuitState.HALF_OPEN.value, "circuit": service_name}
                            )
                    return ActionResult(
                        success=False,
                        message="Circuit open",
                        data={"state": CircuitState.OPEN.value, "circuit": service_name}
                    )

                if circuit["state"] == CircuitState.HALF_OPEN:
                    if circuit["half_open_calls"] >= circuit["half_open_max_calls"]:
                        return ActionResult(
                            success=False,
                            message="Circuit half-open calls exhausted",
                            data={"circuit": service_name}
                        )
                    circuit["half_open_calls"] += 1

                call_result = params.get("call_result", True)
                if call_result:
                    circuit["success_count"] += 1
                    if circuit["state"] == CircuitState.HALF_OPEN:
                        circuit["state"] = CircuitState.CLOSED
                        circuit["failure_count"] = 0
                    return ActionResult(
                        success=True,
                        data={"state": circuit["state"].value, "circuit": service_name},
                        message="Call succeeded"
                    )
                else:
                    circuit["failure_count"] += 1
                    circuit["last_failure_time"] = time.time()
                    if circuit["failure_count"] >= circuit["failure_threshold"]:
                        circuit["state"] = CircuitState.OPEN
                    return ActionResult(
                        success=False,
                        data={"state": circuit["state"].value, "failure_count": circuit["failure_count"]},
                        message="Call failed"
                    )

            elif operation == "status":
                if service_name not in self._circuits:
                    return ActionResult(success=False, message=f"Circuit '{service_name}' not found")
                circuit = self._circuits[service_name]
                return ActionResult(
                    success=True,
                    data={"circuit": service_name, "state": circuit["state"].value, "failures": circuit["failure_count"]},
                    message=f"Circuit '{service_name}': {circuit['state'].value}"
                )

            elif operation == "reset":
                if service_name in self._circuits:
                    self._circuits[service_name]["state"] = CircuitState.CLOSED
                    self._circuits[service_name]["failure_count"] = 0
                return ActionResult(success=True, message=f"Circuit '{service_name}' reset")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker error: {str(e)}")
