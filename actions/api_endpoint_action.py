"""API endpoint management action module for RabAI AutoClick.

Provides API endpoint operations:
- EndpointRegisterAction: Register API endpoints
- EndpointDiscoverAction: Discover available endpoints
- EndpointHealthCheckAction: Health check for endpoints
- EndpointMetricsAction: Collect endpoint metrics
"""

import sys
import os
import time
import logging
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class EndpointInfo:
    """Information about a registered API endpoint."""
    path: str
    method: str
    handler: Optional[Callable] = None
    description: str = ""
    tags: List[str] = field(default_factory=list)
    rate_limit: int = 100
    timeout: float = 30.0
    last_check: Optional[datetime] = None
    status: str = "unknown"
    call_count: int = 0
    error_count: int = 0
    avg_latency: float = 0.0


class EndpointRegistry:
    """Registry for API endpoints."""
    
    def __init__(self) -> None:
        self._endpoints: Dict[str, EndpointInfo] = {}
    
    def register(
        self,
        path: str,
        method: str,
        handler: Optional[Callable] = None,
        description: str = "",
        tags: Optional[List[str]] = None,
        rate_limit: int = 100,
        timeout: float = 30.0
    ) -> bool:
        key = f"{method.upper()}:{path}"
        if key in self._endpoints:
            logger.warning(f"Endpoint {key} already registered")
            return False
        self._endpoints[key] = EndpointInfo(
            path=path,
            method=method.upper(),
            handler=handler,
            description=description,
            tags=tags or [],
            rate_limit=rate_limit,
            timeout=timeout
        )
        return True
    
    def unregister(self, path: str, method: str) -> bool:
        key = f"{method.upper()}:{path}"
        if key in self._endpoints:
            del self._endpoints[key]
            return True
        return False
    
    def get(self, path: str, method: str) -> Optional[EndpointInfo]:
        key = f"{method.upper()}:{path}"
        return self._endpoints.get(key)
    
    def list_all(self, tag: Optional[str] = None) -> List[EndpointInfo]:
        endpoints = list(self._endpoints.values())
        if tag:
            endpoints = [e for e in endpoints if tag in e.tags]
        return endpoints
    
    def update_status(self, path: str, method: str, status: str) -> bool:
        endpoint = self.get(path, method)
        if endpoint:
            endpoint.status = status
            endpoint.last_check = datetime.now()
            return True
        return False


_registry = EndpointRegistry()


class EndpointRegisterAction(BaseAction):
    """Register an API endpoint in the registry."""
    action_type = "api_endpoint_register"
    display_name = "注册API端点"
    description = "在端点注册表中注册一个新的API端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        description = params.get("description", "")
        tags = params.get("tags", [])
        rate_limit = params.get("rate_limit", 100)
        timeout = params.get("timeout", 30.0)

        if not path:
            return ActionResult(success=False, message="路径不能为空")

        valid, msg = self.validate_in(method.upper(), ["GET", "POST", "PUT", "DELETE", "PATCH"], "method")
        if not valid:
            return ActionResult(success=False, message=msg)

        success = _registry.register(
            path=path,
            method=method,
            description=description,
            tags=tags,
            rate_limit=rate_limit,
            timeout=timeout
        )

        if success:
            return ActionResult(
                success=True,
                message=f"端点 {method.upper()} {path} 注册成功",
                data={"path": path, "method": method.upper()}
            )
        return ActionResult(success=False, message=f"端点 {method.upper()} {path} 已存在")


class EndpointDiscoverAction(BaseAction):
    """Discover API endpoints from the registry."""
    action_type = "api_endpoint_discover"
    display_name = "发现API端点"
    description = "从注册表中发现可用的API端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        tag = params.get("tag")
        path_prefix = params.get("path_prefix", "")
        method = params.get("method")

        endpoints = _registry.list_all(tag=tag)

        if path_prefix:
            endpoints = [e for e in endpoints if e.path.startswith(path_prefix)]
        if method:
            endpoints = [e for e in endpoints if e.method == method.upper()]

        result = [
            {
                "path": e.path,
                "method": e.method,
                "description": e.description,
                "tags": e.tags,
                "status": e.status,
                "rate_limit": e.rate_limit
            }
            for e in endpoints
        ]

        return ActionResult(
            success=True,
            message=f"发现 {len(result)} 个端点",
            data={"endpoints": result, "count": len(result)}
        )


class EndpointHealthCheckAction(BaseAction):
    """Perform health check on API endpoints."""
    action_type = "api_endpoint_health"
    display_name = "API端点健康检查"
    description = "对API端点执行健康状态检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method", "GET")
        timeout = params.get("timeout", 5.0)

        endpoint = _registry.get(path, method)
        if not endpoint:
            return ActionResult(success=False, message=f"端点 {method.upper()} {path} 不存在")

        start = time.time()
        status = "healthy"
        error_msg = ""

        try:
            if endpoint.handler:
                endpoint.handler()
        except Exception as e:
            status = "unhealthy"
            error_msg = str(e)
            endpoint.error_count += 1

        latency = time.time() - start
        endpoint.avg_latency = (endpoint.avg_latency * 0.7 + latency * 0.3)
        endpoint.last_check = datetime.now()
        endpoint.status = status

        return ActionResult(
            success=(status == "healthy"),
            message=f"健康检查完成: {status}",
            data={
                "path": path,
                "method": method,
                "status": status,
                "latency_ms": round(latency * 1000, 2),
                "error": error_msg
            }
        )


class EndpointMetricsAction(BaseAction):
    """Collect metrics from API endpoints."""
    action_type = "api_endpoint_metrics"
    display_name = "API端点指标收集"
    description = "收集API端点的调用指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        path = params.get("path", "")
        method = params.get("method")

        if path and method:
            endpoint = _registry.get(path, method)
            if not endpoint:
                return ActionResult(success=False, message=f"端点不存在")
            endpoints = [endpoint]
        else:
            endpoints = _registry.list_all()

        metrics = []
        for e in endpoints:
            uptime = 0.0
            if e.last_check:
                uptime = (datetime.now() - e.last_check).total_seconds()
            error_rate = (e.error_count / e.call_count * 100) if e.call_count > 0 else 0.0
            metrics.append({
                "path": e.path,
                "method": e.method,
                "call_count": e.call_count,
                "error_count": e.error_count,
                "error_rate": round(error_rate, 2),
                "avg_latency_ms": round(e.avg_latency * 1000, 2),
                "status": e.status,
                "uptime_seconds": round(uptime, 2)
            })

        return ActionResult(
            success=True,
            message=f"收集到 {len(metrics)} 个端点指标",
            data={"metrics": metrics}
        )
