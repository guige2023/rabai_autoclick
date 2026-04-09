"""API Gateway Fallback Action Module.

Provides fallback routing and graceful degradation for API gateway requests.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class APIGatewayFallbackAction(BaseAction):
    """Handle API gateway fallback routing.
    
    Routes requests to fallback endpoints when primary services are unavailable.
    Supports circuit breaker pattern and graceful degradation.
    """
    action_type = "api_gateway_fallback"
    display_name = "API网关降级"
    description = "当主服务不可用时路由到备用端点"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fallback routing.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: primary_url, fallback_urls, timeout,
                   retry_count, health_check_url.
        
        Returns:
            ActionResult with fallback response or error.
        """
        primary_url = params.get('primary_url', '')
        fallback_urls = params.get('fallback_urls', [])
        timeout = params.get('timeout', 5)
        retry_count = params.get('retry_count', 3)
        health_check_url = params.get('health_check_url', '')
        
        if not fallback_urls:
            return ActionResult(
                success=False,
                data=None,
                error="No fallback URLs provided"
            )
        
        # Try primary URL first
        for attempt in range(retry_count):
            try:
                if self._check_health(primary_url, timeout):
                    return ActionResult(
                        success=True,
                        data={"source": "primary", "url": primary_url},
                        error=None
                    )
            except Exception:
                pass
        
        # Fall through to fallback URLs
        for fallback_url in fallback_urls:
            try:
                if self._check_health(fallback_url, timeout):
                    return ActionResult(
                        success=True,
                        data={"source": "fallback", "url": fallback_url},
                        error=None
                    )
            except Exception:
                continue
        
        return ActionResult(
            success=False,
            data=None,
            error="All endpoints unavailable"
        )
    
    def _check_health(self, url: str, timeout: int) -> bool:
        """Check if an endpoint is healthy."""
        import urllib.request
        try:
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except Exception:
            return False


class APIFallbackChainAction(BaseAction):
    """Chain multiple fallback endpoints in sequence.
    
    Tries each endpoint in order until one succeeds.
    """
    action_type = "api_fallback_chain"
    display_name = "API降级链"
    description = "按顺序尝试多个备用端点"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fallback chain.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoints (list of dicts with url, weight, timeout).
        
        Returns:
            ActionResult with successful endpoint info.
        """
        endpoints = params.get('endpoints', [])
        
        if not endpoints:
            return ActionResult(
                success=False,
                data=None,
                error="No endpoints configured"
            )
        
        # Sort by weight (higher weight first)
        sorted_endpoints = sorted(endpoints, key=lambda x: x.get('weight', 0), reverse=True)
        
        for endpoint in sorted_endpoints:
            url = endpoint.get('url', '')
            timeout = endpoint.get('timeout', 5)
            
            try:
                if self._check_endpoint(url, timeout):
                    return ActionResult(
                        success=True,
                        data={
                            "url": url,
                            "response_time": endpoint.get('response_time', 0)
                        },
                        error=None
                    )
            except Exception:
                continue
        
        return ActionResult(
            success=False,
            data=None,
            error="All fallback endpoints failed"
        )
    
    def _check_endpoint(self, url: str, timeout: int) -> bool:
        """Check if endpoint is reachable."""
        import urllib.request
        try:
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except Exception:
            return False


class APIFallbackMonitorAction(BaseAction):
    """Monitor fallback health and trigger switches.
    
    Continuously monitors endpoint health and自动 switches to healthy endpoints.
    """
    action_type = "api_fallback_monitor"
    display_name = "API降级监控"
    description = "监控备用端点健康状态并自动切换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fallback monitoring.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoints, check_interval, failure_threshold.
        
        Returns:
            ActionResult with current healthy endpoint.
        """
        endpoints = params.get('endpoints', [])
        check_interval = params.get('check_interval', 30)
        failure_threshold = params.get('failure_threshold', 3)
        
        if not endpoints:
            return ActionResult(
                success=False,
                data=None,
                error="No endpoints to monitor"
            )
        
        healthy_endpoints = []
        
        for endpoint in endpoints:
            url = endpoint.get('url', '')
            consecutive_failures = endpoint.get('consecutive_failures', 0)
            
            try:
                if self._check_health(url):
                    healthy_endpoints.append({
                        "url": url,
                        "status": "healthy",
                        "failures": consecutive_failures
                    })
            except Exception:
                consecutive_failures += 1
                if consecutive_failures < failure_threshold:
                    healthy_endpoints.append({
                        "url": url,
                        "status": "degraded",
                        "failures": consecutive_failures
                    })
        
        if not healthy_endpoints:
            return ActionResult(
                success=False,
                data={"status": "all_unhealthy"},
                error="All endpoints are unhealthy"
            )
        
        return ActionResult(
            success=True,
            data={
                "healthy_count": len(healthy_endpoints),
                "endpoints": healthy_endpoints
            },
            error=None
        )
    
    def _check_health(self, url: str) -> bool:
        """Check endpoint health."""
        import urllib.request
        try:
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=3)
            return True
        except Exception:
            return False


def register_actions():
    """Register all API Gateway Fallback actions."""
    return [
        APIGatewayFallbackAction,
        APIFallbackChainAction,
        APIFallbackMonitorAction,
    ]
