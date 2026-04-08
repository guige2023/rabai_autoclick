"""Proxy Pattern Action Module.

Provides proxy pattern for controlled
access to objects.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Proxy:
    """Proxy implementation."""
    proxy_id: str
    name: str
    real_object: Any
    pre_hook: Optional[Callable] = None
    post_hook: Optional[Callable] = None
    access_count: int = 0


class ProxyManager:
    """Manages proxy pattern."""

    def __init__(self):
        self._proxies: Dict[str, Proxy] = {}

    def create_proxy(
        self,
        name: str,
        real_object: Any,
        pre_hook: Optional[Callable] = None,
        post_hook: Optional[Callable] = None
    ) -> str:
        """Create a proxy."""
        proxy_id = f"proxy_{name.lower().replace(' ', '_')}"

        proxy = Proxy(
            proxy_id=proxy_id,
            name=name,
            real_object=real_object,
            pre_hook=pre_hook,
            post_hook=post_hook
        )

        self._proxies[proxy_id] = proxy
        return proxy_id

    def access(self, proxy_id: str, method: str, *args, **kwargs) -> Any:
        """Access through proxy."""
        proxy = self._proxies.get(proxy_id)
        if not proxy:
            raise ValueError(f"Proxy not found: {proxy_id}")

        proxy.access_count += 1

        if proxy.pre_hook:
            proxy.pre_hook(method, args, kwargs)

        result = getattr(proxy.real_object, method)(*args, **kwargs)

        if proxy.post_hook:
            proxy.post_hook(method, result)

        return result

    def get_stats(self, proxy_id: str) -> Optional[Dict]:
        """Get proxy statistics."""
        proxy = self._proxies.get(proxy_id)
        if not proxy:
            return None

        return {
            "proxy_id": proxy.proxy_id,
            "name": proxy.name,
            "access_count": proxy.access_count
        }


class ProxyPatternAction(BaseAction):
    """Action for proxy pattern operations."""

    def __init__(self):
        super().__init__("proxy")
        self._manager = ProxyManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute proxy action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "access":
                return self._access(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create proxy."""
        proxy_id = self._manager.create_proxy(
            name=params.get("name", ""),
            real_object=params.get("real_object") or {},
            pre_hook=params.get("pre_hook"),
            post_hook=params.get("post_hook")
        )
        return ActionResult(success=True, data={"proxy_id": proxy_id})

    def _access(self, params: Dict) -> ActionResult:
        """Access through proxy."""
        try:
            result = self._manager.access(
                params.get("proxy_id", ""),
                params.get("method", ""),
                params.get("args", []),
                params.get("kwargs", {})
            )
            return ActionResult(success=True, data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _stats(self, params: Dict) -> ActionResult:
        """Get stats."""
        stats = self._manager.get_stats(params.get("proxy_id", ""))
        if not stats:
            return ActionResult(success=False, message="Proxy not found")
        return ActionResult(success=True, data=stats)
