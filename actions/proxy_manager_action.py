"""
Proxy Manager Action Module.

Manages proxy rotation, health checking, geolocation,
and session-based proxy persistence for web scraping.

Example:
    >>> from proxy_manager_action import ProxyManager
    >>> pm = ProxyManager()
    >>> pm.add_proxy("http://1.2.3.4:8080", country="US")
    >>> proxy = pm.get_next_proxy()
"""
from __future__ import annotations

import asyncio
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class Proxy:
    """Represents a proxy server."""
    url: str
    country: str = ""
    city: str = ""
    latency_ms: float = 0
    success_count: int = 0
    fail_count: int = 0
    last_used: float = 0
    last_checked: float = 0
    is_healthy: bool = True
    tags: list[str] = field(default_factory=list)


@dataclass
class ProxyStats:
    """Statistics for proxy usage."""
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    avg_latency_ms: float = 0


class ProxyManager:
    """Manage proxy pool with rotation and health checks."""

    def __init__(self, health_check_interval: float = 300):
        self._proxies: list[Proxy] = []
        self._index: int = 0
        self._health_check_interval = health_check_interval
        self._stats: ProxyStats = ProxyStats()
        self._lock = asyncio.Lock()

    def add_proxy(
        self,
        url: str,
        country: str = "",
        city: str = "",
        tags: Optional[list[str]] = None,
    ) -> None:
        """Add a proxy to the pool."""
        proxy = Proxy(
            url=url,
            country=country,
            city=city,
            tags=tags or [],
        )
        self._proxies.append(proxy)

    def remove_proxy(self, url: str) -> bool:
        """Remove a proxy by URL."""
        for i, p in enumerate(self._proxies):
            if p.url == url:
                self._proxies.pop(i)
                return True
        return False

    def get_proxy(
        self,
        country: Optional[str] = None,
        tags: Optional[list[str]] = None,
        min_success_rate: float = 0.0,
    ) -> Optional[Proxy]:
        """Get a proxy matching criteria, using round-robin."""
        candidates = self._proxies
        if country:
            candidates = [p for p in candidates if p.country == country]
        if tags:
            candidates = [p for p in candidates if any(t in p.tags for t in tags)]
        if min_success_rate > 0:
            candidates = [p for p in candidates if self._success_rate(p) >= min_success_rate]

        candidates = [p for p in candidates if p.is_healthy]

        if not candidates:
            return None

        proxy = candidates[self._index % len(candidates)]
        self._index = (self._index + 1) % len(candidates)
        proxy.last_used = time.time()
        return proxy

    def get_next_proxy(self) -> Optional[Proxy]:
        """Get next healthy proxy in rotation."""
        candidates = [p for p in self._proxies if p.is_healthy]
        if not candidates:
            return None
        proxy = candidates[self._index % len(candidates)]
        self._index = (self._index + 1) % len(candidates)
        proxy.last_used = time.time()
        return proxy

    def mark_success(self, proxy_url: str, latency_ms: float) -> None:
        """Record successful proxy request."""
        for p in self._proxies:
            if p.url == proxy_url:
                p.success_count += 1
                p.latency_ms = (p.latency_ms * p.success_count + latency_ms) / (p.success_count + 1)
                p.last_used = time.time()
                self._stats.successful += 1
                self._stats.total_requests += 1
                break

    def mark_failure(self, proxy_url: str) -> None:
        """Record failed proxy request."""
        for p in self._proxies:
            if p.url == proxy_url:
                p.fail_count += 1
                p.last_used = time.time()
                if p.fail_count >= 3:
                    p.is_healthy = False
                self._stats.failed += 1
                self._stats.total_requests += 1
                break

    async def health_check(self, timeout: float = 5.0) -> list[dict[str, Any]]:
        """Check health of all proxies by making test request."""
        test_url = "http://httpbin.org/ip"
        results: list[dict[str, Any]] = []

        for proxy in self._proxies:
            try:
                proxy.last_checked = time.time()
                start = time.monotonic()
                req = urllib.request.Request(test_url)
                req.set_proxy(proxy.url, "http")
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    latency = (time.monotonic() - start) * 1000
                    if resp.status == 200:
                        proxy.is_healthy = True
                        proxy.latency_ms = latency
                        results.append({"url": proxy.url, "healthy": True, "latency_ms": latency})
                    else:
                        proxy.is_healthy = False
                        results.append({"url": proxy.url, "healthy": False, "latency_ms": 0})
            except Exception as e:
                proxy.is_healthy = False
                results.append({"url": proxy.url, "healthy": False, "error": str(e)})

        return results

    def get_stats(self) -> ProxyStats:
        """Get aggregate proxy statistics."""
        if self._stats.total_requests > 0:
            self._stats.avg_latency_ms = (
                sum(p.latency_ms for p in self._proxies if p.latency_ms > 0) /
                max(1, sum(1 for p in self._proxies if p.latency_ms > 0))
            )
        return self._stats

    def _success_rate(self, proxy: Proxy) -> float:
        total = proxy.success_count + proxy.fail_count
        if total == 0:
            return 1.0
        return proxy.success_count / total

    def get_all_proxies(self) -> list[dict[str, Any]]:
        """Get all proxies with their status."""
        return [
            {
                "url": p.url,
                "country": p.country,
                "city": p.city,
                "latency_ms": p.latency_ms,
                "success_rate": self._success_rate(p),
                "is_healthy": p.is_healthy,
                "last_used": p.last_used,
                "tags": p.tags,
            }
            for p in self._proxies
        ]

    def set_proxy_country(self, url: str, country: str, city: str = "") -> bool:
        """Update country info for a proxy."""
        for p in self._proxies:
            if p.url == url:
                p.country = country
                p.city = city
                return True
        return False

    def reset_stats(self) -> None:
        """Reset usage statistics."""
        for p in self._proxies:
            p.success_count = 0
            p.fail_count = 0
            p.latency_ms = 0
        self._stats = ProxyStats()

    def get_geo_for_ip(self, ip: str) -> Optional[dict[str, str]]:
        """Get geolocation info for an IP address."""
        try:
            req = urllib.request.Request(f"http://ip-api.com/json/{ip}")
            with urllib.request.urlopen(req, timeout=5) as resp:
                import json
                data = json.loads(resp.read())
                if data.get("status") == "success":
                    return {
                        "country": data.get("country", ""),
                        "city": data.get("city", ""),
                        "region": data.get("regionName", ""),
                        "isp": data.get("isp", ""),
                    }
        except Exception:
            pass
        return None


if __name__ == "__main__":
    pm = ProxyManager()
    pm.add_proxy("http://1.2.3.4:8080", country="US")
    pm.add_proxy("http://5.6.7.8:3128", country="DE")
    print(f"Proxies: {len(pm.get_all_proxies())}")
    proxy = pm.get_proxy(country="US")
    print(f"Selected: {proxy.url if proxy else 'None'}")
