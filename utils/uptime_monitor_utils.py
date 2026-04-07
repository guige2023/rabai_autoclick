"""
Uptime Monitoring Utilities.

Helpers for building custom uptime monitoring checks, alerting on
latency regressions, and integrating with Prometheus/blackbox-exporter
style monitoring setups.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import time
import urllib.request
import urllib.error
import ssl
import socket
from dataclasses import dataclass, field
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Check Result Dataclass
# --------------------------------------------------------------------------- #

@dataclass
class CheckResult:
    """Result of a single uptime check."""

    url: str
    status_code: int
    latency_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    dns_ms: Optional[float] = None
    connect_ms: Optional[float] = None
    tls_ms: Optional[float] = None
    response_bytes: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "success": self.success,
            "error": self.error,
            "timestamp": self.timestamp,
            "dns_ms": self.dns_ms,
            "connect_ms": self.connect_ms,
            "tls_ms": self.tls_ms,
            "response_bytes": self.response_bytes,
        }


# --------------------------------------------------------------------------- #
# HTTP Health Check
# --------------------------------------------------------------------------- #

def check_http(
    url: str,
    method: str = "GET",
    timeout: float = 10.0,
    expected_status: Optional[int] = None,
    expected_text: Optional[str] = None,
    headers: Optional[dict[str, str]] = None,
    follow_redirects: bool = True,
) -> CheckResult:
    """
    Perform an HTTP health check on a URL.

    Args:
        url: Target URL.
        method: HTTP method.
        timeout: Request timeout in seconds.
        expected_status: Expected HTTP status code.
        expected_text: Substring expected in the response body.
        headers: Optional request headers.
        follow_redirects: Whether to follow HTTP redirects.

    Returns:
        CheckResult with timing and status information.
    """
    start = time.monotonic()
    result = CheckResult(url=url, status_code=0, latency_ms=0.0, success=False)
    h = dict(headers) if headers else {}
    h.setdefault("User-Agent", "rabai-uptime-monitor/1.0")
    try:
        req = urllib.request.Request(url, headers=h, method=method)
        ctx = ssl.create_default_context()
        if follow_redirects:
            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ctx)
            )
        else:
            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ctx),
                NoRedirectHandler,
            )
        with opener.open(req, timeout=timeout) as resp:
            latency = (time.monotonic() - start) * 1000
            result.latency_ms = latency
            result.status_code = resp.status
            result.success = True
            result.response_bytes = int(resp.headers.get("Content-Length", 0))
            if expected_status and resp.status != expected_status:
                result.success = False
                result.error = f"Unexpected status: {resp.status}"
            if expected_text:
                body = resp.read().decode("utf-8", errors="replace")
                if expected_text not in body:
                    result.success = False
                    result.error = f"Expected text not found: {expected_text!r}"
    except urllib.error.HTTPError as exc:
        result.status_code = exc.code
        result.latency_ms = (time.monotonic() - start) * 1000
        result.error = f"HTTP {exc.code}"
        if expected_status == exc.code:
            result.success = True
    except urllib.error.URLError as exc:
        result.latency_ms = (time.monotonic() - start) * 1000
        result.error = str(exc.reason)
    except Exception as exc:
        result.latency_ms = (time.monotonic() - start) * 1000
        result.error = str(exc)
    return result


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


# --------------------------------------------------------------------------- #
# TCP / Port Check
# --------------------------------------------------------------------------- #

def check_tcp(
    host: str,
    port: int,
    timeout: float = 5.0,
) -> CheckResult:
    """
    Check if a TCP port is open and responsive.

    Args:
        host: Target hostname.
        port: TCP port number.
        timeout: Connection timeout in seconds.

    Returns:
        CheckResult with latency and status.
    """
    start = time.monotonic()
    result = CheckResult(
        url=f"tcp://{host}:{port}",
        status_code=0,
        latency_ms=0.0,
        success=False,
    )
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        result.connect_ms = (time.monotonic() - start) * 1000
        result.latency_ms = result.connect_ms
        result.status_code = 1
        result.success = True
        sock.close()
    except socket.timeout:
        result.error = "Connection timeout"
        result.latency_ms = (time.monotonic() - start) * 1000
    except ConnectionRefusedError:
        result.error = "Connection refused"
        result.latency_ms = (time.monotonic() - start) * 1000
    except Exception as exc:
        result.error = str(exc)
        result.latency_ms = (time.monotonic() - start) * 1000
    return result


# --------------------------------------------------------------------------- #
# DNS Check
# --------------------------------------------------------------------------- #

def check_dns(
    hostname: str,
    expected_ip: Optional[str] = None,
    timeout: float = 5.0,
) -> CheckResult:
    """
    Resolve a hostname and optionally verify the returned IP.

    Args:
        hostname: Hostname to resolve.
        expected_ip: Expected IP address.
        timeout: Resolution timeout.

    Returns:
        CheckResult with resolved IP and timing.
    """
    start = time.monotonic()
    result = CheckResult(
        url=f"dns://{hostname}",
        status_code=0,
        latency_ms=0.0,
        success=False,
    )
    try:
        import socket
        socket.setdefaulttimeout(timeout)
        t0 = time.monotonic()
        ip = socket.gethostbyname(hostname)
        result.dns_ms = (time.monotonic() - t0) * 1000
        result.latency_ms = result.dns_ms
        result.status_code = 1
        result.success = True
        if expected_ip and ip != expected_ip:
            result.success = False
            result.error = f"IP mismatch: expected {expected_ip}, got {ip}"
    except socket.gaierror as exc:
        result.error = f"DNS lookup failed: {exc}"
        result.latency_ms = (time.monotonic() - start) * 1000
    except Exception as exc:
        result.error = str(exc)
        result.latency_ms = (time.monotonic() - start) * 1000
    return result


# --------------------------------------------------------------------------- #
# Batch Monitoring
# --------------------------------------------------------------------------- #

def run_monitoring_batch(
    targets: list[dict[str, Any]],
) -> list[CheckResult]:
    """
    Run checks against a list of targets.

    Each target dict supports:
        - url: HTTP URL (checked with check_http)
        - host + port: TCP check
        - hostname: DNS check
    """
    results: list[CheckResult] = []
    for target in targets:
        if "url" in target:
            result = check_http(
                target["url"],
                timeout=target.get("timeout", 10.0),
                expected_status=target.get("expected_status"),
            )
        elif "host" in target and "port" in target:
            result = check_tcp(
                target["host"],
                int(target["port"]),
                timeout=target.get("timeout", 5.0),
            )
        elif "hostname" in target:
            result = check_dns(
                target["hostname"],
                expected_ip=target.get("expected_ip"),
            )
        else:
            continue
        results.append(result)
    return results


def check_result_to_prometheus(
    results: list[CheckResult],
    job_name: str = "uptime_monitor",
) -> str:
    """
    Format check results as Prometheus metrics in text exposition format.

    Outputs metrics:
        up{job="$job", instance="$url"} 1|0
        http_duration_ms{job="$job", instance="$url"} $latency
    """
    lines: list[str] = []
    for r in results:
        labels = f'job="{job_name}",instance="{r.url}"'
        up = 1 if r.success else 0
        lines.append(f'up{{{labels}}} {up}')
        lines.append(f'http_duration_ms{{{labels}}} {r.latency_ms:.2f}')
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Alerting Thresholds
# --------------------------------------------------------------------------- #

def check_thresholds(
    results: list[CheckResult],
    latency_threshold_ms: float = 5000.0,
    failure_threshold: int = 3,
) -> list[CheckResult]:
    """
    Filter results that breach alerting thresholds.

    Args:
        results: List of CheckResults.
        latency_threshold_ms: Max acceptable latency.
        failure_threshold: Number of consecutive failures to alert on.

    Returns:
        List of failing CheckResults.
    """
    failures: list[CheckResult] = []
    for r in results:
        if not r.success or r.latency_ms > latency_threshold_ms:
            failures.append(r)
    return failures
