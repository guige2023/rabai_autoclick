# Copyright (c) 2024. coded by claude
"""API Security Action Module.

Provides security utilities for API protection including
rate limiting, IP blocking, request signing, and input sanitization.
"""
from typing import Optional, Dict, Any, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import hmac
import logging

logger = logging.getLogger(__name__)


@dataclass
class SecurityConfig:
    enable_rate_limiting: bool = True
    enable_ip_blacklist: bool = True
    enable_request_signing: bool = False
    max_requests_per_minute: int = 60
    secret_key: Optional[str] = None


@dataclass
class RequestInfo:
    ip_address: str
    user_agent: Optional[str]
    timestamp: datetime
    path: str
    method: str


@dataclass
class SecurityResult:
    allowed: bool
    reason: Optional[str] = None
    remaining_requests: Optional[int] = None


class APISecurity:
    def __init__(self, config: Optional[SecurityConfig] = None):
        self.config = config or SecurityConfig()
        self._ip_blacklist: Set[str] = set()
        self._ip_request_counts: Dict[str, list] = {}
        self._lock_hmac = None

    def add_ip_to_blacklist(self, ip: str) -> None:
        self._ip_blacklist.add(ip)

    def remove_ip_from_blacklist(self, ip: str) -> bool:
        if ip in self._ip_blacklist:
            self._ip_blacklist.discard(ip)
            return True
        return False

    def check_request(self, request: RequestInfo) -> SecurityResult:
        if self.config.enable_ip_blacklist and request.ip_address in self._ip_blacklist:
            return SecurityResult(allowed=False, reason="IP address blocked")
        if self.config.enable_rate_limiting:
            rate_result = self._check_rate_limit(request.ip_address)
            if not rate_result:
                return SecurityResult(allowed=False, reason="Rate limit exceeded")
            return rate_result
        return SecurityResult(allowed=True, remaining_requests=self.config.max_requests_per_minute)

    def _check_rate_limit(self, ip: str) -> Optional[SecurityResult]:
        now = datetime.now()
        if ip not in self._ip_request_counts:
            self._ip_request_counts[ip] = []
        request_times = self._ip_request_counts[ip]
        request_times = [t for t in request_times if (now - t).total_seconds() < 60]
        self._ip_request_counts[ip] = request_times
        if len(request_times) >= self.config.max_requests_per_minute:
            return SecurityResult(allowed=False, reason="Rate limit exceeded", remaining_requests=0)
        request_times.append(now)
        remaining = self.config.max_requests_per_minute - len(request_times)
        return SecurityResult(allowed=True, remaining_requests=remaining)

    def sign_request(self, method: str, path: str, body: Optional[str], timestamp: str) -> str:
        if not self.config.secret_key:
            raise ValueError("Secret key not configured")
        message = f"{method}{path}{body or ''}{timestamp}"
        signature = hmac.new(
            self.config.secret_key.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()
        return signature

    def verify_request_signature(self, signature: str, method: str, path: str, body: Optional[str], timestamp: str) -> bool:
        if not self.config.secret_key:
            return False
        expected = self.sign_request(method, path, body, timestamp)
        return hmac.compare_digest(signature, expected)

    def sanitize_input(self, text: str) -> str:
        dangerous_chars = ["<", ">", '"', "'", "&", ";", "|", "`"]
        for char in dangerous_chars:
            text = text.replace(char, "")
        return text

    def get_blocked_ips(self) -> Set[str]:
        return set(self._ip_blacklist)

    def clear_rate_limit_data(self) -> None:
        self._ip_request_counts.clear()
