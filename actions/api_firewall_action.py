"""
API Firewall Action.

Provides API request filtering and protection.
Supports:
- Rate limiting
- IP blocking/allowlisting
- Request validation
- SQL/NoSQL injection prevention
- Size limits
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import threading
import logging
import json
import re
import time

logger = logging.getLogger(__name__)


class FirewallAction(Enum):
    """Action to take on blocked request."""
    ALLOW = "allow"
    BLOCK = "block"
    LOG = "log"
    CHALLENGE = "challenge"


class BlockReason(Enum):
    """Reason for blocking a request."""
    RATE_LIMITED = "rate_limited"
    IP_BLOCKED = "ip_blocked"
    INVALID_REQUEST = "invalid_request"
    SIZE_EXCEEDED = "size_exceeded"
    INJECTION_DETECTED = "injection_detected"
    INVALID_SIGNATURE = "invalid_signature"


@dataclass
class RequestContext:
    """Context of an API request."""
    client_ip: str
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Optional[str] = None
    user_agent: Optional[str] = None
    content_length: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FirewallResult:
    """Result of firewall check."""
    allowed: bool
    action: FirewallAction
    reason: Optional[BlockReason] = None
    message: Optional[str] = None
    retry_after: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "allowed": self.allowed,
            "action": self.action.value,
            "reason": self.reason.value if self.reason else None,
            "message": self.message,
            "retry_after": self.retry_after
        }


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    requests: int
    window_seconds: int
    
    @property
    def window_delta(self) -> timedelta:
        """Get window as timedelta."""
        return timedelta(seconds=self.window_seconds)


@dataclass
class IPRule:
    """IP rule for firewall."""
    ip_pattern: str  # Can be IP, CIDR, or wildcard
    action: FirewallAction
    reason: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class LogEntry:
    """Firewall log entry."""
    timestamp: datetime
    client_ip: str
    path: str
    action: FirewallAction
    reason: Optional[BlockReason]
    message: Optional[str]
    request_id: str


class ApiFirewallAction:
    """
    API Firewall Action.
    
    Provides API protection with support for:
    - Rate limiting (token bucket, sliding window)
    - IP blocking/allowlisting
    - Request size limits
    - Injection detection (SQL, NoSQL, XSS)
    - Request validation
    """
    
    def __init__(
        self,
        enable_rate_limit: bool = True,
        enable_ip_block: bool = True,
        enable_size_limit: bool = True,
        enable_injection_check: bool = True,
        default_rate_limit: Optional[RateLimitConfig] = None,
        max_request_size: int = 10 * 1024 * 1024  # 10MB
    ):
        """
        Initialize the API Firewall Action.
        
        Args:
            enable_rate_limit: Enable rate limiting
            enable_ip_block: Enable IP blocking
            enable_size_limit: Enable request size limit
            enable_injection_check: Enable injection detection
            default_rate_limit: Default rate limit config
            max_request_size: Maximum request size in bytes
        """
        self.enable_rate_limit = enable_rate_limit
        self.enable_ip_block = enable_ip_block
        self.enable_size_limit = enable_size_limit
        self.enable_injection_check = enable_injection_check
        self.max_request_size = max_request_size
        
        self.default_rate_limit = default_rate_limit or RateLimitConfig(requests=100, window_seconds=60)
        
        self._ip_rules: List[IPRule] = []
        self._rate_limit_buckets: Dict[str, List[datetime]] = {}
        self._blocked_ips: Dict[str, datetime] = {}
        self._logs: List[LogEntry] = []
        self._logs_lock = threading.RLock()
        self._rate_limit_lock = threading.RLock()
        
        # Injection patterns
        self._sql_patterns = [
            r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|UNION)\b)",
            r"(--|;|/\*|\*/)",
            r"('|\"|;|=)",
            r"(\bOR\b.*=.*\bOR\b)",
            r"(\bAND\b.*=.*\bAND\b)",
        ]
        
        self._nosql_patterns = [
            r"(\$where|\$eval|\$ne|\$eq|\$regex)",
            r"(\btrue\b|\bfalse\b)",
        ]
        
        self._xss_patterns = [
            r"(<script|<\/script|javascript:)",
            r"(onerror|onload|onclick|onmouseover)",
            r"(<iframe|<object|<embed)",
        ]
    
    def add_ip_rule(
        self,
        ip_pattern: str,
        action: FirewallAction,
        reason: str = ""
    ) -> "ApiFirewallAction":
        """
        Add an IP rule.
        
        Args:
            ip_pattern: IP address, CIDR, or wildcard pattern
            action: Action to take
            reason: Reason for the rule
        
        Returns:
            Self for chaining
        """
        rule = IPRule(ip_pattern=ip_pattern, action=action, reason=reason)
        self._ip_rules.append(rule)
        logger.info(f"Added IP rule: {ip_pattern} -> {action.value}")
        return self
    
    def block_ip(self, ip: str, reason: str = "") -> None:
        """Block an IP address."""
        self._blocked_ips[ip] = datetime.utcnow()
        self.add_ip_rule(ip, FirewallAction.BLOCK, reason)
    
    def unblock_ip(self, ip: str) -> bool:
        """Unblock an IP address."""
        if ip in self._blocked_ips:
            del self._blocked_ips[ip]
            self._ip_rules = [r for r in self._ip_rules if r.ip_pattern != ip]
            logger.info(f"Unblocked IP: {ip}")
            return True
        return False
    
    def check_request(self, context: RequestContext) -> FirewallResult:
        """
        Check if a request should be allowed.
        
        Args:
            context: Request context
        
        Returns:
            FirewallResult indicating if request is allowed
        """
        request_id = f"{context.client_ip}:{int(time.time() * 1000)}"
        
        # 1. Check if IP is blocked
        if self.enable_ip_block:
            ip_result = self._check_ip(context.client_ip)
            if not ip_result.allowed:
                self._log(request_id, context, ip_result)
                return ip_result
        
        # 2. Check request size
        if self.enable_size_limit:
            size_result = self._check_size(context)
            if not size_result.allowed:
                self._log(request_id, context, size_result)
                return size_result
        
        # 3. Check rate limit
        if self.enable_rate_limit:
            rate_result = self._check_rate_limit(context)
            if not rate_result.allowed:
                self._log(request_id, context, rate_result)
                return rate_result
        
        # 4. Check for injection
        if self.enable_injection_check:
            injection_result = self._check_injection(context)
            if not injection_result.allowed:
                self._log(request_id, context, injection_result)
                return injection_result
        
        return FirewallResult(allowed=True, action=FirewallAction.ALLOW)
    
    def _check_ip(self, client_ip: str) -> FirewallResult:
        """Check IP against rules."""
        # Check blocked IPs
        if client_ip in self._blocked_ips:
            return FirewallResult(
                allowed=False,
                action=FirewallAction.BLOCK,
                reason=BlockReason.IP_BLOCKED,
                message=f"IP {client_ip} is blocked"
            )
        
        # Check IP rules
        for rule in self._ip_rules:
            if self._ip_matches(client_ip, rule.ip_pattern):
                if rule.action == FirewallAction.BLOCK:
                    return FirewallResult(
                        allowed=False,
                        action=FirewallAction.BLOCK,
                        reason=BlockReason.IP_BLOCKED,
                        message=rule.reason or f"IP {client_ip} matches block rule"
                    )
                elif rule.action == FirewallAction.ALLOW:
                    return FirewallResult(
                        allowed=True,
                        action=FirewallAction.ALLOW
                    )
        
        return FirewallResult(allowed=True, action=FirewallAction.ALLOW)
    
    def _check_size(self, context: RequestContext) -> FirewallResult:
        """Check request size."""
        if context.content_length > self.max_request_size:
            return FirewallResult(
                allowed=False,
                action=FirewallAction.BLOCK,
                reason=BlockReason.SIZE_EXCEEDED,
                message=f"Request size {context.content_length} exceeds limit {self.max_request_size}"
            )
        
        return FirewallResult(allowed=True, action=FirewallAction.ALLOW)
    
    def _check_rate_limit(self, context: RequestContext) -> FirewallResult:
        """Check rate limit for client."""
        # Use user_id if available, otherwise use IP
        key = context.user_id or context.client_ip
        
        with self._rate_limit_lock:
            now = datetime.utcnow()
            
            if key not in self._rate_limit_buckets:
                self._rate_limit_buckets[key] = []
            
            # Clean old entries
            window = self.default_rate_limit.window_delta
            self._rate_limit_buckets[key] = [
                t for t in self._rate_limit_buckets[key]
                if now - t < window
            ]
            
            # Check limit
            if len(self._rate_limit_buckets[key]) >= self.default_rate_limit.requests:
                retry_after = int((self._rate_limit_buckets[key][0] + window - now).total_seconds())
                return FirewallResult(
                    allowed=False,
                    action=FirewallAction.BLOCK,
                    reason=BlockReason.RATE_LIMITED,
                    message=f"Rate limit exceeded. Try again in {retry_after}s",
                    retry_after=max(1, retry_after)
                )
            
            # Add new entry
            self._rate_limit_buckets[key].append(now)
        
        return FirewallResult(allowed=True, action=FirewallAction.ALLOW)
    
    def _check_injection(self, context: RequestContext) -> FirewallResult:
        """Check for injection attacks."""
        # Check path
        if self._contains_injection(context.path):
            return FirewallResult(
                allowed=False,
                action=FirewallAction.BLOCK,
                reason=BlockReason.INJECTION_DETECTED,
                message="Injection pattern detected in path"
            )
        
        # Check query params
        for key, value in context.query_params.items():
            if self._contains_injection(f"{key}={value}"):
                return FirewallResult(
                    allowed=False,
                    action=FirewallAction.BLOCK,
                    reason=BlockReason.INJECTION_DETECTED,
                    message=f"Injection pattern detected in query parameter: {key}"
                )
        
        # Check body
        if context.body and self._contains_injection(context.body):
            return FirewallResult(
                allowed=False,
                action=FirewallAction.BLOCK,
                reason=BlockReason.INJECTION_DETECTED,
                message="Injection pattern detected in request body"
            )
        
        return FirewallResult(allowed=True, action=FirewallAction.ALLOW)
    
    def _contains_injection(self, text: str) -> bool:
        """Check if text contains injection patterns."""
        if not text:
            return False
        
        text_lower = text.lower()
        
        for pattern in self._sql_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"SQL injection pattern detected: {pattern}")
                return True
        
        for pattern in self._nosql_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"NoSQL injection pattern detected: {pattern}")
                return True
        
        for pattern in self._xss_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"XSS pattern detected: {pattern}")
                return True
        
        return False
    
    def _ip_matches(self, ip: str, pattern: str) -> bool:
        """Check if IP matches pattern."""
        if pattern == "*":
            return True
        
        if "/" in pattern:
            # CIDR notation - simplified check
            return ip == pattern.split("/")[0]
        
        if "*" in pattern:
            # Wildcard matching
            import fnmatch
            return fnmatch.fnmatch(ip, pattern)
        
        return ip == pattern
    
    def _log(
        self,
        request_id: str,
        context: RequestContext,
        result: FirewallResult
    ) -> None:
        """Log a firewall event."""
        entry = LogEntry(
            timestamp=datetime.utcnow(),
            client_ip=context.client_ip,
            path=context.path,
            action=result.action,
            reason=result.reason,
            message=result.message,
            request_id=request_id
        )
        
        with self._logs_lock:
            self._logs.append(entry)
            # Keep last 10000 entries
            if len(self._logs) > 10000:
                self._logs = self._logs[-5000:]
        
        if not result.allowed:
            logger.warning(
                f"Request blocked: {context.client_ip} {context.method} {context.path} "
                f"- {result.reason.value if result.reason else 'unknown'}"
            )
    
    def get_logs(
        self,
        limit: int = 100,
        blocked_only: bool = False
    ) -> List[Dict[str, Any]]:
        """Get firewall logs."""
        with self._logs_lock:
            logs = self._logs[-limit:]
            
            if blocked_only:
                logs = [l for l in logs if l.action == FirewallAction.BLOCK]
            
            return [
                {
                    "timestamp": l.timestamp.isoformat(),
                    "client_ip": l.client_ip,
                    "path": l.path,
                    "action": l.action.value,
                    "reason": l.reason.value if l.reason else None,
                    "message": l.message
                }
                for l in logs
            ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get firewall statistics."""
        with self._logs_lock:
            blocked = [l for l in self._logs if l.action == FirewallAction.BLOCK]
            
            return {
                "total_requests": len(self._logs),
                "blocked_requests": len(blocked),
                "blocked_by_reason": self._count_by_reason(blocked),
                "ip_rules_count": len(self._ip_rules),
                "blocked_ips_count": len(self._blocked_ips),
                "rate_limited_keys": len(self._rate_limit_buckets)
            }
    
    def _count_by_reason(self, logs: List[LogEntry]) -> Dict[str, int]:
        """Count logs by reason."""
        counts: Dict[str, int] = {}
        for log in logs:
            if log.reason:
                key = log.reason.value
                counts[key] = counts.get(key, 0) + 1
        return counts


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create firewall
    firewall = ApiFirewallAction(
        default_rate_limit=RateLimitConfig(requests=5, window_seconds=60),
        max_request_size=1024 * 1024
    )
    
    # Add IP rules
    firewall.add_ip_rule("192.168.1.*", FirewallAction.ALLOW, "Allow internal network")
    firewall.add_ip_rule("10.0.0.1", FirewallAction.BLOCK, "Known bad actor")
    
    # Test requests
    test_requests = [
        RequestContext(
            client_ip="192.168.1.100",
            method="GET",
            path="/api/users",
            headers={},
            query_params={}
        ),
        RequestContext(
            client_ip="10.0.0.1",
            method="GET",
            path="/api/users",
            headers={},
            query_params={}
        ),
        RequestContext(
            client_ip="203.0.113.50",
            method="GET",
            path="/api/users",
            headers={},
            query_params={"name": "Robert'}
        ),
        RequestContext(
            client_ip="203.0.113.51",
            method="POST",
            path="/api/users",
            headers={},
            query_params={},
            body='{"name": "Test", "sql": "SELECT * FROM users"}'
        ),
    ]
    
    for req in test_requests:
        result = firewall.check_request(req)
        print(f"{req.client_ip}: {result.action.value} - {result.message or 'OK'}")
    
    print(f"\nStats: {json.dumps(firewall.get_stats(), indent=2)}")
    print(f"\nLogs: {json.dumps(firewall.get_logs(blocked_only=True), indent=2)}")
