"""
API Security Action Module

Provides API security features: CORS, headers, sanitization, and protection.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import re
import html


class ThreatType(Enum):
    """Types of security threats."""
    SQL_INJECTION = "sql_injection"
    XSS = "xss"
    CSRF = "csrf"
    RATE_LIMIT = "rate_limit"
    INVALID_INPUT = "invalid_input"
    AUTH_BYPASS = "auth_bypass"


@dataclass
class SecurityConfig:
    """Security configuration."""
    enable_cors: bool = True
    enable_csrf_protection: bool = True
    enable_input_validation: bool = True
    enable_rate_limiting: bool = True
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    allowed_methods: list[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    allowed_headers: list[str] = field(default_factory=lambda: ["Content-Type", "Authorization"])
    max_request_size_kb: int = 1024
    sanitization_rules: dict[str, str] = field(default_factory=dict)


@dataclass
class ThreatDetection:
    """A detected threat."""
    threat_type: ThreatType
    severity: str  # low, medium, high, critical
    description: str
    request_path: str
    request_method: str
    detected_at: datetime = field(default_factory=datetime.now)
    blocked: bool = False
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SanitizationResult:
    """Result of sanitization."""
    original: Any
    sanitized: Any
    threats_removed: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class InputSanitizer:
    """Sanitizes user input to prevent attacks."""
    
    SQL_PATTERNS = [
        r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|EXECUTE)\b)",
        r"(--|;|/\*|\*/)",
        r"(\bUNION\b.*\bSELECT\b)",
        r"('|(\\'\\'))",
    ]
    
    XSS_PATTERNS = [
        r"<script[^>]*>.*?</script>",
        r"javascript:",
        r"on\w+\s*=",
        r"<iframe[^>]*>",
        r"<object[^>]*>",
        r"<embed[^>]*>",
    ]
    
    def __init__(self):
        self._sql_regex = [re.compile(p, re.IGNORECASE) for p in self.SQL_PATTERNS]
        self._xss_regex = [re.compile(p, re.IGNORECASE) for p in self.XSS_PATTERNS]
    
    def sanitize_string(self, value: str) -> SanitizationResult:
        """Sanitize a string value."""
        original = value
        threats = []
        warnings = []
        
        # Check for SQL injection patterns
        for pattern in self._sql_regex:
            if pattern.search(value):
                threats.append("sql_injection_pattern")
                value = pattern.sub("BLOCKED", value)
        
        # Check for XSS patterns
        for pattern in self._xss_regex:
            matches = pattern.findall(value)
            if matches:
                threats.append("xss_pattern")
                value = pattern.sub("", value)
        
        # HTML escape
        if "<" in value or ">" in value or "&" in value:
            value = html.escape(value)
        
        # Remove null bytes
        value = value.replace("\x00", "")
        
        return SanitizationResult(
            original=original,
            sanitized=value,
            threats_removed=threats
        )
    
    def sanitize_dict(self, data: dict) -> tuple[dict, list[str]]:
        """Recursively sanitize dictionary values."""
        threats = []
        result = {}
        
        for key, value in data.items():
            if isinstance(value, str):
                sanitized = self.sanitize_string(value)
                result[key] = sanitized.sanitized
                threats.extend(sanitized.threats_removed)
            elif isinstance(value, dict):
                sanitized_value, sub_threats = self.sanitize_dict(value)
                result[key] = sanitized_value
                threats.extend(sub_threats)
            elif isinstance(value, list):
                sanitized_list, list_threats = self.sanitize_list(value)
                result[key] = sanitized_list
                threats.extend(list_threats)
            else:
                result[key] = value
        
        return result, threats
    
    def sanitize_list(self, data: list) -> tuple[list, list[str]]:
        """Recursively sanitize list values."""
        threats = []
        result = []
        
        for item in data:
            if isinstance(item, str):
                sanitized = self.sanitize_string(item)
                result.append(sanitized.sanitized)
                threats.extend(sanitized.threats_removed)
            elif isinstance(item, dict):
                sanitized_item, sub_threats = self.sanitize_dict(item)
                result.append(sanitized_item)
                threats.extend(sub_threats)
            else:
                result.append(item)
        
        return result, threats


class ApiSecurityAction:
    """Main API security action handler."""
    
    def __init__(self, config: Optional[SecurityConfig] = None):
        self.config = config or SecurityConfig()
        self._sanitizer = InputSanitizer()
        self._threat_log: list[ThreatDetection] = []
        self._rate_limiters: dict[str, list[datetime]] = {}
        self._csrf_tokens: dict[str, tuple[str, datetime]] = {}
        self._stats: dict[str, Any] = defaultdict(int)
    
    async def check_request_security(
        self,
        request: dict[str, Any]
    ) -> tuple[bool, list[ThreatDetection]]:
        """
        Check request for security threats.
        
        Args:
            request: Request data
            
        Returns:
            Tuple of (is_safe, list of detected threats)
        """
        threats = []
        
        # Check request size
        body_size = len(str(request.get("body", "")))
        if body_size > self.config.max_request_size_kb * 1024:
            threats.append(ThreatDetection(
                threat_type=ThreatType.INVALID_INPUT,
                severity="medium",
                description=f"Request body too large: {body_size} bytes",
                request_path=request.get("path", ""),
                request_method=request.get("method", "")
            ))
        
        # Check input sanitization
        if self.config.enable_input_validation:
            input_threats = await self._check_input(request)
            threats.extend(input_threats)
        
        # Check rate limiting
        if self.config.enable_rate_limiting:
            client_ip = request.get("client_ip", "unknown")
            if self._is_rate_limited(client_ip):
                threats.append(ThreatDetection(
                    threat_type=ThreatType.RATE_LIMIT,
                    severity="high",
                    description="Rate limit exceeded",
                    request_path=request.get("path", ""),
                    request_method=request.get("method", ""),
                    blocked=True
                ))
        
        # Check CSRF
        if self.config.enable_csrf_protection:
            csrf_threat = await self._check_csrf(request)
            if csrf_threat:
                threats.append(csrf_threat)
        
        # Log threats
        for threat in threats:
            self._threat_log.append(threat)
            self._stats["threats_detected"] += 1
            if threat.blocked:
                self._stats["threats_blocked"] += 1
        
        return len([t for t in threats if t.blocked]) == 0, threats
    
    async def _check_input(self, request: dict) -> list[ThreatDetection]:
        """Check input for malicious patterns."""
        threats = []
        
        body = request.get("body")
        if isinstance(body, dict):
            _, threats_found = self._sanitizer.sanitize_dict(body.copy())
            
            for threat in threats_found:
                threats.append(ThreatDetection(
                    threat_type=ThreatType.SQL_INJECTION if "sql" in threat else ThreatType.XSS,
                    severity="high",
                    description=f"Malicious input pattern detected: {threat}",
                    request_path=request.get("path", ""),
                    request_method=request.get("method", "")
                ))
        
        return threats
    
    def _is_rate_limited(self, client_ip: str, max_requests: int = 100, window_seconds: int = 60) -> bool:
        """Check if client IP is rate limited."""
        now = datetime.now()
        
        if client_ip not in self._rate_limiters:
            self._rate_limiters[client_ip] = []
        
        # Clean old requests
        self._rate_limiters[client_ip] = [
            t for t in self._rate_limiters[client_ip]
            if (now - t).total_seconds() < window_seconds
        ]
        
        # Check limit
        if len(self._rate_limiters[client_ip]) >= max_requests:
            return True
        
        self._rate_limiters[client_ip].append(now)
        return False
    
    async def _check_csrf(self, request: dict) -> Optional[ThreatDetection]:
        """Check for CSRF token validity."""
        method = request.get("method", "")
        
        # Only check state-changing methods
        if method not in ["POST", "PUT", "DELETE", "PATCH"]:
            return None
        
        token = request.get("headers", {}).get("X-CSRF-Token")
        
        if not token:
            # CSRF token required
            return ThreatDetection(
                threat_type=ThreatType.CSRF,
                severity="high",
                description="Missing CSRF token",
                request_path=request.get("path", ""),
                request_method=method,
                blocked=True
            )
        
        # Validate token
        if token not in self._csrf_tokens:
            return ThreatDetection(
                threat_type=ThreatType.CSRF,
                severity="critical",
                description="Invalid CSRF token",
                request_path=request.get("path", ""),
                request_method=method,
                blocked=True
            )
        
        token_value, token_time = self._csrf_tokens[token]
        age = (datetime.now() - token_time).total_seconds()
        
        # Token expires after 1 hour
        if age > 3600:
            return ThreatDetection(
                threat_type=ThreatType.CSRF,
                severity="medium",
                description="CSRF token expired",
                request_path=request.get("path", ""),
                request_method=method
            )
        
        return None
    
    def generate_csrf_token(self, session_id: str) -> str:
        """Generate a CSRF token for a session."""
        import secrets
        token = secrets.token_urlsafe(32)
        self._csrf_tokens[token] = (session_id, datetime.now())
        return token
    
    def sanitize_input(self, data: Any) -> SanitizationResult:
        """Sanitize input data."""
        if isinstance(data, str):
            return self._sanitizer.sanitize_string(data)
        elif isinstance(data, dict):
            sanitized, threats = self._sanitizer.sanitize_dict(data)
            return SanitizationResult(
                original=data,
                sanitized=sanitized,
                threats_removed=threats
            )
        elif isinstance(data, list):
            sanitized, threats = self._sanitizer.sanitize_list(data)
            return SanitizationResult(
                original=data,
                sanitized=sanitized,
                threats_removed=threats
            )
        
        return SanitizationResult(
            original=data,
            sanitized=data,
            threats_removed=[]
        )
    
    def get_security_headers(self) -> dict[str, str]:
        """Get security headers for responses."""
        headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Content-Security-Policy": "default-src 'self'",
            "Referrer-Policy": "strict-origin-when-cross-origin",
        }
        
        if self.config.enable_cors:
            headers["Access-Control-Allow-Origin"] = ", ".join(self.config.allowed_origins)
            headers["Access-Control-Allow-Methods"] = ", ".join(self.config.allowed_methods)
            headers["Access-Control-Allow-Headers"] = ", ".join(self.config.allowed_headers)
        
        return headers
    
    def get_threat_log(
        self,
        severity: Optional[str] = None,
        limit: int = 100
    ) -> list[dict[str, Any]]:
        """Get threat detection log."""
        log = self._threat_log
        
        if severity:
            log = [t for t in log if t.severity == severity]
        
        log = log[-limit:]
        
        return [
            {
                "type": t.threat_type.value,
                "severity": t.severity,
                "description": t.description,
                "path": t.request_path,
                "method": t.request_method,
                "blocked": t.blocked,
                "detected_at": t.detected_at.isoformat()
            }
            for t in log
        ]
    
    def get_stats(self) -> dict[str, Any]:
        """Get security statistics."""
        return {
            **dict(self._stats),
            "threats_logged": len(self._threat_log),
            "rate_limited_ips": len(self._rate_limiters),
            "active_csrf_tokens": len(self._csrf_tokens)
        }
