"""API Security Validator Action Module.

Validates API security aspects with:
- Authentication verification
- Authorization checks
- Input sanitization
- Rate limiting validation
- Vulnerability scanning

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SecurityLevel(Enum):
    """Security validation levels."""
    BASIC = auto()
    STANDARD = auto()
    STRICT = auto()


class ThreatType(Enum):
    """Types of security threats."""
    SQL_INJECTION = auto()
    XSS = auto()
    CSRF = auto()
    RATE_LIMIT = auto()
    AUTH_BYPASS = auto()
    DATA_EXPOSURE = auto()
    INVALID_INPUT = auto()


@dataclass
class SecurityIssue:
    """A detected security issue."""
    threat_type: ThreatType
    severity: str
    message: str
    location: str
    evidence: Optional[str] = None
    remediation: Optional[str] = None


@dataclass
class SecurityValidationResult:
    """Result of security validation."""
    passed: bool
    issues: List[SecurityIssue] = field(default_factory=list)
    score: float = 100.0
    validated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


class InputSanitizer:
    """Sanitizes user input to prevent injection attacks."""
    
    def __init__(self):
        self._sql_patterns = [
            r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|UNION)\b)",
            r"(--|\#|\/\*|\*\/)",
            r"('|\"|;|\\)",
        ]
        self._xss_patterns = [
            r"<script[^>]*>.*?<\/script>",
            r"javascript:",
            r"on\w+\s*=",
            r"<iframe",
            r"<object",
            r"<embed",
        ]
        self._compiled_sql = [re.compile(p, re.IGNORECASE) for p in self._sql_patterns]
        self._compiled_xss = [re.compile(p, re.IGNORECASE) for p in self._xss_patterns]
    
    def sanitize_string(self, value: str) -> str:
        """Sanitize a string value.
        
        Args:
            value: String to sanitize
            
        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            return value
        
        sanitized = value
        
        sanitized = sanitized.replace("<", "&lt;").replace(">", "&gt;")
        sanitized = sanitized.replace("'", "&#x27;").replace('"', "&quot;")
        
        return sanitized
    
    def detect_sql_injection(self, value: str) -> Optional[List[str]]:
        """Detect potential SQL injection patterns.
        
        Args:
            value: Value to check
            
        Returns:
            List of detected patterns or None
        """
        if not isinstance(value, str):
            return None
        
        matches = []
        for pattern in self._compiled_sql:
            found = pattern.findall(value)
            if found:
                matches.extend(found)
        
        return matches if matches else None
    
    def detect_xss(self, value: str) -> Optional[List[str]]:
        """Detect potential XSS patterns.
        
        Args:
            value: Value to check
            
        Returns:
            List of detected patterns or None
        """
        if not isinstance(value, str):
            return None
        
        matches = []
        for pattern in self._compiled_xss:
            found = pattern.findall(value)
            if found:
                matches.extend(found)
        
        return matches if matches else None
    
    def validate_input(self, value: Any, field_name: str = "") -> List[SecurityIssue]:
        """Validate input for security issues.
        
        Args:
            value: Value to validate
            field_name: Field name for context
            
        Returns:
            List of detected issues
        """
        issues = []
        
        if isinstance(value, str):
            if self.detect_sql_injection(value):
                issues.append(SecurityIssue(
                    threat_type=ThreatType.SQL_INJECTION,
                    severity="high",
                    message=f"Potential SQL injection detected in '{field_name}'",
                    location=field_name,
                    evidence=value[:100],
                    remediation="Use parameterized queries"
                ))
            
            if self.detect_xss(value):
                issues.append(SecurityIssue(
                    threat_type=ThreatType.XSS,
                    severity="high",
                    message=f"Potential XSS detected in '{field_name}'",
                    location=field_name,
                    evidence=value[:100],
                    remediation="Sanitize HTML and encode output"
                ))
        
        return issues


class APIAuthValidator:
    """Validates API authentication and authorization."""
    
    def __init__(self):
        self._valid_tokens: Set[str] = set()
        self._token_expiry: Dict[str, float] = {}
        self._user_permissions: Dict[str, Set[str]] = defaultdict(set)
    
    def add_valid_token(self, token: str, expiry: Optional[float] = None) -> None:
        """Add a valid authentication token.
        
        Args:
            token: Token to add
            expiry: Optional expiry timestamp
        """
        self._valid_tokens.add(token)
        if expiry:
            self._token_expiry[token] = expiry
    
    def validate_token(self, token: str) -> bool:
        """Validate an authentication token.
        
        Args:
            token: Token to validate
            
        Returns:
            True if valid
        """
        if token not in self._valid_tokens:
            return False
        
        if token in self._token_expiry:
            if datetime.now().timestamp() > self._token_expiry[token]:
                self._valid_tokens.discard(token)
                return False
        
        return True
    
    def set_permissions(self, user_id: str, permissions: Set[str]) -> None:
        """Set user permissions.
        
        Args:
            user_id: User identifier
            permissions: Set of permission strings
        """
        self._user_permissions[user_id] = permissions
    
    def check_permission(self, user_id: str, permission: str) -> bool:
        """Check if user has a permission.
        
        Args:
            user_id: User identifier
            permission: Permission to check
            
        Returns:
            True if user has permission
        """
        return permission in self._user_permissions.get(user_id, set())
    
    def hash_password(self, password: str) -> str:
        """Hash a password.
        
        Args:
            password: Password to hash
            
        Returns:
            Hashed password
        """
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against hash.
        
        Args:
            password: Password to verify
            hashed: Hash to compare
            
        Returns:
            True if matches
        """
        return self.hash_password(password) == hashed


class RateLimitValidator:
    """Validates rate limiting compliance."""
    
    def __init__(self, window_seconds: float = 60.0, max_requests: int = 100):
        self.window_seconds = window_seconds
        self.max_requests = max_requests
        self._request_history: Dict[str, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()
    
    async def check_rate_limit(
        self,
        client_id: str,
        increment: bool = True
    ) -> Tuple[bool, int]:
        """Check if request is within rate limit.
        
        Args:
            client_id: Client identifier
            increment: Whether to count this request
            
        Returns:
            Tuple of (is_allowed, current_count)
        """
        async with self._lock:
            now = datetime.now().timestamp()
            window_start = now - self.window_seconds
            
            history = self._request_history[client_id]
            history[:] = [t for t in history if t > window_start]
            
            count = len(history)
            
            if increment:
                if count >= self.max_requests:
                    return False, count
                history.append(now)
                count += 1
            
            return True, count
    
    def get_remaining(self, client_id: str) -> int:
        """Get remaining requests for client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Number of remaining requests
        """
        now = datetime.now().timestamp()
        window_start = now - self.window_seconds
        
        history = self._request_history.get(client_id, [])
        recent = [t for t in history if t > window_start]
        
        return max(0, self.max_requests - len(recent))


class APISecurityValidator:
    """Validates API security across multiple dimensions.
    
    Features:
    - Input sanitization
    - Authentication verification
    - Authorization checks
    - Rate limiting validation
    - Threat detection
    - Security scoring
    """
    
    def __init__(self, security_level: SecurityLevel = SecurityLevel.STANDARD):
        self.security_level = security_level
        self.input_sanitizer = InputSanitizer()
        self.auth_validator = APIAuthValidator()
        self._rate_limiters: Dict[str, RateLimitValidator] = {}
        self._lock = asyncio.Lock()
    
    async def validate_request(
        self,
        request: Dict[str, Any],
        auth_token: Optional[str] = None,
        client_id: Optional[str] = None
    ) -> SecurityValidationResult:
        """Validate an API request for security issues.
        
        Args:
            request: Request data
            auth_token: Optional authentication token
            client_id: Optional client identifier for rate limiting
            
        Returns:
            Security validation result
        """
        issues: List[SecurityIssue] = []
        
        input_issues = await self._validate_inputs(request)
        issues.extend(input_issues)
        
        if auth_token:
            if not self.auth_validator.validate_token(auth_token):
                issues.append(SecurityIssue(
                    threat_type=ThreatType.AUTH_BYPASS,
                    severity="critical",
                    message="Invalid or expired authentication token",
                    location="auth"
                ))
        
        if client_id:
            rate_ok, count = await self._check_rate_limit(client_id)
            if not rate_ok:
                issues.append(SecurityIssue(
                    threat_type=ThreatType.RATE_LIMIT,
                    severity="medium",
                    message=f"Rate limit exceeded: {count} requests",
                    location="rate_limit"
                ))
        
        score = self._calculate_security_score(issues)
        
        return SecurityValidationResult(
            passed=len([i for i in issues if i.severity == "critical"]) == 0,
            issues=issues,
            score=score
        )
    
    async def _validate_inputs(self, request: Dict[str, Any]) -> List[SecurityIssue]:
        """Validate request inputs for security issues.
        
        Args:
            request: Request data
            
        Returns:
            List of security issues
        """
        issues = []
        
        def check_recursive(data: Any, path: str = "") -> None:
            if isinstance(data, dict):
                for key, value in data.items():
                    field_path = f"{path}.{key}" if path else key
                    
                    if isinstance(value, str):
                        field_issues = self.input_sanitizer.validate_input(value, field_path)
                        issues.extend(field_issues)
                    elif isinstance(value, (dict, list)):
                        check_recursive(value, field_path)
            
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    check_recursive(item, f"{path}[{i}]")
        
        check_recursive(request)
        
        return issues
    
    async def _check_rate_limit(self, client_id: str) -> Tuple[bool, int]:
        """Check rate limit for client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Tuple of (is_allowed, current_count)
        """
        if client_id not in self._rate_limiters:
            self._rate_limiters[client_id] = RateLimitValidator()
        
        return await self._rate_limiters[client_id].check_rate_limit(client_id)
    
    def _calculate_security_score(self, issues: List[SecurityIssue]) -> float:
        """Calculate security score based on issues.
        
        Args:
            issues: List of detected issues
            
        Returns:
            Score from 0-100
        """
        if not issues:
            return 100.0
        
        deductions = {
            "critical": 25.0,
            "high": 15.0,
            "medium": 5.0,
            "low": 1.0
        }
        
        total_deduction = sum(deductions.get(i.severity, 5.0) for i in issues)
        
        return max(0.0, 100.0 - total_deduction)
    
    async def validate_response(
        self,
        response: Dict[str, Any]
    ) -> SecurityValidationResult:
        """Validate API response for data exposure issues.
        
        Args:
            response: Response data
            
        Returns:
            Security validation result
        """
        issues = []
        
        sensitive_patterns = [
            ("password", "Password exposure"),
            ("secret", "Secret exposure"),
            ("token", "Token exposure"),
            ("key", "API key exposure"),
            ("credential", "Credential exposure"),
        ]
        
        def check_sensitive(data: Any, path: str = "") -> None:
            if isinstance(data, dict):
                for key, value in data.items():
                    field_path = f"{path}.{key}" if path else key
                    key_lower = key.lower()
                    
                    for pattern, issue_type in sensitive_patterns:
                        if pattern in key_lower and isinstance(value, str) and len(value) > 0:
                            issues.append(SecurityIssue(
                                threat_type=ThreatType.DATA_EXPOSURE,
                                severity="high",
                                message=f"{issue_type} in response field '{field_path}'",
                                location=field_path,
                                remediation="Remove sensitive data from response"
                            ))
                    
                    if isinstance(value, (dict, list)):
                        check_sensitive(value, field_path)
            
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    check_sensitive(item, f"{path}[{i}]")
        
        check_sensitive(response)
        
        score = self._calculate_security_score(issues)
        
        return SecurityValidationResult(
            passed=len(issues) == 0,
            issues=issues,
            score=score
        )
