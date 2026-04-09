"""API Security Action Module.

Provides API security utilities: rate limiting, input validation, sanitization,
authentication helpers, and threat detection.

Example:
    result = execute(context, {"action": "validate_request", "schema": schema})
"""
from typing import Any, Optional
import hashlib
import hmac
import time
from collections import defaultdict


class RateLimiter:
    """Token bucket rate limiter for API requests.
    
    Implements token bucket algorithm with configurable refill rates.
    """
    
    def __init__(
        self,
        capacity: int = 100,
        refill_rate: float = 10.0,
        window_seconds: float = 60.0,
    ) -> None:
        """Initialize rate limiter.
        
        Args:
            capacity: Maximum tokens in bucket
            refill_rate: Tokens added per second
            window_seconds: Time window for tracking
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.window_seconds = window_seconds
        
        self._buckets: dict[str, tuple[float, float]] = defaultdict(
            lambda: (float(capacity), time.time())
        )
    
    def _refill(self, key: str) -> tuple[float, float]:
        """Refill tokens for given key."""
        tokens, last_refill = self._buckets[key]
        now = time.time()
        elapsed = now - last_refill
        
        new_tokens = min(self.capacity, tokens + elapsed * self.refill_rate)
        self._buckets[key] = (new_tokens, now)
        return new_tokens, now
    
    def acquire(self, key: str, tokens: int = 1) -> bool:
        """Attempt to acquire tokens.
        
        Args:
            key: Client identifier
            tokens: Number of tokens to acquire
            
        Returns:
            True if tokens acquired, False otherwise
        """
        current_tokens, _ = self._refill(key)
        
        if current_tokens >= tokens:
            new_tokens = current_tokens - tokens
            self._buckets[key] = (new_tokens, time.time())
            return True
        
        return False
    
    def get_remaining(self, key: str) -> int:
        """Get remaining tokens for key."""
        tokens, _ = self._refill(key)
        return int(tokens)
    
    def get_state(self) -> dict[str, Any]:
        """Get rate limiter state."""
        return {
            "capacity": self.capacity,
            "refill_rate": self.refill_rate,
            "tracked_keys": len(self._buckets),
        }


class InputSanitizer:
    """Sanitizes user input for safe processing.
    
    Prevents injection attacks and validates input format.
    """
    
    DANGEROUS_PATTERNS = [
        "<script",
        "javascript:",
        "onerror=",
        "onclick=",
        "onload=",
        "onmouseover=",
        "<iframe",
        "union select",
        "--",
        "drop table",
        "exec(",
    ]
    
    @classmethod
    def sanitize_string(cls, value: str, max_length: int = 1000) -> str:
        """Sanitize a string value.
        
        Args:
            value: Input string
            max_length: Maximum allowed length
            
        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            return ""
        
        result = value[:max_length]
        
        for pattern in cls.DANGEROUS_PATTERNS:
            result = result.replace(pattern, "")
        
        return result.strip()
    
    @classmethod
    def sanitize_dict(cls, data: dict[str, Any], max_length: int = 1000) -> dict[str, Any]:
        """Sanitize dictionary values.
        
        Args:
            data: Input dictionary
            max_length: Maximum string length
            
        Returns:
            Sanitized dictionary
        """
        result = {}
        for key, value in data.items():
            if isinstance(value, str):
                result[key] = cls.sanitize_string(value, max_length)
            elif isinstance(value, dict):
                result[key] = cls.sanitize_dict(value, max_length)
            elif isinstance(value, list):
                result[key] = [
                    cls.sanitize_string(v, max_length) if isinstance(v, str) else v
                    for v in value
                ]
            else:
                result[key] = value
        return result
    
    @classmethod
    def validate_schema(cls, data: dict[str, Any], schema: dict[str, type]) -> tuple[bool, list[str]]:
        """Validate data against schema.
        
        Args:
            data: Input data
            schema: Expected schema {field: type}
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        for field, expected_type in schema.items():
            if field not in data:
                errors.append(f"Missing required field: {field}")
                continue
            
            if not isinstance(data[field], expected_type):
                errors.append(
                    f"Field '{field}' expected {expected_type.__name__}, "
                    f"got {type(data[field]).__name__}"
                )
        
        return len(errors) == 0, errors


class ThreatDetector:
    """Detects potential security threats in API requests.
    
    Analyzes request patterns for suspicious activity.
    """
    
    def __init__(self, threshold: int = 10) -> None:
        """Initialize threat detector.
        
        Args:
            threshold: Failed attempts before flagging as threat
        """
        self.threshold = threshold
        self._failed_attempts: dict[str, list[float]] = defaultdict(list)
    
    def record_failed_attempt(self, identifier: str) -> None:
        """Record a failed authentication attempt.
        
        Args:
            identifier: Client identifier (IP, user ID, etc.)
        """
        now = time.time()
        self._failed_attempts[identifier].append(now)
        
        cutoff = now - 300
        self._failed_attempts[identifier] = [
            t for t in self._failed_attempts[identifier] if t > cutoff
        ]
    
    def is_threat(self, identifier: str) -> bool:
        """Check if identifier is flagged as threat.
        
        Args:
            identifier: Client identifier
            
        Returns:
            True if threat level exceeded
        """
        return len(self._failed_attempts[identifier]) >= self.threshold
    
    def get_threat_level(self, identifier: str) -> str:
        """Get threat level for identifier.
        
        Args:
            identifier: Client identifier
            
        Returns:
            Threat level: "none", "low", "medium", "high", "critical"
        """
        count = len(self._failed_attempts[identifier])
        
        if count == 0:
            return "none"
        elif count < 3:
            return "low"
        elif count < self.threshold:
            return "medium"
        elif count < self.threshold * 2:
            return "high"
        else:
            return "critical"
    
    def reset(self, identifier: str) -> None:
        """Reset threat tracking for identifier.
        
        Args:
            identifier: Client identifier
        """
        if identifier in self._failed_attempts:
            del self._failed_attempts[identifier]


class HMACHelper:
    """HMAC-based request signing for API authentication."""
    
    @staticmethod
    def generate_signature(
        secret: str,
        message: str,
        algorithm: str = "sha256",
    ) -> str:
        """Generate HMAC signature for message.
        
        Args:
            secret: Secret key
            message: Message to sign
            algorithm: Hash algorithm (sha256, sha512, etc.)
            
        Returns:
            Hex-encoded signature
        """
        return hmac.new(
            secret.encode(),
            message.encode(),
            hashlib.new(algorithm),
        ).hexdigest()
    
    @staticmethod
    def verify_signature(
        secret: str,
        message: str,
        signature: str,
        algorithm: str = "sha256",
    ) -> bool:
        """Verify HMAC signature.
        
        Args:
            secret: Secret key
            message: Original message
            signature: Signature to verify
            algorithm: Hash algorithm
            
        Returns:
            True if signature is valid
        """
        expected = HMACHelper.generate_signature(secret, message, algorithm)
        return hmac.compare_digest(expected, signature)


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute API security action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "rate_limit_check":
        limiter = RateLimiter(
            capacity=params.get("capacity", 100),
            refill_rate=params.get("refill_rate", 10.0),
        )
        key = params.get("key", "default")
        allowed = limiter.acquire(key, params.get("tokens", 1))
        result["data"] = {
            "allowed": allowed,
            "remaining": limiter.get_remaining(key),
        }
    
    elif action == "rate_limit_status":
        limiter = RateLimiter()
        result["data"] = limiter.get_state()
    
    elif action == "sanitize":
        data = params.get("data", {})
        max_length = params.get("max_length", 1000)
        if isinstance(data, dict):
            result["data"] = InputSanitizer.sanitize_dict(data, max_length)
        elif isinstance(data, str):
            result["data"] = InputSanitizer.sanitize_string(data, max_length)
        else:
            result["data"] = data
    
    elif action == "validate":
        data = params.get("data", {})
        schema = params.get("schema", {})
        is_valid, errors = InputSanitizer.validate_schema(data, schema)
        result["data"] = {"valid": is_valid, "errors": errors}
    
    elif action == "threat_check":
        detector = ThreatDetector(threshold=params.get("threshold", 10))
        identifier = params.get("identifier", "")
        level = detector.get_threat_level(identifier)
        result["data"] = {
            "is_threat": detector.is_threat(identifier),
            "level": level,
        }
    
    elif action == "threat_record":
        detector = ThreatDetector(threshold=params.get("threshold", 10))
        detector.record_failed_attempt(params.get("identifier", ""))
        result["data"] = {"recorded": True}
    
    elif action == "sign":
        signature = HMACHelper.generate_signature(
            params.get("secret", ""),
            params.get("message", ""),
            params.get("algorithm", "sha256"),
        )
        result["data"] = {"signature": signature}
    
    elif action == "verify":
        valid = HMACHelper.verify_signature(
            params.get("secret", ""),
            params.get("message", ""),
            params.get("signature", ""),
            params.get("algorithm", "sha256"),
        )
        result["data"] = {"valid": valid}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
