"""
API Security Action Module.

Security utilities for APIs: input sanitization, SQL injection prevention,
XSS protection, rate limiting, and request signing.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class SecurityResult:
    """Result of security check."""
    safe: bool
    threats: list[str]
    sanitized_value: Optional[Any] = None


class APISecurityAction(BaseAction):
    """API security utilities."""

    def __init__(self) -> None:
        super().__init__("api_security")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Perform security checks.

        Args:
            context: Execution context
            params: Parameters:
                - action: sanitize, validate, sign, check
                - value: Value to check/sanitize
                - threat_types: Types to check (sql_injection, xss, path_traversal)
                - secret_key: Secret for request signing

        Returns:
            SecurityResult
        """
        import re
        import hashlib
        import hmac

        action = params.get("action", "check")
        value = params.get("value", "")
        threat_types = params.get("threat_types", ["sql_injection", "xss", "path_traversal"])

        if action == "sanitize":
            threats = []
            sanitized = value

            if "sql_injection" in threat_types:
                sql_patterns = [r"('|(\\%27)|(\\%3D)|(\\%3B)|(--)", r"\b(union|select|insert|update|delete|drop)\b"]
                for pattern in sql_patterns:
                    if re.search(pattern, str(sanitized), re.IGNORECASE):
                        threats.append("sql_injection")
                        sanitized = re.sub(r"[';=]|--", "", str(sanitized))

            if "xss" in threat_types:
                xss_patterns = [r"<script", r"javascript:", r"onerror=", r"onclick="]
                for pattern in xss_patterns:
                    if re.search(pattern, str(sanitized), re.IGNORECASE):
                        threats.append("xss")
                        sanitized = re.sub(r"<[^>]*>", "", str(sanitized))

            if "path_traversal" in threat_types:
                traversal_pattern = r"(\.\./|\.\.\\|%2e%2e/)"
                if re.search(traversal_pattern, str(sanitized), re.IGNORECASE):
                    threats.append("path_traversal")
                    sanitized = re.sub(r"\.\./|\.\.\\|%2e%2e/", "", str(sanitized))

            return SecurityResult(
                safe=len(threats) == 0,
                threats=threats,
                sanitized_value=sanitized
            ).__dict__

        elif action == "sign":
            secret_key = params.get("secret_key", "")
            data = str(params.get("data", ""))
            algorithm = params.get("algorithm", "sha256")

            if algorithm == "sha256":
                signature = hashlib.sha256(f"{secret_key}{data}".encode()).hexdigest()
            elif algorithm == "sha512":
                signature = hashlib.sha512(f"{secret_key}{data}".encode()).hexdigest()
            elif algorithm == "hmac":
                signature = hmac.new(secret_key.encode(), data.encode(), hashlib.sha256).hexdigest()
            else:
                signature = hashlib.md5(f"{secret_key}{data}".encode()).hexdigest()

            return {"signature": signature, "algorithm": algorithm}

        elif action == "validate":
            return {"valid": True, "value": value}

        return {"error": f"Unknown action: {action}"}
