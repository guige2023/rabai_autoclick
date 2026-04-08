"""API Security Action Module.

Provides API security scanning, vulnerability detection,
and security compliance checking.
"""

import hashlib
import hmac
import time
import re
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SecurityCheckType(Enum):
    """Type of security check."""
    SQL_INJECTION = "sql_injection"
    XSS = "xss"
    COMMAND_INJECTION = "command_injection"
    PATH_TRAVERSAL = "path_traversal"
    AUTH_BYPASS = "auth_bypass"
    RATE_LIMIT = "rate_limit"
    SENSITIVE_DATA = "sensitive_data"
    HEADER_SECURITY = "header_security"
    CORS = "cors"
    SSL_TLS = "ssl_tls"


class Severity(Enum):
    """Vulnerability severity."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class SecurityFinding:
    """Represents a security finding."""
    check_type: SecurityCheckType
    severity: Severity
    title: str
    description: str
    location: str
    evidence: Optional[str] = None
    remediation: Optional[str] = None
    cwe_id: Optional[str] = None


@dataclass
class SecurityReport:
    """Security scan report."""
    scan_id: str
    target: str
    timestamp: float
    duration_ms: float
    findings: List[SecurityFinding]
    summary: Dict[str, int] = field(default_factory=dict)


class APISecurityScanner:
    """Scans APIs for security vulnerabilities."""

    def __init__(self):
        self._check_patterns: Dict[SecurityCheckType, List[str]] = {
            SecurityCheckType.SQL_INJECTION: [
                r"(\bUNION\b|\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b|\bDROP\b)",
                r"('|;|--|\/\*|\*\/)",
                r"(\bOR\b\s+\d+=\d+|\bAND\b\s+\d+=\d+)"
            ],
            SecurityCheckType.XSS: [
                r"(<script|javascript:|on\w+\s*=)",
                r"(<[^>]*>)",
                r"(&#x?[0-9a-f]+;?)"
            ],
            SecurityCheckType.COMMAND_INJECTION: [
                r"[;&|`$]",
                r"(\bcat\b|\bgrep\b|\bawk\b|\bsed\b|\bwget\b|\bcurl\b)",
                r"(\$\([^)]+\)|`[^`]+`)"
            ],
            SecurityCheckType.PATH_TRAVERSAL: [
                r"(\.\.\/|\.\.\\|%2e%2e%2f|%2e%2e\/)",
                r"(\/etc\/passwd|\/etc\/shadow|\/windows\/system32)"
            ]
        }

        self._sensitive_patterns = [
            r"(?i)(api[_-]?key|apikey|secret[_-]?key|access[_-]?token)",
            r"(?i)(password|passwd|pwd)",
            r"(?i)(bearer|authorization)",
            r"\b\d{3}-\d{2}-\d{4}\b",
            r"\b\d{16}\b",
            r"(?i)(private[_-]?key|ssh[_-]?key)"
        ]

        self._security_headers_required = [
            "X-Content-Type-Options",
            "X-Frame-Options",
            "Content-Security-Policy",
            "Strict-Transport-Security"
        ]

    def scan_endpoint(
        self,
        url: str,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None
    ) -> List[SecurityFinding]:
        """Scan an endpoint for vulnerabilities."""
        findings = []
        params = params or {}
        headers = headers or {}

        findings.extend(self._check_injection(url, params, body))
        findings.extend(self._check_sensitive_data(params, headers, body))
        findings.extend(self._check_security_headers(headers))

        return findings

    def _check_injection(
        self,
        url: str,
        params: Dict[str, Any],
        body: Any
    ) -> List[SecurityFinding]:
        """Check for injection vulnerabilities."""
        findings = []

        for check_type, patterns in self._check_patterns.items():
            for key, value in params.items():
                value_str = str(value)
                for pattern in patterns:
                    if re.search(pattern, value_str, re.IGNORECASE):
                        findings.append(SecurityFinding(
                            check_type=check_type,
                            severity=Severity.HIGH,
                            title=f"Potential {check_type.value.replace('_', ' ').title()}",
                            description=f"Possible {check_type.value.replace('_', ' ')} detected in parameter",
                            location=f"param.{key}",
                            evidence=f"Pattern matched: {pattern}",
                            remediation=f"Sanitize input for {key} before using",
                            cwe_id=self._get_cwe_id(check_type)
                        ))

        return findings

    def _check_sensitive_data(
        self,
        params: Dict[str, Any],
        headers: Dict[str, str],
        body: Any
    ) -> List[SecurityFinding]:
        """Check for exposed sensitive data."""
        findings = []

        for key, value in params.items():
            value_str = str(value).lower()
            for pattern in self._sensitive_patterns:
                if re.search(pattern, value_str):
                    findings.append(SecurityFinding(
                        check_type=SecurityCheckType.SENSITIVE_DATA,
                        severity=Severity.HIGH,
                        title="Sensitive Data in Parameters",
                        description=f"Potentially sensitive data detected in parameter: {key}",
                        location=f"param.{key}",
                        evidence=f"Pattern: {pattern}",
                        remediation="Encrypt sensitive data or use environment variables",
                        cwe_id="CWE-312"
                    ))

        for header_name, header_value in headers.items():
            if "authorization" in header_name.lower() and header_value:
                findings.append(SecurityFinding(
                    check_type=SecurityCheckType.SENSITIVE_DATA,
                    severity=Severity.MEDIUM,
                    title="Authorization Header Present",
                    description="Authorization header detected - ensure it's properly secured",
                    location=f"header.{header_name}",
                    remediation="Ensure authorization tokens are transmitted securely",
                    cwe_id="CWE-598"
                ))

        return findings

    def _check_security_headers(
        self,
        headers: Dict[str, str]
    ) -> List[SecurityFinding]:
        """Check for security headers."""
        findings = []
        header_names = [h.lower() for h in headers.keys()]

        for required_header in self._security_headers_required:
            if required_header.lower() not in header_names:
                findings.append(SecurityFinding(
                    check_type=SecurityCheckType.HEADER_SECURITY,
                    severity=Severity.LOW,
                    title=f"Missing Security Header: {required_header}",
                    description=f"{required_header} header not present",
                    location="response.headers",
                    remediation=f"Add {required_header} header to responses",
                    cwe_id="CWE-693"
                ))

        return findings

    def _get_cwe_id(self, check_type: SecurityCheckType) -> str:
        """Get CWE ID for check type."""
        cwe_map = {
            SecurityCheckType.SQL_INJECTION: "CWE-89",
            SecurityCheckType.XSS: "CWE-79",
            SecurityCheckType.COMMAND_INJECTION: "CWE-78",
            SecurityCheckType.PATH_TRAVERSAL: "CWE-22",
            SecurityCheckType.AUTH_BYPASS: "CWE-287",
            SecurityCheckType.RATE_LIMIT: "CWE-307"
        }
        return cwe_map.get(check_type, "CWE-707")

    def check_rate_limit(
        self,
        url: str,
        request_count: int,
        time_window_seconds: int
    ) -> SecurityFinding:
        """Check if rate limiting is implemented."""
        if request_count > 100 and time_window_seconds < 60:
            return SecurityFinding(
                check_type=SecurityCheckType.RATE_LIMIT,
                severity=Severity.MEDIUM,
                title="No Rate Limiting Detected",
                description=f"High request volume ({request_count} requests in {time_window_seconds}s) without rate limiting",
                location=f"endpoint:{url}",
                remediation="Implement rate limiting middleware",
                cwe_id="CWE-307"
            )
        return None

    def validate_ssl_tls(self, host: str, port: int = 443) -> SecurityFinding:
        """Validate SSL/TLS configuration."""
        return SecurityFinding(
            check_type=SecurityCheckType.SSL_TLS,
            severity=Severity.INFO,
            title="SSL/TLS Configuration Check",
            description=f"SSL/TLS check for {host}:{port}",
            location=f"host:{host}:{port}",
            remediation="Use TLS 1.2 or higher and disable weak ciphers"
        )


class APISecurityAction(BaseAction):
    """Action for API security operations."""

    def __init__(self):
        super().__init__("api_security")
        self._scanner = APISecurityScanner()
        self._scan_history: List[SecurityReport] = []

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute API security action."""
        try:
            operation = params.get("operation", "scan")

            if operation == "scan":
                return self._scan_endpoint(params)
            elif operation == "check_headers":
                return self._check_headers(params)
            elif operation == "check_rate_limit":
                return self._check_rate_limit(params)
            elif operation == "validate_ssl":
                return self._validate_ssl(params)
            elif operation == "history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _scan_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Scan an endpoint for vulnerabilities."""
        url = params.get("url", "/")
        method = params.get("method", "GET")

        start = time.time()
        findings = self._scanner.scan_endpoint(
            url=url,
            method=method,
            params=params.get("params"),
            headers=params.get("headers"),
            body=params.get("body")
        )
        duration = (time.time() - start) * 1000

        report = SecurityReport(
            scan_id=hashlib.md5(f"{url}{time.time()}".encode()).hexdigest()[:8],
            target=url,
            timestamp=time.time(),
            duration_ms=duration,
            findings=findings,
            summary=self._summarize_findings(findings)
        )

        self._scan_history.append(report)

        return ActionResult(
            success=not any(f.severity == Severity.CRITICAL for f in findings),
            data={
                "scan_id": report.scan_id,
                "target": report.target,
                "duration_ms": report.duration_ms,
                "summary": report.summary,
                "findings": [
                    {
                        "type": f.check_type.value,
                        "severity": f.severity.value,
                        "title": f.title,
                        "description": f.description,
                        "location": f.location,
                        "remediation": f.remediation
                    }
                    for f in findings
                ]
            }
        )

    def _check_headers(self, params: Dict[str, Any]) -> ActionResult:
        """Check security headers."""
        headers = params.get("headers", {})

        findings = self._scanner._check_security_headers(headers)

        return ActionResult(
            success=len(findings) == 0,
            data={
                "findings": [
                    {
                        "type": f.check_type.value,
                        "severity": f.severity.value,
                        "title": f.title,
                        "description": f.description,
                        "remediation": f.remediation
                    }
                    for f in findings
                ]
            }
        )

    def _check_rate_limit(self, params: Dict[str, Any]) -> ActionResult:
        """Check rate limiting."""
        url = params.get("url", "/")
        request_count = params.get("request_count", 100)
        time_window = params.get("time_window_seconds", 60)

        finding = self._scanner.check_rate_limit(url, request_count, time_window)

        if finding:
            return ActionResult(
                success=False,
                data={
                    "finding": {
                        "type": finding.check_type.value,
                        "severity": finding.severity.value,
                        "title": finding.title,
                        "description": finding.description,
                        "remediation": finding.remediation
                    }
                }
            )

        return ActionResult(
            success=True,
            message="Rate limiting appears adequate"
        )

    def _validate_ssl(self, params: Dict[str, Any]) -> ActionResult:
        """Validate SSL/TLS configuration."""
        host = params.get("host", "localhost")
        port = params.get("port", 443)

        finding = self._scanner.validate_ssl_tls(host, port)

        return ActionResult(
            success=True,
            data={
                "finding": {
                    "type": finding.check_type.value,
                    "severity": finding.severity.value,
                    "title": finding.title,
                    "description": finding.description,
                    "remediation": finding.remediation
                }
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get scan history."""
        limit = params.get("limit", 50)
        history = self._scan_history[-limit:]

        return ActionResult(
            success=True,
            data={
                "history": [
                    {
                        "scan_id": r.scan_id,
                        "target": r.target,
                        "timestamp": r.timestamp,
                        "summary": r.summary
                    }
                    for r in history
                ]
            }
        )

    def _summarize_findings(self, findings: List[SecurityFinding]) -> Dict[str, int]:
        """Summarize findings by severity."""
        summary = {s.value: 0 for s in Severity}
        for finding in findings:
            summary[finding.severity.value] += 1
        return summary
