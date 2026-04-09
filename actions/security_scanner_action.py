"""Security scanner action for scanning and analyzing security issues.

Provides vulnerability scanning, security audit,
and threat detection capabilities.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SeverityLevel(Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IssueType(Enum):
    SQL_INJECTION = "sql_injection"
    XSS = "xss"
    CSRF = "csrf"
    WEAK_CRYPTO = "weak_crypto"
    HARDCODED_SECRET = "hardcoded_secret"
    INSECURE_STORAGE = "insecure_storage"
    MISSING_AUTH = "missing_auth"
    PERMISSION_ISSUE = "permission_issue"
    DEPENDENCY_VULN = "dependency_vuln"


@dataclass
class SecurityIssue:
    issue_id: str
    issue_type: IssueType
    severity: SeverityLevel
    title: str
    description: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    code_snippet: Optional[str] = None
    remediation: str = ""
    cwe_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class ScanResult:
    scan_id: str
    target: str
    started_at: float
    completed_at: Optional[float] = None
    issues: list[SecurityIssue] = field(default_factory=list)
    files_scanned: int = 0
    status: str = "pending"


class SecurityScannerAction:
    """Scan for security issues and vulnerabilities.

    Args:
        enable_dependencies: Enable dependency vulnerability scanning.
        severity_threshold: Minimum severity to report.
    """

    def __init__(
        self,
        enable_dependencies: bool = True,
        severity_threshold: SeverityLevel = SeverityLevel.INFO,
    ) -> None:
        self._enable_dependencies = enable_dependencies
        self._severity_threshold = severity_threshold
        self._scan_history: list[ScanResult] = []
        self._issue_handlers: list[Callable[[SecurityIssue], None]] = []

    def scan_code(
        self,
        code: str,
        language: str = "python",
        file_path: Optional[str] = None,
    ) -> list[SecurityIssue]:
        """Scan code for security issues.

        Args:
            code: Code to scan.
            language: Programming language.
            file_path: Optional file path.

        Returns:
            List of detected issues.
        """
        issues = []

        issues.extend(self._check_sql_injection(code, file_path))
        issues.extend(self._check_xss(code, file_path))
        issues.extend(self._check_hardcoded_secrets(code, file_path))
        issues.extend(self._check_weak_crypto(code, file_path))
        issues.extend(self._check_weak_permissions(code, file_path))

        for issue in issues:
            if self._meets_threshold(issue.severity):
                for handler in self._issue_handlers:
                    try:
                        handler(issue)
                    except Exception as e:
                        logger.error(f"Issue handler error: {e}")

        return issues

    def _check_sql_injection(self, code: str, file_path: Optional[str]) -> list[SecurityIssue]:
        """Check for SQL injection vulnerabilities.

        Args:
            code: Code to check.
            file_path: Optional file path.

        Returns:
            List of issues.
        """
        issues = []
        patterns = [
            (r'execute\s*\(\s*f["\'].*\%', "F-string in SQL execute"),
            (r'format\s*\(\s*f["\'].*\%', "F-string in SQL format"),
            (r'\.format\s*\(.*\%', ".format with % in SQL"),
            (r'"\s*%\s*\(', "Old-style % formatting in SQL"),
        ]

        for pattern, desc in patterns:
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                line_num = code[:match.start()].count('\n') + 1
                issues.append(SecurityIssue(
                    issue_id=f"sqli_{line_num}",
                    issue_type=IssueType.SQL_INJECTION,
                    severity=SeverityLevel.HIGH,
                    title="Potential SQL Injection",
                    description=desc,
                    file_path=file_path,
                    line_number=line_num,
                    remediation="Use parameterized queries instead of string formatting",
                    cwe_id="CWE-89",
                ))

        return issues

    def _check_xss(self, code: str, file_path: Optional[str]) -> list[SecurityIssue]:
        """Check for XSS vulnerabilities.

        Args:
            code: Code to check.
            file_path: Optional file path.

        Returns:
            List of issues.
        """
        issues = []
        patterns = [
            (r'innerHTML\s*=\s*', "Direct innerHTML assignment"),
            (r'dangerouslySetInnerHTML', "Dangerous React innerHTML"),
            (r'render\s*\(\s*.*\+', "String concatenation in render"),
        ]

        for pattern, desc in patterns:
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                line_num = code[:match.start()].count('\n') + 1
                issues.append(SecurityIssue(
                    issue_id=f"xss_{line_num}",
                    issue_type=IssueType.XSS,
                    severity=SeverityLevel.MEDIUM,
                    title="Potential XSS Vulnerability",
                    description=desc,
                    file_path=file_path,
                    line_number=line_num,
                    remediation="Use proper sanitization and escaping",
                    cwe_id="CWE-79",
                ))

        return issues

    def _check_hardcoded_secrets(self, code: str, file_path: Optional[str]) -> list[SecurityIssue]:
        """Check for hardcoded secrets.

        Args:
            code: Code to check.
            file_path: Optional file path.

        Returns:
            List of issues.
        """
        issues = []
        patterns = [
            (r'api[_-]?key["\s]*[:=]["\s]*[a-zA-Z0-9]{20,}', "Hardcoded API key"),
            (r'secret["\s]*[:=]["\s]*[a-zA-Z0-9]{16,}', "Hardcoded secret"),
            (r'password["\s]*[:=]["\s]*["\'][^"\']{8,}["\']', "Hardcoded password"),
            (r'token["\s]*[:=]["\s]*[a-zA-Z0-9]{20,}', "Hardcoded token"),
            (r'private[_-]?key["\s]*[:=]["\s]*["\']-----BEGIN', "Hardcoded private key"),
        ]

        for pattern, desc in patterns:
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                line_num = code[:match.start()].count('\n') + 1
                issues.append(SecurityIssue(
                    issue_id=f"secret_{line_num}",
                    issue_type=IssueType.HARDCODED_SECRET,
                    severity=SeverityLevel.CRITICAL,
                    title="Hardcoded Secret Detected",
                    description=desc,
                    file_path=file_path,
                    line_number=line_num,
                    remediation="Use environment variables or a secrets manager",
                    cwe_id="CWE-798",
                ))

        return issues

    def _check_weak_crypto(self, code: str, file_path: Optional[str]) -> list[SecurityIssue]:
        """Check for weak cryptographic usage.

        Args:
            code: Code to check.
            file_path: Optional file path.

        Returns:
            List of issues.
        """
        issues = []
        patterns = [
            (r'md5', "MD5 hash usage"),
            (r'sha1', "SHA-1 hash usage"),
            (r'DES\b', "DES encryption"),
            (r'RC4', "RC4 cipher"),
            (r'random\.random\(', "Weak random number generator"),
        ]

        for pattern, desc in patterns:
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                line_num = code[:match.start()].count('\n') + 1
                issues.append(SecurityIssue(
                    issue_id=f"crypto_{line_num}",
                    issue_type=IssueType.WEAK_CRYPTO,
                    severity=SeverityLevel.MEDIUM,
                    title="Weak Cryptographic Usage",
                    description=desc,
                    file_path=file_path,
                    line_number=line_num,
                    remediation=f"Use stronger cryptographic algorithms",
                    cwe_id="CWE-327",
                ))

        return issues

    def _check_weak_permissions(self, code: str, file_path: Optional[str]) -> list[SecurityIssue]:
        """Check for weak permission settings.

        Args:
            code: Code to check.
            file_path: Optional file path.

        Returns:
            List of issues.
        """
        issues = []
        patterns = [
            (r'chmod\s+0?777', "World-writable permissions"),
            (r'ALLOW\s*=\s*\*', "Permissive CORS configuration"),
            (r'Authorization.*Basic', "Basic auth without TLS"),
        ]

        for pattern, desc in patterns:
            matches = re.finditer(pattern, code, re.IGNORECASE)
            for match in matches:
                line_num = code[:match.start()].count('\n') + 1
                issues.append(SecurityIssue(
                    issue_id=f"perm_{line_num}",
                    issue_type=IssueType.PERMISSION_ISSUE,
                    severity=SeverityLevel.MEDIUM,
                    title="Weak Permission Configuration",
                    description=desc,
                    file_path=file_path,
                    line_number=line_num,
                    remediation="Use restrictive permissions",
                    cwe_id="CWE-276",
                ))

        return issues

    def _meets_threshold(self, severity: SeverityLevel) -> bool:
        """Check if severity meets threshold.

        Args:
            severity: Issue severity.

        Returns:
            True if severity meets threshold.
        """
        severity_order = [
            SeverityLevel.INFO,
            SeverityLevel.LOW,
            SeverityLevel.MEDIUM,
            SeverityLevel.HIGH,
            SeverityLevel.CRITICAL,
        ]
        return severity_order.index(severity) >= severity_order.index(self._severity_threshold)

    def register_issue_handler(self, handler: Callable[[SecurityIssue], None]) -> None:
        """Register a handler for security issues.

        Args:
            handler: Callback function.
        """
        self._issue_handlers.append(handler)

    def get_issues_by_severity(
        self,
        issues: list[SecurityIssue],
        severity: SeverityLevel,
    ) -> list[SecurityIssue]:
        """Filter issues by severity.

        Args:
            issues: List of issues.
            severity: Severity level.

        Returns:
            Filtered issues.
        """
        return [i for i in issues if i.severity == severity]

    def get_stats(self) -> dict[str, Any]:
        """Get security scanner statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "total_scans": len(self._scan_history),
            "severity_threshold": self._severity_threshold.value,
            "dependency_scanning": self._enable_dependencies,
        }
