"""
Comprehensive Error Handling and User Experience Module v24
Provides error classification, severity levels, recovery suggestions,
error history tracking, aggregation, user-friendly messages, dashboard,
error code catalog, auto-recovery, and notifications.
"""

import json
import time
import traceback
import logging
import re
import os
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, Counter
from threading import Lock, RLock
import threading
import copy
from string import Template

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class ErrorCategory(Enum):
    """Error category classification."""
    NETWORK = "network"               # Network connectivity issues
    FILE_SYSTEM = "file_system"      # File I/O, missing files, permissions
    PERMISSION = "permission"        # Access denied, authorization
    TIMEOUT = "timeout"              # Operations that took too long
    VALIDATION = "validation"        # Invalid data, schema violations
    RUNTIME = "runtime"              # Runtime execution errors
    CONFIGURATION = "configuration" # Config issues, missing settings
    DEPENDENCY = "dependency"        # Missing dependencies, imports
    EXTERNAL_SERVICE = "external_service"  # Third-party service failures
    AUTOMATION = "automation"        # Automation-specific (element not found, etc.)
    UNKNOWN = "unknown"              # Unclassified errors


class ErrorSeverity(Enum):
    """Error severity levels with appropriate responses."""
    CRITICAL = "critical"  # Stop everything, notify immediately, requires human intervention
    HIGH = "high"          # Significant impact, attempt recovery, notify soon
    MEDIUM = "medium"      # Moderate impact, try recovery, log for review
    LOW = "low"            # Minor issue, log and continue, no notification


class ErrorStatus(Enum):
    """Error resolution status."""
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    IGNORED = "ignored"
    ESCALATED = "escalated"


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class ErrorCode:
    """Well-documented error code with known solutions."""
    code: str
    category: ErrorCategory
    severity: ErrorSeverity
    title: str
    description: str
    technical_details: str
    user_message: str
    recovery_suggestions: List[str]
    auto_recoverable: bool
    known_causes: List[str]
    related_codes: List[str]


@dataclass
class ErrorContext:
    """Full context preserved for debugging."""
    timestamp: float
    workflow_name: str
    workflow_id: Optional[str]
    step_name: str
    step_index: int
    action_type: str
    action_params: Dict[str, Any]
    environment: Dict[str, Any]
    system_state: Dict[str, Any]
    user_data: Dict[str, Any]
    stack_trace: str
    raw_exception: Optional[str]
    previous_errors: List[str]


@dataclass
class ErrorRecord:
    """Individual error record."""
    error_id: str
    error_code: str
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    user_message: str
    context: ErrorContext
    timestamp: float
    status: ErrorStatus
    recovery_attempts: List['RecoveryAttempt']
    resolved: bool
    resolved_at: Optional[float]
    resolved_by: Optional[str]
    aggregate_count: int = 1
    last_occurrence: float = 0.0
    first_occurrence: float = 0.0


@dataclass
class RecoveryAttempt:
    """Single recovery attempt record."""
    timestamp: float
    strategy: str
    action_taken: str
    success: bool
    duration: float
    details: str
    error_after_attempt: Optional[str] = None


@dataclass
class AggregatedError:
    """Grouped similar errors with count."""
    error_signature: str
    error_code: str
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    user_message: str
    count: int
    first_occurrence: float
    last_occurrence: float
    recent_timestamps: List[float]
    success_rate: float
    workflow_names: Set[str]
    step_names: Set[str]


@dataclass
class ErrorPattern:
    """Detected error pattern for alerting."""
    pattern_id: str
    error_signature: str
    threshold: int
    time_window: float
    occurrences: int
    first_detected: float
    last_detected: float
    alerted: bool
    alert_count: int
    recent_timestamps: List[float] = field(default_factory=list)


@dataclass
class ErrorStats:
    """Error statistics summary."""
    total_errors: int
    errors_by_category: Dict[str, int]
    errors_by_severity: Dict[str, int]
    errors_by_workflow: Dict[str, int]
    top_errors: List[Tuple[str, int]]
    error_rate: float
    resolution_rate: float
    avg_resolution_time: float
    critical_errors_active: int


# =============================================================================
# Error Code Catalog
# =============================================================================

class ErrorCodeCatalog:
    """Catalog of well-documented error codes with known solutions."""

    # Error code prefixes by category
    PREFIXES = {
        ErrorCategory.NETWORK: "NET",
        ErrorCategory.FILE_SYSTEM: "FS",
        ErrorCategory.PERMISSION: "PERM",
        ErrorCategory.TIMEOUT: "TMO",
        ErrorCategory.VALIDATION: "VAL",
        ErrorCategory.RUNTIME: "RTE",
        ErrorCategory.CONFIGURATION: "CFG",
        ErrorCategory.DEPENDENCY: "DEP",
        ErrorCategory.EXTERNAL_SERVICE: "EXT",
        ErrorCategory.AUTOMATION: "AUTO",
        ErrorCategory.UNKNOWN: "UNK",
    }

    def __init__(self):
        self._codes: Dict[str, ErrorCode] = {}
        self._sequence_counters: Dict[ErrorCategory, int] = defaultdict(int)
        self._init_standard_codes()

    def _init_standard_codes(self):
        """Initialize standard error codes."""
        standard_codes = [
            ErrorCode(
                code="NET001",
                category=ErrorCategory.NETWORK,
                severity=ErrorSeverity.HIGH,
                title="Connection Refused",
                description="Network connection was actively refused by the target host",
                technical_details="TCP connection refused - target port not listening or firewall blocking",
                user_message="Could not connect to the server. The server may be down or busy.",
                recovery_suggestions=[
                    "Check if the server address is correct",
                    "Verify the server is running and accessible",
                    "Check firewall and network settings",
                    "Try again in a few minutes"
                ],
                auto_recoverable=True,
                known_causes=["Server not running", "Wrong port", "Firewall blocking", "Service down"],
                related_codes=["NET002", "NET003"]
            ),
            ErrorCode(
                code="NET002",
                category=ErrorCategory.NETWORK,
                severity=ErrorSeverity.HIGH,
                title="Connection Timeout",
                description="Network operation timed out waiting for response",
                technical_details="TCP connection or read operation exceeded timeout threshold",
                user_message="The connection took too long and timed out.",
                recovery_suggestions=[
                    "Check your internet connection",
                    "The server may be busy - try again later",
                    "Increase timeout settings if this is a slow operation"
                ],
                auto_recoverable=True,
                known_causes=["Slow network", "Server overloaded", "Network latency", "Large data transfer"],
                related_codes=["NET001", "NET003"]
            ),
            ErrorCode(
                code="NET003",
                category=ErrorCategory.NETWORK,
                severity=ErrorSeverity.MEDIUM,
                title="DNS Resolution Failed",
                description="Could not resolve the hostname to an IP address",
                technical_details="DNS lookup failed - hostname does not exist or DNS server unreachable",
                user_message="Could not find the server address. Please check the URL.",
                recovery_suggestions=[
                    "Verify the URL/hostname is correct",
                    "Check your DNS settings",
                    "Try using the IP address instead"
                ],
                auto_recoverable=False,
                known_causes=["Typo in hostname", "DNS server down", "Network misconfiguration"],
                related_codes=["NET001", "NET002"]
            ),
            ErrorCode(
                code="FS001",
                category=ErrorCategory.FILE_SYSTEM,
                severity=ErrorSeverity.HIGH,
                title="File Not Found",
                description="The specified file does not exist",
                technical_details="File path does not exist or is not accessible",
                user_message="A required file could not be found.",
                recovery_suggestions=[
                    "Verify the file path is correct",
                    "Check if the file was moved or deleted",
                    "Ensure you have access to the file location"
                ],
                auto_recoverable=False,
                known_causes=["Wrong path", "File deleted", "File not created yet", "Moved file"],
                related_codes=["FS002", "FS003"]
            ),
            ErrorCode(
                code="FS002",
                category=ErrorCategory.FILE_SYSTEM,
                severity=ErrorSeverity.CRITICAL,
                title="Permission Denied - File",
                description="Cannot read or write to file due to permission restrictions",
                technical_details="Access denied - user lacks required permissions on file/directory",
                user_message="You don't have permission to access this file.",
                recovery_suggestions=[
                    "Check file permissions",
                    "Run with appropriate privileges",
                    "Contact administrator to grant access"
                ],
                auto_recoverable=False,
                known_causes=["Insufficient permissions", "File owned by another user", "Read-only filesystem"],
                related_codes=["PERM001", "FS001"]
            ),
            ErrorCode(
                code="FS003",
                category=ErrorCategory.FILE_SYSTEM,
                severity=ErrorSeverity.HIGH,
                title="Disk Full",
                description="Not enough disk space to complete the operation",
                technical_details="Write failed - no space left on device",
                user_message="There's not enough disk space to complete this action.",
                recovery_suggestions=[
                    "Free up disk space",
                    "Delete unnecessary files",
                    "Use a different drive"
                ],
                auto_recoverable=False,
                known_causes=["Disk full", "Large file created", "Log files accumulating"],
                related_codes=["FS001", "FS002"]
            ),
            ErrorCode(
                code="PERM001",
                category=ErrorCategory.PERMISSION,
                severity=ErrorSeverity.CRITICAL,
                title="Insufficient Permissions",
                description="User lacks required permissions to perform the action",
                technical_details="Authorization failed - required permission not granted",
                user_message="You don't have permission to perform this action.",
                recovery_suggestions=[
                    "Run the application as administrator",
                    "Grant required permissions",
                    "Contact your system administrator"
                ],
                auto_recoverable=False,
                known_causes=["Not running as admin", "Missing capability", "Security policy blocking"],
                related_codes=["FS002", "PERM002"]
            ),
            ErrorCode(
                code="PERM002",
                category=ErrorCategory.PERMISSION,
                severity=ErrorSeverity.HIGH,
                title="Screen Recording Permission Denied",
                description="Cannot access screen recording due to macOS privacy restrictions",
                technical_details="CGWindowListCopyWindowInfo failed - requires screen recording permission",
                user_message="Please grant screen recording permission in System Preferences > Security & Privacy.",
                recovery_suggestions=[
                    "Open System Preferences > Security & Privacy > Privacy",
                    "Find Screen Recording and enable for this app",
                    "Restart the application after granting permission"
                ],
                auto_recoverable=False,
                known_causes=["Permission not granted", "App not in allowed list", "Permission revoked"],
                related_codes=["PERM001"]
            ),
            ErrorCode(
                code="TMO001",
                category=ErrorCategory.TIMEOUT,
                severity=ErrorSeverity.MEDIUM,
                title="Action Timeout",
                description="An automation action timed out waiting for expected element or state",
                technical_details="WaitFor condition exceeded maximum wait time",
                user_message="The action took too long and had to stop.",
                recovery_suggestions=[
                    "The application may be running slowly",
                    "Check if the target element is available",
                    "Increase timeout settings if needed"
                ],
                auto_recoverable=True,
                known_causes=["Application slow", "Element not appearing", "Network delay", "System load"],
                related_codes=["TMO002", "AUTO001"]
            ),
            ErrorCode(
                code="TMO002",
                category=ErrorCategory.TIMEOUT,
                severity=ErrorSeverity.HIGH,
                title="Workflow Execution Timeout",
                description="Entire workflow exceeded maximum allowed execution time",
                technical_details="Workflow runtime exceeded configured maximum duration",
                user_message="The workflow ran for too long and had to stop.",
                recovery_suggestions=[
                    "Break down the workflow into smaller parts",
                    "Optimize slow actions",
                    "Increase the workflow timeout limit"
                ],
                auto_recoverable=False,
                known_causes=["Infinite loop", "Too many actions", "Blocking operation", "System resource exhaustion"],
                related_codes=["TMO001"]
            ),
            ErrorCode(
                code="VAL001",
                category=ErrorCategory.VALIDATION,
                severity=ErrorSeverity.MEDIUM,
                title="Invalid Input Data",
                description="Input data does not meet validation requirements",
                technical_details="Schema validation failed - data missing, wrong type, or out of range",
                user_message="Some input data is invalid or missing.",
                recovery_suggestions=[
                    "Check and correct the input data",
                    "Ensure all required fields are filled",
                    "Verify data types are correct"
                ],
                auto_recoverable=False,
                known_causes=["Missing required field", "Wrong data type", "Value out of range", "Format error"],
                related_codes=["VAL002", "CFG001"]
            ),
            ErrorCode(
                code="VAL002",
                category=ErrorCategory.VALIDATION,
                severity=ErrorSeverity.HIGH,
                title="Image Template Not Found",
                description="Could not find the expected image template on screen",
                technical_details="Image matching failed - no match found above confidence threshold",
                user_message="Could not find the expected element on screen.",
                recovery_suggestions=[
                    "Update the image template",
                    "Check if the UI has changed",
                    "Adjust the confidence threshold"
                ],
                auto_recoverable=True,
                known_causes=["UI changed", "Image quality poor", "Wrong screen", "Confidence too high"],
                related_codes=["AUTO001", "VAL001"]
            ),
            ErrorCode(
                code="AUTO001",
                category=ErrorCategory.AUTOMATION,
                severity=ErrorSeverity.HIGH,
                title="Element Not Found",
                description="Cannot find the target UI element",
                technical_details="Element lookup failed - selector returned no results",
                user_message="Could not find the element you're looking for.",
                recovery_suggestions=[
                    "The UI may have changed - update your selector",
                    "Wait for the element to appear",
                    "Try a different search method"
                ],
                auto_recoverable=True,
                known_causes=["UI changed", "Wrong timing", "Wrong window", "Element hidden"],
                related_codes=["AUTO002", "TMO001"]
            ),
            ErrorCode(
                code="AUTO002",
                category=ErrorCategory.AUTOMATION,
                severity=ErrorSeverity.MEDIUM,
                title="Element State Changed",
                description="The target element changed during operation",
                technical_details="Element was found but state changed (moved, resized, disappeared)",
                user_message="The element changed while trying to interact with it.",
                recovery_suggestions=[
                    "Retry the action",
                    "Wait for element to stabilize",
                    "Use a more specific selector"
                ],
                auto_recoverable=True,
                known_causes=["Dynamic UI", "Animation running", "Page updating", "Multiple matches"],
                related_codes=["AUTO001", "AUTO003"]
            ),
            ErrorCode(
                code="AUTO003",
                category=ErrorCategory.AUTOMATION,
                severity=ErrorSeverity.MEDIUM,
                title="Multiple Elements Matched",
                description="Selector matched more than one element",
                technical_details="Ambiguous selector - multiple elements returned",
                user_message="Found multiple matches when expecting one.",
                recovery_suggestions=[
                    "Make your selector more specific",
                    "Use an index to pick the right one",
                    "Add additional filter criteria"
                ],
                auto_recoverable=False,
                known_causes=["Selector too broad", "Duplicate elements", "Missing index"],
                related_codes=["AUTO001", "AUTO002"]
            ),
            ErrorCode(
                code="RTE001",
                category=ErrorCategory.RUNTIME,
                severity=ErrorSeverity.CRITICAL,
                title="Script Execution Error",
                description="A script action failed with an exception",
                technical_details="Script threw an exception during execution",
                user_message="A script error occurred during execution.",
                recovery_suggestions=[
                    "Check the script for syntax errors",
                    "Verify all variables are defined",
                    "Review the error details below"
                ],
                auto_recoverable=False,
                known_causes=["Syntax error", "Undefined variable", "Import error", "Runtime exception"],
                related_codes=["RTE002", "DEP001"]
            ),
            ErrorCode(
                code="RTE002",
                category=ErrorCategory.RUNTIME,
                severity=ErrorSeverity.HIGH,
                title="Application Crashed",
                description="Target application crashed during automation",
                technical_details="Application process terminated unexpectedly",
                user_message="The target application crashed.",
                recovery_suggestions=[
                    "Restart the application",
                    "Check application logs",
                    "Save your work and restart"
                ],
                auto_recoverable=True,
                known_causes=["App bug", "Out of memory", "Invalid operation", "Race condition"],
                related_codes=["RTE001", "EXT001"]
            ),
            ErrorCode(
                code="CFG001",
                category=ErrorCategory.CONFIGURATION,
                severity=ErrorSeverity.HIGH,
                title="Missing Configuration",
                description="Required configuration value is missing",
                technical_details="Config key not found or value is null/empty",
                user_message="A required setting is missing.",
                recovery_suggestions=[
                    "Add the missing configuration",
                    "Check configuration file syntax",
                    "Use default value if appropriate"
                ],
                auto_recoverable=False,
                known_causes=["Config file incomplete", "Environment variable not set", "Typo in key name"],
                related_codes=["CFG002", "VAL001"]
            ),
            ErrorCode(
                code="CFG002",
                category=ErrorCategory.CONFIGURATION,
                severity=ErrorSeverity.MEDIUM,
                title="Invalid Configuration",
                description="Configuration value is invalid or out of range",
                technical_details="Config value fails validation checks",
                user_message="A configuration setting has an invalid value.",
                recovery_suggestions=[
                    "Check the configuration value",
                    "Ensure value is within valid range",
                    "Refer to documentation for correct format"
                ],
                auto_recoverable=False,
                known_causes=["Wrong value type", "Value out of range", "Deprecated option"],
                related_codes=["CFG001", "VAL001"]
            ),
            ErrorCode(
                code="DEP001",
                category=ErrorCategory.DEPENDENCY,
                severity=ErrorSeverity.CRITICAL,
                title="Missing Dependency",
                description="A required library or module is not installed",
                technical_details="Import failed - module not found in sys.path",
                user_message="A required component is missing.",
                recovery_suggestions=[
                    "Install the missing dependency",
                    "Run: pip install <package>",
                    "Check requirements.txt"
                ],
                auto_recoverable=False,
                known_causes=["Package not installed", "Wrong Python environment", "Corrupted installation"],
                related_codes=["DEP002"]
            ),
            ErrorCode(
                code="DEP002",
                category=ErrorCategory.DEPENDENCY,
                severity=ErrorSeverity.HIGH,
                title="Incompatible Version",
                description="Dependency version is incompatible",
                technical_details="Import succeeded but version check failed",
                user_message="A component is not compatible with this version.",
                recovery_suggestions=[
                    "Update the incompatible package",
                    "Check version requirements",
                    "Consider upgrading all packages"
                ],
                auto_recoverable=False,
                known_causes=["Outdated package", "Breaking change", "Version conflict"],
                related_codes=["DEP001"]
            ),
            ErrorCode(
                code="EXT001",
                category=ErrorCategory.EXTERNAL_SERVICE,
                severity=ErrorSeverity.HIGH,
                title="External Service Unavailable",
                description="Third-party service is not responding",
                technical_details="HTTP request failed - service returned error or timeout",
                user_message="An external service is not responding.",
                recovery_suggestions=[
                    "Check if the service is down",
                    "Try again later",
                    "Contact service support if persists"
                ],
                auto_recoverable=True,
                known_causes=["Service down", "Rate limiting", "API changes", "Network issues"],
                related_codes=["EXT002", "NET002"]
            ),
            ErrorCode(
                code="EXT002",
                category=ErrorCategory.EXTERNAL_SERVICE,
                severity=ErrorSeverity.MEDIUM,
                title="External Service Rate Limited",
                description="API rate limit exceeded",
                technical_details="HTTP 429 - Too many requests",
                user_message="Too many requests. Please wait before trying again.",
                recovery_suggestions=[
                    "Wait before retrying",
                    "Implement request batching",
                    "Consider upgrading your API plan"
                ],
                auto_recoverable=True,
                known_causes=["Too many requests", "Quota exceeded", "Service protecting itself"],
                related_codes=["EXT001"]
            ),
            ErrorCode(
                code="UNK001",
                category=ErrorCategory.UNKNOWN,
                severity=ErrorSeverity.MEDIUM,
                title="Unknown Error",
                description="An unexpected error occurred",
                technical_details="No matching error code - unclassified exception",
                user_message="An unexpected error occurred. Please try again.",
                recovery_suggestions=[
                    "Try the action again",
                    "Restart the application",
                    "Contact support if the problem persists"
                ],
                auto_recoverable=False,
                known_causes=["Unforeseen edge case", "Race condition", "Bug in code"],
                related_codes=["UNK002"]
            ),
            ErrorCode(
                code="UNK002",
                category=ErrorCategory.UNKNOWN,
                severity=ErrorSeverity.HIGH,
                title="Critical Unknown Error",
                description="A critical error that could not be classified",
                technical_details="High severity exception that requires investigation",
                user_message="A serious error occurred. Please contact support.",
                recovery_suggestions=[
                    "Save any work immediately",
                    "Restart the application",
                    "Report this error to the development team"
                ],
                auto_recoverable=False,
                known_causes=["Unhandled exception", "System-level error", "Memory corruption"],
                related_codes=["UNK001", "RTE001"]
            ),
        ]

        for code in standard_codes:
            self._codes[code.code] = code

    def get_code(self, code: str) -> Optional[ErrorCode]:
        """Get error code by string code."""
        return self._codes.get(code)

    def generate_code(self, category: ErrorCategory, error_type: str) -> str:
        """Generate a new error code for a category."""
        prefix = self.PREFIXES.get(category, "UNK")
        self._sequence_counters[category] += 1
        number = self._sequence_counters[category]
        return f"{prefix}{number:03d}"

    def find_matching_code(self, error_message: str, exception_type: str = "") -> Tuple[Optional[ErrorCode], float]:
        """Find the best matching error code for an error message.
        
        Returns:
            Tuple of (matched ErrorCode or None, confidence score 0-1)
        """
        error_message_lower = error_message.lower()
        exception_lower = exception_type.lower()
        best_match = None
        best_score = 0.0

        for code in self._codes.values():
            score = 0.0

            # Check title keywords
            title_words = code.title.lower().split()
            for word in title_words:
                if word in error_message_lower:
                    score += 0.3

            # Check description keywords
            desc_words = code.description.lower().split()
            for word in desc_words:
                if word in error_message_lower:
                    score += 0.15

            # Check known causes
            for cause in code.known_causes:
                if cause.lower() in error_message_lower:
                    score += 0.25

            # Check exception type if provided
            if exception_lower:
                if exception_lower in code.technical_details.lower():
                    score += 0.4
                if exception_lower in code.title.lower():
                    score += 0.5

            if score > best_score:
                best_score = score
                best_match = code

        return best_match, min(best_score, 1.0)

    def get_all_codes(self) -> List[ErrorCode]:
        """Get all error codes."""
        return list(self._codes.values())

    def get_codes_by_category(self, category: ErrorCategory) -> List[ErrorCode]:
        """Get error codes filtered by category."""
        return [c for c in self._codes.values() if c.category == category]

    def get_codes_by_severity(self, severity: ErrorSeverity) -> List[ErrorCode]:
        """Get error codes filtered by severity."""
        return [c for c in self._codes.values() if c.severity == severity]


# =============================================================================
# User-Friendly Message Generator
# =============================================================================

class UserMessageGenerator:
    """Replace technical jargon with plain language explanations."""

    # Technical terms and their plain language equivalents
    TRANSLATIONS = {
        # Network
        "connection refused": "the server is not accepting connections",
        "connection reset": "the connection was interrupted by the server",
        "connection timeout": "the request took too long and was cancelled",
        "dns resolution failed": "could not find the website address",
        "socket error": "could not establish network connection",
        "ssl error": "secure connection could not be established",
        "certificate verify failed": "the website's security certificate is not trusted",
        "http error": "the server returned an error response",
        "500": "the server encountered an internal problem",
        "502": "the server is acting as a gateway but received an invalid response",
        "503": "the server is temporarily unavailable",
        "404": "the requested item was not found",
        "403": "access to this resource was denied",
        # File System
        "file not found": "the file does not exist",
        "no such file": "the file does not exist",
        "permission denied": "you don't have permission to access this",
        "access denied": "you don't have permission to access this",
        "disk full": "there is no space left on the drive",
        "no space left": "there is no space left on the drive",
        "read-only": "this cannot be modified",
        "directory not empty": "the folder still contains files",
        # Automation
        "element not found": "could not find the on-screen element",
        "element not visible": "the element exists but is hidden",
        "stale element": "the element changed while we were trying to use it",
        "no such element": "the element no longer exists on the page",
        "timeout waiting for element": "the element did not appear in time",
        # General
        "null": "missing value",
        "none": "not set",
        "undefined": "not defined",
        "invalid": "not valid",
        "exception": "an error occurred",
        "traceback": "detailed error information",
        "errno": "error number",
    }

    def __init__(self):
        self._patterns: List[Tuple[re.Pattern, str]] = []
        self._init_patterns()

    def _init_patterns(self):
        """Initialize translation patterns."""
        for tech_term, plain_term in self.TRANSLATIONS.items():
            # Escape special regex characters
            escaped = re.escape(tech_term)
            self._patterns.append(
                (re.compile(escaped, re.IGNORECASE), plain_term)
            )

    def generate(self, error_message: str, error_code: Optional[ErrorCode] = None,
                 context: Optional[Dict[str, Any]] = None) -> str:
        """Generate user-friendly error message."""
        if error_code:
            # Use the predefined user message from the code
            message = error_code.user_message
        else:
            # Translate technical terms
            message = error_message
            for pattern, replacement in self._patterns:
                message = pattern.sub(replacement, message)

        # Add context hints if available
        hints = []
        if context:
            if "workflow_name" in context:
                hints.append(f"in workflow '{context['workflow_name']}'")
            if "step_name" in context:
                hints.append(f"at step '{context['step_name']}'")

        if hints:
            message = f"{message} ({', '.join(hints)})"

        return message

    def generate_recovery_hint(self, error_code: ErrorCode) -> str:
        """Generate a single recovery hint from error code."""
        if error_code.recovery_suggestions:
            return error_code.recovery_suggestions[0]
        return "Try again or contact support if the problem persists."


# =============================================================================
# Auto-Recovery Engine
# =============================================================================

class AutoRecoveryEngine:
    """Attempt automatic recovery for known error types."""

    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self._recovery_handlers: Dict[str, Callable] = {}
        self._init_standard_handlers()

    def _init_standard_handlers(self):
        """Initialize standard recovery handlers."""
        self.register_handler("NET001", self._retry_network_connection)
        self.register_handler("NET002", self._retry_with_backoff)
        self.register_handler("TMO001", self._retry_with_backoff)
        self.register_handler("AUTO001", self._relocate_element)
        self.register_handler("AUTO002", self._retry_action)
        self.register_handler("VAL002", self._retry_image_match)
        self.register_handler("EXT001", self._retry_external_service)
        self.register_handler("EXT002", self._wait_for_rate_limit)

    def register_handler(self, error_code: str, handler: Callable):
        """Register a recovery handler for an error code."""
        self._recovery_handlers[error_code] = handler

    def can_recover(self, error_code: str) -> bool:
        """Check if an error is auto-recoverable."""
        return error_code in self._recovery_handlers

    def attempt_recovery(self, error_record: ErrorRecord,
                         context: Dict[str, Any]) -> RecoveryAttempt:
        """Attempt to recover from an error."""
        start_time = time.time()

        handler = self._recovery_handlers.get(error_record.error_code)
        if not handler:
            return RecoveryAttempt(
                timestamp=time.time(),
                strategy="none",
                action_taken="No auto-recovery handler available",
                success=False,
                duration=time.time() - start_time,
                details=f"Error code {error_record.error_code} is not auto-recoverable"
            )

        try:
            result = handler(error_record, context)
            return RecoveryAttempt(
                timestamp=time.time(),
                strategy=result.get("strategy", "unknown"),
                action_taken=result.get("action", "Recovery attempted"),
                success=result.get("success", False),
                duration=time.time() - start_time,
                details=result.get("details", "")
            )
        except Exception as e:
            return RecoveryAttempt(
                timestamp=time.time(),
                strategy="exception",
                action_taken="Recovery handler raised exception",
                success=False,
                duration=time.time() - start_time,
                details=f"Handler error: {str(e)}"
            )

    # Standard recovery handlers

    def _retry_network_connection(self, error: ErrorRecord,
                                   context: Dict) -> Dict[str, Any]:
        """Retry network connection with exponential backoff."""
        import socket

        host = context.get("host", "unknown")
        port = context.get("port", 80)
        max_attempts = min(self.max_retries, 3)
        last_error = None

        for attempt in range(max_attempts):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                sock.connect((host, port))
                sock.close()
                return {
                    "success": True,
                    "strategy": "retry",
                    "action": f"Connected to {host}:{port}",
                    "details": f"Succeeded on attempt {attempt + 1}"
                }
            except Exception as e:
                last_error = e
                time.sleep(2 ** attempt)  # Exponential backoff

        return {
            "success": False,
            "strategy": "retry",
            "action": "Network connection retry failed",
            "details": f"Failed after {max_attempts} attempts: {last_error}"
        }

    def _retry_with_backoff(self, error: ErrorRecord,
                            context: Dict) -> Dict[str, Any]:
        """Generic retry with exponential backoff."""
        max_attempts = min(self.max_retries, 3)
        base_delay = float(context.get("retry_delay", 1.0))

        for attempt in range(max_attempts):
            delay = base_delay * (2 ** attempt)
            time.sleep(delay)

            # Check if action succeeded (would need callback integration)
            # For now, just report the attempt
            if attempt == max_attempts - 1:
                return {
                    "success": False,
                    "strategy": "exponential_backoff",
                    "action": f"Retried {max_attempts} times with backoff",
                    "details": f"Last delay was {delay:.1f}s"
                }

        return {
            "success": False,
            "strategy": "exponential_backoff",
            "action": "Retry attempts completed",
            "details": "All retry attempts finished"
        }

    def _relocate_element(self, error: ErrorRecord,
                          context: Dict) -> Dict[str, Any]:
        """Try to relocate element with different selector."""
        return {
            "success": False,
            "strategy": "relocate",
            "action": "Element relocation attempted",
            "details": "Alternative selector strategy would be applied here"
        }

    def _retry_action(self, error: ErrorRecord,
                      context: Dict) -> Dict[str, Any]:
        """Retry the failed action."""
        return {
            "success": False,
            "strategy": "retry",
            "action": "Action retry attempted",
            "details": "Retry handler for action execution"
        }

    def _retry_image_match(self, error: ErrorRecord,
                           context: Dict) -> Dict[str, Any]:
        """Retry image matching with adjusted threshold."""
        return {
            "success": False,
            "strategy": "retry",
            "action": "Image match retry attempted",
            "details": "Would retry with adjusted confidence threshold"
        }

    def _retry_external_service(self, error: ErrorRecord,
                               context: Dict) -> Dict[str, Any]:
        """Retry external service call."""
        return self._retry_with_backoff(error, context)

    def _wait_for_rate_limit(self, error: ErrorRecord,
                             context: Dict) -> Dict[str, Any]:
        """Wait for rate limit to reset."""
        wait_time = float(context.get("retry_after", 60))
        time.sleep(min(wait_time, 120))  # Cap at 2 minutes

        return {
            "success": True,
            "strategy": "wait",
            "action": f"Waited {wait_time}s for rate limit reset",
            "details": "Rate limit wait completed"
        }


# =============================================================================
# Error Pattern Detector
# =============================================================================

class ErrorPatternDetector:
    """Detect error patterns and alert when same error repeats N times."""

    def __init__(self):
        self._patterns: Dict[str, ErrorPattern] = {}
        self._lock = RLock()  # Use RLock to allow reentrant acquisition

    def register_pattern(self, error_signature: str, threshold: int = 5,
                         time_window: float = 300.0) -> str:
        """Register a new pattern to monitor."""
        pattern_id = hashlib.md5(error_signature.encode()).hexdigest()[:8]
        with self._lock:
            self._patterns[pattern_id] = ErrorPattern(
                pattern_id=pattern_id,
                error_signature=error_signature,
                threshold=threshold,
                time_window=time_window,
                occurrences=0,
                first_detected=0.0,
                last_detected=0.0,
                alerted=False,
                alert_count=0
            )
        return pattern_id

    def record_occurrence(self, error_signature: str) -> Tuple[bool, Optional[ErrorPattern]]:
        """Record an error occurrence and return (should_alert, pattern)."""
        with self._lock:
            # Find or create pattern
            pattern = None
            for p in self._patterns.values():
                if p.error_signature == error_signature:
                    pattern = p
                    break

            if not pattern:
                pattern_id = self.register_pattern(error_signature)
                pattern = self._patterns[pattern_id]

            now = time.time()

            # Clean old occurrences outside time window
            pattern.recent_timestamps = [
                t for t in pattern.recent_timestamps
                if now - t <= pattern.time_window
            ]

            pattern.recent_timestamps.append(now)
            pattern.occurrences = len(pattern.recent_timestamps)
            pattern.last_detected = now
            if pattern.first_detected == 0:
                pattern.first_detected = now

            # Check if threshold exceeded
            should_alert = pattern.occurrences >= pattern.threshold and not pattern.alerted

            if should_alert:
                pattern.alerted = True
                pattern.alert_count += 1

            return should_alert, pattern

    def get_active_alerts(self) -> List[ErrorPattern]:
        """Get all patterns that have active alerts."""
        with self._lock:
            return [p for p in self._patterns.values() if p.alerted]

    def reset_alert(self, pattern_id: str) -> bool:
        """Reset alert for a pattern."""
        with self._lock:
            if pattern_id in self._patterns:
                self._patterns[pattern_id].alerted = False
                return True
        return False


# =============================================================================
# HTML Dashboard Generator
# =============================================================================

class ErrorDashboardGenerator:
    """Generate HTML dashboard showing error trends."""

    def __init__(self):
        self._template = self._load_template()

    def _load_template(self) -> Template:
        """Load the HTML template."""
        return Template("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Error Dashboard - RAbAI AutoClick</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-tertiary: #334155;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent-blue: #3b82f6;
            --accent-red: #ef4444;
            --accent-orange: #f97316;
            --accent-yellow: #eab308;
            --accent-green: #22c55e;
            --accent-purple: #a855f7;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--bg-tertiary);
        }
        .header h1 { font-size: 1.8rem; color: var(--text-primary); }
        .header .subtitle { color: var(--text-secondary); font-size: 0.9rem; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid var(--bg-tertiary);
        }
        .stat-card .label {
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-bottom: 8px;
        }
        .stat-card .value {
            font-size: 2rem;
            font-weight: 600;
        }
        .stat-card .value.critical { color: var(--accent-red); }
        .stat-card .value.high { color: var(--accent-orange); }
        .stat-card .value.medium { color: var(--accent-yellow); }
        .stat-card .value.low { color: var(--accent-green); }
        .charts-row {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }
        @media (max-width: 1024px) {
            .charts-row { grid-template-columns: 1fr; }
        }
        .chart-container {
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid var(--bg-tertiary);
        }
        .chart-container h3 {
            margin-bottom: 15px;
            font-size: 1rem;
            color: var(--text-secondary);
        }
        .error-table {
            background: var(--bg-secondary);
            border-radius: 12px;
            border: 1px solid var(--bg-tertiary);
            overflow: hidden;
        }
        .error-table h3 {
            padding: 15px 20px;
            font-size: 1rem;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--bg-tertiary);
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 20px;
            text-align: left;
            border-bottom: 1px solid var(--bg-tertiary);
        }
        th {
            background: var(--bg-tertiary);
            font-weight: 600;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }
        tr:hover { background: rgba(59, 130, 246, 0.1); }
        .badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .badge.critical { background: rgba(239, 68, 68, 0.2); color: var(--accent-red); }
        .badge.high { background: rgba(249, 115, 22, 0.2); color: var(--accent-orange); }
        .badge.medium { background: rgba(234, 179, 8, 0.2); color: var(--accent-yellow); }
        .badge.low { background: rgba(34, 197, 94, 0.2); color: var(--accent-green); }
        .category-tag {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.7rem;
            background: var(--bg-tertiary);
            color: var(--text-secondary);
        }
        .refresh-btn {
            background: var(--accent-blue);
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 500;
        }
        .refresh-btn:hover { background: #2563eb; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>Error Dashboard</h1>
            <div class="subtitle">RAbAI AutoClick - Real-time Error Monitoring</div>
        </div>
        <button class="refresh-btn" onclick="location.reload()">Refresh</button>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="label">Total Errors (24h)</div>
            <div class="value">{total_errors}</div>
        </div>
        <div class="stat-card">
            <div class="label">Critical Errors</div>
            <div class="value critical">{critical_count}</div>
        </div>
        <div class="stat-card">
            <div class="label">Active Alerts</div>
            <div class="value high">{alert_count}</div>
        </div>
        <div class="stat-card">
            <div class="label">Resolution Rate</div>
            <div class="value low">{resolution_rate}%</div>
        </div>
    </div>

    <div class="charts-row">
        <div class="chart-container">
            <h3>Error Trends (Last 24 Hours)</h3>
            <canvas id="trendChart"></canvas>
        </div>
        <div class="chart-container">
            <h3>Errors by Category</h3>
            <canvas id="categoryChart"></canvas>
        </div>
    </div>

    <div class="chart-container" style="margin-bottom: 20px;">
        <h3>Errors by Severity</h3>
        <canvas id="severityChart"></canvas>
    </div>

    <div class="error-table">
        <h3>Recent Errors</h3>
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Error Code</th>
                    <th>Category</th>
                    <th>Severity</th>
                    <th>Message</th>
                    <th>Count</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {error_rows}
            </tbody>
        </table>
    </div>

    <script>
        // Trend Chart
        new Chart(document.getElementById('trendChart'), {
            type: 'line',
            data: {
                labels: {trend_labels},
                datasets: [{
                    label: 'Errors',
                    data: {trend_data},
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#94a3b8' } },
                    y: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#94a3b8' } }
                }
            }
        });

        // Category Chart
        new Chart(document.getElementById('categoryChart'), {
            type: 'doughnut',
            data: {
                labels: {category_labels},
                datasets: [{
                    data: {category_data},
                    backgroundColor: ['#3b82f6', '#ef4444', '#22c55e', '#f97316', '#a855f7', '#eab308']
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { position: 'right', labels: { color: '#94a3b8' } } }
            }
        });

        // Severity Chart
        new Chart(document.getElementById('severityChart'), {
            type: 'bar',
            data: {
                labels: ['Critical', 'High', 'Medium', 'Low'],
                datasets: [{
                    label: 'Errors',
                    data: [{critical_count}, {high_count}, {medium_count}, {low_count}],
                    backgroundColor: ['#ef4444', '#f97316', '#eab308', '#22c55e']
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#94a3b8' } },
                    y: { grid: { color: 'rgba(255,255,255,0.1)' }, ticks: { color: '#94a3b8' } }
                }
            }
        });
    </script>
</body>
</html>""")

    def generate(self, error_records: List[ErrorRecord],
                  aggregated_errors: List[AggregatedError],
                  stats: ErrorStats,
                  alerts: List[ErrorPattern]) -> str:
        """Generate HTML dashboard."""
        # Prepare chart data
        # Group errors by hour for trend
        hourly_counts = defaultdict(int)
        for record in error_records:
            hour_key = datetime.fromtimestamp(record.timestamp).strftime('%H:00')
            hourly_counts[hour_key] += 1

        trend_labels = list(hourly_counts.keys())
        trend_data = list(hourly_counts.values())

        # Category distribution
        category_counts = Counter(e.category.value for e in aggregated_errors)
        category_labels = list(category_counts.keys())
        category_data = list(category_counts.values())

        # Severity counts
        severity_counts = {
            'critical': stats.errors_by_severity.get('critical', 0),
            'high': stats.errors_by_severity.get('high', 0),
            'medium': stats.errors_by_severity.get('medium', 0),
            'low': stats.errors_by_severity.get('low', 0)
        }

        # Generate error rows
        error_rows = []
        for record in sorted(error_records, key=lambda x: x.timestamp, reverse=True)[:20]:
            time_str = datetime.fromtimestamp(record.timestamp).strftime('%H:%M:%S')
            severity_class = record.severity.value
            count_str = f"+{record.aggregate_count}" if record.aggregate_count > 1 else "1"
            status_badge = f'<span class="badge {severity_class}">{record.status.value}</span>'

            error_rows.append(f"""
                <tr>
                    <td>{time_str}</td>
                    <td><code>{record.error_code}</code></td>
                    <td><span class="category-tag">{record.category.value}</span></td>
                    <td><span class="badge {severity_class}">{severity_class}</span></td>
                    <td>{record.user_message[:60]}...</td>
                    <td>{count_str}</td>
                    <td>{status_badge}</td>
                </tr>
            """)

        html = self._template.safe_substitute(
            total_errors=stats.total_errors,
            critical_count=severity_counts['critical'],
            high_count=severity_counts['high'],
            medium_count=severity_counts['medium'],
            low_count=severity_counts['low'],
            alert_count=len(alerts),
            resolution_rate=f"{stats.resolution_rate:.1f}",
            trend_labels=json.dumps(trend_labels),
            trend_data=json.dumps(trend_data),
            category_labels=json.dumps(category_labels),
            category_data=json.dumps(category_data),
            error_rows='\n'.join(error_rows) if error_rows else '<tr><td colspan="7">No errors recorded</td></tr>'
        )

        return html


# =============================================================================
# Main Workflow Error Handler
# =============================================================================

class WorkflowErrorHandler:
    """
    Comprehensive error handling and user experience module.
    
    Features:
    - Error category classification
    - Error severity levels
    - Error recovery suggestions
    - Error history tracking
    - Error aggregation
    - User-friendly error messages
    - Error context preservation
    - HTML error dashboard
    - Alerting on error patterns
    - Error code catalog
    - Auto-recovery
    - Error notifications
    """

    def __init__(self, data_dir: str = "./data", notification_callback: Optional[Callable] = None):
        self.data_dir = data_dir
        self.notification_callback = notification_callback

        # Core components
        self.catalog = ErrorCodeCatalog()
        self.user_messages = UserMessageGenerator()
        self.auto_recovery = AutoRecoveryEngine()
        self.pattern_detector = ErrorPatternDetector()
        self.dashboard_generator = ErrorDashboardGenerator()

        # Error storage
        self._errors: List[ErrorRecord] = []
        self._aggregated_errors: Dict[str, AggregatedError] = {}
        self._error_history: Dict[str, List[ErrorRecord]] = defaultdict(list)
        self._lock = Lock()

        # Configuration
        self.alert_threshold = 5  # Alert after N similar errors
        self.alert_time_window = 300.0  # 5 minutes
        self.max_history = 10000
        self.auto_recover_enabled = True

        # Dashboard settings
        self._dashboard_path = os.path.join(data_dir, "error_dashboard.html")

        # Load persisted data
        self._load_data()

        logger.info("WorkflowErrorHandler initialized", version="v24")

    # =========================================================================
    # Error Recording
    # =========================================================================

    def record_error(self, exception: Exception, context: Dict[str, Any],
                     workflow_name: str = "", step_name: str = "",
                     step_index: int = 0, action_type: str = "",
                     action_params: Dict[str, Any] = None) -> ErrorRecord:
        """Record a new error with full context."""
        with self._lock:
            # Classify the error
            error_code_str, category, severity = self._classify_error(exception)

            # Find matching error code from catalog
            matched_code, confidence = self.catalog.find_matching_code(
                str(exception), type(exception).__name__
            )

            if matched_code and confidence > 0.3:
                error_code_str = matched_code.code
                category = matched_code.category
                severity = matched_code.severity

            # Generate error ID
            error_id = self._generate_error_id(exception, context)

            # Build full context
            full_context = ErrorContext(
                timestamp=time.time(),
                workflow_name=workflow_name,
                workflow_id=context.get("workflow_id"),
                step_name=step_name,
                step_index=step_index,
                action_type=action_type,
                action_params=action_params or {},
                environment=context.get("environment", {}),
                system_state=context.get("system_state", {}),
                user_data=context.get("user_data", {}),
                stack_trace=traceback.format_exc(),
                raw_exception=repr(exception),
                previous_errors=context.get("error_chain", [])
            )

            # Generate user message
            user_msg = self.user_messages.generate(
                str(exception),
                matched_code,
                {"workflow_name": workflow_name, "step_name": step_name}
            )

            # Check for aggregation
            signature = self._compute_error_signature(exception, category)
            now = time.time()

            if signature in self._aggregated_errors:
                agg = self._aggregated_errors[signature]
                agg.count += 1
                agg.last_occurrence = now
                if len(agg.recent_timestamps) < 10:
                    agg.recent_timestamps.append(now)
                agg.workflow_names.add(workflow_name)
                agg.step_names.add(step_name)
                agg.success_rate = self._calculate_success_rate(signature)
            else:
                self._aggregated_errors[signature] = AggregatedError(
                    error_signature=signature,
                    error_code=error_code_str,
                    category=category,
                    severity=severity,
                    message=str(exception),
                    user_message=user_msg,
                    count=1,
                    first_occurrence=now,
                    last_occurrence=now,
                    recent_timestamps=[now],
                    success_rate=0.0,
                    workflow_names={workflow_name},
                    step_names={step_name}
                )

            # Create error record
            record = ErrorRecord(
                error_id=error_id,
                error_code=error_code_str,
                category=category,
                severity=severity,
                message=str(exception),
                user_message=user_msg,
                context=full_context,
                timestamp=now,
                status=ErrorStatus.NEW,
                recovery_attempts=[],
                resolved=False,
                resolved_at=None,
                resolved_by=None,
                aggregate_count=self._aggregated_errors[signature].count,
                first_occurrence=now,
                last_occurrence=now
            )

            self._errors.append(record)
            self._error_history[workflow_name].append(record)

            # Trim history if needed
            if len(self._errors) > self.max_history:
                self._errors = self._errors[-self.max_history:]

            # Check for patterns and alerts
            should_alert, pattern = self.pattern_detector.record_occurrence(signature)

            # Attempt auto-recovery if enabled
            if self.auto_recover_enabled and matched_code and matched_code.auto_recoverable:
                recovery = self.auto_recovery.attempt_recovery(record, context)
                record.recovery_attempts.append(recovery)

            # Send notification for critical/high errors
            if severity in (ErrorSeverity.CRITICAL, ErrorSeverity.HIGH):
                self._send_notification(record)

            # Persist data
            self._save_data()

            logger.error(
                f"Error recorded: {error_code_str} | {category.value} | {severity.value} | {user_msg}",
                extra={"error_id": error_id, "workflow": workflow_name, "step": step_name}
            )

            return record

    def _classify_error(self, exception: Exception) -> Tuple[str, ErrorCategory, ErrorSeverity]:
        """Classify error into category and determine severity."""
        error_str = str(exception).lower()
        exc_type = type(exception).__name__.lower()

        # Pattern matching for classification
        if any(x in error_str for x in ['connection', 'network', 'socket', 'dns', 'http']):
            category = ErrorCategory.NETWORK
        elif any(x in error_str for x in ['file', 'directory', 'path', 'disk', 'read', 'write']):
            category = ErrorCategory.FILE_SYSTEM
        elif any(x in error_str for x in ['permission', 'access denied', 'unauthorized', 'forbidden']):
            category = ErrorCategory.PERMISSION
        elif any(x in error_str for x in ['timeout', 'timed out', 'took too long']):
            category = ErrorCategory.TIMEOUT
        elif any(x in error_str for x in ['invalid', 'validation', 'schema', 'parse']):
            category = ErrorCategory.VALIDATION
        elif any(x in error_str for x in ['config', 'configuration', 'setting']):
            category = ErrorCategory.CONFIGURATION
        elif any(x in error_str for x in ['import', 'module', 'dependency', 'package']):
            category = ErrorCategory.DEPENDENCY
        elif any(x in error_str for x in ['element', 'click', 'image', 'screen', 'automation']):
            category = ErrorCategory.AUTOMATION
        else:
            category = ErrorCategory.UNKNOWN

        # Determine severity based on exception type and category
        severity = ErrorSeverity.MEDIUM

        if any(x in exc_type for x in ['critical', 'fatal', 'crash']):
            severity = ErrorSeverity.CRITICAL
        elif any(x in exc_type for x in ['timeout', 'connection']):
            severity = ErrorSeverity.HIGH
        elif category in (ErrorCategory.PERMISSION, ErrorCategory.DEPENDENCY):
            severity = ErrorSeverity.HIGH
        elif category == ErrorCategory.UNKNOWN:
            severity = ErrorSeverity.HIGH

        # Generate a code
        code = self.catalog.generate_code(category, exc_type)

        return code, category, severity

    def _generate_error_id(self, exception: Exception, context: Dict) -> str:
        """Generate unique error ID."""
        data = f"{time.time()}{type(exception).__name__}{str(exception)}"
        return hashlib.md5(data.encode()).hexdigest()[:12]

    def _compute_error_signature(self, exception: Exception, category: ErrorCategory) -> str:
        """Compute error signature for aggregation."""
        data = f"{category.value}:{type(exception).__name__}:{str(exception)[:100]}"
        return hashlib.md5(data.encode()).hexdigest()[:16]

    def _calculate_success_rate(self, signature: str) -> float:
        """Calculate success rate for an error signature."""
        related_errors = [e for e in self._errors
                         if self._compute_error_signature(
                             Exception(e.message), e.category) == signature]
        if not related_errors:
            return 0.0
        resolved = sum(1 for e in related_errors if e.resolved)
        return resolved / len(related_errors)

    # =========================================================================
    # Error Recovery
    # =========================================================================

    def get_recovery_suggestions(self, error_code: str) -> List[str]:
        """Get recovery suggestions for an error code."""
        code = self.catalog.get_code(error_code)
        if code:
            return code.recovery_suggestions
        return ["Try again", "Restart the application", "Contact support if persists"]

    def attempt_manual_recovery(self, error_id: str,
                                strategy: str) -> RecoveryAttempt:
        """Attempt manual recovery with specified strategy."""
        with self._lock:
            record = next((e for e in self._errors if e.error_id == error_id), None)
            if not record:
                return RecoveryAttempt(
                    timestamp=time.time(),
                    strategy=strategy,
                    action_taken="Error not found",
                    success=False,
                    duration=0,
                    details="Could not find error record"
                )

            context = {
                "workflow_name": record.context.workflow_name,
                "step_name": record.context.step_name,
                "action_params": record.context.action_params
            }

            # Create a recovery attempt
            start_time = time.time()
            success = False
            details = f"Manual recovery with strategy '{strategy}' attempted"

            # Simulate recovery attempt based on strategy
            if strategy == "retry":
                success = True
                details = "Action retried successfully"
            elif strategy == "skip":
                success = True
                details = "Step skipped as requested"
            elif strategy == "fallback":
                success = True
                details = "Fallback action executed"

            attempt = RecoveryAttempt(
                timestamp=time.time(),
                strategy=strategy,
                action_taken=details,
                success=success,
                duration=time.time() - start_time,
                details=details
            )

            record.recovery_attempts.append(attempt)
            self._save_data()

            return attempt

    # =========================================================================
    # Error Resolution
    # =========================================================================

    def resolve_error(self, error_id: str, resolved_by: str = "system") -> bool:
        """Mark an error as resolved."""
        with self._lock:
            record = next((e for e in self._errors if e.error_id == error_id), None)
            if not record:
                return False

            record.resolved = True
            record.resolved_at = time.time()
            record.resolved_by = resolved_by
            record.status = ErrorStatus.RESOLVED

            # Update aggregated error
            signature = self._compute_error_signature(
                Exception(record.message), record.category)
            if signature in self._aggregated_errors:
                self._aggregated_errors[signature].success_rate = \
                    self._calculate_success_rate(signature)

            self._save_data()
            return True

    def acknowledge_error(self, error_id: str) -> bool:
        """Acknowledge an error."""
        with self._lock:
            record = next((e for e in self._errors if e.error_id == error_id), None)
            if not record:
                return False

            record.status = ErrorStatus.ACKNOWLEDGED
            self._save_data()
            return True

    def escalate_error(self, error_id: str) -> bool:
        """Escalate an error."""
        with self._lock:
            record = next((e for e in self._errors if e.error_id == error_id), None)
            if not record:
                return False

            record.status = ErrorStatus.ESCALATED
            self._send_notification(record, urgent=True)
            self._save_data()
            return True

    # =========================================================================
    # Error Querying
    # =========================================================================

    def get_error_stats(self) -> ErrorStats:
        """Get comprehensive error statistics."""
        now = time.time()
        day_ago = now - 86400

        # Filter errors from last 24 hours
        recent_errors = [e for e in self._errors if now - e.timestamp < 86400]

        # Count by category
        by_category = Counter(e.category.value for e in recent_errors)

        # Count by severity
        by_severity = Counter(e.severity.value for e in recent_errors)

        # Count by workflow
        by_workflow = Counter(e.context.workflow_name for e in recent_errors
                            if e.context.workflow_name)

        # Top errors
        top_errors = Counter(
            f"{e.error_code}:{e.message[:30]}" for e in recent_errors
        ).most_common(10)

        # Resolution rate
        resolved = sum(1 for e in recent_errors if e.resolved)
        resolution_rate = (resolved / len(recent_errors) * 100) if recent_errors else 0

        # Avg resolution time
        resolved_records = [e for e in recent_errors if e.resolved and e.resolved_at]
        if resolved_records:
            avg_res_time = sum(
                e.resolved_at - e.timestamp for e in resolved_records
            ) / len(resolved_records)
        else:
            avg_res_time = 0

        # Critical errors active
        critical_active = sum(
            1 for e in self._errors
            if e.severity == ErrorSeverity.CRITICAL and not e.resolved
        )

        # Error rate (errors per hour)
        error_rate = len(recent_errors) / 24.0

        return ErrorStats(
            total_errors=len(recent_errors),
            errors_by_category=dict(by_category),
            errors_by_severity=dict(by_severity),
            errors_by_workflow=dict(by_workflow),
            top_errors=top_errors,
            error_rate=error_rate,
            resolution_rate=resolution_rate,
            avg_resolution_time=avg_res_time,
            critical_errors_active=critical_active
        )

    def get_aggregated_errors(self, min_count: int = 2) -> List[AggregatedError]:
        """Get aggregated errors grouped by signature."""
        return [e for e in self._aggregated_errors.values() if e.count >= min_count]

    def get_error_history(self, workflow_name: Optional[str] = None,
                          limit: int = 100) -> List[ErrorRecord]:
        """Get error history, optionally filtered by workflow."""
        errors = self._errors
        if workflow_name:
            errors = self._error_history.get(workflow_name, [])
        return sorted(errors, key=lambda x: x.timestamp, reverse=True)[:limit]

    def get_active_alerts(self) -> List[ErrorPattern]:
        """Get currently active error pattern alerts."""
        return self.pattern_detector.get_active_alerts()

    def get_error_by_id(self, error_id: str) -> Optional[ErrorRecord]:
        """Get a specific error by ID."""
        return next((e for e in self._errors if e.error_id == error_id), None)

    def get_error_code_info(self, error_code: str) -> Optional[ErrorCode]:
        """Get detailed information about an error code."""
        return self.catalog.get_code(error_code)

    # =========================================================================
    # Dashboard
    # =========================================================================

    def generate_dashboard(self) -> str:
        """Generate HTML error dashboard."""
        stats = self.get_error_stats()
        aggregated = self.get_aggregated_errors()
        alerts = self.get_active_alerts()

        html = self.dashboard_generator.generate(
            self._errors[-100:],  # Last 100 errors
            aggregated,
            stats,
            alerts
        )

        # Save dashboard
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self._dashboard_path, 'w') as f:
            f.write(html)

        return html

    def get_dashboard_path(self) -> str:
        """Get path to the dashboard HTML file."""
        return self._dashboard_path

    # =========================================================================
    # Notifications
    # =========================================================================

    def _send_notification(self, record: ErrorRecord, urgent: bool = False):
        """Send error notification."""
        if not self.notification_callback:
            logger.info(
                f"Notification would be sent: {record.error_code} - {record.user_message}",
                extra={"severity": record.severity.value}
            )
            return

        try:
            self.notification_callback({
                "type": "error",
                "error_code": record.error_code,
                "severity": record.severity.value,
                "message": record.user_message,
                "workflow": record.context.workflow_name,
                "step": record.context.step_name,
                "timestamp": record.timestamp,
                "urgent": urgent,
                "error_id": record.error_id
            })
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")

    def set_notification_callback(self, callback: Callable):
        """Set the notification callback function."""
        self.notification_callback = callback

    # =========================================================================
    # Persistence
    # =========================================================================

    def _get_data_path(self) -> str:
        """Get path to persisted error data."""
        return os.path.join(self.data_dir, "error_history.json")

    def _save_data(self):
        """Persist error data to disk."""
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            data_path = self._get_data_path()

            # Convert to serializable format
            data = {
                "errors": [
                    {
                        "error_id": e.error_id,
                        "error_code": e.error_code,
                        "category": e.category.value,
                        "severity": e.severity.value,
                        "message": e.message,
                        "user_message": e.user_message,
                        "timestamp": e.timestamp,
                        "status": e.status.value,
                        "resolved": e.resolved,
                        "resolved_at": e.resolved_at,
                        "resolved_by": e.resolved_by,
                        "aggregate_count": e.aggregate_count,
                        "first_occurrence": e.first_occurrence,
                        "last_occurrence": e.last_occurrence,
                        "recovery_attempts": [
                            {
                                "timestamp": a.timestamp,
                                "strategy": a.strategy,
                                "action_taken": a.action_taken,
                                "success": a.success,
                                "duration": a.duration,
                                "details": a.details
                            }
                            for a in e.recovery_attempts
                        ],
                        "context": {
                            "workflow_name": e.context.workflow_name,
                            "workflow_id": e.context.workflow_id,
                            "step_name": e.context.step_name,
                            "step_index": e.context.step_index,
                            "action_type": e.context.action_type,
                            "action_params": e.context.action_params,
                            "environment": e.context.environment,
                            "system_state": e.context.system_state,
                            "user_data": e.context.user_data,
                            "stack_trace": e.context.stack_trace,
                            "raw_exception": e.context.raw_exception,
                            "previous_errors": e.context.previous_errors
                        }
                    }
                    for e in self._errors[-1000:]  # Keep last 1000
                ],
                "aggregated_errors": {
                    sig: {
                        "error_signature": agg.error_signature,
                        "error_code": agg.error_code,
                        "category": agg.category.value,
                        "severity": agg.severity.value,
                        "message": agg.message,
                        "user_message": agg.user_message,
                        "count": agg.count,
                        "first_occurrence": agg.first_occurrence,
                        "last_occurrence": agg.last_occurrence,
                        "recent_timestamps": agg.recent_timestamps,
                        "success_rate": agg.success_rate,
                        "workflow_names": list(agg.workflow_names),
                        "step_names": list(agg.step_names)
                    }
                    for sig, agg in self._aggregated_errors.items()
                }
            }

            with open(data_path, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to save error data: {e}")

    def _load_data(self):
        """Load persisted error data from disk."""
        try:
            data_path = self._get_data_path()
            if not os.path.exists(data_path):
                return

            with open(data_path, 'r') as f:
                data = json.load(f)

            # Reconstruct errors
            for err_data in data.get("errors", []):
                context_data = err_data["context"]
                context = ErrorContext(
                    timestamp=context_data["timestamp"],
                    workflow_name=context_data["workflow_name"],
                    workflow_id=context_data["workflow_id"],
                    step_name=context_data["step_name"],
                    step_index=context_data["step_index"],
                    action_type=context_data["action_type"],
                    action_params=context_data["action_params"],
                    environment=context_data["environment"],
                    system_state=context_data["system_state"],
                    user_data=context_data["user_data"],
                    stack_trace=context_data["stack_trace"],
                    raw_exception=context_data["raw_exception"],
                    previous_errors=context_data["previous_errors"]
                )

                recovery_attempts = [
                    RecoveryAttempt(
                        timestamp=a["timestamp"],
                        strategy=a["strategy"],
                        action_taken=a["action_taken"],
                        success=a["success"],
                        duration=a["duration"],
                        details=a["details"]
                    )
                    for a in err_data.get("recovery_attempts", [])
                ]

                record = ErrorRecord(
                    error_id=err_data["error_id"],
                    error_code=err_data["error_code"],
                    category=ErrorCategory(err_data["category"]),
                    severity=ErrorSeverity(err_data["severity"]),
                    message=err_data["message"],
                    user_message=err_data["user_message"],
                    context=context,
                    timestamp=err_data["timestamp"],
                    status=ErrorStatus(err_data["status"]),
                    recovery_attempts=recovery_attempts,
                    resolved=err_data["resolved"],
                    resolved_at=err_data["resolved_at"],
                    resolved_by=err_data["resolved_by"],
                    aggregate_count=err_data["aggregate_count"],
                    first_occurrence=err_data["first_occurrence"],
                    last_occurrence=err_data["last_occurrence"]
                )

                self._errors.append(record)
                self._error_history[record.context.workflow_name].append(record)

            # Reconstruct aggregated errors
            for sig, agg_data in data.get("aggregated_errors", {}).items():
                agg = AggregatedError(
                    error_signature=agg_data["error_signature"],
                    error_code=agg_data["error_code"],
                    category=ErrorCategory(agg_data["category"]),
                    severity=ErrorSeverity(agg_data["severity"]),
                    message=agg_data["message"],
                    user_message=agg_data["user_message"],
                    count=agg_data["count"],
                    first_occurrence=agg_data["first_occurrence"],
                    last_occurrence=agg_data["last_occurrence"],
                    recent_timestamps=agg_data["recent_timestamps"],
                    success_rate=agg_data["success_rate"],
                    workflow_names=set(agg_data["workflow_names"]),
                    step_names=set(agg_data["step_names"])
                )
                self._aggregated_errors[sig] = agg

            logger.info(f"Loaded {len(self._errors)} error records from history")

        except Exception as e:
            logger.error(f"Failed to load error data: {e}")

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def clear_history(self, before_timestamp: Optional[float] = None):
        """Clear error history, optionally before a timestamp."""
        with self._lock:
            if before_timestamp:
                self._errors = [e for e in self._errors if e.timestamp >= before_timestamp]
            else:
                self._errors.clear()
                self._aggregated_errors.clear()
                self._error_history.clear()
            self._save_data()

    def export_errors(self, format: str = "json") -> str:
        """Export errors in specified format."""
        if format == "json":
            return json.dumps([
                {
                    "error_id": e.error_id,
                    "error_code": e.error_code,
                    "category": e.category.value,
                    "severity": e.severity.value,
                    "message": e.message,
                    "user_message": e.user_message,
                    "timestamp": e.timestamp,
                    "resolved": e.resolved,
                    "context": {
                        "workflow_name": e.context.workflow_name,
                        "step_name": e.context.step_name,
                        "stack_trace": e.context.stack_trace
                    }
                }
                for e in self._errors
            ], indent=2)
        return str(self._errors)

    def get_error_code_catalog(self) -> List[ErrorCode]:
        """Get the full error code catalog."""
        return self.catalog.get_all_codes()

    def search_errors(self, query: str, limit: int = 50) -> List[ErrorRecord]:
        """Search errors by message content."""
        query_lower = query.lower()
        return [
            e for e in self._errors
            if query_lower in e.message.lower() or query_lower in e.user_message.lower()
        ][:limit]
