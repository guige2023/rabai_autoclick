"""
Workflow Security Module - Security hardening for RabAI AutoClick
Provides sandbox execution, permission system, audit logging, workflow signing,
secure variable storage, IP allowlist, rate limiting, content filtering,
and intrusion detection.
"""
import hashlib
import hmac
import json
import os
import subprocess
import time
import uuid
import re
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from functools import wraps
import logging

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key
    from cryptography.hazmat.backends import default_backend
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


# ============================================================================
# Enums and Data Classes
# ============================================================================

class SecurityLevel(Enum):
    """Security levels for workflow execution."""
    NONE = auto()
    BASIC = auto()
    STANDARD = auto()
    HIGH = auto()
    ULTRA = auto()  # Ultra-restricted mode for untrusted workflows


class Permission(Enum):
    """Granular permissions for workflow actions."""
    FILE_READ = auto()
    FILE_WRITE = auto()
    FILE_DELETE = auto()
    NETWORK_REQUEST = auto()
    NETWORK_CONNECT = auto()
    EXECUTE_COMMAND = auto()
    SCREEN_CAPTURE = auto()
    KEYBOARD_INPUT = auto()
    MOUSE_INPUT = auto()
    CLIPBOARD_READ = auto()
    CLIPBOARD_WRITE = auto()
    ENV_READ = auto()
    ENV_WRITE = auto()
    REGISTRY_READ = auto()
    REGISTRY_WRITE = auto()


class AuditEventType(Enum):
    """Types of security-auditable events."""
    WORKFLOW_START = auto()
    WORKFLOW_COMPLETE = auto()
    WORKFLOW_FAIL = auto()
    WORKFLOW_SIGNATURE_VERIFY = auto()
    PERMISSION_DENIED = auto()
    RATE_LIMIT_EXCEEDED = auto()
    IP_BLOCKED = auto()
    INTRUSION_DETECTED = auto()
    SUSPICIOUS_PATTERN = auto()
    SECURE_VAR_ACCESS = auto()
    SANDBOX_VIOLATION = auto()
    POLICY_CHANGE = auto()


class IntrusionPattern(Enum):
    """Known intrusion patterns to detect."""
    RAPID_KEYSTROKES = auto()
    UNEXPECTED_SCREEN_CAPTURE = auto()
    FILE_ENUMERATION = auto()
    PRIVILEGE_ESCALATION = auto()
    DATA_EXFILTRATION = auto()
    TIMING_ANOMALY = auto()


@dataclass
class SecurityPolicy:
    """Configurable security policy for a workflow."""
    name: str = "default"
    security_level: SecurityLevel = SecurityLevel.STANDARD
    allowed_permissions: Set[Permission] = field(default_factory=set)
    denied_permissions: Set[Permission] = field(default_factory=set)
    allowed_ips: Set[str] = field(default_factory=set)  # CIDR or exact IP
    denied_ips: Set[str] = field(default_factory=set)
    max_steps_per_minute: int = 60
    max_concurrent_workflows: int = 5
    max_execution_time_seconds: int = 3600
    require_signature: bool = False
    require_encrypted_vars: bool = False
    enable_sandbox: bool = True
    enable_intrusion_detection: bool = True
    allowed_domains: Set[str] = field(default_factory=set)
    content_filter_patterns: List[str] = field(default_factory=list)

    def has_permission(self, permission: Permission) -> bool:
        if permission in self.denied_permissions:
            return False
        if permission in self.allowed_permissions:
            return True
        # Default permissions based on security level
        if self.security_level == SecurityLevel.NONE:
            return True
        elif self.security_level == SecurityLevel.BASIC:
            return permission in {
                Permission.FILE_READ, Permission.NETWORK_REQUEST,
                Permission.SCREEN_CAPTURE, Permission.CLIPBOARD_READ
            }
        elif self.security_level == SecurityLevel.STANDARD:
            return permission in {
                Permission.FILE_READ, Permission.FILE_WRITE,
                Permission.NETWORK_REQUEST, Permission.SCREEN_CAPTURE,
                Permission.KEYBOARD_INPUT, Permission.MOUSE_INPUT,
                Permission.CLIPBOARD_READ, Permission.CLIPBOARD_WRITE
            }
        elif self.security_level == SecurityLevel.HIGH:
            return permission in {
                Permission.FILE_READ, Permission.NETWORK_REQUEST,
                Permission.SCREEN_CAPTURE
            }
        elif self.security_level == SecurityLevel.ULTRA:
            return permission == Permission.SCREEN_CAPTURE
        return False


@dataclass
class AuditEvent:
    """Security audit log entry."""
    event_id: str
    timestamp: datetime
    event_type: AuditEventType
    workflow_id: Optional[str]
    user_id: Optional[str]
    details: Dict[str, Any]
    ip_address: Optional[str] = None
    blocked: bool = False
    severity: str = "INFO"


@dataclass
class WorkflowSignature:
    """Digital signature for workflow verification."""
    workflow_id: str
    signature: bytes
    public_key_fingerprint: str
    timestamp: datetime
    version: str = "1.0"


@dataclass
class SecureVariable:
    """Encrypted variable storage."""
    name: str
    encrypted_value: bytes
    key_fingerprint: str
    created_at: datetime
    last_accessed: Optional[datetime] = None
    access_count: int = 0


# ============================================================================
# Security Utilities
# ============================================================================

class SecurityUtils:
    """Utility functions for security operations."""

    @staticmethod
    def compute_hash(data: str, algorithm: str = "sha256") -> str:
        """Compute hash of data."""
        h = hashlib.new(algorithm)
        h.update(data.encode('utf-8'))
        return h.hexdigest()

    @staticmethod
    def compute_hmac(data: str, key: bytes) -> str:
        """Compute HMAC of data."""
        return hmac.new(key, data.encode('utf-8'), hashlib.sha256).hexdigest()

    @staticmethod
    def is_ip_in_cidr(ip: str, cidr: str) -> bool:
        """Check if IP is in CIDR range."""
        import ipaddress
        try:
            return ipaddress.ip_address(ip) in ipaddress.ip_network(cidr, strict=False)
        except ValueError:
            return ip == cidr

    @staticmethod
    def is_domain_allowed(domain: str, allowed_domains: Set[str]) -> bool:
        """Check if domain matches allowed list with wildcard support."""
        if "*" in allowed_domains or domain in allowed_domains:
            return True
        for allowed in allowed_domains:
            if allowed.startswith("*."):
                base_domain = allowed[2:]
                if domain.endswith(base_domain) or domain == base_domain[1:]:
                    return True
        return False


# ============================================================================
# Encryption Manager
# ============================================================================

class EncryptionManager:
    """Manages encryption for secure variable storage."""

    def __init__(self, master_key: Optional[bytes] = None):
        if not CRYPTO_AVAILABLE:
            raise ImportError("cryptography library not available")
        self._master_key = master_key or Fernet.generate_key()
        self._fernet = Fernet(self._master_key)
        self._rsa_private = None
        self._rsa_public = None

    @property
    def public_key_pem(self) -> bytes:
        """Get RSA public key in PEM format."""
        if self._rsa_public is None:
            self._generate_rsa_keypair()
        return self._rsa_public.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

    def _generate_rsa_keypair(self):
        """Generate RSA key pair for workflow signing."""
        from cryptography.hazmat.primitives import serialization
        self._rsa_private = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        self._rsa_public = self._rsa_private.public_key()

    def get_key_fingerprint(self) -> str:
        """Get fingerprint of current RSA key."""
        if self._rsa_public is None:
            self._generate_rsa_keypair()
        from cryptography.hazmat.primitives import serialization
        pub_bytes = self._rsa_public.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return hashlib.sha256(pub_bytes).hexdigest()[:16]

    def encrypt(self, data: str) -> bytes:
        """Encrypt data using Fernet symmetric encryption."""
        return self._fernet.encrypt(data.encode('utf-8'))

    def decrypt(self, encrypted_data: bytes) -> str:
        """Decrypt data using Fernet symmetric encryption."""
        return self._fernet.decrypt(encrypted_data).decode('utf-8')

    def sign(self, data: str) -> bytes:
        """Sign data using RSA private key."""
        if self._rsa_private is None:
            self._generate_rsa_keypair()
        return self._rsa_private.sign(
            data.encode('utf-8'),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

    def verify_signature(self, data: str, signature: bytes) -> bool:
        """Verify RSA signature."""
        if self._rsa_public is None:
            return False
        try:
            self._rsa_public.verify(
                signature,
                data.encode('utf-8'),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False


# ============================================================================
# Audit Logger
# ============================================================================

class AuditLogger:
    """Logs all security-relevant events."""

    def __init__(self, log_dir: str = "logs"):
        self._log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._log_file = os.path.join(log_dir, f"security_audit_{datetime.now().strftime('%Y%m%d')}.json")
        self._logger = logging.getLogger("WorkflowSecurity")
        self._logger.setLevel(logging.INFO)
        if not self._logger.handlers:
            handler = logging.FileHandler(os.path.join(log_dir, "security.log"))
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self._logger.addHandler(handler)

    def log_event(self, event: AuditEvent):
        """Log a security audit event."""
        event_dict = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "event_type": event.event_type.name,
            "workflow_id": event.workflow_id,
            "user_id": event.user_id,
            "details": event.details,
            "ip_address": event.ip_address,
            "blocked": event.blocked,
            "severity": event.severity
        }
        try:
            with open(self._log_file, 'a') as f:
                f.write(json.dumps(event_dict) + "\n")
        except Exception as e:
            self._logger.error(f"Failed to write audit log: {e}")
        self._logger.info(f"Security event: {event.event_type.name} - {event.details}")

    def create_event(
        self,
        event_type: AuditEventType,
        details: Dict[str, Any],
        workflow_id: Optional[str] = None,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        blocked: bool = False,
        severity: str = "INFO"
    ) -> AuditEvent:
        """Create and log a new audit event."""
        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            event_type=event_type,
            workflow_id=workflow_id,
            user_id=user_id,
            details=details,
            ip_address=ip_address,
            blocked=blocked,
            severity=severity
        )
        self.log_event(event)
        return event


# ============================================================================
# Rate Limiter
# ============================================================================

class RateLimiter:
    """Prevents workflow DoS through rate limiting."""

    def __init__(self):
        self._step_timestamps: Dict[str, List[datetime]] = {}
        self._workflow_count: Dict[str, int] = {}
        self._lock = asyncio.Lock() if AIOHTTP_AVAILABLE else None

    def check_step_rate(
        self,
        workflow_id: str,
        max_steps_per_minute: int
    ) -> Tuple[bool, int]:
        """
        Check if workflow is within step rate limit.
        Returns (is_allowed, current_rate).
        """
        now = datetime.now()
        cutoff = now - timedelta(minutes=1)

        if workflow_id not in self._step_timestamps:
            self._step_timestamps[workflow_id] = []

        # Remove old timestamps
        self._step_timestamps[workflow_id] = [
            ts for ts in self._step_timestamps[workflow_id] if ts > cutoff
        ]

        current_rate = len(self._step_timestamps[workflow_id])
        if current_rate >= max_steps_per_minute:
            return False, current_rate

        self._step_timestamps[workflow_id].append(now)
        return True, current_rate + 1

    def check_concurrent_limit(
        self,
        workflow_id: str,
        max_concurrent: int
    ) -> Tuple[bool, int]:
        """
        Check if workflow is within concurrent execution limit.
        Returns (is_allowed, current_count).
        """
        current = self._workflow_count.get(workflow_id, 0)
        if current >= max_concurrent:
            return False, current
        self._workflow_count[workflow_id] = current + 1
        return True, current + 1

    def release_concurrent(self, workflow_id: str):
        """Release a concurrent execution slot."""
        if workflow_id in self._workflow_count:
            self._workflow_count[workflow_id] = max(0, self._workflow_count[workflow_id] - 1)

    def get_current_rate(self, workflow_id: str) -> int:
        """Get current step rate for workflow."""
        if workflow_id not in self._step_timestamps:
            return 0
        cutoff = datetime.now() - timedelta(minutes=1)
        return len([ts for ts in self._step_timestamps[workflow_id] if ts > cutoff])


# ============================================================================
# IP Allowlist Checker
# ============================================================================

class IPAllowlistChecker:
    """Validates network requests against IP/domain allowlist."""

    def __init__(self, policy: SecurityPolicy):
        self._policy = policy

    def is_ip_allowed(self, ip: str) -> bool:
        """Check if IP address is allowed."""
        if ip in self._policy.denied_ips:
            return False
        if self._policy.allowed_ips:
            for cidr in self._policy.allowed_ips:
                if SecurityUtils.is_ip_in_cidr(ip, cidr):
                    return True
            return False
        return True

    def is_domain_allowed(self, domain: str) -> bool:
        """Check if domain is allowed."""
        if self._policy.allowed_domains:
            return SecurityUtils.is_domain_allowed(
                domain.lower(),
                self._policy.allowed_domains
            )
        return True

    def check_request(
        self,
        destination: str,
        is_ip: bool = False
    ) -> Tuple[bool, str]:
        """
        Check if network request is allowed.
        Returns (is_allowed, reason).
        """
        if is_ip:
            if not self.is_ip_allowed(destination):
                return False, f"IP {destination} is not in allowlist"
        else:
            if not self.is_domain_allowed(destination):
                return False, f"Domain {destination} is not in allowlist"
        return True, "allowed"


# ============================================================================
# Content Filter
# ============================================================================

class ContentFilter:
    """Scans workflow code for suspicious patterns."""

    SUSPICIOUS_PATTERNS = [
        (r'(os\.system|subprocess\.run|eval|exec)\s*\(', 'Code execution'),
        (r'__import__|import\s+os|import\s+subprocess', 'Dynamic import'),
        (r'base64\.b64decode|codecs\.decode.*base64', 'Obfuscated code'),
        (r'socket\.socket|urllib\.request', 'Network sockets'),
        (r'ctypes\.windll|ctypes\.cdll', 'Native code access'),
        (r'os\.chmod|os\.chown|os\.setuid', 'Privilege modification'),
        (r'sys\.exit|raise\s+SystemExit', 'Forced exit'),
        (r'requests\.post.*password|auth.*=', 'Credential transmission'),
        (r'keyboard| mouse| pyautogui', 'Input simulation'),
        (r'win32api|win32gui|pywinauto', 'Windows API'),
    ]

    def __init__(self, custom_patterns: Optional[List[str]] = None):
        self._patterns = []
        for pattern, desc in self.SUSPICIOUS_PATTERNS:
            self._patterns.append((re.compile(pattern, re.IGNORECASE), desc))
        if custom_patterns:
            for pattern in custom_patterns:
                try:
                    self._patterns.append((re.compile(pattern, re.IGNORECASE), "Custom"))
                except re.error:
                    pass

    def scan(self, content: str) -> List[Tuple[str, str, int]]:
        """
        Scan content for suspicious patterns.
        Returns list of (pattern, description, line_number).
        """
        findings = []
        lines = content.split('\n')
        for pattern, desc in self._patterns:
            for i, line in enumerate(lines, 1):
                if pattern.search(line):
                    findings.append((pattern.pattern, desc, i))
        return findings

    def scan_workflow(self, workflow_data: Any) -> List[Tuple[str, str, str]]:
        """
        Scan workflow data structure for threats.
        Returns list of (location, description, severity).
        """
        findings = []
        serialized = json.dumps(workflow_data, default=str)
        for pattern, desc in self._patterns:
            if pattern.search(serialized):
                findings.append((pattern.pattern[:50], desc, "HIGH"))
        return findings


# ============================================================================
# Intrusion Detection System
# ============================================================================

class IntrusionDetectionSystem:
    """Detects unusual workflow behavior patterns."""

    def __init__(self, policy: SecurityPolicy):
        self._policy = policy
        self._behavior_history: Dict[str, List[Dict]] = {}
        self._known_benign_sequences: Set[Tuple] = set()

    def record_behavior(
        self,
        workflow_id: str,
        action: str,
        metadata: Optional[Dict] = None
    ):
        """Record workflow behavior for analysis."""
        if workflow_id not in self._behavior_history:
            self._behavior_history[workflow_id] = []
        self._behavior_history[workflow_id].append({
            "action": action,
            "timestamp": datetime.now(),
            "metadata": metadata or {}
        })

    def detect_anomalies(self, workflow_id: str) -> List[Tuple[str, float]]:
        """
        Detect anomalous behavior patterns.
        Returns list of (anomaly_type, confidence).
        """
        anomalies = []
        if workflow_id not in self._behavior_history:
            return anomalies

        history = self._behavior_history[workflow_id]
        if len(history) < 3:
            return anomalies

        # Check for rapid keystrokes
        recent = [h for h in history if datetime.now() - h["timestamp"] < timedelta(seconds=1)]
        if len(recent) > 10:
            anomalies.append(("RAPID_INPUT_DETECTED", 0.9))

        # Check for rapid screen captures
        captures = [h for h in history if h["action"] == "screen_capture"]
        if len(captures) > 30:
            anomalies.append(("EXCESSIVE_SCREEN_CAPTURES", 0.8))

        # Check for file enumeration patterns
        file_ops = [h for h in history if h["action"].startswith("file_")]
        if len(set(f["metadata"].get("path", "")[:20] for f in file_ops)) > 50:
            anomalies.append(("FILE_ENUMERATION", 0.85))

        # Check for timing anomalies
        timestamps = [h["timestamp"] for h in history[-10:]]
        intervals = [(timestamps[i+1] - timestamps[i]).total_seconds()
                      for i in range(len(timestamps)-1)]
        if intervals and (max(intervals) / (min(intervals) + 0.001)) > 100:
            anomalies.append(("TIMING_ANOMALY", 0.75))

        # Check for data exfiltration patterns
        clipboard_writes = len([h for h in history if h["action"] == "clipboard_write"])
        network_requests = len([h for h in history if h["action"] == "network_request"])
        if clipboard_writes > 20 and network_requests > 10:
            anomalies.append(("POTENTIAL_DATA_EXFILTRATION", 0.7))

        return anomalies

    def is_behavior_suspicious(
        self,
        workflow_id: str,
        threshold: float = 0.7
    ) -> bool:
        """Check if workflow behavior is suspicious."""
        anomalies = self.detect_anomalies(workflow_id)
        return any(conf >= threshold for _, conf in anomalies)


# ============================================================================
# Sandbox Executor
# ============================================================================

class SandboxExecutor:
    """Executes workflows in restricted sandbox environment."""

    def __init__(
        self,
        policy: SecurityPolicy,
        audit_logger: AuditLogger,
        timeout_seconds: int = 300
    ):
        self._policy = policy
        self._audit_logger = audit_logger
        self._timeout = timeout_seconds
        self._execution_count = 0

    def execute_in_sandbox(
        self,
        workflow_id: str,
        code: str,
        context: Optional[Dict] = None
    ) -> Tuple[bool, Any, Optional[str]]:
        """
        Execute code in sandboxed environment.
        Returns (success, result, error_message).
        """
        if not self._policy.enable_sandbox:
            return self._execute_direct(code, context)

        self._execution_count += 1
        start_time = time.time()

        # Create restricted globals
        restricted_globals = {
            "__name__": "__sandbox__",
            "__builtins__": self._get_restricted_builtins(),
            "__file__": f"<sandbox:{workflow_id}>",
            "_workflow_id": workflow_id,
            "_policy": self._policy,
            "_audit_logger": self._audit_logger,
        }

        try:
            # Compile with restrictions
            compiled = compile(code, f"<sandbox:{workflow_id}>", 'exec')

            # Execute in time limit
            local_scope = {}
            exec(compiled, restricted_globals, local_scope)

            elapsed = time.time() - start_time
            if elapsed > self._policy.max_execution_time_seconds:
                raise TimeoutError(f"Execution exceeded {self._policy.max_execution_time_seconds}s")

            return True, local_scope.get("result"), None

        except Exception as e:
            error_msg = f"Sandbox execution failed: {str(e)}"
            self._audit_logger.create_event(
                AuditEventType.SANDBOX_VIOLATION,
                {"workflow_id": workflow_id, "error": error_msg},
                workflow_id=workflow_id,
                severity="WARNING"
            )
            return False, None, error_msg

    def _get_restricted_builtins(self) -> Dict:
        """Get restricted builtin functions."""
        safe_builtins = {
            "print": print,
            "len": len,
            "range": range,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict,
            "tuple": tuple,
            "set": set,
            "min": min,
            "max": max,
            "sum": sum,
            "sorted": sorted,
            "enumerate": enumerate,
            "zip": zip,
            "map": map,
            "filter": filter,
            "open": self._restricted_open,
            "input": self._restricted_input,
        }
        return safe_builtins

    def _restricted_open(self, filename: str, mode: str = 'r', *args, **kwargs):
        """Restricted file open function."""
        if 'w' in mode or 'a' in mode or 'x' in mode:
            if not self._policy.has_permission(Permission.FILE_WRITE):
                raise PermissionError("FILE_WRITE not permitted")
        elif 'r' in mode:
            if not self._policy.has_permission(Permission.FILE_READ):
                raise PermissionError("FILE_READ not permitted")
        return open(filename, mode, *args, **kwargs)

    def _restricted_input(self, prompt: str = "") -> str:
        """Restricted input function."""
        if not self._policy.has_permission(Permission.KEYBOARD_INPUT):
            raise PermissionError("KEYBOARD_INPUT not permitted")
        return input(prompt)

    def _execute_direct(self, code: str, context: Optional[Dict]) -> Tuple[bool, Any, Optional[str]]:
        """Direct execution without sandbox (use with caution)."""
        try:
            local_scope = context or {}
            exec(code, {"__builtins__": __builtins__}, local_scope)
            return True, local_scope.get("result"), None
        except Exception as e:
            return False, None, str(e)


# ============================================================================
# Workflow Signature Manager
# ============================================================================

class WorkflowSignatureManager:
    """Signs and verifies workflow digital signatures."""

    def __init__(self, encryption_manager: EncryptionManager):
        self._encryption = encryption_manager
        self._signatures: Dict[str, WorkflowSignature] = {}
        self._trusted_keys: Set[str] = set()

    def sign_workflow(self, workflow_id: str, workflow_data: Any) -> WorkflowSignature:
        """Sign a workflow with digital certificate."""
        # Serialize workflow data deterministically
        serialized = json.dumps(workflow_data, sort_keys=True, default=str)
        signature_bytes = self._encryption.sign(serialized)

        sig = WorkflowSignature(
            workflow_id=workflow_id,
            signature=signature_bytes,
            public_key_fingerprint=self._encryption.get_key_fingerprint(),
            timestamp=datetime.now()
        )
        self._signatures[workflow_id] = sig
        return sig

    def verify_workflow(
        self,
        workflow_id: str,
        workflow_data: Any,
        signature: WorkflowSignature
    ) -> Tuple[bool, str]:
        """
        Verify workflow signature before execution.
        Returns (is_valid, reason).
        """
        # Check if signing key is trusted
        if signature.public_key_fingerprint not in self._trusted_keys:
            # Verify using our own key
            serialized = json.dumps(workflow_data, sort_keys=True, default=str)
            if not self._encryption.verify_signature(serialized, signature.signature):
                return False, "Invalid signature"
            if signature.public_key_fingerprint != self._encryption.get_key_fingerprint():
                return False, "Unknown signing key"
        return True, "valid"

    def trust_key(self, fingerprint: str):
        """Add a public key fingerprint to trusted list."""
        self._trusted_keys.add(fingerprint)

    def get_signature(self, workflow_id: str) -> Optional[WorkflowSignature]:
        """Get stored signature for workflow."""
        return self._signatures.get(workflow_id)


# ============================================================================
# Secure Variable Store
# ============================================================================

class SecureVariableStore:
    """Encrypts and securely stores sensitive variables."""

    def __init__(self, encryption_manager: EncryptionManager):
        self._encryption = encryption_manager
        self._variables: Dict[str, SecureVariable] = {}
        self._audit_logger: Optional[AuditLogger] = None

    def set_audit_logger(self, logger: AuditLogger):
        """Set audit logger for access tracking."""
        self._audit_logger = logger

    def store(
        self,
        name: str,
        value: str,
        encrypt: bool = True
    ) -> SecureVariable:
        """Store a variable with optional encryption."""
        if encrypt and CRYPTO_AVAILABLE:
            encrypted = self._encryption.encrypt(value)
        else:
            encrypted = value.encode('utf-8')

        var = SecureVariable(
            name=name,
            encrypted_value=encrypted,
            key_fingerprint=self._encryption.get_key_fingerprint(),
            created_at=datetime.now()
        )
        self._variables[name] = var
        return var

    def retrieve(self, name: str, decrypt: bool = True) -> Optional[str]:
        """Retrieve and decrypt a variable."""
        var = self._variables.get(name)
        if not var:
            return None

        var.access_count += 1
        var.last_accessed = datetime.now()

        if self._audit_logger:
            self._audit_logger.create_event(
                AuditEventType.SECURE_VAR_ACCESS,
                {"variable_name": name, "access_count": var.access_count},
                severity="DEBUG"
            )

        if decrypt and CRYPTO_AVAILABLE:
            return self._encryption.decrypt(var.encrypted_value)
        return var.encrypted_value.decode('utf-8')

    def delete(self, name: str) -> bool:
        """Securely delete a variable."""
        if name in self._variables:
            del self._variables[name]
            return True
        return False

    def list_variables(self) -> List[str]:
        """List all stored variable names (not values)."""
        return list(self._variables.keys())


# ============================================================================
# Main Security Module
# ============================================================================

class WorkflowSecurityModule:
    """
    Main security hardening module for workflow execution.
    Integrates all security features: sandbox, permissions, audit,
    signing, encryption, rate limiting, and intrusion detection.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        log_dir: str = "logs"
    ):
        self._config = config or {}
        self._log_dir = log_dir

        # Initialize core components
        self._audit_logger = AuditLogger(log_dir)
        self._encryption = EncryptionManager()
        self._rate_limiter = RateLimiter()
        self._signature_manager = WorkflowSignatureManager(self._encryption)
        self._variable_store = SecureVariableStore(self._encryption)
        self._variable_store.set_audit_logger(self._audit_logger)

        # Default security policy
        self._default_policy = SecurityPolicy()

        # Per-workflow policies
        self._policies: Dict[str, SecurityPolicy] = {}

        # Initialize sandbox executor
        self._sandbox = SandboxExecutor(
            self._default_policy,
            self._audit_logger,
            self._default_policy.max_execution_time_seconds
        )

        # Initialize content filter
        self._content_filter = ContentFilter()

        # Initialize intrusion detection
        self._intrusion_detector = IntrusionDetectionSystem(self._default_policy)

        # Execution tracking
        self._active_workflows: Set[str] = set()

    def create_policy(
        self,
        name: str,
        security_level: SecurityLevel = SecurityLevel.STANDARD,
        **kwargs
    ) -> SecurityPolicy:
        """Create a new security policy."""
        policy = SecurityPolicy(
            name=name,
            security_level=security_level,
            **kwargs
        )
        self._policies[name] = policy
        return policy

    def get_policy(self, name: str) -> SecurityPolicy:
        """Get security policy by name."""
        return self._policies.get(name, self._default_policy)

    def set_policy(self, workflow_id: str, policy: SecurityPolicy):
        """Assign a policy to a workflow."""
        self._policies[workflow_id] = policy

    def _get_workflow_policy(self, workflow_id: str) -> SecurityPolicy:
        """Get effective policy for workflow."""
        return self._policies.get(workflow_id, self._default_policy)

    def check_permission(
        self,
        workflow_id: str,
        permission: Permission
    ) -> Tuple[bool, str]:
        """Check if workflow has permission for action."""
        policy = self._get_workflow_policy(workflow_id)
        if not policy.has_permission(permission):
            self._audit_logger.create_event(
                AuditEventType.PERMISSION_DENIED,
                {
                    "workflow_id": workflow_id,
                    "permission": permission.name,
                    "security_level": policy.security_level.name
                },
                workflow_id=workflow_id,
                blocked=True,
                severity="WARNING"
            )
            return False, f"Permission {permission.name} denied"
        return True, "allowed"

    def pre_execution_check(
        self,
        workflow_id: str,
        workflow_data: Any,
        signature: Optional[WorkflowSignature] = None
    ) -> Tuple[bool, str, List[str]]:
        """
        Perform all pre-execution security checks.
        Returns (passed, reason, warnings).
        """
        policy = self._get_workflow_policy(workflow_id)
        warnings = []

        # Check signature requirement
        if policy.require_signature:
            if signature is None:
                return False, "Workflow signature required", warnings
            valid, reason = self._signature_manager.verify_workflow(
                workflow_id, workflow_data, signature
            )
            if not valid:
                return False, f"Signature verification failed: {reason}", warnings
            self._audit_logger.create_event(
                AuditEventType.WORKFLOW_SIGNATURE_VERIFY,
                {"workflow_id": workflow_id, "result": "valid"},
                workflow_id=workflow_id
            )

        # Content filtering
        findings = self._content_filter.scan_workflow(workflow_data)
        for pattern, desc, severity in findings:
            warnings.append(f"Suspicious pattern: {desc}")
            self._audit_logger.create_event(
                AuditEventType.SUSPICIOUS_PATTERN,
                {"workflow_id": workflow_id, "pattern": pattern, "description": desc},
                workflow_id=workflow_id,
                severity=severity
            )

        # Rate limiting
        allowed, rate = self._rate_limiter.check_step_rate(
            workflow_id, policy.max_steps_per_minute
        )
        if not allowed:
            return False, f"Rate limit exceeded ({rate} steps/min)", warnings

        # Intrusion detection
        if policy.enable_intrusion_detection:
            if self._intrusion_detector.is_behavior_suspicious(workflow_id):
                self._audit_logger.create_event(
                    AuditEventType.INTRUSION_DETECTED,
                    {"workflow_id": workflow_id},
                    workflow_id=workflow_id,
                    severity="HIGH"
                )
                return False, "Suspicious behavior detected", warnings

        return True, "all checks passed", warnings

    def execute_workflow(
        self,
        workflow_id: str,
        workflow_data: Any,
        signature: Optional[WorkflowSignature] = None
    ) -> Tuple[bool, Any, Optional[str]]:
        """
        Execute workflow with full security enforcement.
        Returns (success, result, error).
        """
        # Pre-execution checks
        passed, reason, warnings = self.pre_execution_check(
            workflow_id, workflow_data, signature
        )
        if not passed:
            self._audit_logger.create_event(
                AuditEventType.WORKFLOW_FAIL,
                {"workflow_id": workflow_id, "reason": reason},
                workflow_id=workflow_id,
                severity="ERROR"
            )
            return False, None, reason

        policy = self._get_workflow_policy(workflow_id)
        self._active_workflows.add(workflow_id)

        # Update sandbox with policy
        sandbox = SandboxExecutor(
            policy,
            self._audit_logger,
            policy.max_execution_time_seconds
        )

        try:
            # Concurrent limit check
            allowed, count = self._rate_limiter.check_concurrent_limit(
                workflow_id, policy.max_concurrent_workflows
            )
            if not allowed:
                return False, None, f"Concurrent limit reached ({count})"

            self._audit_logger.create_event(
                AuditEventType.WORKFLOW_START,
                {"workflow_id": workflow_id, "policy": policy.name},
                workflow_id=workflow_id
            )

            # Serialize and execute workflow
            workflow_code = json.dumps(workflow_data.get("code", ""))
            success, result, error = sandbox.execute_in_sandbox(
                workflow_id, workflow_code, workflow_data.get("context")
            )

            if success:
                self._audit_logger.create_event(
                    AuditEventType.WORKFLOW_COMPLETE,
                    {"workflow_id": workflow_id, "result_type": type(result).__name__},
                    workflow_id=workflow_id
                )
            else:
                self._audit_logger.create_event(
                    AuditEventType.WORKFLOW_FAIL,
                    {"workflow_id": workflow_id, "error": error},
                    workflow_id=workflow_id,
                    severity="ERROR"
                )

            return success, result, error

        finally:
            self._active_workflows.discard(workflow_id)
            self._rate_limiter.release_concurrent(workflow_id)

    def check_network_request(
        self,
        workflow_id: str,
        destination: str,
        is_ip: bool = False
    ) -> Tuple[bool, str]:
        """Check if network request is allowed."""
        policy = self._get_workflow_policy(workflow_id)
        checker = IPAllowlistChecker(policy)
        allowed, reason = checker.check_request(destination, is_ip)

        if not allowed:
            self._audit_logger.create_event(
                AuditEventType.IP_BLOCKED,
                {"workflow_id": workflow_id, "destination": destination, "is_ip": is_ip},
                workflow_id=workflow_id,
                blocked=True,
                severity="WARNING"
            )
        return allowed, reason

    def sign_workflow(
        self,
        workflow_id: str,
        workflow_data: Any
    ) -> WorkflowSignature:
        """Sign a workflow for trusted execution."""
        sig = self._signature_manager.sign_workflow(workflow_id, workflow_data)
        self._audit_logger.create_event(
            AuditEventType.POLICY_CHANGE,
            {"workflow_id": workflow_id, "action": "signed"},
            workflow_id=workflow_id
        )
        return sig

    def store_secure_variable(
        self,
        name: str,
        value: str,
        encrypt: bool = True
    ) -> SecureVariable:
        """Store a secure variable."""
        return self._variable_store.store(name, value, encrypt)

    def get_secure_variable(self, name: str, decrypt: bool = True) -> Optional[str]:
        """Retrieve a secure variable."""
        return self._variable_store.retrieve(name, decrypt)

    def record_behavior(
        self,
        workflow_id: str,
        action: str,
        metadata: Optional[Dict] = None
    ):
        """Record behavior for intrusion detection."""
        policy = self._get_workflow_policy(workflow_id)
        self._intrusion_detector._policy = policy
        self._intrusion_detector.record_behavior(workflow_id, action, metadata)

    def get_security_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get current security status for workflow."""
        policy = self._get_workflow_policy(workflow_id)
        return {
            "workflow_id": workflow_id,
            "policy": policy.name,
            "security_level": policy.security_level.name,
            "active": workflow_id in self._active_workflows,
            "current_rate": self._rate_limiter.get_current_rate(workflow_id),
            "max_rate": policy.max_steps_per_minute,
            "signature_required": policy.require_signature,
            "sandbox_enabled": policy.enable_sandbox,
            "intrusion_detection": policy.enable_intrusion_detection,
        }

    def enable_secure_mode(self, workflow_id: str):
        """Enable ultra-restricted mode for untrusted workflows."""
        policy = SecurityPolicy(
            name="secure_mode",
            security_level=SecurityLevel.ULTRA,
            enable_sandbox=True,
            require_signature=True,
            enable_intrusion_detection=True,
            max_steps_per_minute=10,
            max_concurrent_workflows=1,
        )
        self.set_policy(workflow_id, policy)
        self._audit_logger.create_event(
            AuditEventType.POLICY_CHANGE,
            {"workflow_id": workflow_id, "mode": "secure"},
            workflow_id=workflow_id,
            severity="HIGH"
        )


def create_security_module(
    config: Optional[Dict[str, Any]] = None,
    log_dir: str = "logs"
) -> WorkflowSecurityModule:
    """Factory function to create security module."""
    return WorkflowSecurityModule(config, log_dir)


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "WorkflowSecurityModule",
    "create_security_module",
    "SecurityLevel",
    "Permission",
    "SecurityPolicy",
    "AuditEvent",
    "AuditEventType",
    "WorkflowSignature",
    "SecureVariable",
    "EncryptionManager",
    "AuditLogger",
    "RateLimiter",
    "IPAllowlistChecker",
    "ContentFilter",
    "IntrusionDetectionSystem",
    "SandboxExecutor",
    "WorkflowSignatureManager",
    "SecureVariableStore",
    "SecurityUtils",
    "IntrusionPattern",
]
