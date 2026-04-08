"""Secret Rotation Action Module.

Provides automated secret/credential rotation with support for
API keys, passwords, certificates, and symmetric/asymmetric keys.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class SecretType(Enum):
    """Secret type."""
    API_KEY = "api_key"
    PASSWORD = "password"
    CERTIFICATE = "certificate"
    SSH_KEY = "ssh_key"
    SYMMETRIC_KEY = "symmetric_key"
    JWT_SECRET = "jwt_secret"
    OAUTH_SECRET = "oauth_secret"


class RotationStatus(Enum):
    """Rotation status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class Secret:
    """Secret metadata."""
    id: str
    name: str
    secret_type: SecretType
    current_version: int
    versions: List[Dict[str, Any]] = field(default_factory=list)
    rotation_period_days: int = 90
    last_rotated: float = field(default_factory=time.time)
    next_rotation: float = field(default_factory=lambda: time.time() + 90*86400)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RotationResult:
    """Secret rotation result."""
    success: bool
    secret_id: str
    old_version: int
    new_version: int
    status: RotationStatus
    error: Optional[str] = None
    duration_ms: float = 0.0


def _generate_api_key(prefix: str = "sk") -> str:
    """Generate random API key."""
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def _generate_password(length: int = 32) -> str:
    """Generate random password."""
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
    return "".join(secrets.choice(chars) for _ in range(length))


def _generate_symmetric_key(bits: int = 256) -> str:
    """Generate symmetric encryption key."""
    return secrets.token_hex(bits // 8)


class SecretStore:
    """In-memory secret store."""

    def __init__(self):
        self._secrets: Dict[str, Secret] = {}

    def create(self, name: str, secret_type: SecretType,
               initial_value: str,
               rotation_period_days: int = 90,
               metadata: Optional[Dict[str, Any]] = None) -> Secret:
        """Create new secret."""
        secret_id = uuid.uuid4().hex
        version_id = uuid.uuid4().hex[:8]

        secret = Secret(
            id=secret_id,
            name=name,
            secret_type=secret_type,
            current_version=1,
            versions=[{
                "version": 1,
                "version_id": version_id,
                "created_at": time.time(),
                "created_by": "system"
            }],
            rotation_period_days=rotation_period_days,
            metadata=metadata or {}
        )

        self._secrets[secret_id] = secret
        return secret

    def get(self, secret_id: str) -> Optional[Secret]:
        """Get secret by ID."""
        return self._secrets.get(secret_id)

    def get_value(self, secret_id: str, version: Optional[int] = None) -> Optional[str]:
        """Get secret value (simulated - returns placeholder)."""
        secret = self._secrets.get(secret_id)
        if not secret:
            return None
        ver = version or secret.current_version
        return f"***SECRET_VALUE_V{ver}***"

    def rotate(self, secret_id: str,
               generate_func: Optional[Callable[[], str]] = None) -> RotationResult:
        """Rotate secret."""
        start = time.time()
        secret = self._secrets.get(secret_id)

        if not secret:
            return RotationResult(
                success=False,
                secret_id=secret_id,
                old_version=0,
                new_version=0,
                status=RotationStatus.FAILED,
                error="Secret not found",
                duration_ms=(time.time() - start) * 1000
            )

        old_version = secret.current_version
        new_version = old_version + 1
        version_id = uuid.uuid4().hex[:8]

        secret.versions.append({
            "version": new_version,
            "version_id": version_id,
            "created_at": time.time(),
            "created_by": "system"
        })
        secret.current_version = new_version
        secret.last_rotated = time.time()
        secret.next_rotation = time.time() + secret.rotation_period_days * 86400

        return RotationResult(
            success=True,
            secret_id=secret_id,
            old_version=old_version,
            new_version=new_version,
            status=RotationStatus.COMPLETED,
            duration_ms=(time.time() - start) * 1000
        )


_global_store = SecretStore()


class SecretRotationAction:
    """Secret rotation action.

    Example:
        action = SecretRotationAction()

        secret = action.create("my-api-key", "api_key")
        result = action.rotate("secret-id")
    """

    def __init__(self, store: Optional[SecretStore] = None):
        self._store = store or _global_store

    def create(self, name: str, secret_type: str,
               rotation_period_days: int = 90,
               metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create new secret."""
        try:
            st = SecretType(secret_type)
        except ValueError:
            return {"success": False, "message": f"Invalid secret type: {secret_type}"}

        if st == SecretType.API_KEY:
            initial = _generate_api_key()
        elif st == SecretType.PASSWORD:
            initial = _generate_password()
        elif st == SecretType.SYMMETRIC_KEY:
            initial = _generate_symmetric_key()
        else:
            initial = secrets.token_urlsafe(32)

        secret = self._store.create(name, st, initial, rotation_period_days, metadata)

        return {
            "success": True,
            "secret": {
                "id": secret.id,
                "name": secret.name,
                "type": secret.secret_type.value,
                "current_version": secret.current_version,
                "rotation_period_days": secret.rotation_period_days,
                "next_rotation": secret.next_rotation
            },
            "message": f"Created secret: {name}"
        }

    def get(self, secret_id: str) -> Dict[str, Any]:
        """Get secret info."""
        secret = self._store.get(secret_id)
        if secret:
            return {
                "success": True,
                "secret": {
                    "id": secret.id,
                    "name": secret.name,
                    "type": secret.secret_type.value,
                    "current_version": secret.current_version,
                    "last_rotated": secret.last_rotated,
                    "next_rotation": secret.next_rotation,
                    "rotation_period_days": secret.rotation_period_days
                }
            }
        return {"success": False, "message": "Secret not found"}

    def rotate(self, secret_id: str) -> Dict[str, Any]:
        """Rotate secret."""
        result = self._store.rotate(secret_id)
        return {
            "success": result.success,
            "secret_id": result.secret_id,
            "old_version": result.old_version,
            "new_version": result.new_version,
            "status": result.status.value,
            "error": result.error,
            "duration_ms": result.duration_ms,
            "message": f"Rotated from v{result.old_version} to v{result.new_version}"
        }

    def list_secrets(self) -> Dict[str, Any]:
        """List all secrets."""
        secrets = list(self._store._secrets.values())
        return {
            "success": True,
            "secrets": [
                {
                    "id": s.id,
                    "name": s.name,
                    "type": s.secret_type.value,
                    "current_version": s.current_version,
                    "next_rotation": s.next_rotation
                }
                for s in secrets
            ],
            "count": len(secrets)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute secret rotation action."""
    operation = params.get("operation", "")
    action = SecretRotationAction()

    try:
        if operation == "create":
            name = params.get("name", "")
            secret_type = params.get("secret_type", "api_key")
            if not name:
                return {"success": False, "message": "name required"}
            return action.create(
                name=name,
                secret_type=secret_type,
                rotation_period_days=params.get("rotation_period_days", 90),
                metadata=params.get("metadata")
            )

        elif operation == "get":
            secret_id = params.get("secret_id", "")
            if not secret_id:
                return {"success": False, "message": "secret_id required"}
            return action.get(secret_id)

        elif operation == "rotate":
            secret_id = params.get("secret_id", "")
            if not secret_id:
                return {"success": False, "message": "secret_id required"}
            return action.rotate(secret_id)

        elif operation == "list":
            return action.list_secrets()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Secret rotation error: {str(e)}"}
