"""
Authentication and authorization utilities - JWT, session management, permission checking.
"""
from typing import Any, Dict, List, Optional, Set
import hashlib
import hmac
import logging
import time
import secrets
import base64
import json

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _base64url_decode(data: Union[str, bytes]) -> bytes:
    if isinstance(data, str):
        data = data.encode("ascii")
    data += b"=" * (4 - len(data) % 4)
    return base64.urlsafe_b64decode(data)


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _sign_payload(header: str, payload: str, secret: str) -> str:
    message = f"{header}.{payload}".encode()
    signature = hmac.new(secret.encode(), message, hashlib.sha256).digest()
    return _base64url_encode(signature)


class SessionStore:
    """In-memory session store."""

    def __init__(self) -> None:
        self._sessions: Dict[str, Dict[str, Any]] = {}

    def create(self, user_id: str, ttl: int = 3600, extra: Optional[Dict[str, Any]] = None) -> str:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = {
            "user_id": user_id,
            "created_at": time.time(),
            "expires_at": time.time() + ttl,
            "data": extra or {},
        }
        return session_id

    def get(self, session_id: str) -> Optional[Dict[str, Any]]:
        session = self._sessions.get(session_id)
        if session and session["expires_at"] > time.time():
            return session
        if session_id in self._sessions:
            del self._sessions[session_id]
        return None

    def delete(self, session_id: str) -> bool:
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False

    def extend(self, session_id: str, ttl: int = 3600) -> bool:
        session = self._sessions.get(session_id)
        if session:
            session["expires_at"] = time.time() + ttl
            return True
        return False


class RBACStore:
    """Role-Based Access Control store."""

    def __init__(self) -> None:
        self._roles: Dict[str, Set[str]] = {}
        self._permissions: Dict[str, Set[str]] = {}
        self._user_roles: Dict[str, Set[str]] = {}

    def add_role(self, role: str, permissions: List[str]) -> None:
        self._roles[role] = set(permissions)

    def assign_role(self, user_id: str, role: str) -> None:
        if user_id not in self._user_roles:
            self._user_roles[user_id] = set()
        self._user_roles[user_id].add(role)

    def has_permission(self, user_id: str, permission: str) -> bool:
        user_roles = self._user_roles.get(user_id, set())
        for role in user_roles:
            if permission in self._roles.get(role, set()):
                return True
        return False

    def user_permissions(self, user_id: str) -> Set[str]:
        user_roles = self._user_roles.get(user_id, set())
        perms: Set[str] = set()
        for role in user_roles:
            perms.update(self._roles.get(role, set()))
        return perms


class AuthAction(BaseAction):
    """Authentication and authorization operations.

    Provides JWT decode, session management, password hashing, RBAC permission checking.
    """

    def __init__(self) -> None:
        self._sessions = SessionStore()
        self._rbac = RBACStore()

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "jwt_decode")
        token = params.get("token", "")
        secret = params.get("secret", "default-secret")

        try:
            if operation == "jwt_decode":
                if not token:
                    return {"success": False, "error": "token required"}
                parts = token.split(".")
                if len(parts) != 3:
                    return {"success": False, "error": "Invalid JWT format"}
                try:
                    header = json.loads(_base64url_decode(parts[0]))
                    payload = json.loads(_base64url_decode(parts[1]))
                    return {"success": True, "header": header, "payload": payload, "valid": True}
                except Exception as e:
                    return {"success": False, "error": f"JWT decode error: {e}"}

            elif operation == "jwt_verify":
                if not token:
                    return {"success": False, "error": "token required"}
                parts = token.split(".")
                if len(parts) != 3:
                    return {"success": False, "error": "Invalid JWT format", "valid": False}
                header_b64 = parts[0]
                payload_b64 = parts[1]
                signature_b64 = parts[2]
                expected_sig = _sign_payload(header_b64, payload_b64, secret)
                valid = hmac.compare_digest(signature_b64, expected_sig)
                if valid:
                    payload = json.loads(_base64url_decode(payload_b64))
                    if payload.get("exp", float("inf")) < time.time():
                        return {"success": True, "valid": False, "error": "Token expired"}
                return {"success": True, "valid": valid}

            elif operation == "jwt_create":
                payload = params.get("payload", {})
                header = params.get("header", {"alg": "HS256", "typ": "JWT"})
                if "iat" not in payload:
                    payload["iat"] = int(time.time())
                header_json = json.dumps(header, separators=(",", ":"))
                payload_json = json.dumps(payload, separators=(",", ":"))
                header_b64 = _base64url_encode(header_json.encode())
                payload_b64 = _base64url_encode(payload_json.encode())
                signature = _sign_payload(header_b64, payload_b64, secret)
                return {"success": True, "token": f"{header_b64}.{payload_b64}.{signature}"}

            elif operation == "session_create":
                user_id = params.get("user_id", "")
                if not user_id:
                    return {"success": False, "error": "user_id required"}
                ttl = int(params.get("ttl", 3600))
                extra = params.get("data", {})
                session_id = self._sessions.create(user_id, ttl, extra)
                return {"success": True, "session_id": session_id, "expires_in": ttl}

            elif operation == "session_get":
                if not token:
                    return {"success": False, "error": "session_id required"}
                session = self._sessions.get(token)
                if session:
                    return {"success": True, "session": session}
                return {"success": False, "error": "Session not found or expired"}

            elif operation == "session_delete":
                if not token:
                    return {"success": False, "error": "session_id required"}
                deleted = self._sessions.delete(token)
                return {"success": True, "deleted": deleted}

            elif operation == "session_extend":
                if not token:
                    return {"success": False, "error": "session_id required"}
                ttl = int(params.get("ttl", 3600))
                extended = self._sessions.extend(token, ttl)
                return {"success": True, "extended": extended}

            elif operation == "password_hash":
                import hashlib
                password = params.get("password", "")
                salt = secrets.token_hex(16)
                hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
                return {"success": True, "hash": f"{salt}${hashed}", "salt": salt}

            elif operation == "password_verify":
                password = params.get("password", "")
                stored = params.get("stored_hash", "")
                if "$" not in stored:
                    return {"success": False, "error": "Invalid hash format"}
                salt, hashed = stored.split("$", 1)
                computed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
                return {"success": True, "valid": hmac.compare_digest(computed, hashed)}

            elif operation == "rbac_add_role":
                role = params.get("role", "")
                permissions = params.get("permissions", [])
                if not role:
                    return {"success": False, "error": "role required"}
                self._rbac.add_role(role, permissions)
                return {"success": True, "role": role, "permissions": permissions}

            elif operation == "rbac_assign":
                user_id = params.get("user_id", "")
                role = params.get("role", "")
                if not user_id or not role:
                    return {"success": False, "error": "user_id and role required"}
                self._rbac.assign_role(user_id, role)
                return {"success": True, "user_id": user_id, "role": role}

            elif operation == "rbac_check":
                user_id = params.get("user_id", "")
                permission = params.get("permission", "")
                if not user_id or not permission:
                    return {"success": False, "error": "user_id and permission required"}
                allowed = self._rbac.has_permission(user_id, permission)
                return {"success": True, "allowed": allowed, "user_id": user_id, "permission": permission}

            elif operation == "rbac_permissions":
                user_id = params.get("user_id", "")
                if not user_id:
                    return {"success": False, "error": "user_id required"}
                perms = self._rbac.user_permissions(user_id)
                return {"success": True, "permissions": list(perms), "count": len(perms)}

            elif operation == "generate_token":
                length = int(params.get("length", 32))
                token_type = params.get("type", "hex")
                if token_type == "hex":
                    token = secrets.token_hex(length)
                elif token_type == "urlsafe":
                    token = secrets.token_urlsafe(length)
                else:
                    token = secrets.token_hex(length)
                return {"success": True, "token": token, "length": length}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"AuthAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for auth operations."""
    return AuthAction().execute(context, params)
