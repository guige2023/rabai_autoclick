"""
API security action for authentication and authorization.

Provides OAuth2, API key, and role-based access control.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import hashlib
import hmac
import secrets


class APISecurityAction:
    """API security with authentication and authorization."""

    def __init__(
        self,
        token_expiry: float = 3600.0,
        refresh_token_expiry: float = 86400.0,
        max_login_attempts: int = 5,
        lockout_duration: float = 300.0,
    ) -> None:
        """
        Initialize API security.

        Args:
            token_expiry: Access token expiry in seconds
            refresh_token_expiry: Refresh token expiry in seconds
            max_login_attempts: Max failed attempts before lockout
            lockout_duration: Account lockout duration in seconds
        """
        self.token_expiry = token_expiry
        self.refresh_token_expiry = refresh_token_expiry
        self.max_login_attempts = max_login_attempts
        self.lockout_duration = lockout_duration

        self._users: Dict[str, Dict[str, Any]] = {}
        self._api_keys: Dict[str, Dict[str, Any]] = {}
        self._tokens: Dict[str, Dict[str, Any]] = {}
        self._roles: Dict[str, List[str]] = {
            "admin": ["read", "write", "delete", "admin"],
            "user": ["read", "write"],
            "guest": ["read"],
        }
        self._rate_limiter: Dict[str, List[float]] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute security operation.

        Args:
            params: Dictionary containing:
                - operation: 'register', 'login', 'logout', 'verify', 'authorize'
                - username: User identifier
                - password: User password
                - token: Access token
                - role: User role

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "verify")

        if operation == "register":
            return self._register_user(params)
        elif operation == "login":
            return self._login_user(params)
        elif operation == "logout":
            return self._logout_user(params)
        elif operation == "verify":
            return self._verify_token(params)
        elif operation == "authorize":
            return self._authorize_request(params)
        elif operation == "create_api_key":
            return self._create_api_key(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _register_user(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register new user."""
        username = params.get("username", "")
        password = params.get("password", "")
        email = params.get("email", "")
        role = params.get("role", "user")

        if not username or not password:
            return {"success": False, "error": "Username and password are required"}

        if username in self._users:
            return {"success": False, "error": "User already exists"}

        password_hash = self._hash_password(password)

        self._users[username] = {
            "username": username,
            "email": email,
            "password_hash": password_hash,
            "role": role,
            "created_at": time.time(),
            "failed_attempts": 0,
            "locked_until": None,
        }

        return {"success": True, "username": username, "role": role}

    def _login_user(self, params: dict[str, Any]) -> dict[str, Any]:
        """Authenticate user and generate token."""
        username = params.get("username", "")
        password = params.get("password", "")

        if not username or not password:
            return {"success": False, "error": "Username and password are required"}

        if username not in self._users:
            return {"success": False, "error": "Invalid credentials"}

        user = self._users[username]

        if user.get("locked_until") and time.time() < user["locked_until"]:
            remaining = user["locked_until"] - time.time()
            return {"success": False, "error": "Account is locked", "retry_after": remaining}

        if not self._verify_password(password, user["password_hash"]):
            user["failed_attempts"] += 1
            if user["failed_attempts"] >= self.max_login_attempts:
                user["locked_until"] = time.time() + self.lockout_duration
                return {"success": False, "error": "Account locked due to failed attempts"}
            return {"success": False, "error": "Invalid credentials"}

        user["failed_attempts"] = 0
        user["last_login"] = time.time()

        access_token = self._generate_token()
        refresh_token = self._generate_token()

        self._tokens[access_token] = {
            "username": username,
            "role": user["role"],
            "created_at": time.time(),
            "expires_at": time.time() + self.token_expiry,
            "refresh_token": refresh_token,
        }

        return {
            "success": True,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": self.token_expiry,
            "role": user["role"],
        }

    def _logout_user(self, params: dict[str, Any]) -> dict[str, Any]:
        """Logout user by invalidating token."""
        token = params.get("token", "")

        if token in self._tokens:
            del self._tokens[token]
            return {"success": True, "message": "Logged out"}

        return {"success": False, "error": "Invalid token"}

    def _verify_token(self, params: dict[str, Any]) -> dict[str, Any]:
        """Verify access token."""
        token = params.get("token", "")

        if not token:
            return {"success": False, "error": "Token is required"}

        if token not in self._tokens:
            return {"success": False, "error": "Invalid token"}

        token_data = self._tokens[token]

        if time.time() > token_data["expires_at"]:
            return {"success": False, "error": "Token expired"}

        return {
            "success": True,
            "username": token_data["username"],
            "role": token_data["role"],
            "expires_at": token_data["expires_at"],
        }

    def _authorize_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Check if token has required permission."""
        token = params.get("token", "")
        required_permission = params.get("permission", "read")

        verify_result = self._verify_token({"token": token})
        if not verify_result.get("success"):
            return verify_result

        role = verify_result.get("role", "guest")
        permissions = self._roles.get(role, [])

        if required_permission not in permissions:
            return {"success": False, "error": "Permission denied", "required": required_permission}

        return {"success": True, "authorized": True, "role": role}

    def _create_api_key(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create API key for user."""
        username = params.get("username", "")
        key_name = params.get("key_name", "default")
        permissions = params.get("permissions", ["read"])

        if username not in self._users:
            return {"success": False, "error": "User not found"}

        api_key = f"ak_{secrets.token_urlsafe(32)}"
        api_secret = secrets.token_urlsafe(32)

        self._api_keys[api_key] = {
            "username": username,
            "key_name": key_name,
            "permissions": permissions,
            "secret_hash": self._hash_password(api_secret),
            "created_at": time.time(),
            "last_used": None,
        }

        return {
            "success": True,
            "api_key": api_key,
            "api_secret": api_secret,
            "message": "Store the secret securely - it cannot be retrieved later",
        }

    def _hash_password(self, password: str) -> str:
        """Hash password using SHA-256 with salt."""
        salt = secrets.token_hex(16)
        hashed = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
        return f"{salt}${hashed}"

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        try:
            salt, hashed = password_hash.split("$")
            computed = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
            return hmac.compare_digest(computed, hashed)
        except ValueError:
            return False

    def _generate_token(self) -> str:
        """Generate secure random token."""
        return secrets.token_urlsafe(32)
