"""
API Authentication Action Module

Provides authentication, authorization, and identity management for APIs.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import hmac
import secrets
import jwt
import asyncio


class AuthMethod(Enum):
    """Authentication methods."""
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC_AUTH = "basic_auth"
    OAUTH2 = "oauth2"
    HMAC_SIGNATURE = "hmac_signature"
    JWT = "jwt"
    ANONYMOUS = "anonymous"


class Permission(Enum):
    """Permission types."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    EXECUTE = "execute"


@dataclass
class User:
    """User identity."""
    user_id: str
    username: str
    email: Optional[str] = None
    roles: list[str] = field(default_factory=list)
    permissions: set[str] = field(default_factory=set)
    metadata: dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class AuthToken:
    """Authentication token."""
    token: str
    token_type: str
    expires_at: datetime
    scopes: list[str] = field(default_factory=list)
    user_id: Optional[str] = None


@dataclass
class AuthResult:
    """Result of authentication attempt."""
    success: bool
    user: Optional[User] = None
    token: Optional[AuthToken] = None
    error: Optional[str] = None
    requires_2fa: bool = False


@dataclass
class ApiKey:
    """API key for authentication."""
    key_id: str
    key_hash: str
    user_id: str
    name: str
    scopes: list[str] = field(default_factory=list)
    expires_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    is_active: bool = True


class ApiAuthenticationAction:
    """Main authentication action handler."""
    
    def __init__(self, secret_key: Optional[str] = None):
        self.secret_key = secret_key or secrets.token_hex(32)
        self._users: dict[str, User] = {}
        self._api_keys: dict[str, ApiKey] = {}
        self._tokens: dict[str, AuthToken] = {}
        self._refresh_tokens: dict[str, str] = {}  # refresh_token -> user_id
        self._role_permissions: dict[str, set[str]] = {
            "admin": {p.value for p in Permission},
            "editor": {Permission.READ, Permission.WRITE, Permission.EXECUTE},
            "viewer": {Permission.READ},
        }
    
    async def register_user(
        self,
        username: str,
        email: Optional[str] = None,
        password: Optional[str] = None,
        roles: Optional[list[str]] = None
    ) -> User:
        """Register a new user."""
        user_id = secrets.token_urlsafe(16)
        
        user = User(
            user_id=user_id,
            username=username,
            email=email,
            roles=roles or ["viewer"],
            permissions=self._get_permissions_for_roles(roles or ["viewer"])
        )
        
        self._users[user_id] = user
        
        if password:
            # Store password hash
            pass_hash = self._hash_password(password)
            self._users[user_id].metadata["password_hash"] = pass_hash
        
        return user
    
    async def authenticate(
        self,
        credentials: dict[str, Any]
    ) -> AuthResult:
        """
        Authenticate user with credentials.
        
        Supports multiple auth methods based on credential fields.
        """
        method = credentials.get("method", AuthMethod.ANONYMOUS)
        
        if method == AuthMethod.API_KEY or "api_key" in credentials:
            return await self._authenticate_api_key(credentials.get("api_key"))
        
        elif method == AuthMethod.BEARER_TOKEN or "token" in credentials:
            return await self._authenticate_bearer(credentials.get("token"))
        
        elif method == AuthMethod.JWT or "jwt" in credentials:
            return await self._authenticate_jwt(credentials.get("jwt"))
        
        elif method == AuthMethod.BASIC_AUTH or ("username" in credentials and "password" in credentials):
            return await self._authenticate_basic(
                credentials.get("username"),
                credentials.get("password")
            )
        
        elif method == AuthMethod.HMAC_SIGNATURE:
            return await self._authenticate_hmac(credentials)
        
        else:
            return AuthResult(success=False, error="Unsupported authentication method")
    
    async def _authenticate_api_key(self, api_key: str) -> AuthResult:
        """Authenticate using API key."""
        if not api_key:
            return AuthResult(success=False, error="API key required")
        
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        for key_id, api_key_obj in self._api_keys.items():
            if api_key_obj.key_hash == key_hash and api_key_obj.is_active:
                if api_key_obj.expires_at and api_key_obj.expires_at < datetime.now():
                    return AuthResult(success=False, error="API key expired")
                
                api_key_obj.last_used = datetime.now()
                user = self._users.get(api_key_obj.user_id)
                
                if user and user.is_active:
                    return AuthResult(
                        success=True,
                        user=user,
                        token=AuthToken(
                            token=api_key,
                            token_type="api_key",
                            expires_at=api_key_obj.expires_at or datetime.now() + timedelta(days=365),
                            scopes=api_key_obj.scopes
                        )
                    )
        
        return AuthResult(success=False, error="Invalid API key")
    
    async def _authenticate_bearer(self, token: str) -> AuthResult:
        """Authenticate using bearer token."""
        auth_token = self._tokens.get(token)
        
        if not auth_token:
            return AuthResult(success=False, error="Invalid token")
        
        if auth_token.expires_at < datetime.now():
            return AuthResult(success=False, error="Token expired")
        
        user = self._users.get(auth_token.user_id) if auth_token.user_id else None
        
        if not user or not user.is_active:
            return AuthResult(success=False, error="User not found or inactive")
        
        return AuthResult(success=True, user=user, token=auth_token)
    
    async def _authenticate_jwt(self, token: str) -> AuthResult:
        """Authenticate using JWT."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])
            user_id = payload.get("user_id")
            
            if not user_id or user_id not in self._users:
                return AuthResult(success=False, error="Invalid token payload")
            
            user = self._users[user_id]
            
            if not user.is_active:
                return AuthResult(success=False, error="User inactive")
            
            return AuthResult(
                success=True,
                user=user,
                token=AuthToken(
                    token=token,
                    token_type="jwt",
                    expires_at=datetime.fromtimestamp(payload.get("exp", 0)),
                    scopes=payload.get("scopes", [])
                )
            )
            
        except jwt.ExpiredSignatureError:
            return AuthResult(success=False, error="JWT expired")
        except jwt.InvalidTokenError as e:
            return AuthResult(success=False, error=f"Invalid JWT: {e}")
    
    async def _authenticate_basic(self, username: str, password: str) -> AuthResult:
        """Authenticate using basic auth."""
        for user in self._users.values():
            if user.username == username:
                stored_hash = user.metadata.get("password_hash")
                if stored_hash and self._verify_password(password, stored_hash):
                    return AuthResult(success=True, user=user)
                break
        
        return AuthResult(success=False, error="Invalid credentials")
    
    async def _authenticate_hmac(self, credentials: dict[str, Any]) -> AuthResult:
        """Authenticate using HMAC signature."""
        api_key = credentials.get("api_key", "")
        signature = credentials.get("signature", "")
        timestamp = credentials.get("timestamp", 0)
        
        # Check timestamp freshness
        if abs(datetime.now().timestamp() - timestamp) > 300:
            return AuthResult(success=False, error="Request too old")
        
        # Find API key
        key_obj = None
        for k in self._api_keys.values():
            if k.key_id == api_key:
                key_obj = k
                break
        
        if not key_obj:
            return AuthResult(success=False, error="Unknown API key")
        
        # Verify signature
        message = f"{api_key}:{timestamp}"
        expected = hmac.new(
            self.secret_key.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected):
            return AuthResult(success=False, error="Invalid signature")
        
        user = self._users.get(key_obj.user_id)
        return AuthResult(success=True, user=user)
    
    def _hash_password(self, password: str) -> str:
        """Hash a password."""
        return hashlib.pbkdf2_hmac(
            "sha256",
            password.encode(),
            self.secret_key.encode(),
            100000
        ).hex()
    
    def _verify_password(self, password: str, stored_hash: str) -> bool:
        """Verify a password against stored hash."""
        return self._hash_password(password) == stored_hash
    
    def _get_permissions_for_roles(self, roles: list[str]) -> set[str]:
        """Get combined permissions for roles."""
        perms = set()
        for role in roles:
            perms.update(self._role_permissions.get(role, set()))
        return perms
    
    async def authorize(
        self,
        user: User,
        required_permission: Permission,
        resource: Optional[str] = None
    ) -> bool:
        """Check if user has required permission."""
        if not user.is_active:
            return False
        
        if required_permission.value in user.permissions:
            return True
        
        if "admin" in user.roles:
            return True
        
        return False
    
    async def create_api_key(
        self,
        user_id: str,
        name: str,
        scopes: Optional[list[str]] = None,
        expires_in_days: Optional[int] = None
    ) -> tuple[str, ApiKey]:
        """Create a new API key for a user."""
        if user_id not in self._users:
            raise ValueError(f"User {user_id} not found")
        
        api_key = secrets.token_urlsafe(32)
        key_id = secrets.token_urlsafe(8)
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        expires_at = None
        if expires_in_days:
            expires_at = datetime.now() + timedelta(days=expires_in_days)
        
        api_key_obj = ApiKey(
            key_id=key_id,
            key_hash=key_hash,
            user_id=user_id,
            name=name,
            scopes=scopes or [],
            expires_at=expires_at
        )
        
        self._api_keys[key_id] = api_key_obj
        
        return api_key, api_key_obj
    
    async def create_jwt_token(
        self,
        user: User,
        expires_in_hours: int = 24,
        scopes: Optional[list[str]] = None
    ) -> AuthToken:
        """Create a JWT token for a user."""
        import time
        
        payload = {
            "user_id": user.user_id,
            "username": user.username,
            "scopes": scopes or list(user.permissions),
            "iat": int(time.time()),
            "exp": int(time.time()) + (expires_in_hours * 3600)
        }
        
        token = jwt.encode(payload, self.secret_key, algorithm="HS256")
        
        auth_token = AuthToken(
            token=token,
            token_type="jwt",
            expires_at=datetime.now() + timedelta(hours=expires_in_hours),
            scopes=payload["scopes"],
            user_id=user.user_id
        )
        
        self._tokens[token] = auth_token
        
        return auth_token
    
    async def revoke_token(self, token: str) -> bool:
        """Revoke an authentication token."""
        if token in self._tokens:
            del self._tokens[token]
            return True
        return False
    
    async def refresh_access_token(self, refresh_token: str) -> Optional[AuthToken]:
        """Refresh an access token using refresh token."""
        user_id = self._refresh_tokens.get(refresh_token)
        if not user_id or user_id not in self._users:
            return None
        
        user = self._users[user_id]
        return await self.create_jwt_token(user)
