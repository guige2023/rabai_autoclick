"""API Key Action Module.

Manages API key lifecycle including generation,
rotation, revocation, and access control.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
import secrets
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class KeyStatus(Enum):
    """API key status."""
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


@dataclass
class APIKey:
    """An API key record."""
    key_id: str
    key_prefix: str
    key_hash: str
    client_id: str
    status: KeyStatus
    created_at: float
    expires_at: Optional[float] = None
    last_used_at: Optional[float] = None
    scopes: List[str] = field(default_factory=list)


class APIKeyAction(BaseAction):
    """
    API key lifecycle management.

    Generates, rotates, revokes API keys and
    manages access scopes and permissions.

    Example:
        key_mgr = APIKeyAction()
        result = key_mgr.execute(ctx, {"action": "generate", "client_id": "app-123"})
    """
    action_type = "api_key"
    display_name = "API密钥管理"
    description = "API密钥生成、轮换、撤销和访问控制"

    def __init__(self) -> None:
        super().__init__()
        self._keys: Dict[str, APIKey] = {}
        self._prefix_index: Dict[str, str] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "generate":
                return self._generate_key(params)
            elif action == "revoke":
                return self._revoke_key(params)
            elif action == "validate":
                return self._validate_key(params)
            elif action == "rotate":
                return self._rotate_key(params)
            elif action == "list":
                return self._list_keys(params)
            elif action == "update":
                return self._update_key(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"API Key error: {str(e)}")

    def _generate_key(self, params: Dict[str, Any]) -> ActionResult:
        client_id = params.get("client_id", "")
        scopes = params.get("scopes", [])
        expires_in = params.get("expires_in")
        prefix_length = params.get("prefix_length", 8)

        if not client_id:
            return ActionResult(success=False, message="client_id is required")

        key_raw = secrets.token_urlsafe(32)
        key_prefix = key_raw[:prefix_length]
        key_hash = hashlib.sha256(key_raw.encode()).hexdigest()
        key_id = secrets.token_urlsafe(16)

        expires_at = None
        if expires_in:
            expires_at = time.time() + expires_in

        api_key = APIKey(
            key_id=key_id,
            key_prefix=key_prefix,
            key_hash=key_hash,
            client_id=client_id,
            status=KeyStatus.ACTIVE,
            created_at=time.time(),
            expires_at=expires_at,
            scopes=scopes,
        )

        self._keys[key_id] = api_key
        self._prefix_index[key_prefix] = key_id

        return ActionResult(success=True, message=f"API key generated for {client_id}", data={"key_id": key_id, "key_prefix": key_prefix, "key": key_raw, "scopes": scopes})

    def _revoke_key(self, params: Dict[str, Any]) -> ActionResult:
        key_id = params.get("key_id", "")

        if key_id not in self._keys:
            return ActionResult(success=False, message=f"Key not found: {key_id}")

        self._keys[key_id].status = KeyStatus.REVOKED

        return ActionResult(success=True, message=f"Key revoked: {key_id}")

    def _validate_key(self, params: Dict[str, Any]) -> ActionResult:
        key_raw = params.get("key", "")

        if not key_raw:
            return ActionResult(success=False, message="key is required")

        key_prefix = key_raw[:8]

        if key_prefix not in self._prefix_index:
            return ActionResult(success=False, message="Invalid key")

        key_id = self._prefix_index[key_prefix]
        api_key = self._keys[key_id]

        if api_key.status == KeyStatus.REVOKED:
            return ActionResult(success=False, message="Key has been revoked")
        if api_key.status == KeyStatus.SUSPENDED:
            return ActionResult(success=False, message="Key is suspended")
        if api_key.expires_at and api_key.expires_at < time.time():
            api_key.status = KeyStatus.EXPIRED
            return ActionResult(success=False, message="Key has expired")

        api_key.last_used_at = time.time()

        return ActionResult(success=True, message="Key valid", data={"client_id": api_key.client_id, "scopes": api_key.scopes})

    def _rotate_key(self, params: Dict[str, Any]) -> ActionResult:
        key_id = params.get("key_id", "")

        if key_id not in self._keys:
            return ActionResult(success=False, message=f"Key not found: {key_id}")

        old_key = self._keys[key_id]
        old_key.status = KeyStatus.REVOKED

        new_key_raw = secrets.token_urlsafe(32)
        new_key_prefix = new_key_raw[:8]
        new_key_hash = hashlib.sha256(new_key_raw.encode()).hexdigest()
        new_key_id = secrets.token_urlsafe(16)

        new_key = APIKey(
            key_id=new_key_id,
            key_prefix=new_key_prefix,
            key_hash=new_key_hash,
            client_id=old_key.client_id,
            status=KeyStatus.ACTIVE,
            created_at=time.time(),
            expires_at=old_key.expires_at,
            scopes=old_key.scopes,
        )

        self._keys[new_key_id] = new_key
        self._prefix_index[new_key_prefix] = new_key_id

        return ActionResult(success=True, message="Key rotated", data={"new_key_id": new_key_id, "new_key": new_key_raw})

    def _list_keys(self, params: Dict[str, Any]) -> ActionResult:
        client_id = params.get("client_id")
        status_filter = params.get("status")

        keys = list(self._keys.values())

        if client_id:
            keys = [k for k in keys if k.client_id == client_id]

        if status_filter:
            try:
                status = KeyStatus(status_filter)
                keys = [k for k in keys if k.status == status]
            except ValueError:
                pass

        return ActionResult(success=True, data={"keys": [{"key_id": k.key_id, "client_id": k.client_id, "status": k.status.value, "created_at": k.created_at} for k in keys], "count": len(keys)})

    def _update_key(self, params: Dict[str, Any]) -> ActionResult:
        key_id = params.get("key_id", "")
        scopes = params.get("scopes")
        expires_at = params.get("expires_at")
        status = params.get("status")

        if key_id not in self._keys:
            return ActionResult(success=False, message=f"Key not found: {key_id}")

        api_key = self._keys[key_id]

        if scopes is not None:
            api_key.scopes = scopes
        if expires_at is not None:
            api_key.expires_at = expires_at
        if status:
            try:
                api_key.status = KeyStatus(status)
            except ValueError:
                pass

        return ActionResult(success=True, message=f"Key updated: {key_id}")
