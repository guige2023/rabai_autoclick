"""Secret manager action module for RabAI AutoClick.

Provides secret management operations:
- SecretStoreAction: Store secret
- SecretRetrieveAction: Retrieve secret
- SecretDeleteAction: Delete secret
- SecretListAction: List secrets
- SecretRotateAction: Rotate secret
- SecretEncryptAction: Encrypt secret
- SecretDecryptAction: Decrypt secret
- SecretAccessAction: Control secret access
"""

import base64
import hashlib
import os
import sys
import time
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SecretStore:
    """In-memory secret storage."""
    
    _secrets: Dict[str, Dict[str, Any]] = {}
    
    @classmethod
    def store(cls, name: str, value: str, metadata: Dict[str, Any] = None) -> None:
        cls._secrets[name] = {
            "name": name,
            "value": value,
            "created_at": time.time(),
            "updated_at": time.time(),
            "version": 1,
            "metadata": metadata or {}
        }
    
    @classmethod
    def retrieve(cls, name: str) -> Optional[str]:
        if name in cls._secrets:
            return cls._secrets[name]["value"]
        return None
    
    @classmethod
    def delete(cls, name: str) -> bool:
        if name in cls._secrets:
            del cls._secrets[name]
            return True
        return False
    
    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        return [{"name": s["name"], "metadata": s.get("metadata", {})} for s in cls._secrets.values()]
    
    @classmethod
    def rotate(cls, name: str, new_value: str = None) -> bool:
        if name in cls._secrets:
            if new_value is None:
                new_value = base64.b64encode(os.urandom(32)).decode()
            cls._secrets[name]["value"] = new_value
            cls._secrets[name]["updated_at"] = time.time()
            cls._secrets[name]["version"] += 1
            return True
        return False


class SecretStoreAction(BaseAction):
    """Store a secret."""
    action_type = "secret_store"
    display_name = "存储密钥"
    description = "存储密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            value = params.get("value", "")
            metadata = params.get("metadata", {})
            
            if not name or not value:
                return ActionResult(success=False, message="name and value required")
            
            SecretStore.store(name, value, metadata)
            
            return ActionResult(
                success=True,
                message=f"Stored secret: {name}",
                data={"name": name, "stored": True}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret store failed: {str(e)}")


class SecretRetrieveAction(BaseAction):
    """Retrieve a secret."""
    action_type = "secret_retrieve"
    display_name = "获取密钥"
    description = "获取密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            decrypt = params.get("decrypt", False)
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            value = SecretStore.retrieve(name)
            
            if value is None:
                return ActionResult(success=False, message=f"Secret not found: {name}")
            
            return ActionResult(
                success=True,
                message=f"Retrieved secret: {name}",
                data={"name": name, "value": value, "decrypted": decrypt}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret retrieve failed: {str(e)}")


class SecretDeleteAction(BaseAction):
    """Delete a secret."""
    action_type = "secret_delete"
    display_name = "删除密钥"
    description = "删除密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            deleted = SecretStore.delete(name)
            
            return ActionResult(
                success=deleted,
                message=f"Deleted secret: {name}" if deleted else f"Secret not found: {name}",
                data={"name": name, "deleted": deleted}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret delete failed: {str(e)}")


class SecretListAction(BaseAction):
    """List all secrets."""
    action_type = "secret_list"
    display_name = "密钥列表"
    description = "列出所有密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            secrets = SecretStore.list_all()
            
            return ActionResult(
                success=True,
                message=f"Found {len(secrets)} secrets",
                data={"secrets": secrets, "count": len(secrets)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret list failed: {str(e)}")


class SecretRotateAction(BaseAction):
    """Rotate a secret."""
    action_type = "secret_rotate"
    display_name = "轮换密钥"
    description = "轮换密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            new_value = params.get("new_value")
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            rotated = SecretStore.rotate(name, new_value)
            
            if not rotated:
                return ActionResult(success=False, message=f"Secret not found: {name}")
            
            secret = SecretStore._secrets.get(name, {})
            
            return ActionResult(
                success=True,
                message=f"Rotated secret: {name}",
                data={"name": name, "version": secret.get("version", 1)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret rotate failed: {str(e)}")


class SecretEncryptAction(BaseAction):
    """Encrypt a secret."""
    action_type = "secret_encrypt"
    display_name = "加密密钥"
    description = "加密密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value", "")
            key = params.get("key", "")
            
            if not value:
                return ActionResult(success=False, message="value required")
            
            if not key:
                key = base64.b64encode(os.urandom(32)).decode()
            
            import hashlib
            key_hash = hashlib.sha256(key.encode()).digest()
            encrypted = bytes(a ^ b for a, b in zip(value.encode(), (key_hash * (len(value) // 32 + 1))[:len(value)].encode()))
            encrypted_b64 = base64.b64encode(encrypted).decode()
            
            return ActionResult(
                success=True,
                message="Encrypted secret",
                data={"encrypted": encrypted_b64, "key_provided": bool(params.get("key"))}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret encrypt failed: {str(e)}")


class SecretDecryptAction(BaseAction):
    """Decrypt a secret."""
    action_type = "secret_decrypt"
    display_name = "解密密钥"
    description = "解密密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            encrypted = params.get("encrypted", "")
            key = params.get("key", "")
            
            if not encrypted or not key:
                return ActionResult(success=False, message="encrypted and key required")
            
            import hashlib
            key_hash = hashlib.sha256(key.encode()).digest()
            encrypted_bytes = base64.b64decode(encrypted)
            decrypted = bytes(a ^ b for a, b in zip(encrypted_bytes, (key_hash * (len(encrypted_bytes) // 32 + 1))[:len(encrypted_bytes)]))
            
            return ActionResult(
                success=True,
                message="Decrypted secret",
                data={"decrypted": decrypted.decode()}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret decrypt failed: {str(e)}")


class SecretAccessAction(BaseAction):
    """Control secret access."""
    action_type = "secret_access"
    display_name = "密钥访问控制"
    description = "控制密钥访问"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            action = params.get("action", "grant")
            principal = params.get("principal", "")
            permissions = params.get("permissions", ["read"])
            
            if not name:
                return ActionResult(success=False, message="name required")
            
            if name not in SecretStore._secrets:
                return ActionResult(success=False, message=f"Secret not found: {name}")
            
            if "access_control" not in SecretStore._secrets[name]:
                SecretStore._secrets[name]["access_control"] = {}
            
            if action == "grant":
                SecretStore._secrets[name]["access_control"][principal] = permissions
                message = f"Granted {permissions} to {principal}"
            elif action == "revoke":
                if principal in SecretStore._secrets[name]["access_control"]:
                    del SecretStore._secrets[name]["access_control"][principal]
                message = f"Revoked access from {principal}"
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
            
            return ActionResult(
                success=True,
                message=message,
                data={"name": name, "action": action, "principal": principal, "permissions": permissions}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Secret access failed: {str(e)}")
