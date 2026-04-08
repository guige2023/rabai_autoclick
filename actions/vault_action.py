"""Vault action module for RabAI AutoClick.

Provides HashiCorp Vault operations for
secrets management, encryption, and identity management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VaultClient:
    """Vault client for secrets management.
    
    Provides methods for reading, writing, and managing
    secrets in HashiCorp Vault.
    """
    
    def __init__(
        self,
        url: str = "http://localhost:8200",
        token: str = "",
        namespace: Optional[str] = None
    ) -> None:
        """Initialize Vault client.
        
        Args:
            url: Vault server URL.
            token: Authentication token.
            namespace: Optional Vault namespace.
        """
        self.url = url.rstrip("/")
        self.token = token
        self.namespace = namespace
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Vault server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        if not self.token:
            return False
        
        try:
            self._session = requests.Session()
            self._session.headers.update({
                "X-Vault-Token": self.token,
                "Content-Type": "application/json"
            })
            
            if self.namespace:
                self._session.headers["X-Vault-Namespace"] = self.namespace
            
            response = self._session.get(
                f"{self.url}/v1/sys/health",
                timeout=30
            )
            
            return response.status_code in (200, 429)
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Vault session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def read_secret(self, path: str) -> Optional[Dict[str, Any]]:
        """Read a secret.
        
        Args:
            path: Secret path.
            
        Returns:
            Secret data or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/{path}",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "data": data.get("data", {}),
                    "metadata": data.get("metadata", {})
                }
            
            return None
        
        except Exception:
            return None
    
    def write_secret(
        self,
        path: str,
        secret: Dict[str, Any]
    ) -> bool:
        """Write a secret.
        
        Args:
            path: Secret path.
            secret: Secret data.
            
        Returns:
            True if write succeeded.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.post(
                f"{self.url}/v1/{path}",
                json={"data": secret},
                timeout=30
            )
            
            return response.status_code in (200, 201, 204)
        
        except Exception:
            return False
    
    def delete_secret(self, path: str) -> bool:
        """Delete a secret.
        
        Args:
            path: Secret path.
            
        Returns:
            True if delete succeeded.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.delete(
                f"{self.url}/v1/{path}",
                timeout=30
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False
    
    def list_secrets(self, path: str) -> List[str]:
        """List secrets at a path.
        
        Args:
            path: Directory path.
            
        Returns:
            List of secret keys.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.list(
                f"{self.url}/v1/{path}",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("data", {}).get("keys", [])
            
            return []
        
        except Exception:
            return []
    
    def read_kv_version(self, path: str, version: int = 1) -> Optional[Dict[str, Any]]:
        """Read a specific version of a KV secret.
        
        Args:
            path: Secret path.
            version: Secret version.
            
        Returns:
            Secret data or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/{path}",
                params={"version": version},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "data": data.get("data", {}),
                    "metadata": data.get("metadata", {})
                }
            
            return None
        
        except Exception:
            return None
    
    def read_health(self) -> Dict[str, Any]:
        """Get Vault health status.
        
        Returns:
            Health information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/sys/health",
                timeout=30
            )
            
            if response.status_code in (200, 429):
                return response.json()
            
            return {}
        
        except Exception as e:
            raise Exception(f"Read health failed: {str(e)}")
    
    def read_status(self) -> Dict[str, Any]:
        """Get Vault status.
        
        Returns:
            Status information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/sys/status",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return {}
        
        except Exception as e:
            raise Exception(f"Read status failed: {str(e)}")
    
    def list_policies(self) -> List[str]:
        """List all policies.
        
        Returns:
            List of policy names.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/sys/policies/acl",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("policies", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List policies failed: {str(e)}")
    
    def read_policy(self, name: str) -> Optional[str]:
        """Read a policy.
        
        Args:
            name: Policy name.
            
        Returns:
            Policy rules or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/sys/policies/acl/{name}",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("policy", "")
            
            return None
        
        except Exception:
            return None
    
    def create_or_update_policy(self, name: str, rules: str) -> bool:
        """Create or update a policy.
        
        Args:
            name: Policy name.
            rules: Policy rules in HCL format.
            
        Returns:
            True if successful.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.put(
                f"{self.url}/v1/sys/policies/acl/{name}",
                json={"policy": rules},
                timeout=30
            )
            
            return response.status_code in (200, 201, 204)
        
        except Exception:
            return False
    
    def delete_policy(self, name: str) -> bool:
        """Delete a policy.
        
        Args:
            name: Policy name.
            
        Returns:
            True if deleted.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.delete(
                f"{self.url}/v1/sys/policies/acl/{name}",
                timeout=30
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False
    
    def create_token(
        self,
        policies: Optional[List[str]] = None,
        ttl: Optional[str] = None,
        renew: bool = False
    ) -> Optional[str]:
        """Create a token.
        
        Args:
            policies: List of policies.
            ttl: Token TTL.
            renew: Allow token renewal.
            
        Returns:
            Token string or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            data: Dict[str, Any] = {"policies": policies or []}
            
            if ttl:
                data["ttl"] = ttl
            
            data["renewable"] = renew
            
            response = self._session.post(
                f"{self.url}/v1/auth/token/create",
                json=data,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                data = response.json()
                return data.get("auth", {}).get("client_token")
            
            return None
        
        except Exception:
            return None
    
    def renew_token(self, increment: Optional[str] = None) -> bool:
        """Renew the current token.
        
        Args:
            increment: Optional TTL increment.
            
        Returns:
            True if renewed.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            data = {}
            if increment:
                data["increment"] = increment
            
            response = self._session.post(
                f"{self.url}/v1/auth/token/renew",
                json=data,
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def revoke_token(self, orphan: bool = False) -> bool:
        """Revoke the current token.
        
        Args:
            orphan: Leave child tokens active.
            
        Returns:
            True if revoked.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.post(
                f"{self.url}/v1/auth/token/revoke",
                json={"orphan": orphan},
                timeout=30
            )
            
            return response.status_code in (200, 204)
        
        except Exception:
            return False
    
    def encrypt_data(
        self,
        key: str,
        plaintext: str
    ) -> Optional[str]:
        """Encrypt data using Transit engine.
        
        Args:
            key: Encryption key name.
            plaintext: Base64 encoded plaintext.
            
        Returns:
            Ciphertext or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.post(
                f"{self.url}/v1/transit/encrypt/{key}",
                json={"plaintext": plaintext},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("data", {}).get("ciphertext")
            
            return None
        
        except Exception:
            return None
    
    def decrypt_data(
        self,
        key: str,
        ciphertext: str
    ) -> Optional[str]:
        """Decrypt data using Transit engine.
        
        Args:
            key: Encryption key name.
            ciphertext: Ciphertext to decrypt.
            
        Returns:
            Base64 decoded plaintext or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.post(
                f"{self.url}/v1/transit/decrypt/{key}",
                json={"ciphertext": ciphertext},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("data", {}).get("plaintext")
            
            return None
        
        except Exception:
            return None
    
    def enable_secrets_engine(
        self,
        engine_type: str,
        path: Optional[str] = None
    ) -> bool:
        """Enable a secrets engine.
        
        Args:
            engine_type: Type of secrets engine.
            path: Optional mount path.
            
        Returns:
            True if enabled.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            mount_path = path or engine_type
            
            response = self._session.post(
                f"{self.url}/v1/sys/mounts/{mount_path}",
                json={"type": engine_type},
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def list_secrets_engines(self) -> List[Dict[str, Any]]:
        """List enabled secrets engines.
        
        Returns:
            List of secrets engine information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Vault")
        
        try:
            response = self._session.get(
                f"{self.url}/v1/sys/mounts",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                mounts = data.get("data", {})
                return [
                    {"path": k, "type": v.get("type", "")}
                    for k, v in mounts.items()
                ]
            
            return []
        
        except Exception as e:
            raise Exception(f"List secrets engines failed: {str(e)}")


class VaultAction(BaseAction):
    """Vault action for secrets management.
    
    Supports secrets CRUD, encryption, and policy management.
    """
    action_type: str = "vault"
    display_name: str = "Vault动作"
    description: str = "HashiCorp Vault密钥管理和加密操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[VaultClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Vault operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "read":
                return self._read(params, start_time)
            elif operation == "write":
                return self._write(params, start_time)
            elif operation == "delete":
                return self._delete(params, start_time)
            elif operation == "list":
                return self._list(params, start_time)
            elif operation == "read_kv_version":
                return self._read_kv_version(params, start_time)
            elif operation == "health":
                return self._health(start_time)
            elif operation == "status":
                return self._status(start_time)
            elif operation == "list_policies":
                return self._list_policies(start_time)
            elif operation == "read_policy":
                return self._read_policy(params, start_time)
            elif operation == "create_policy":
                return self._create_policy(params, start_time)
            elif operation == "delete_policy":
                return self._delete_policy(params, start_time)
            elif operation == "create_token":
                return self._create_token(params, start_time)
            elif operation == "renew_token":
                return self._renew_token(params, start_time)
            elif operation == "revoke_token":
                return self._revoke_token(params, start_time)
            elif operation == "encrypt":
                return self._encrypt(params, start_time)
            elif operation == "decrypt":
                return self._decrypt(params, start_time)
            elif operation == "list_engines":
                return self._list_engines(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Vault operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Vault."""
        url = params.get("url", "http://localhost:8200")
        token = params.get("token", "")
        namespace = params.get("namespace")
        
        if not token:
            return ActionResult(success=False, message="token is required", duration=time.time() - start_time)
        
        self._client = VaultClient(url=url, token=token, namespace=namespace)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Vault at {url}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Vault."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from Vault", duration=time.time() - start_time)
    
    def _read(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Read a secret."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        path = params.get("path", "")
        if not path:
            return ActionResult(success=False, message="path is required", duration=time.time() - start_time)
        
        try:
            secret = self._client.read_secret(path)
            return ActionResult(success=secret is not None, message=f"Secret read: {path}" if secret else f"Secret not found: {path}", data={"secret": secret}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _write(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Write a secret."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        path = params.get("path", "")
        secret = params.get("secret", {})
        
        if not path or not secret:
            return ActionResult(success=False, message="path and secret are required", duration=time.time() - start_time)
        
        try:
            success = self._client.write_secret(path, secret)
            return ActionResult(success=success, message=f"Secret written: {path}" if success else "Write failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a secret."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        path = params.get("path", "")
        if not path:
            return ActionResult(success=False, message="path is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_secret(path)
            return ActionResult(success=success, message=f"Secret deleted: {path}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List secrets at a path."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        path = params.get("path", "")
        
        try:
            keys = self._client.list_secrets(path)
            return ActionResult(success=True, message=f"Found {len(keys)} keys", data={"keys": keys}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _read_kv_version(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Read a specific version of a KV secret."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        path = params.get("path", "")
        version = params.get("version", 1)
        
        if not path:
            return ActionResult(success=False, message="path is required", duration=time.time() - start_time)
        
        try:
            secret = self._client.read_kv_version(path, version)
            return ActionResult(success=secret is not None, message=f"Secret v{version} read", data={"secret": secret}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _health(self, start_time: float) -> ActionResult:
        """Get Vault health status."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            health = self._client.read_health()
            return ActionResult(success=True, message="Health status retrieved", data={"health": health}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _status(self, start_time: float) -> ActionResult:
        """Get Vault status."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            status = self._client.read_status()
            return ActionResult(success=True, message="Status retrieved", data={"status": status}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_policies(self, start_time: float) -> ActionResult:
        """List all policies."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            policies = self._client.list_policies()
            return ActionResult(success=True, message=f"Found {len(policies)} policies", data={"policies": policies}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _read_policy(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Read a policy."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            policy = self._client.read_policy(name)
            return ActionResult(success=policy is not None, message=f"Policy read: {name}", data={"policy": policy}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_policy(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create or update a policy."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        rules = params.get("rules", "")
        
        if not name or not rules:
            return ActionResult(success=False, message="name and rules are required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_or_update_policy(name, rules)
            return ActionResult(success=success, message=f"Policy created: {name}" if success else "Create policy failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_policy(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a policy."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_policy(name)
            return ActionResult(success=success, message=f"Policy deleted: {name}" if success else "Delete policy failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a token."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            token = self._client.create_token(
                policies=params.get("policies"),
                ttl=params.get("ttl"),
                renew=params.get("renew", False)
            )
            return ActionResult(success=token is not None, message="Token created" if token else "Create token failed", data={"token": token}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _renew_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Renew the current token."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            success = self._client.renew_token(params.get("increment"))
            return ActionResult(success=success, message="Token renewed" if success else "Renew token failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _revoke_token(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Revoke the current token."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            success = self._client.revoke_token(params.get("orphan", False))
            return ActionResult(success=success, message="Token revoked" if success else "Revoke token failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _encrypt(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Encrypt data using Transit engine."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        key = params.get("key", "")
        plaintext = params.get("plaintext", "")
        
        if not key or not plaintext:
            return ActionResult(success=False, message="key and plaintext are required", duration=time.time() - start_time)
        
        try:
            ciphertext = self._client.encrypt_data(key, plaintext)
            return ActionResult(success=ciphertext is not None, message="Data encrypted" if ciphertext else "Encrypt failed", data={"ciphertext": ciphertext}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _decrypt(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Decrypt data using Transit engine."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        key = params.get("key", "")
        ciphertext = params.get("ciphertext", "")
        
        if not key or not ciphertext:
            return ActionResult(success=False, message="key and ciphertext are required", duration=time.time() - start_time)
        
        try:
            plaintext = self._client.decrypt_data(key, ciphertext)
            return ActionResult(success=plaintext is not None, message="Data decrypted" if plaintext else "Decrypt failed", data={"plaintext": plaintext}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_engines(self, start_time: float) -> ActionResult:
        """List enabled secrets engines."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            engines = self._client.list_secrets_engines()
            return ActionResult(success=True, message=f"Found {len(engines)} secrets engines", data={"engines": engines}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
