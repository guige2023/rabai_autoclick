"""Workflow encryption utilities for RabAI AutoClick.

Provides Fernet encryption for sensitive workflow parameters
like passwords, tokens, and API keys.
"""

import base64
import os
from typing import Any, Dict, List, Optional

try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False


class WorkflowCrypto:
    """Encrypts and decrypts sensitive workflow parameters using Fernet.
    
    Fernet guarantees that a message encrypted using it cannot be
    manipulated or read without the key.
    """
    
    ENCRYPTED_PREFIX = "_enc_"
    SENSITIVE_KEYS = {
        "password", "passwd", "pwd", "secret", "token",
        "api_key", "apikey", "api-key", "access_token",
        "auth_token", "private_key", "credential"
    }
    
    def __init__(self, encryption_key: Optional[str] = None) -> None:
        """Initialize the crypto handler.
        
        Args:
            encryption_key: Optional encryption key. If not provided,
                           reads from RABAI_ENCRYPTION_KEY environment variable.
        """
        self._fernet: Optional[Fernet] = None
        
        if encryption_key:
            self.set_key(encryption_key)
        else:
            env_key = os.environ.get("RABAI_ENCRYPTION_KEY")
            if env_key:
                self.set_key(env_key)
    
    def set_key(self, encryption_key: str) -> None:
        """Set the encryption key.
        
        Args:
            encryption_key: Base64-encoded Fernet key or a string that
                           will be used to derive a key.
        """
        if not CRYPTO_AVAILABLE:
            raise ImportError(
                "cryptography library is required. Install with: pip install cryptography"
            )
        
        # If it looks like a Fernet key (base64 and correct length), use it directly
        try:
            key_bytes = encryption_key.encode("utf-8")
            # Try to decode as base64 first
            decoded = base64.urlsafe_b64decode(key_bytes)
            if len(decoded) == 32:
                self._fernet = Fernet(key_bytes)
                return
        except Exception:
            pass
        
        # Otherwise derive a key from the string using SHA256
        import hashlib
        derived = hashlib.sha256(encryption_key.encode("utf-8")).digest()
        fernet_key = base64.urlsafe_b64encode(derived)
        self._fernet = Fernet(fernet_key)
    
    def _is_sensitive_key(self, key: str) -> bool:
        """Check if a key name suggests sensitive content.
        
        Args:
            key: Parameter key name.
            
        Returns:
            True if key appears to be sensitive.
        """
        key_lower = key.lower()
        return any(
            sensitive in key_lower
            for sensitive in self.SENSITIVE_KEYS
        )
    
    def _is_encrypted(self, value: Any) -> bool:
        """Check if a value is already encrypted.
        
        Args:
            value: Value to check.
            
        Returns:
            True if value appears to be encrypted.
        """
        return isinstance(value, str) and value.startswith(self.ENCRYPTED_PREFIX)
    
    def encrypt_value(self, value: str) -> str:
        """Encrypt a single string value.
        
        Args:
            value: String value to encrypt.
            
        Returns:
            Encrypted string with prefix.
            
        Raises:
            ValueError: If Fernet is not initialized.
        """
        if not self._fernet:
            raise ValueError("Encryption key not set. Call set_key() first.")
        
        encrypted = self._fernet.encrypt(value.encode("utf-8"))
        return self.ENCRYPTED_PREFIX + encrypted.decode("utf-8")
    
    def decrypt_value(self, encrypted_value: str) -> str:
        """Decrypt an encrypted value.
        
        Args:
            encrypted_value: Encrypted string with prefix.
            
        Returns:
            Decrypted string.
            
        Raises:
            ValueError: If Fernet is not initialized or value format is invalid.
        """
        if not self._fernet:
            raise ValueError("Encryption key not set. Call set_key() first.")
        
        if not encrypted_value.startswith(self.ENCRYPTED_PREFIX):
            raise ValueError("Value is not encrypted or missing prefix.")
        
        actual_encrypted = encrypted_value[len(self.ENCRYPTED_PREFIX):]
        decrypted = self._fernet.decrypt(actual_encrypted.encode("utf-8"))
        return decrypted.decode("utf-8")
    
    def encrypt_params(
        self,
        params: Dict[str, Any],
        auto_sensitive: bool = True
    ) -> Dict[str, Any]:
        """Encrypt sensitive parameters in a workflow params dictionary.
        
        Args:
            params: Workflow parameters dictionary.
            auto_sensitive: If True, auto-detect sensitive keys. If False,
                           only encrypt values that appear to be encrypted
                           strings (allowing manual marking).
            
        Returns:
            Dictionary with sensitive values encrypted.
        """
        if not self._fernet:
            raise ValueError("Encryption key not set. Call set_key() first.")
        
        result = {}
        
        for key, value in params.items():
            if isinstance(value, str) and not self._is_encrypted(value):
                if auto_sensitive and self._is_sensitive_key(key):
                    result[key] = self.encrypt_value(value)
                else:
                    result[key] = value
            elif isinstance(value, dict):
                result[key] = self.encrypt_params(value, auto_sensitive)
            elif isinstance(value, list):
                result[key] = [
                    self.encrypt_value(item) if isinstance(item, str) and
                        auto_sensitive and self._is_sensitive_key(key) else item
                    for item in value
                ]
            else:
                result[key] = value
        
        return result
    
    def decrypt_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt encrypted parameters in a workflow params dictionary.
        
        Args:
            params: Workflow parameters with potentially encrypted values.
            
        Returns:
            Dictionary with all encrypted values decrypted.
        """
        if not self._fernet:
            raise ValueError("Encryption key not set. Call set_key() first.")
        
        result = {}
        
        for key, value in params.items():
            if isinstance(value, str) and self._is_encrypted(value):
                try:
                    result[key] = self.decrypt_value(value)
                except Exception:
                    # If decryption fails, leave as-is
                    result[key] = value
            elif isinstance(value, dict):
                result[key] = self.decrypt_params(value)
            elif isinstance(value, list):
                decrypted_list = []
                for item in value:
                    if isinstance(item, str) and self._is_encrypted(item):
                        try:
                            decrypted_list.append(self.decrypt_value(item))
                        except Exception:
                            decrypted_list.append(item)
                    else:
                        decrypted_list.append(item)
                result[key] = decrypted_list
            else:
                result[key] = value
        
        return result
