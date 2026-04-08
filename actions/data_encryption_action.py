"""
Data Encryption Action Module.

Encrypts and decrypts data using AES, RSA, Fernet, and other
cryptographic algorithms with key management support.

Author: RabAi Team
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


class EncryptionAlgorithm(Enum):
    """Supported encryption algorithms."""
    FERNET = "fernet"
    AES_CBC = "aes_cbc"
    AES_GCM = "aes_gcm"
    RSA_OAEP = "rsa_oaep"
    PLAINTEXT = "plaintext"


class KeyDerivationFunction(Enum):
    """Key derivation functions."""
    PBKDF2 = "pbkdf2"
    SCRYPT = "scrypt"
    HKDF = "hkdf"
    NONE = "none"


@dataclass
class EncryptionKey:
    """An encryption key."""
    key_id: str
    algorithm: EncryptionAlgorithm
    key_data: bytes
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EncryptionResult:
    """Result of an encryption/decryption operation."""
    success: bool
    operation: str
    algorithm: EncryptionAlgorithm
    output: Any
    key_id: Optional[str] = None
    error: Optional[str] = None


class DataEncryptionAction(BaseAction):
    """Data encryption action.
    
    Encrypts and decrypts data using multiple algorithms
    with key management and secure key derivation.
    """
    action_type = "data_encryption"
    display_name = "数据加密"
    description = "数据加密解密"
    
    def __init__(self):
        super().__init__()
        self._keys: Dict[str, EncryptionKey] = {}
        self._default_key_id: Optional[str] = None
    
    def generate_key(
        self, algorithm: EncryptionAlgorithm = EncryptionAlgorithm.FERNET,
        key_id: Optional[str] = None, key_size: int = 256,
        password: Optional[str] = None, kdf: KeyDerivationFunction = KeyDerivationFunction.PBKDF2
    ) -> str:
        """Generate a new encryption key."""
        if not HAS_CRYPTO:
            raise ImportError("cryptography library not installed")
        
        if key_id is None:
            key_id = f"key_{int(time.time() * 1000)}"
        
        if algorithm == EncryptionAlgorithm.FERNET:
            key_data = Fernet.generate_key()
        elif algorithm in (EncryptionAlgorithm.AES_CBC, EncryptionAlgorithm.AES_GCM):
            key_bytes = os.urandom(key_size // 8)
            if password:
                key_data = self._derive_key(password, key_bytes, kdf)
            else:
                key_data = key_bytes
        elif algorithm == EncryptionAlgorithm.RSA_OAEP:
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size,
                backend=default_backend()
            )
            public_key = private_key.public_key()
            key_data = private_key.private_bytes(
                encoding=1, format=1, encryption_algorithm=2
            )
        else:
            key_data = os.urandom(32)
        
        self._keys[key_id] = EncryptionKey(
            key_id=key_id,
            algorithm=algorithm,
            key_data=key_data
        )
        
        if self._default_key_id is None:
            self._default_key_id = key_id
        
        return key_id
    
    def _derive_key(self, password: str, salt: bytes, kdf: KeyDerivationFunction) -> bytes:
        """Derive a key from a password."""
        if kdf == KeyDerivationFunction.PBKDF2:
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
            kdf_obj = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
                backend=default_backend()
            )
            return base64.urlsafe_b64encode(kdf_obj.derive(password.encode()))
        elif kdf == KeyDerivationFunction.HKDF:
            from cryptography.hazmat.primitives.kdf.hkdf import HKDF
            kdf_obj = HKDF(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                backend=default_backend()
            )
            return kdf_obj.derive(password.encode())
        return hashlib.sha256(password.encode()).digest()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Encrypt or decrypt data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: encrypt or decrypt
                - algorithm: Encryption algorithm
                - value: Value to encrypt/decrypt
                - key_id: Key ID to use
                - password: Password for key derivation
                - key: Raw key bytes (base64)
                
        Returns:
            ActionResult with encrypted/decrypted data.
        """
        start_time = time.time()
        
        if not HAS_CRYPTO:
            return ActionResult(
                success=False,
                message="cryptography library not installed",
                duration=time.time() - start_time
            )
        
        operation = params.get("operation", "encrypt")
        algorithm_str = params.get("algorithm", "fernet")
        value = params.get("value")
        key_id = params.get("key_id", self._default_key_id)
        password = params.get("password")
        
        try:
            algorithm = EncryptionAlgorithm(algorithm_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown algorithm: {algorithm_str}",
                duration=time.time() - start_time
            )
        
        if value is None:
            return ActionResult(
                success=False,
                message="No value provided",
                duration=time.time() - start_time
            )
        
        key = None
        if key_id and key_id in self._keys:
            key = self._keys[key_id]
        elif operation == "encrypt" and algorithm != EncryptionAlgorithm.PLAINTEXT:
            key_id = self.generate_key(algorithm)
            key = self._keys[key_id]
        
        if operation == "encrypt":
            result = self._encrypt(algorithm, value, key, password)
        elif operation == "decrypt":
            result = self._decrypt(algorithm, value, key)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )
        
        return ActionResult(
            success=result.success,
            message=f"{operation.capitalize()} {'succeeded' if result.success else 'failed'}",
            data={
                "output": result.output,
                "algorithm": algorithm.value,
                "operation": operation,
                "key_id": result.key_id or key_id,
                "error": result.error
            },
            duration=time.time() - start_time
        )
    
    def _encrypt(
        self, algorithm: EncryptionAlgorithm, value: Any, key: Optional[EncryptionKey], password: Optional[str]
    ) -> EncryptionResult:
        """Encrypt a value."""
        try:
            value_bytes = self._to_bytes(value)
            
            if algorithm == EncryptionAlgorithm.PLAINTEXT:
                return EncryptionResult(
                    success=True,
                    operation="encrypt",
                    algorithm=algorithm,
                    output=base64.b64encode(value_bytes).decode()
                )
            
            if key is None:
                return EncryptionResult(
                    success=False,
                    operation="encrypt",
                    algorithm=algorithm,
                    output=None,
                    error="No key provided"
                )
            
            if algorithm == EncryptionAlgorithm.FERNET:
                f = Fernet(key.key_data)
                output = f.encrypt(value_bytes)
                return EncryptionResult(
                    success=True,
                    operation="encrypt",
                    algorithm=algorithm,
                    output=output.decode(),
                    key_id=key.key_id
                )
            
            elif algorithm == EncryptionAlgorithm.AES_CBC:
                iv = os.urandom(16)
                pad_len = 16 - (len(value_bytes) % 16)
                padded = value_bytes + bytes([pad_len] * pad_len)
                
                cipher = Cipher(
                    algorithms.AES(key.key_data[:32]),
                    modes.CBC(iv),
                    backend=default_backend()
                )
                encryptor = cipher.encryptor()
                ciphertext = encryptor.update(padded) + encryptor.finalize()
                
                output = base64.b64encode(iv + ciphertext).decode()
                return EncryptionResult(
                    success=True,
                    operation="encrypt",
                    algorithm=algorithm,
                    output=output,
                    key_id=key.key_id
                )
            
            elif algorithm == EncryptionAlgorithm.AES_GCM:
                iv = os.urandom(12)
                
                cipher = Cipher(
                    algorithms.AES(key.key_data[:32]),
                    modes.GCM(iv),
                    backend=default_backend()
                )
                encryptor = cipher.encryptor()
                ciphertext = encryptor.update(value_bytes) + encryptor.finalize()
                
                output = base64.b64encode(iv + ciphertext + encryptor.tag).decode()
                return EncryptionResult(
                    success=True,
                    operation="encrypt",
                    algorithm=algorithm,
                    output=output,
                    key_id=key.key_id
                )
            
            elif algorithm == EncryptionAlgorithm.RSA_OAEP:
                from cryptography.hazmat.primitives.asymmetric import rsa
                from cryptography.hazmat.primitives.serialization import load_der_private_key
                
                private_key = load_der_private_key(
                    key.key_data, password=None, backend=default_backend()
                )
                public_key = private_key.public_key()
                
                ciphertext = public_key.encrypt(
                    value_bytes,
                    padding.OAEP(
                        mgf=padding.MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )
                
                output = base64.b64encode(ciphertext).decode()
                return EncryptionResult(
                    success=True,
                    operation="encrypt",
                    algorithm=algorithm,
                    output=output,
                    key_id=key.key_id
                )
            
            return EncryptionResult(
                success=False,
                operation="encrypt",
                algorithm=algorithm,
                output=None,
                error=f"Algorithm not supported: {algorithm}"
            )
            
        except Exception as e:
            return EncryptionResult(
                success=False,
                operation="encrypt",
                algorithm=algorithm,
                output=None,
                error=str(e)
            )
    
    def _decrypt(self, algorithm: EncryptionAlgorithm, value: Any, key: Optional[EncryptionKey]) -> EncryptionResult:
        """Decrypt a value."""
        try:
            if algorithm == EncryptionAlgorithm.PLAINTEXT:
                return EncryptionResult(
                    success=True,
                    operation="decrypt",
                    algorithm=algorithm,
                    output=base64.b64decode(value).decode()
                )
            
            if key is None:
                return EncryptionResult(
                    success=False,
                    operation="decrypt",
                    algorithm=algorithm,
                    output=None,
                    error="No key provided"
                )
            
            value_bytes = base64.b64decode(value)
            
            if algorithm == EncryptionAlgorithm.FERNET:
                f = Fernet(key.key_data)
                output = f.decrypt(value_bytes)
                return EncryptionResult(
                    success=True,
                    operation="decrypt",
                    algorithm=algorithm,
                    output=output.decode(),
                    key_id=key.key_id
                )
            
            elif algorithm == EncryptionType.AES_CBC:
                iv = value_bytes[:16]
                ciphertext = value_bytes[16:]
                
                cipher = Cipher(
                    algorithms.AES(key.key_data[:32]),
                    modes.CBC(iv),
                    backend=default_backend()
                )
                decryptor = cipher.decryptor()
                padded = decryptor.update(ciphertext) + decryptor.finalize()
                
                pad_len = padded[-1]
                output = padded[:-pad_len]
                
                return EncryptionResult(
                    success=True,
                    operation="decrypt",
                    algorithm=algorithm,
                    output=output.decode(),
                    key_id=key.key_id
                )
            
            elif algorithm == EncryptionAlgorithm.AES_GCM:
                iv = value_bytes[:12]
                tag = value_bytes[-16:]
                ciphertext = value_bytes[12:-16]
                
                cipher = Cipher(
                    algorithms.AES(key.key_data[:32]),
                    modes.GCM(iv, tag),
                    backend=default_backend()
                )
                decryptor = cipher.decryptor()
                output = decryptor.update(ciphertext) + decryptor.finalize()
                
                return EncryptionResult(
                    success=True,
                    operation="decrypt",
                    algorithm=algorithm,
                    output=output.decode(),
                    key_id=key.key_id
                )
            
            return EncryptionResult(
                success=False,
                operation="decrypt",
                algorithm=algorithm,
                output=None,
                error=f"Algorithm not supported: {algorithm}"
            )
            
        except Exception as e:
            return EncryptionResult(
                success=False,
                operation="decrypt",
                algorithm=algorithm,
                output=None,
                error=str(e)
            )
    
    def _to_bytes(self, value: Any) -> bytes:
        """Convert value to bytes."""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")
        elif isinstance(value, (dict, list)):
            return json.dumps(value).encode("utf-8")
        else:
            return str(value).encode("utf-8")
    
    def get_key_info(self, key_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a key."""
        if key_id not in self._keys:
            return None
        key = self._keys[key_id]
        return {
            "key_id": key.key_id,
            "algorithm": key.algorithm.value,
            "created_at": key.created_at,
            "metadata": key.metadata
        }
    
    def list_keys(self) -> List[str]:
        """List all key IDs."""
        return list(self._keys.keys())
    
    def delete_key(self, key_id: str) -> bool:
        """Delete a key."""
        if key_id in self._keys:
            del self._keys[key_id]
            if self._default_key_id == key_id:
                self._default_key_id = next(iter(self._keys), None)
            return True
        return False
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate encryption parameters."""
        if "value" not in params:
            return False, "Missing required parameter: value"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
