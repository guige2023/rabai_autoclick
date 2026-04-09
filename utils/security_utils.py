"""
Security Utilities for UI Automation.

This module provides utilities for security-related operations including
credential management, secure storage, and encryption helpers.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass, field
from typing import Optional


class SecureString:
    """
    A string that attempts to avoid leaving traces in memory.
    
    Note: This is a best-effort approach. Python strings are immutable
    and may still exist in memory after the object is deleted.
    """
    
    def __init__(self, value: str):
        self._value = value.encode()
        self._hash = hashlib.sha256(self._value).hexdigest()
    
    def __str__(self) -> str:
        return self._value.decode()
    
    def __repr__(self) -> str:
        return "SecureString(***)"
    
    def reveal(self) -> str:
        """Reveal the actual value."""
        return self._value.decode()
    
    @property
    def hash(self) -> str:
        """Get SHA256 hash of the value."""
        return self._hash
    
    def clear(self) -> None:
        """Attempt to clear the underlying data."""
        try:
            self._value = b'\x00' * len(self._value)
        except Exception:
            pass


class PasswordGenerator:
    """
    Generates secure passwords.
    
    Example:
        gen = PasswordGenerator()
        password = gen.generate(length=16, include_special=True)
    """
    
    LOWERCASE = "abcdefghijklmnopqrstuvwxyz"
    UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    DIGITS = "0123456789"
    SPECIAL = "!@#$%^&*()_+-=[]{}|;:,.<>?"
    
    def generate(
        self,
        length: int = 16,
        include_lowercase: bool = True,
        include_uppercase: bool = True,
        include_digits: bool = True,
        include_special: bool = True,
        exclude_ambiguous: bool = False
    ) -> str:
        """
        Generate a secure password.
        
        Args:
            length: Password length
            include_lowercase: Include lowercase letters
            include_uppercase: Include uppercase letters
            include_digits: Include digits
            include_special: Include special characters
            exclude_ambiguous: Exclude ambiguous characters (0, O, l, 1, etc.)
            
        Returns:
            Generated password string
        """
        import random
        
        charset = ""
        required_chars = []
        
        if include_lowercase:
            chars = self.LOWERCASE
            if exclude_ambiguous:
                chars = chars.replace('l', '')
            charset += chars
            required_chars.append(random.choice(chars))
        
        if include_uppercase:
            chars = self.UPPERCASE
            if exclude_ambiguous:
                chars = chars.replace('O', '').replace('I', '')
            charset += chars
            required_chars.append(random.choice(chars))
        
        if include_digits:
            chars = self.DIGITS
            if exclude_ambiguous:
                chars = chars.replace('0', '').replace('1', '')
            charset += chars
            required_chars.append(random.choice(chars))
        
        if include_special:
            charset += self.SPECIAL
            required_chars.append(random.choice(self.SPECIAL))
        
        if not charset:
            raise ValueError("At least one character set must be included")
        
        # Generate remaining characters
        remaining = max(0, length - len(required_chars))
        random_chars = [random.choice(charset) for _ in range(remaining)]
        
        # Combine and shuffle
        all_chars = required_chars + random_chars
        random.shuffle(all_chars)
        
        return ''.join(all_chars)


class HashUtils:
    """Utilities for hashing operations."""
    
    @staticmethod
    def sha256(data: str) -> str:
        """Generate SHA256 hash of a string."""
        return hashlib.sha256(data.encode()).hexdigest()
    
    @staticmethod
    def sha512(data: str) -> str:
        """Generate SHA512 hash of a string."""
        return hashlib.sha512(data.encode()).hexdigest()
    
    @staticmethod
    def md5(data: str) -> str:
        """Generate MD5 hash of a string."""
        return hashlib.md5(data.encode()).hexdigest()
    
    @staticmethod
    def hmac_sha256(key: str, message: str) -> str:
        """Generate HMAC-SHA256 of a message."""
        return hmac.new(
            key.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
    
    @staticmethod
    def verify_hmac(key: str, message: str, signature: str) -> bool:
        """Verify an HMAC signature."""
        expected = HashUtils.hmac_sha256(key, message)
        return hmac.compare_digest(expected, signature)


class TokenGenerator:
    """
    Generates secure tokens for various purposes.
    
    Example:
        gen = TokenGenerator()
        token = gen.generate_token(length=32)
    """
    
    @staticmethod
    def generate_token(length: int = 32) -> str:
        """
        Generate a secure random token.
        
        Args:
            length: Token length in bytes (output will be longer due to encoding)
            
        Returns:
            Base64-encoded token string
        """
        return base64.urlsafe_b64encode(
            secrets.token_bytes(length)
        ).decode().rstrip('=')
    
    @staticmethod
    def generate_hex(length: int = 32) -> str:
        """
        Generate a secure random hex token.
        
        Args:
            length: Token length in bytes
            
        Returns:
            Hex-encoded token string
        """
        return secrets.token_hex(length)


@dataclass
class Credential:
    """
    A stored credential.
    
    Attributes:
        username: Username or identifier
        password: Password (should be encrypted in production)
        service: Service name
        url: Optional service URL
        notes: Optional notes
    """
    username: str
    password: str
    service: str
    url: Optional[str] = None
    notes: Optional[str] = None
    metadata: dict = field(default_factory=dict)


class CredentialManager:
    """
    Manages stored credentials.
    
    Note: In production, credentials should be encrypted at rest
    and retrieved from a secure vault.
    
    Example:
        manager = CredentialManager()
        manager.store("myapp", "admin", "password123")
        
        cred = manager.get("myapp")
        if cred:
            print(f"Username: {cred.username}")
    """
    
    def __init__(self):
        self._credentials: dict[str, Credential] = {}
    
    def store(
        self,
        service: str,
        username: str,
        password: str,
        url: Optional[str] = None,
        notes: Optional[str] = None
    ) -> None:
        """
        Store a credential.
        
        Args:
            service: Service name (key)
            username: Username
            password: Password
            url: Optional service URL
            notes: Optional notes
        """
        self._credentials[service] = Credential(
            username=username,
            password=password,
            service=service,
            url=url,
            notes=notes
        )
    
    def get(self, service: str) -> Optional[Credential]:
        """
        Retrieve a credential.
        
        Args:
            service: Service name
            
        Returns:
            Credential if found, None otherwise
        """
        return self._credentials.get(service)
    
    def delete(self, service: str) -> bool:
        """
        Delete a credential.
        
        Args:
            service: Service name
            
        Returns:
            True if deleted, False if not found
        """
        if service in self._credentials:
            del self._credentials[service]
            return True
        return False
    
    def list_services(self) -> list[str]:
        """List all stored service names."""
        return list(self._credentials.keys())
    
    def has_service(self, service: str) -> bool:
        """Check if a service is stored."""
        return service in self._credentials


class EncryptionHelper:
    """
    Helper for basic encryption/decryption operations.
    
    Note: This is for demonstration only. In production,
    use a proper encryption library like cryptography.
    """
    
    def __init__(self, key: str):
        """
        Initialize with an encryption key.
        
        Args:
            key: Encryption key (should be at least 32 bytes)
        """
        self._key = hashlib.sha256(key.encode()).digest()
    
    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string (XOR-based, not secure for production).
        
        Args:
            plaintext: String to encrypt
            
        Returns:
            Base64-encoded encrypted string
        """
        key_bytes = self._key
        plaintext_bytes = plaintext.encode()
        
        encrypted = bytes(
            p ^ key_bytes[i % len(key_bytes)]
            for i, p in enumerate(plaintext_bytes)
        )
        
        return base64.b64encode(encrypted).decode()
    
    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a string.
        
        Args:
            ciphertext: Base64-encoded encrypted string
            
        Returns:
            Decrypted string
        """
        import binascii
        
        try:
            encrypted = base64.b64decode(ciphertext.encode())
            key_bytes = self._key
            
            decrypted = bytes(
                e ^ key_bytes[i % len(key_bytes)]
                for i, e in enumerate(encrypted)
            )
            
            return decrypted.decode()
        except Exception:
            return ""
