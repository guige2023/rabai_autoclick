"""API Key Manager Action Module.

Manages API keys with:
- Key generation and rotation
- Key storage with encryption
- Access control and quotas
- Usage tracking and analytics
- Key expiration handling

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class KeyStatus(Enum):
    """API key status."""
    ACTIVE = auto()
    SUSPENDED = auto()
    EXPIRED = auto()
    REVOKED = auto()


class KeyType(Enum):
    """API key types."""
    FULL_ACCESS = auto()
    READ_ONLY = auto()
    CUSTOM = auto()


@dataclass
class APIKey:
    """API key configuration."""
    key_id: str
    name: str
    key_hash: str
    key_prefix: str
    key_type: KeyType = KeyType.FULL_ACCESS
    status: KeyStatus = KeyStatus.ACTIVE
    scopes: Set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    last_used: Optional[float] = None
    expires_at: Optional[float] = None
    rate_limit: int = 1000
    rate_window_seconds: float = 60.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KeyUsage:
    """API key usage record."""
    key_id: str
    timestamp: float
    endpoint: str
    method: str
    status_code: int
    response_time_ms: float
    bytes_sent: int = 0
    bytes_received: int = 0


@dataclass
class KeyMetrics:
    """API key metrics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_requests: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    avg_response_time_ms: float = 0.0


class APIKeyManager:
    """Manages API keys for authentication and access control.
    
    Features:
    - Secure key generation with hashing
    - Key rotation support
    - Rate limiting per key
    - Usage tracking and analytics
    - Expiration management
    - Scope-based access control
    """
    
    def __init__(
        self,
        name: str = "default",
        hash_algorithm: str = "sha256",
        key_length: int = 32
    ):
        self.name = name
        self.hash_algorithm = hash_algorithm
        self.key_length = key_length
        self._keys: Dict[str, APIKey] = {}
        self._keys_by_hash: Dict[str, str] = {}
        self._usage_records: Dict[str, List[KeyUsage]] = defaultdict(list)
        self._rate_limiters: Dict[str, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._metrics = {
            "keys_generated": 0,
            "keys_active": 0,
            "keys_revoked": 0,
            "total_requests": 0,
            "rate_limited_requests": 0
        }
    
    async def generate_key(
        self,
        name: str,
        key_type: KeyType = KeyType.FULL_ACCESS,
        scopes: Optional[Set[str]] = None,
        expires_in_days: Optional[int] = None,
        rate_limit: int = 1000,
        metadata: Optional[Dict[str, Any]] = None
    ) -> tuple[str, APIKey]:
        """Generate a new API key.
        
        Args:
            name: Key name/description
            key_type: Type of key
            scopes: Access scopes
            expires_in_days: Days until expiration
            rate_limit: Requests per window
            metadata: Additional metadata
            
        Returns:
            Tuple of (plaintext key, APIKey object)
        """
        plaintext_key = secrets.token_urlsafe(self.key_length)
        key_hash = self._hash_key(plaintext_key)
        key_prefix = plaintext_key[:8]
        key_id = f"key_{int(time.time() * 1000000)}"
        
        expires_at = None
        if expires_in_days:
            expires_at = time.time() + (expires_in_days * 86400)
        
        api_key = APIKey(
            key_id=key_id,
            name=name,
            key_hash=key_hash,
            key_prefix=key_prefix,
            key_type=key_type,
            scopes=scopes or set(),
            expires_at=expires_at,
            rate_limit=rate_limit,
            metadata=metadata or {}
        )
        
        async with self._lock:
            self._keys[key_id] = api_key
            self._keys_by_hash[key_hash] = key_id
        
        self._metrics["keys_generated"] += 1
        self._metrics["keys_active"] += 1
        
        logger.info(f"Generated API key: {key_id}")
        return plaintext_key, api_key
    
    async def verify_key(self, plaintext_key: str) -> Optional[APIKey]:
        """Verify a plaintext key and return its configuration.
        
        Args:
            plaintext_key: Key to verify
            
        Returns:
            APIKey if valid, None otherwise
        """
        key_hash = self._hash_key(plaintext_key)
        
        async with self._lock:
            if key_hash not in self._keys_by_hash:
                return None
            
            key_id = self._keys_by_hash[key_hash]
            api_key = self._keys.get(key_id)
            
            if not api_key:
                return None
            
            if api_key.status != KeyStatus.ACTIVE:
                return None
            
            if api_key.expires_at and time.time() > api_key.expires_at:
                api_key.status = KeyStatus.EXPIRED
                return None
            
            api_key.last_used = time.time()
            
            return api_key
    
    async def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key.
        
        Args:
            key_id: Key ID to revoke
            
        Returns:
            True if revoked
        """
        async with self._lock:
            if key_id not in self._keys:
                return False
            
            api_key = self._keys[key_id]
            api_key.status = KeyStatus.REVOKED
            self._metrics["keys_revoked"] += 1
            self._metrics["keys_active"] -= 1
            
            logger.info(f"Revoked API key: {key_id}")
            return True
    
    async def suspend_key(self, key_id: str) -> bool:
        """Suspend an API key temporarily.
        
        Args:
            key_id: Key ID to suspend
            
        Returns:
            True if suspended
        """
        async with self._lock:
            if key_id not in self._keys:
                return False
            
            self._keys[key_id].status = KeyStatus.SUSPENDED
            return True
    
    async def resume_key(self, key_id: str) -> bool:
        """Resume a suspended API key.
        
        Args:
            key_id: Key ID to resume
            
        Returns:
            True if resumed
        """
        async with self._lock:
            if key_id not in self._keys:
                return False
            
            self._keys[key_id].status = KeyStatus.ACTIVE
            return True
    
    async def check_rate_limit(self, key_id: str) -> bool:
        """Check if request is within rate limit.
        
        Args:
            key_id: Key ID to check
            
        Returns:
            True if within limit, False if rate limited
        """
        async with self._lock:
            if key_id not in self._keys:
                return False
            
            api_key = self._keys[key_id]
            now = time.time()
            
            timestamps = self._rate_limiters[key_id]
            window_start = now - api_key.rate_window_seconds
            timestamps = [t for t in timestamps if t > window_start]
            self._rate_limiters[key_id] = timestamps
            
            if len(timestamps) >= api_key.rate_limit:
                self._metrics["rate_limited_requests"] += 1
                return False
            
            timestamps.append(now)
            return True
    
    async def record_usage(self, usage: KeyUsage) -> None:
        """Record API key usage.
        
        Args:
            usage: Usage record to save
        """
        async with self._lock:
            self._usage_records[usage.key_id].append(usage)
            
            if usage.status_code >= 400:
                self._metrics["failed_requests"] += 1
            else:
                self._metrics["successful_requests"] += 1
            
            self._metrics["total_requests"] += 1
    
    def _hash_key(self, plaintext_key: str) -> str:
        """Hash a plaintext key.
        
        Args:
            plaintext_key: Key to hash
            
        Returns:
            Hashed key
        """
        if self.hash_algorithm == "sha256":
            return hashlib.sha256(plaintext_key.encode()).hexdigest()
        elif self.hash_algorithm == "sha512":
            return hashlib.sha512(plaintext_key.encode()).hexdigest()
        else:
            return hashlib.pbkdf2_hmac(
                self.hash_algorithm,
                plaintext_key.encode(),
                b"salt_placeholder",
                100000
            ).hex()
    
    def _hash_key_old(self, plaintext_key: str) -> str:
        """Legacy hash method."""
        return hashlib.sha256(plaintext_key.encode()).hexdigest()
    
    async def get_key_info(self, key_id: str) -> Optional[Dict[str, Any]]:
        """Get API key information.
        
        Args:
            key_id: Key ID
            
        Returns:
            Key information dictionary
        """
        async with self._lock:
            if key_id not in self._keys:
                return None
            
            api_key = self._keys[key_id]
            return {
                "key_id": api_key.key_id,
                "name": api_key.name,
                "key_prefix": api_key.key_prefix,
                "key_type": api_key.key_type.name,
                "status": api_key.status.name,
                "scopes": list(api_key.scopes),
                "created_at": datetime.fromtimestamp(api_key.created_at).isoformat(),
                "last_used": datetime.fromtimestamp(api_key.last_used).isoformat() if api_key.last_used else None,
                "expires_at": datetime.fromtimestamp(api_key.expires_at).isoformat() if api_key.expires_at else None,
                "rate_limit": api_key.rate_limit,
                "metadata": api_key.metadata
            }
    
    async def get_key_metrics(self, key_id: str) -> Optional[KeyMetrics]:
        """Get usage metrics for a specific key.
        
        Args:
            key_id: Key ID
            
        Returns:
            Key metrics
        """
        async with self._lock:
            records = self._usage_records.get(key_id, [])
            
            if not records:
                return KeyMetrics()
            
            metrics = KeyMetrics()
            
            for record in records:
                metrics.total_requests += 1
                
                if record.status_code >= 400:
                    metrics.failed_requests += 1
                else:
                    metrics.successful_requests += 1
                
                metrics.total_bytes_sent += record.bytes_sent
                metrics.total_bytes_received += record.bytes_received
            
            if records:
                response_times = [r.response_time_ms for r in records]
                metrics.avg_response_time_ms = sum(response_times) / len(response_times)
            
            return metrics
    
    async def list_keys(self, status: Optional[KeyStatus] = None) -> List[Dict[str, Any]]:
        """List all API keys.
        
        Args:
            status: Optional status filter
            
        Returns:
            List of key info dictionaries
        """
        async with self._lock:
            keys = list(self._keys.values())
            
            if status:
                keys = [k for k in keys if k.status == status]
            
            return [
                {
                    "key_id": k.key_id,
                    "name": k.name,
                    "key_prefix": k.key_prefix,
                    "status": k.status.name,
                    "created_at": datetime.fromtimestamp(k.created_at).isoformat()
                }
                for k in keys
            ]
    
    async def rotate_key(self, key_id: str) -> Optional[tuple[str, APIKey]]:
        """Rotate an API key, generating a new key with same permissions.
        
        Args:
            key_id: Key ID to rotate
            
        Returns:
            Tuple of (new plaintext key, APIKey object) or None
        """
        async with self._lock:
            old_key = self._keys.get(key_id)
            if not old_key:
                return None
        
        expires_at = None
        if old_key.expires_at:
            expires_at = old_key.expires_at - time.time()
            expires_at = expires_at / 86400
        
        return await self.generate_key(
            name=old_key.name,
            key_type=old_key.key_type,
            scopes=old_key.scopes,
            expires_in_days=int(expires_at) if expires_at else None,
            rate_limit=old_key.rate_limit,
            metadata=old_key.metadata
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get key manager metrics."""
        return {
            **self._metrics,
            "keys_stored": len(self._keys)
        }
