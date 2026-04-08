# Copyright (c) 2024. coded by claude
"""API Key Rotation Action Module.

Manages automatic rotation of API keys with support for
multiple providers, rotation schedules, and key lifecycle management.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class RotationStrategy(Enum):
    TIME_BASED = "time_based"
    USAGE_BASED = "usage_based"
    MANUAL = "manual"


@dataclass
class APIKey:
    key_id: str
    key_value: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    usage_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RotationConfig:
    strategy: RotationStrategy
    rotation_interval_days: Optional[int] = None
    max_usage: Optional[int] = None
    grace_period_hours: int = 24
    pre_rotation_callback: Optional[Callable] = None
    post_rotation_callback: Optional[Callable] = None


@dataclass
class RotationResult:
    success: bool
    old_key_id: Optional[str]
    new_key_id: Optional[str]
    error: Optional[str] = None


class APIKeyRotation:
    def __init__(self):
        self._keys: Dict[str, List[APIKey]] = {}
        self._configs: Dict[str, RotationConfig] = {}
        self._rotation_task: Optional[asyncio.Task] = None
        self._running = False

    def register_key_set(self, provider: str, config: RotationConfig) -> None:
        self._configs[provider] = config
        self._keys[provider] = []

    def add_key(self, provider: str, key: APIKey) -> None:
        if provider not in self._keys:
            self._keys[provider] = []
        self._keys[provider].append(key)

    async def rotate_key(self, provider: str) -> RotationResult:
        if provider not in self._configs or provider not in self._keys:
            return RotationResult(success=False, old_key_id=None, new_key_id=None, error="Provider not registered")
        config = self._configs[provider]
        keys = self._keys[provider]
        if not keys:
            return RotationResult(success=False, old_key_id=None, new_key_id=None, error="No keys available")
        current_key = keys[-1]
        try:
            if config.pre_rotation_callback:
                await config.pre_rotation_callback(provider, current_key)
            new_key_id = f"{provider}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            new_key = APIKey(
                key_id=new_key_id,
                key_value=current_key.key_value,
                created_at=datetime.now(),
                expires_at=datetime.now() + timedelta(days=config.rotation_interval_days or 90) if config.rotation_interval_days else None,
            )
            keys.append(new_key)
            if config.post_rotation_callback:
                await config.post_rotation_callback(provider, new_key)
            return RotationResult(success=True, old_key_id=current_key.key_id, new_key_id=new_key_id)
        except Exception as e:
            logger.error(f"Key rotation failed: {e}")
            return RotationResult(success=False, old_key_id=None, new_key_id=None, error=str(e))

    async def check_and_rotate(self, provider: str) -> Optional[RotationResult]:
        if provider not in self._configs or provider not in self._keys:
            return None
        config = self._configs[provider]
        keys = self._keys[provider]
        if not keys:
            return None
        current_key = keys[-1]
        should_rotate = False
        if config.strategy == RotationStrategy.TIME_BASED:
            if current_key.expires_at and datetime.now() >= current_key.expires_at - timedelta(hours=config.grace_period_hours):
                should_rotate = True
        elif config.strategy == RotationStrategy.USAGE_BASED:
            if config.max_usage and current_key.usage_count >= config.max_usage:
                should_rotate = True
        if should_rotate:
            return await self.rotate_key(provider)
        return None

    async def start_auto_rotation(self, check_interval: int = 3600) -> None:
        if self._running:
            return
        self._running = True
        self._rotation_task = asyncio.create_task(self._rotation_loop(check_interval))

    async def stop_auto_rotation(self) -> None:
        self._running = False
        if self._rotation_task:
            self._rotation_task.cancel()
            try:
                await self._rotation_task
            except asyncio.CancelledError:
                pass

    async def _rotation_loop(self, check_interval: int) -> None:
        while self._running:
            for provider in list(self._configs.keys()):
                await self.check_and_rotate(provider)
            await asyncio.sleep(check_interval)

    def get_current_key(self, provider: str) -> Optional[APIKey]:
        if provider in self._keys and self._keys[provider]:
            return self._keys[provider][-1]
        return None

    def record_usage(self, provider: str, usage_count: int = 1) -> None:
        key = self.get_current_key(provider)
        if key:
            key.usage_count += usage_count
            key.last_used = datetime.now()
