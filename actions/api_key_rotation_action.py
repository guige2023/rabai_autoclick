"""API Key Rotation Action Module for RabAI AutoClick.

Automatic API key rotation with expiration tracking,
staggered roll-over, and backup key management.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class KeyInfo:
    """API key metadata."""
    key_id: str
    key_value: str
    created_at: float
    expires_at: float
    is_active: bool
    is_primary: bool
    usage_count: int = 0


class ApiKeyRotationAction(BaseAction):
    """Automatic API key rotation management.

    Manages rotation of API keys with support for primary/backup
    key pairs, staggered rotation, and automatic rollover based
    on expiration time or usage limits.
    """
    action_type = "api_key_rotation"
    display_name = "API密钥轮换"
    description = "API密钥自动轮换，过期跟踪和备份管理"

    _key_sets: Dict[str, Dict[str, Any]] = {}
    _rotation_hooks: List[Dict[str, Any]] = []

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute key rotation operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create_set', 'add_key', 'get_active',
                               'rotate', 'revoke', 'list_sets', 'stats'
                - set_name: str - name of the key set
                - key_value: str (optional) - API key value
                - key_id: str (optional) - key identifier
                - ttl: float (optional) - key TTL in seconds
                - max_usage: int (optional) - max usage before rotation

        Returns:
            ActionResult with key rotation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'get_active')

            if operation == 'create_set':
                return self._create_key_set(params, start_time)
            elif operation == 'add_key':
                return self._add_key(params, start_time)
            elif operation == 'get_active':
                return self._get_active_key(params, start_time)
            elif operation == 'rotate':
                return self._rotate_key(params, start_time)
            elif operation == 'revoke':
                return self._revoke_key(params, start_time)
            elif operation == 'list_sets':
                return self._list_key_sets(start_time)
            elif operation == 'stats':
                return self._get_stats(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Key rotation action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_key_set(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new key rotation set."""
        set_name = params.get('set_name', 'default')
        rotation_strategy = params.get('rotation_strategy', 'ttl')
        ttl = params.get('ttl', 86400)
        max_usage = params.get('max_usage', 10000)

        if set_name in self._key_sets:
            return ActionResult(
                success=True,
                message=f"Key set already exists: {set_name}",
                data={'set_name': set_name, 'created': False},
                duration=time.time() - start_time
            )

        self._key_sets[set_name] = {
            'name': set_name,
            'keys': {},
            'primary_key_id': None,
            'rotation_strategy': rotation_strategy,
            'ttl': ttl,
            'max_usage': max_usage,
            'created_at': time.time(),
            'rotation_count': 0
        }

        return ActionResult(
            success=True,
            message=f"Key set created: {set_name}",
            data={
                'set_name': set_name,
                'rotation_strategy': rotation_strategy,
                'ttl': ttl
            },
            duration=time.time() - start_time
        )

    def _add_key(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a key to a set."""
        set_name = params.get('set_name', 'default')
        key_value = params.get('key_value', '')
        key_id = params.get('key_id', f'key_{time.time()}')
        ttl = params.get('ttl')
        is_primary = params.get('is_primary', False)

        if set_name not in self._key_sets:
            self._create_key_set({'set_name': set_name}, start_time)

        key_set = self._key_sets[set_name]

        if not ttl:
            ttl = key_set['ttl']

        key_info = KeyInfo(
            key_id=key_id,
            key_value=key_value,
            created_at=time.time(),
            expires_at=time.time() + ttl,
            is_active=True,
            is_primary=is_primary,
            usage_count=0
        )

        key_set['keys'][key_id] = key_info

        if is_primary or key_set['primary_key_id'] is None:
            key_set['primary_key_id'] = key_id

        return ActionResult(
            success=True,
            message=f"Key added to set: {set_name}",
            data={
                'set_name': set_name,
                'key_id': key_id,
                'is_primary': is_primary,
                'expires_at': key_info.expires_at
            },
            duration=time.time() - start_time
        )

    def _get_active_key(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get the currently active key for a set."""
        set_name = params.get('set_name', 'default')

        if set_name not in self._key_sets:
            return ActionResult(
                success=False,
                message=f"Key set not found: {set_name}",
                duration=time.time() - start_time
            )

        key_set = self._key_sets[set_name]
        primary_key_id = key_set['primary_key_id']

        if not primary_key_id or primary_key_id not in key_set['keys']:
            active_keys = [k for k in key_set['keys'].values() if k.is_active]
            if active_keys:
                primary_key_id = active_keys[0].key_id
            else:
                return ActionResult(
                    success=False,
                    message="No active keys in set",
                    duration=time.time() - start_time
                )

        key_info = key_set['keys'][primary_key_id]

        if not key_info.is_active or key_info.expires_at < time.time():
            return ActionResult(
                success=False,
                message="Primary key expired or inactive",
                data={
                    'set_name': set_name,
                    'key_id': primary_key_id,
                    'expired': True
                },
                duration=time.time() - start_time
            )

        key_info.usage_count += 1

        should_rotate = self._should_rotate(key_set, key_info)
        if should_rotate:
            self._auto_rotate(set_name)

        return ActionResult(
            success=True,
            message=f"Active key: {primary_key_id}",
            data={
                'set_name': set_name,
                'key_id': primary_key_id,
                'key_value': key_info.key_value[:8] + '...' if key_info.key_value else None,
                'usage_count': key_info.usage_count,
                'expires_at': key_info.expires_at
            },
            duration=time.time() - start_time
        )

    def _rotate_key(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Manually rotate to a new key."""
        set_name = params.get('set_name', 'default')
        new_key_value = params.get('new_key_value', f'rotated_key_{time.time()}')
        keep_old = params.get('keep_old', True)

        if set_name not in self._key_sets:
            return ActionResult(
                success=False,
                message=f"Key set not found: {set_name}",
                duration=time.time() - start_time
            )

        key_set = self._key_sets[set_name]
        old_primary_id = key_set['primary_key_id']

        new_key_id = f'key_{time.time()}'
        key_info = KeyInfo(
            key_id=new_key_id,
            key_value=new_key_value,
            created_at=time.time(),
            expires_at=time.time() + key_set['ttl'],
            is_active=True,
            is_primary=True,
            usage_count=0
        )

        key_set['keys'][new_key_id] = key_info
        key_set['primary_key_id'] = new_key_id
        key_set['rotation_count'] += 1

        if not keep_old and old_primary_id and old_primary_id in key_set['keys']:
            key_set['keys'][old_primary_id].is_active = False
            key_set['keys'][old_primary_id].is_primary = False

        return ActionResult(
            success=True,
            message=f"Key rotated: {set_name}",
            data={
                'set_name': set_name,
                'old_key_id': old_primary_id,
                'new_key_id': new_key_id,
                'rotation_count': key_set['rotation_count']
            },
            duration=time.time() - start_time
        )

    def _revoke_key(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Revoke a specific key."""
        set_name = params.get('set_name', 'default')
        key_id = params.get('key_id', '')

        if set_name not in self._key_sets:
            return ActionResult(
                success=False,
                message=f"Key set not found: {set_name}",
                duration=time.time() - start_time
            )

        key_set = self._key_sets[set_name]

        if key_id not in key_set['keys']:
            return ActionResult(
                success=False,
                message=f"Key not found: {key_id}",
                duration=time.time() - start_time
            )

        key_set['keys'][key_id].is_active = False
        key_set['keys'][key_id].is_primary = False

        if key_set['primary_key_id'] == key_id:
            active_keys = [k for k in key_set['keys'].values() if k.is_active]
            if active_keys:
                key_set['primary_key_id'] = active_keys[0].key_id
            else:
                key_set['primary_key_id'] = None

        return ActionResult(
            success=True,
            message=f"Key revoked: {key_id}",
            data={'set_name': set_name, 'key_id': key_id},
            duration=time.time() - start_time
        )

    def _list_key_sets(self, start_time: float) -> ActionResult:
        """List all key sets."""
        sets = []
        for name, key_set in self._key_sets.items():
            active_count = sum(1 for k in key_set['keys'].values() if k.is_active)
            sets.append({
                'name': name,
                'total_keys': len(key_set['keys']),
                'active_keys': active_count,
                'primary_key_id': key_set['primary_key_id'],
                'rotation_count': key_set['rotation_count']
            })

        return ActionResult(
            success=True,
            message=f"Key sets: {len(sets)}",
            data={'sets': sets, 'count': len(sets)},
            duration=time.time() - start_time
        )

    def _get_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get rotation statistics."""
        set_name = params.get('set_name', 'default')

        if set_name not in self._key_sets:
            return ActionResult(
                success=False,
                message=f"Key set not found: {set_name}",
                duration=time.time() - start_time
            )

        key_set = self._key_sets[set_name]

        return ActionResult(
            success=True,
            message=f"Key set stats: {set_name}",
            data={
                'set_name': set_name,
                'rotation_count': key_set['rotation_count'],
                'total_keys': len(key_set['keys']),
                'strategy': key_set['rotation_strategy']
            },
            duration=time.time() - start_time
        )

    def _should_rotate(self, key_set: Dict[str, Any], key_info: KeyInfo) -> bool:
        """Check if key should be rotated."""
        strategy = key_set['rotation_strategy']

        if strategy == 'ttl':
            return key_info.expires_at < time.time()
        elif strategy == 'usage':
            return key_info.usage_count >= key_set['max_usage']
        elif strategy == 'ttl_or_usage':
            return (key_info.expires_at < time.time() or
                    key_info.usage_count >= key_set['max_usage'])

        return False

    def _auto_rotate(self, set_name: str) -> None:
        """Automatically rotate key."""
        key_set = self._key_sets[set_name]
        self._rotate_key({
            'set_name': set_name,
            'new_key_value': f'auto_rotated_{time.time()}',
            'keep_old': True
        }, time.time())
