"""Cache action module for RabAI AutoClick.

Provides caching operations:
- CacheSetAction: Set cache value
- CacheGetAction: Get cache value
- CacheDeleteAction: Delete cache
- CacheExistsAction: Check if key exists
- CacheClearAction: Clear all cache
- CacheKeysAction: List all keys
- CacheTTLAction: Set TTL on key
"""

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SimpleCache:
    """Simple file-based cache."""

    def __init__(self, cache_dir: str = '/tmp/rabai_cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, key: str) -> Path:
        safe_key = key.replace('/', '_').replace(':', '_')
        return self.cache_dir / f"{safe_key}.json"

    def set(self, key: str, value: Any, ttl: int = 0) -> bool:
        """Set cache value."""
        path = self._get_path(key)

        data = {
            'value': value,
            'created': time.time(),
            'ttl': ttl,
            'expires': time.time() + ttl if ttl > 0 else 0
        }

        try:
            with open(path, 'w') as f:
                json.dump(data, f)
            return True
        except:
            return False

    def get(self, key: str) -> Optional[Any]:
        """Get cache value."""
        path = self._get_path(key)

        if not path.exists():
            return None

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            if data.get('expires', 0) > 0 and time.time() > data['expires']:
                path.unlink()
                return None

            return data.get('value')
        except:
            return None

    def delete(self, key: str) -> bool:
        """Delete cache key."""
        path = self._get_path(key)
        if path.exists():
            path.unlink()
        return True

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        path = self._get_path(key)
        if not path.exists():
            return False

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            if data.get('expires', 0) > 0 and time.time() > data['expires']:
                path.unlink()
                return False

            return True
        except:
            return False

    def clear(self) -> int:
        """Clear all cache."""
        count = 0
        for f in self.cache_dir.glob('*.json'):
            f.unlink()
            count += 1
        return count

    def keys(self) -> List[str]:
        """List all keys."""
        keys = []
        for f in self.cache_dir.glob('*.json'):
            key = f.stem.replace('_', '/')
            keys.append(key)
        return keys

    def set_ttl(self, key: str, ttl: int) -> bool:
        """Set TTL on key."""
        path = self._get_path(key)

        if not path.exists():
            return False

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            data['ttl'] = ttl
            data['expires'] = time.time() + ttl if ttl > 0 else 0

            with open(path, 'w') as f:
                json.dump(data, f)

            return True
        except:
            return False


class CacheSetAction(BaseAction):
    """Set cache value."""
    action_type = "cache_set"
    display_name = "缓存设置"
    description = "设置缓存值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with key, value, ttl, cache_dir.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        value = params.get('value', '')
        ttl = params.get('ttl', 0)
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)
            resolved_ttl = context.resolve_value(ttl)
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            cache.set(resolved_key, resolved_value, int(resolved_ttl))

            return ActionResult(
                success=True,
                message=f"缓存已设置: {resolved_key}",
                data={'key': resolved_key, 'ttl': resolved_ttl}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"缓存设置失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ttl': 0, 'cache_dir': '/tmp/rabai_cache'}


class CacheGetAction(BaseAction):
    """Get cache value."""
    action_type = "cache_get"
    display_name = "缓存获取"
    description = "获取缓存值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with key, default, output_var, cache_dir.

        Returns:
            ActionResult with value.
        """
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'cache_value')
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            value = cache.get(resolved_key)

            if value is None:
                value = resolved_default

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"缓存获取: {'命中' if value != resolved_default else '未命中'}",
                data={'key': resolved_key, 'value': value, 'hit': value != resolved_default, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"缓存获取失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'cache_value', 'cache_dir': '/tmp/rabai_cache'}


class CacheDeleteAction(BaseAction):
    """Delete cache key."""
    action_type = "cache_delete"
    display_name = "缓存删除"
    description = "删除缓存键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with key, cache_dir.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            cache.delete(resolved_key)

            return ActionResult(
                success=True,
                message=f"缓存已删除: {resolved_key}",
                data={'key': resolved_key}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"缓存删除失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'cache_dir': '/tmp/rabai_cache'}


class CacheExistsAction(BaseAction):
    """Check if cache key exists."""
    action_type = "cache_exists"
    display_name = "缓存存在"
    description = "检查缓存是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exists.

        Args:
            context: Execution context.
            params: Dict with key, output_var, cache_dir.

        Returns:
            ActionResult with exists flag.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'cache_exists')
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            exists = cache.exists(resolved_key)

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"缓存{'存在' if exists else '不存在'}: {resolved_key}",
                data={'key': resolved_key, 'exists': exists, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"缓存检查失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cache_exists', 'cache_dir': '/tmp/rabai_cache'}


class CacheClearAction(BaseAction):
    """Clear all cache."""
    action_type = "cache_clear"
    display_name = "清空缓存"
    description = "清空所有缓存"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with cache_dir.

        Returns:
            ActionResult with count.
        """
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        try:
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            count = cache.clear()

            return ActionResult(
                success=True,
                message=f"缓存已清空: {count} 个键",
                data={'cleared': count}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"清空缓存失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'cache_dir': '/tmp/rabai_cache'}


class CacheKeysAction(BaseAction):
    """List all cache keys."""
    action_type = "cache_keys"
    display_name = "缓存键列表"
    description = "列出所有缓存键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keys.

        Args:
            context: Execution context.
            params: Dict with output_var, cache_dir.

        Returns:
            ActionResult with key list.
        """
        output_var = params.get('output_var', 'cache_keys')
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        try:
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            keys = cache.keys()

            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"缓存键: {len(keys)} 个",
                data={'count': len(keys), 'keys': keys, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"列出缓存键失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cache_keys', 'cache_dir': '/tmp/rabai_cache'}


class CacheTTLAction(BaseAction):
    """Set TTL on cache key."""
    action_type = "cache_ttl"
    display_name = "缓存TTL"
    description = "设置缓存过期时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute TTL.

        Args:
            context: Execution context.
            params: Dict with key, ttl, output_var, cache_dir.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        ttl = params.get('ttl', 3600)
        output_var = params.get('output_var', 'cache_ttl_set')
        cache_dir = params.get('cache_dir', '/tmp/rabai_cache')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_ttl = context.resolve_value(ttl)
            resolved_dir = context.resolve_value(cache_dir)

            cache = SimpleCache(resolved_dir)
            success = cache.set_ttl(resolved_key, int(resolved_ttl))

            context.set(output_var, success)

            return ActionResult(
                success=True,
                message=f"TTL已设置: {resolved_key} = {resolved_ttl}s",
                data={'key': resolved_key, 'ttl': resolved_ttl, 'success': success, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"设置TTL失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'ttl']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cache_ttl_set', 'cache_dir': '/tmp/rabai_cache'}
