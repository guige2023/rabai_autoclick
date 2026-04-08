"""API Cache action module for RabAI AutoClick.

Caching layer for API responses with TTL, invalidation,
and stale-while-revalidate support.
"""

import json
import time
import hashlib
import sys
import os
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiCacheAction(BaseAction):
    """Cache API responses with TTL and invalidation.

    In-memory cache with configurable TTL, key generation,
    and cache-aside pattern support.
    """
    action_type = "api_cache"
    display_name = "API缓存"
    description = "带TTL和失效机制的API响应缓存"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage API cache.

        Args:
            context: Execution context.
            params: Dict with keys: action (get/set/invalidate/clear),
                   key, value, ttl_seconds, stale_while_revalidate.

        Returns:
            ActionResult with cache result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'get')
            key = params.get('key', '')
            value = params.get('value')
            ttl_seconds = params.get('ttl_seconds', 300)
            cache_id = params.get('cache_id', 'default')

            if not hasattr(context, '_api_caches'):
                context._api_caches = {}
            caches = context._api_caches
            if cache_id not in caches:
                caches[cache_id] = {}

            cache = caches[cache_id]

            if action == 'set':
                if not key:
                    return ActionResult(success=False, message="key is required", duration=time.time() - start_time)
                cache[key] = {
                    'value': value,
                    'expires_at': time.time() + ttl_seconds,
                    'created_at': time.time(),
                    'hits': 0,
                }
                return ActionResult(
                    success=True,
                    message=f"Cached key '{key}' for {ttl_seconds}s",
                    data={'key': key, 'ttl_seconds': ttl_seconds},
                    duration=time.time() - start_time,
                )

            elif action == 'get':
                if not key:
                    return ActionResult(success=False, message="key is required", duration=time.time() - start_time)
                entry = cache.get(key)
                if not entry:
                    return ActionResult(
                        success=False,
                        message=f"Cache miss: {key}",
                        data={'key': key, 'hit': False},
                        duration=time.time() - start_time,
                    )
                if entry['expires_at'] < time.time():
                    del cache[key]
                    return ActionResult(
                        success=False,
                        message=f"Cache expired: {key}",
                        data={'key': key, 'hit': False, 'expired': True},
                        duration=time.time() - start_time,
                    )
                entry['hits'] += 1
                return ActionResult(
                    success=True,
                    message=f"Cache hit: {key}",
                    data={
                        'key': key,
                        'hit': True,
                        'value': entry['value'],
                        'hits': entry['hits'],
                        'age_seconds': time.time() - entry['created_at'],
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'invalidate':
                if key:
                    if key in cache:
                        del cache[key]
                    return ActionResult(success=True, message=f"Invalidated: {key}", duration=time.time() - start_time)
                pattern = params.get('pattern', '')
                if pattern:
                    import re
                    regex = pattern.replace('*', '.*')
                    to_delete = [k for k in cache if re.match(regex, k)]
                    for k in to_delete:
                        del cache[k]
                    return ActionResult(success=True, message=f"Invalidated {len(to_delete)} keys matching {pattern}", data={'count': len(to_delete)}, duration=time.time() - start_time)
                return ActionResult(success=False, message="key or pattern required", duration=time.time() - start_time)

            elif action == 'clear':
                count = len(cache)
                cache.clear()
                return ActionResult(success=True, message=f"Cleared {count} entries", data={'count': count}, duration=time.time() - start_time)

            elif action == 'stats':
                total = len(cache)
                expired = sum(1 for e in cache.values() if e['expires_at'] < time.time())
                total_hits = sum(e['hits'] for e in cache.values())
                return ActionResult(success=True, message=f"Cache stats: {total} entries", data={'total': total, 'expired': expired, 'active': total - expired, 'total_hits': total_hits}, duration=time.time() - start_time)

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}", duration=time.time() - start_time)

        except Exception as e:
            return ActionResult(success=False, message=f"Cache error: {str(e)}", duration=time.time() - start_time)


class ApiStaleCacheAction(BaseAction):
    """Stale-while-revalidate cache for API responses.

    Serves stale data immediately while fetching
    fresh data in background.
    """
    action_type = "api_stale_cache"
    display_name = "API过期缓存"
    description = "过期重验证缓存模式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle stale-while-revalidate.

        Args:
            context: Execution context.
            params: Dict with keys: action (get/set),
                   key, value, ttl_seconds, stale_ttl_seconds,
                   fetch_fn.

        Returns:
            ActionResult with cache result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'get')
            key = params.get('key', '')
            value = params.get('value')
            ttl = params.get('ttl_seconds', 300)
            stale_ttl = params.get('stale_ttl_seconds', 600)
            fetch_fn = params.get('fetch_fn')
            cache_id = params.get('cache_id', 'stale_default')

            if not hasattr(context, '_stale_caches'):
                context._stale_caches = {}
            caches = context._stale_caches
            if cache_id not in caches:
                caches[cache_id] = {}
            cache = caches[cache_id]

            now = time.time()

            if action == 'set':
                if not key:
                    return ActionResult(success=False, message="key is required", duration=time.time() - start_time)
                cache[key] = {
                    'value': value,
                    'fresh_until': now + ttl,
                    'stale_until': now + stale_ttl,
                    'revalidating': False,
                }
                return ActionResult(success=True, message=f"Set stale cache: {key}", data={'key': key}, duration=time.time() - start_time)

            elif action == 'get':
                if not key:
                    return ActionResult(success=False, message="key is required", duration=time.time() - start_time)
                entry = cache.get(key)
                if not entry:
                    if callable(fetch_fn):
                        fresh = fetch_fn(key, context)
                        cache[key] = {'value': fresh, 'fresh_until': now + ttl, 'stale_until': now + stale_ttl, 'revalidating': False}
                        return ActionResult(success=True, message="Fetched fresh data", data={'value': fresh, 'stale': False}, duration=time.time() - start_time)
                    return ActionResult(success=False, message="Cache miss", data={'key': key, 'hit': False}, duration=time.time() - start_time)

                if now < entry['fresh_until']:
                    return ActionResult(success=True, message="Fresh cache hit", data={'value': entry['value'], 'stale': False, 'fresh': True}, duration=time.time() - start_time)
                elif now < entry['stale_until'] and not entry['revalidating']:
                    entry['revalidating'] = True
                    return ActionResult(success=True, message="Serving stale, revalidating", data={'value': entry['value'], 'stale': True, 'revalidating': True}, duration=time.time() - start_time)
                else:
                    return ActionResult(success=False, message="Cache fully expired", data={'key': key, 'expired': True}, duration=time.time() - start_time)

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}", duration=time.time() - start_time)

        except Exception as e:
            return ActionResult(success=False, message=f"Stale cache error: {str(e)}", duration=time.time() - start_time)
