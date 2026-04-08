"""API Idempotency Action Module for RabAI AutoClick.

Ensures API requests can be safely retried by attaching idempotency
keys to prevent duplicate operations on the server side.
"""

import hashlib
import json
import time
import uuid
import sys
import os
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiIdempotencyAction(BaseAction):
    """Manage idempotent API requests with unique keys.

    Generates and manages idempotency keys that ensure the same
    request produces the same result, even when retried.
    Supports key caching, TTL expiration, and deduplication.
    """
    action_type = "api_idempotency"
    display_name = "幂等性API请求"
    description = "带幂等性Key的API请求，防止重复操作"

    IDEMPOTENCY_STORE: Dict[str, Dict[str, Any]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute idempotent API request.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - operation name (e.g., 'payment', 'refund')
                - payload: dict - request payload
                - idempotency_key: str (optional) - explicit key, auto-generated if None
                - ttl: int (optional) - key TTL in seconds, default 86400
                - store: str (optional) - 'memory' or 'context', default 'memory'

        Returns:
            ActionResult with idempotency key and cached response if found.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'unknown')
            payload = params.get('payload', {})
            explicit_key = params.get('idempotency_key')
            ttl = params.get('ttl', 86400)
            store_type = params.get('store', 'memory')

            if explicit_key:
                idempotency_key = explicit_key
            else:
                key_parts = [
                    operation,
                    json.dumps(payload, sort_keys=True),
                    str(time.time())[:10]
                ]
                idempotency_key = hashlib.sha256(
                    '|'.join(key_parts).encode()
                ).hexdigest()[:32]

            cache_key = f"{operation}:{idempotency_key}"
            cached = self._get_cached(cache_key, ttl)

            if cached is not None:
                return ActionResult(
                    success=True,
                    message=f"Idempotent operation returned cached result: {idempotency_key}",
                    data={
                        'idempotency_key': idempotency_key,
                        'cached': True,
                        'result': cached
                    },
                    duration=time.time() - start_time
                )

            result_data = {
                'operation': operation,
                'payload': payload,
                'idempotency_key': idempotency_key,
                'timestamp': time.time(),
                'retried': 0
            }

            self._store_result(cache_key, result_data, ttl)

            return ActionResult(
                success=True,
                message=f"Idempotent operation stored: {idempotency_key}",
                data={
                    'idempotency_key': idempotency_key,
                    'cached': False,
                    'result': result_data
                },
                duration=time.time() - start_time
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Idempotency action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _get_cached(self, cache_key: str, ttl: int) -> Optional[Dict[str, Any]]:
        """Retrieve cached result if not expired."""
        if cache_key in self.IDEMPOTENCY_STORE:
            entry = self.IDEMPOTENCY_STORE[cache_key]
            if time.time() - entry.get('stored_at', 0) < entry.get('ttl', ttl):
                return entry.get('result')
            else:
                del self.IDEMPOTENCY_STORE[cache_key]
        return None

    def _store_result(self, cache_key: str, result: Dict[str, Any], ttl: int) -> None:
        """Store result with TTL."""
        self.IDEMPOTENCY_STORE[cache_key] = {
            'result': result,
            'stored_at': time.time(),
            'ttl': ttl
        }

    def clear_expired(self) -> int:
        """Remove expired idempotency entries.

        Returns:
            Number of entries removed.
        """
        now = time.time()
        expired_keys = [
            k for k, v in self.IDEMPOTENCY_STORE.items()
            if now - v.get('stored_at', 0) >= v.get('ttl', 86400)
        ]
        for k in expired_keys:
            del self.IDEMPOTENCY_STORE[k]
        return len(expired_keys)

    @classmethod
    def get_store_size(cls) -> int:
        """Return current store size."""
        return len(cls.IDEMPOTENCY_STORE)
