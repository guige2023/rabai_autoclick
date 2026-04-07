"""Rate limit action module for RabAI AutoClick.

Provides rate limiting operations:
- RateLimitCheckAction: Check if action is allowed
- RateLimitWaitAction: Wait until action is allowed
- RateLimitResetAction: Reset rate limit
- RateLimitStatusAction: Get rate limit status
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


class RateLimitStore:
    """Simple file-based rate limit store."""

    def __init__(self, store_dir: str = '/tmp/rabai_rate_limits'):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self._cache = {}

    def _get_key_path(self, key: str) -> Path:
        safe_key = key.replace('/', '_').replace(':', '_')
        return self.store_dir / f"{safe_key}.json"

    def check(self, key: str, max_calls: int, window_seconds: int) -> tuple[bool, int, float]:
        """Check if action is allowed.

        Returns:
            (allowed, remaining_calls, reset_time)
        """
        path = self._get_key_path(key)

        now = time.time()
        data = {'calls': [], 'max_calls': max_calls, 'window': window_seconds}

        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
            except:
                pass

        data['max_calls'] = max_calls
        data['window'] = window_seconds

        data['calls'] = [c for c in data['calls'] if now - c < window_seconds]

        if len(data['calls']) < max_calls:
            data['calls'].append(now)
            with open(path, 'w') as f:
                json.dump(data, f)
            remaining = max_calls - len(data['calls'])
            return True, remaining, 0
        else:
            oldest = min(data['calls'])
            reset_time = oldest + window_seconds - now
            return False, 0, reset_time

    def reset(self, key: str) -> bool:
        """Reset rate limit."""
        path = self._get_key_path(key)
        if path.exists():
            path.unlink()
        return True

    def get_status(self, key: str) -> dict:
        """Get rate limit status."""
        path = self._get_key_path(key)
        now = time.time()

        if not path.exists():
            return {'remaining': -1, 'reset_in': 0, 'calls': 0}

        try:
            with open(path, 'r') as f:
                data = json.load(f)

            data['calls'] = [c for c in data['calls'] if now - c < data['window']]

            remaining = max(0, data['max_calls'] - len(data['calls']))
            reset_in = 0
            if data['calls']:
                oldest = min(data['calls'])
                reset_in = max(0, oldest + data['window'] - now)

            with open(path, 'w') as f:
                json.dump(data, f)

            return {
                'remaining': remaining,
                'reset_in': reset_in,
                'calls': len(data['calls']),
                'max_calls': data['max_calls'],
                'window': data['window']
            }
        except:
            return {'remaining': -1, 'reset_in': 0, 'calls': 0}


class RateLimitCheckAction(BaseAction):
    """Check if action is allowed."""
    action_type = "rate_limit_check"
    display_name = "限流检查"
    description = "检查是否允许执行"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check.

        Args:
            context: Execution context.
            params: Dict with key, max_calls, window, output_var, store_dir.

        Returns:
            ActionResult with allowed status.
        """
        key = params.get('key', '')
        max_calls = params.get('max_calls', 10)
        window = params.get('window', 60)
        output_var = params.get('output_var', 'rate_allowed')
        store_dir = params.get('store_dir', '/tmp/rabai_rate_limits')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_max = context.resolve_value(max_calls)
            resolved_window = context.resolve_value(window)
            resolved_dir = context.resolve_value(store_dir)

            unique_key = f"{resolved_key}_{uuid.uuid4().hex[:8]}"

            store = RateLimitStore(resolved_dir)
            allowed, remaining, reset_time = store.check(unique_key, int(resolved_max), int(resolved_window))

            context.set(output_var, allowed)

            return ActionResult(
                success=True,
                message=f"限流检查: {'允许' if allowed else '限制'}",
                data={
                    'allowed': allowed,
                    'remaining': remaining,
                    'reset_in': reset_time,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"限流检查失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'max_calls', 'window']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rate_allowed', 'store_dir': '/tmp/rabai_rate_limits'}


class RateLimitWaitAction(BaseAction):
    """Wait until action is allowed."""
    action_type = "rate_limit_wait"
    display_name = "限流等待"
    description = "等待直到允许执行"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute wait.

        Args:
            context: Execution context.
            params: Dict with key, max_calls, window, timeout, output_var, store_dir.

        Returns:
            ActionResult with wait result.
        """
        key = params.get('key', '')
        max_calls = params.get('max_calls', 10)
        window = params.get('window', 60)
        timeout = params.get('timeout', 300)
        output_var = params.get('output_var', 'rate_wait_result')
        store_dir = params.get('store_dir', '/tmp/rabai_rate_limits')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_max = context.resolve_value(max_calls)
            resolved_window = context.resolve_value(window)
            resolved_timeout = context.resolve_value(timeout)
            resolved_dir = context.resolve_value(store_dir)

            unique_key = f"{resolved_key}_{uuid.uuid4().hex[:8]}"

            store = RateLimitStore(resolved_dir)
            start_time = time.time()

            while True:
                allowed, remaining, reset_time = store.check(unique_key, int(resolved_max), int(resolved_window))

                if allowed:
                    context.set(output_var, True)
                    return ActionResult(
                        success=True,
                        message="限流等待完成",
                        data={'allowed': True, 'output_var': output_var}
                    )

                elapsed = time.time() - start_time
                if elapsed > resolved_timeout:
                    context.set(output_var, False)
                    return ActionResult(
                        success=False,
                        message=f"限流等待超时 ({resolved_timeout}s)",
                        data={'allowed': False, 'timeout': True, 'output_var': output_var}
                    )

                wait_time = min(reset_time + 0.1, 1)
                time.sleep(min(wait_time, 5))

        except Exception as e:
            return ActionResult(success=False, message=f"限流等待失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key', 'max_calls', 'window']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 300, 'output_var': 'rate_wait_result', 'store_dir': '/tmp/rabai_rate_limits'}


class RateLimitResetAction(BaseAction):
    """Reset rate limit."""
    action_type = "rate_limit_reset"
    display_name = "重置限流"
    description = "重置限流"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reset.

        Args:
            context: Execution context.
            params: Dict with key, store_dir.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')
        store_dir = params.get('store_dir', '/tmp/rabai_rate_limits')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_dir = context.resolve_value(store_dir)

            store = RateLimitStore(resolved_dir)
            store.reset(resolved_key)

            return ActionResult(
                success=True,
                message=f"限流已重置: {resolved_key}",
                data={'key': resolved_key}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"重置限流失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'store_dir': '/tmp/rabai_rate_limits'}


class RateLimitStatusAction(BaseAction):
    """Get rate limit status."""
    action_type = "rate_limit_status"
    display_name = "限流状态"
    description = "获取限流状态"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute status.

        Args:
            context: Execution context.
            params: Dict with key, output_var, store_dir.

        Returns:
            ActionResult with status.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'rate_limit_status')
        store_dir = params.get('store_dir', '/tmp/rabai_rate_limits')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_dir = context.resolve_value(store_dir)

            store = RateLimitStore(resolved_dir)
            status = store.get_status(resolved_key)

            context.set(output_var, status)

            return ActionResult(
                success=True,
                message=f"限流状态: {status.get('remaining', -1)} 剩余",
                data={**status, 'key': resolved_key, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取限流状态失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rate_limit_status', 'store_dir': '/tmp/rabai_rate_limits'}
