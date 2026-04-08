"""Trigger action module for RabAI AutoClick.

Provides trigger-based automation with various trigger types
including schedule, webhook, and event triggers.
"""

import time
import hashlib
import hmac
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TriggerCheckAction(BaseAction):
    """Check if trigger conditions are met.
    
    Evaluates trigger rules and returns match status.
    """
    action_type = "trigger_check"
    display_name = "触发器检查"
    description = "检查触发器条件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check trigger.
        
        Args:
            context: Execution context.
            params: Dict with keys: trigger_type, trigger_config,
                   event_data.
        
        Returns:
            ActionResult with trigger status.
        """
        trigger_type = params.get('trigger_type', 'manual')
        trigger_config = params.get('trigger_config', {})
        event_data = params.get('event_data', {})

        if trigger_type == 'manual':
            return ActionResult(
                success=True,
                message="Manual trigger always fires",
                data={'triggered': True, 'type': 'manual'}
            )

        elif trigger_type == 'schedule':
            return self._check_schedule(trigger_config)

        elif trigger_type == 'event':
            return self._check_event(trigger_config, event_data)

        elif trigger_type == 'webhook':
            return self._check_webhook(trigger_config, event_data)

        elif trigger_type == 'state_change':
            return self._check_state_change(trigger_config, event_data)

        return ActionResult(success=False, message=f"Unknown trigger type: {trigger_type}")

    def _check_schedule(self, config: Dict) -> ActionResult:
        """Check schedule trigger."""
        cron = config.get('cron', '')
        interval = config.get('interval', 0)
        last_run = config.get('last_run', 0)

        if cron:
            try:
                if self._matches_cron(cron):
                    return ActionResult(
                        success=True,
                        message="Schedule trigger matches",
                        data={'triggered': True, 'type': 'schedule', 'cron': cron}
                    )
            except:
                pass

        if interval > 0:
            elapsed = time.time() - last_run
            if elapsed >= interval:
                return ActionResult(
                    success=True,
                    message=f"Interval trigger: {elapsed:.0f}s elapsed",
                    data={'triggered': True, 'type': 'interval', 'elapsed': elapsed}
                )

        return ActionResult(
            success=True,
            message="Schedule trigger not matched",
            data={'triggered': False, 'type': 'schedule'}
        )

    def _check_event(self, config: Dict, event_data: Dict) -> ActionResult:
        """Check event trigger."""
        event_type = config.get('event_type', '')
        data_filter = config.get('filter', {})

        if event_type and event_data.get('type') != event_type:
            return ActionResult(
                success=True,
                message=f"Event type mismatch: expected {event_type}",
                data={'triggered': False, 'type': 'event'}
            )

        for key, expected in data_filter.items():
            actual = event_data.get(key)
            if actual != expected:
                return ActionResult(
                    success=True,
                    message=f"Event filter mismatch: {key}",
                    data={'triggered': False, 'type': 'event', 'mismatch': key}
                )

        return ActionResult(
            success=True,
            message="Event trigger matched",
            data={'triggered': True, 'type': 'event', 'event_data': event_data}
        )

    def _check_webhook(self, config: Dict, event_data: Dict) -> ActionResult:
        """Check webhook trigger."""
        secret = config.get('secret', '')
        expected_signature = config.get('signature', '')

        if secret and expected_signature:
            body = event_data.get('body', '')
            body_str = body if isinstance(body, str) else str(body)
            
            signature = hmac.new(
                secret.encode('utf-8'),
                body_str.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()

            if signature != expected_signature:
                return ActionResult(
                    success=False,
                    message="Webhook signature mismatch",
                    data={'triggered': False, 'type': 'webhook', 'error': 'invalid_signature'}
                )

        return ActionResult(
            success=True,
            message="Webhook trigger validated",
            data={'triggered': True, 'type': 'webhook'}
        )

    def _check_state_change(self, config: Dict, event_data: Dict) -> ActionResult:
        """Check state change trigger."""
        state_field = config.get('state_field', 'state')
        from_values = config.get('from_values', [])
        to_values = config.get('to_values', [])

        old_value = event_data.get(f'{state_field}_old')
        new_value = event_data.get(state_field)

        if from_values and old_value not in from_values:
            return ActionResult(
                success=True,
                message=f"State change: old value not in from_values",
                data={'triggered': False, 'type': 'state_change'}
            )

        if to_values and new_value not in to_values:
            return ActionResult(
                success=True,
                message=f"State change: new value not in to_values",
                data={'triggered': False, 'type': 'state_change'}
            )

        return ActionResult(
            success=True,
            message=f"State change triggered: {old_value} -> {new_value}",
            data={'triggered': True, 'type': 'state_change', 'from': old_value, 'to': new_value}
        )

    def _matches_cron(self, cron_expr: str) -> bool:
        """Simple cron matching."""
        import re
        parts = cron_expr.split()
        if len(parts) < 5:
            return False
        
        now = time.localtime()
        minute, hour, day, month, weekday = now.tm_min, now.tm_hour, now.tm_mday, now.tm_mon, now.tm_wday
        
        if not self._matches_cron_part(parts[0], minute):
            return False
        if not self._matches_cron_part(parts[1], hour):
            return False
        if not self._matches_cron_part(parts[2], day):
            return False
        if not self._matches_cron_part(parts[3], month):
            return False
        if not self._matches_cron_part(parts[4], weekday):
            return False
        
        return True

    def _matches_cron_part(self, pattern: str, value: int) -> bool:
        """Match cron pattern part."""
        if pattern == '*':
            return True
        if ',' in pattern:
            return str(value) in pattern.split(',')
        if '/' in pattern:
            base, step = pattern.split('/')
            step = int(step)
            return value % step == 0
        return str(value) == pattern


class TriggerRegisterAction(BaseAction):
    """Register trigger handler.
    
    Sets up trigger with callback and configuration.
    """
    action_type = "trigger_register"
    display_name = "注册触发器"
    description = "注册触发器处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Register trigger.
        
        Args:
            context: Execution context.
            params: Dict with keys: trigger_id, trigger_type,
                   trigger_config, handler.
        
        Returns:
            ActionResult with registration status.
        """
        trigger_id = params.get('trigger_id', '')
        trigger_type = params.get('trigger_type', 'manual')
        trigger_config = params.get('trigger_config', {})
        handler = params.get('handler', None)

        if not trigger_id:
            return ActionResult(success=False, message="trigger_id required")

        try:
            triggers = getattr(context, '_triggers', None)
            if triggers is None:
                context._triggers = {}

            triggers[trigger_id] = {
                'type': trigger_type,
                'config': trigger_config,
                'handler': handler,
                'enabled': True,
                'last_triggered': None,
                'trigger_count': 0,
                'registered_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }

            return ActionResult(
                success=True,
                message=f"Trigger registered: {trigger_id}",
                data={
                    'trigger_id': trigger_id,
                    'type': trigger_type,
                    'enabled': True
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Registration failed: {str(e)}")


class TriggerListAction(BaseAction):
    """List registered triggers.
    
    Returns all registered triggers and their status.
    """
    action_type = "trigger_list"
    display_name = "触发器列表"
    description = "列出所有触发器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List triggers.
        
        Args:
            context: Execution context.
            params: Dict with keys: enabled_only, trigger_type.
        
        Returns:
            ActionResult with trigger list.
        """
        enabled_only = params.get('enabled_only', False)
        trigger_type = params.get('trigger_type', None)

        try:
            triggers = getattr(context, '_triggers', None)
            if triggers is None:
                return ActionResult(
                    success=True,
                    message="No triggers registered",
                    data={'triggers': [], 'count': 0}
                )

            result = []
            for trigger_id, config in triggers.items():
                if enabled_only and not config.get('enabled'):
                    continue
                if trigger_type and config.get('type') != trigger_type:
                    continue
                
                result.append({
                    'trigger_id': trigger_id,
                    'type': config.get('type'),
                    'enabled': config.get('enabled'),
                    'last_triggered': config.get('last_triggered'),
                    'trigger_count': config.get('trigger_count', 0)
                })

            return ActionResult(
                success=True,
                message=f"Found {len(result)} triggers",
                data={'triggers': result, 'count': len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"List failed: {str(e)}")


class TriggerFireAction(BaseAction):
    """Fire registered trigger.
    
    Manually fires a trigger by ID.
    """
    action_type = "trigger_fire"
    display_name = "触发触发器"
    description = "手动触发触发器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fire trigger.
        
        Args:
            context: Execution context.
            params: Dict with keys: trigger_id, event_data.
        
        Returns:
            ActionResult with fire result.
        """
        trigger_id = params.get('trigger_id', '')
        event_data = params.get('event_data', {})

        if not trigger_id:
            return ActionResult(success=False, message="trigger_id required")

        try:
            triggers = getattr(context, '_triggers', None)
            if triggers is None or trigger_id not in triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger not found: {trigger_id}"
                )

            trigger = triggers[trigger_id]
            
            if not trigger.get('enabled'):
                return ActionResult(
                    success=False,
                    message=f"Trigger disabled: {trigger_id}"
                )

            trigger['last_triggered'] = time.time()
            trigger['trigger_count'] = trigger.get('trigger_count', 0) + 1

            handler = trigger.get('handler')
            handler_result = None
            
            if handler and callable(handler):
                try:
                    handler_result = handler(event_data)
                except Exception as e:
                    handler_result = {'error': str(e)}

            return ActionResult(
                success=True,
                message=f"Trigger fired: {trigger_id}",
                data={
                    'trigger_id': trigger_id,
                    'type': trigger.get('type'),
                    'handler_result': handler_result,
                    'trigger_count': trigger['trigger_count']
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Fire failed: {str(e)}")


class TriggerDeleteAction(BaseAction):
    """Delete registered trigger.
    
    Removes trigger from registry.
    """
    action_type = "trigger_delete"
    display_name = "删除触发器"
    description = "删除触发器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Delete trigger.
        
        Args:
            context: Execution context.
            params: Dict with keys: trigger_id.
        
        Returns:
            ActionResult with deletion status.
        """
        trigger_id = params.get('trigger_id', '')

        if not trigger_id:
            return ActionResult(success=False, message="trigger_id required")

        try:
            triggers = getattr(context, '_triggers', None)
            if triggers is None or trigger_id not in triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger not found: {trigger_id}"
                )

            del triggers[trigger_id]

            return ActionResult(
                success=True,
                message=f"Trigger deleted: {trigger_id}"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Delete failed: {str(e)}")
