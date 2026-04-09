"""Automation Guardian Action Module.

Provides watchdog and protection for automation workflows.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationGuardianAction(BaseAction):
    """Guard automation workflows against failures.
    
    Monitors workflow health and takes protective actions.
    """
    action_type = "automation_guardian"
    display_name = "自动化守护"
    description = "保护自动化工作流免受故障影响"
    
    def __init__(self):
        super().__init__()
        self._guards: Dict[str, Dict] = {}
        self._violations: List[Dict] = []
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute guardian operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, guard_type, value.
        
        Returns:
            ActionResult with guardian result.
        """
        action = params.get('action', 'protect')
        guard_type = params.get('guard_type', 'timeout')
        
        if action == 'protect':
            return self._add_guard(guard_type, params)
        elif action == 'check':
            return self._check_guard(guard_type, params)
        elif action == 'violate':
            return self._record_violation(params)
        elif action == 'get_violations':
            return self._get_violations(params)
        elif action == 'clear':
            return self._clear_guards()
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _add_guard(self, guard_type: str, params: Dict) -> ActionResult:
        """Add a guard."""
        guard_id = params.get('guard_id', f'guard_{int(time.time())}')
        threshold = params.get('threshold', 10)
        callback = params.get('callback', None)
        
        self._guards[guard_id] = {
            'type': guard_type,
            'threshold': threshold,
            'current': 0,
            'created_at': time.time(),
            'callback': callback
        }
        
        return ActionResult(
            success=True,
            data={
                'guard_id': guard_id,
                'type': guard_type,
                'threshold': threshold
            },
            error=None
        )
    
    def _check_guard(self, guard_type: str, params: Dict) -> ActionResult:
        """Check if guard is violated."""
        current_value = params.get('current_value', 0)
        
        violated_guards = []
        safe_guards = []
        
        for guard_id, guard in self._guards.items():
            if guard['type'] == guard_type:
                if current_value >= guard['threshold']:
                    violated_guards.append(guard_id)
                else:
                    safe_guards.append(guard_id)
        
        is_violated = len(violated_guards) > 0
        
        return ActionResult(
            success=not is_violated,
            data={
                'violated': is_violated,
                'violated_guards': violated_guards,
                'safe_guards': safe_guards,
                'current_value': current_value
            },
            error="Guard violated" if is_violated else None
        )
    
    def _record_violation(self, params: Dict) -> ActionResult:
        """Record a guard violation."""
        guard_id = params.get('guard_id', '')
        details = params.get('details', {})
        
        violation = {
            'guard_id': guard_id,
            'timestamp': time.time(),
            'details': details
        }
        
        self._violations.append(violation)
        
        return ActionResult(
            success=True,
            data={'violation': violation},
            error=None
        )
    
    def _get_violations(self, params: Dict) -> ActionResult:
        """Get all violations."""
        since = params.get('since', 0)
        
        filtered = [
            v for v in self._violations
            if v['timestamp'] >= since
        ]
        
        return ActionResult(
            success=True,
            data={
                'violations': filtered,
                'count': len(filtered)
            },
            error=None
        )
    
    def _clear_guards(self) -> ActionResult:
        """Clear all guards."""
        count = len(self._guards)
        self._guards.clear()
        
        return ActionResult(
            success=True,
            data={'cleared_count': count},
            error=None
        )


class AutomationWatchdogAction(BaseAction):
    """Watchdog timer for automation tasks.
    
    Monitors task execution and triggers timeout actions.
    """
    action_type = "automation_watchdog"
    display_name = "自动化看门狗"
    description = "监控任务执行并触发超时操作"
    
    def __init__(self):
        super().__init__()
        self._timers: Dict[str, Dict] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute watchdog operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, timer_id, timeout.
        
        Returns:
            ActionResult with watchdog result.
        """
        action = params.get('action', 'start')
        timer_id = params.get('timer_id', '')
        
        if action == 'start':
            return self._start_timer(timer_id, params)
        elif action == 'check':
            return self._check_timer(timer_id)
        elif action == 'stop':
            return self._stop_timer(timer_id)
        elif action == 'expired':
            return self._check_expired(timer_id)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _start_timer(self, timer_id: str, params: Dict) -> ActionResult:
        """Start a watchdog timer."""
        timeout = params.get('timeout', 60)
        
        self._timers[timer_id] = {
            'started_at': time.time(),
            'timeout': timeout,
            'stopped': False
        }
        
        return ActionResult(
            success=True,
            data={
                'timer_id': timer_id,
                'timeout': timeout,
                'started': True
            },
            error=None
        )
    
    def _check_timer(self, timer_id: str) -> ActionResult:
        """Check if timer is still valid."""
        if timer_id not in self._timers:
            return ActionResult(
                success=False,
                data=None,
                error="Timer not found"
            )
        
        timer = self._timers[timer_id]
        elapsed = time.time() - timer['started_at']
        remaining = timer['timeout'] - elapsed
        
        return ActionResult(
            success=True,
            data={
                'timer_id': timer_id,
                'elapsed': elapsed,
                'remaining': remaining,
                'expired': remaining <= 0
            },
            error=None
        )
    
    def _stop_timer(self, timer_id: str) -> ActionResult:
        """Stop a watchdog timer."""
        if timer_id not in self._timers:
            return ActionResult(
                success=False,
                data=None,
                error="Timer not found"
            )
        
        self._timers[timer_id]['stopped'] = True
        
        return ActionResult(
            success=True,
            data={'timer_id': timer_id, 'stopped': True},
            error=None
        )
    
    def _check_expired(self, timer_id: str) -> ActionResult:
        """Check if timer has expired."""
        if timer_id not in self._timers:
            return ActionResult(
                success=False,
                data=None,
                error="Timer not found"
            )
        
        timer = self._timers[timer_id]
        elapsed = time.time() - timer['started_at']
        expired = elapsed >= timer['timeout']
        
        return ActionResult(
            success=not expired,
            data={
                'timer_id': timer_id,
                'expired': expired,
                'elapsed': elapsed
            },
            error="Timer expired" if expired else None
        )


def register_actions():
    """Register all Automation Guardian actions."""
    return [
        AutomationGuardianAction,
        AutomationWatchdogAction,
    ]
