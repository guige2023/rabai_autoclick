"""Trigger action module for RabAI AutoClick.

Provides trigger management with condition evaluation,
scheduled triggers, and event-based triggers.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from enum import Enum
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TriggerType(Enum):
    """Trigger types."""
    CONDITION = "condition"
    SCHEDULE = "schedule"
    EVENT = "event"
    COUNTDOWN = "countdown"


class TriggerState(Enum):
    """Trigger states."""
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"


class Trigger:
    """Represents a trigger."""
    
    def __init__(
        self,
        trigger_id: str,
        trigger_type: TriggerType,
        config: Dict[str, Any],
        callback: Optional[Callable] = None
    ):
        self.trigger_id = trigger_id
        self.trigger_type = trigger_type
        self.config = config
        self.callback = callback
        self.state = TriggerState.ACTIVE
        self.created_at = time.time()
        self.last_fired = None
        self.fire_count = 0
        self.next_fire = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'trigger_id': self.trigger_id,
            'trigger_type': self.trigger_type.value,
            'config': self.config,
            'state': self.state.value,
            'created_at': self.created_at,
            'last_fired': self.last_fired,
            'fire_count': self.fire_count,
            'next_fire': self.next_fire
        }


class TriggerAction(BaseAction):
    """Manage triggers for conditional and scheduled execution.
    
    Supports condition-based, schedule-based, and event-based triggers.
    Provides trigger lifecycle management.
    """
    action_type = "trigger"
    display_name = "触发器"
    description = "条件触发器和调度触发器"
    
    def __init__(self):
        super().__init__()
        self._triggers: Dict[str, Trigger] = {}
        self._lock = threading.RLock()
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute trigger operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (create, fire, list,
                   pause, resume, delete, start_monitor), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'create')
        
        if action == 'create':
            return self._create_trigger(params)
        elif action == 'fire':
            return self._fire_trigger(params)
        elif action == 'list':
            return self._list_triggers(params)
        elif action == 'pause':
            return self._pause_trigger(params)
        elif action == 'resume':
            return self._resume_trigger(params)
        elif action == 'delete':
            return self._delete_trigger(params)
        elif action == 'start_monitor':
            return self._start_monitor(params)
        elif action == 'stop_monitor':
            return self._stop_monitor(params)
        elif action == 'evaluate':
            return self._evaluate_condition(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _create_trigger(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a new trigger."""
        trigger_id = params.get('trigger_id')
        if not trigger_id:
            return ActionResult(success=False, message="trigger_id is required")
        
        trigger_type_str = params.get('trigger_type', 'condition').lower()
        try:
            trigger_type = TriggerType(trigger_type_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown trigger type: {trigger_type_str}"
            )
        
        config = params.get('config', {})
        
        if trigger_type == TriggerType.SCHEDULE:
            if 'cron' not in config and 'interval' not in config:
                return ActionResult(
                    success=False,
                    message="Schedule trigger requires 'cron' or 'interval'"
                )
        elif trigger_type == TriggerType.CONDITION:
            if 'conditions' not in config:
                return ActionResult(
                    success=False,
                    message="Condition trigger requires 'conditions'"
                )
        
        with self._lock:
            if trigger_id in self._triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger '{trigger_id}' already exists"
                )
            
            trigger = Trigger(
                trigger_id=trigger_id,
                trigger_type=trigger_type,
                config=config
            )
            
            self._triggers[trigger_id] = trigger
        
        return ActionResult(
            success=True,
            message=f"Created {trigger_type.value} trigger '{trigger_id}'",
            data={'trigger': trigger.to_dict()}
        )
    
    def _fire_trigger(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manually fire a trigger."""
        trigger_id = params.get('trigger_id')
        
        if not trigger_id:
            return ActionResult(success=False, message="trigger_id is required")
        
        with self._lock:
            if trigger_id not in self._triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger '{trigger_id}' not found"
                )
            
            trigger = self._triggers[trigger_id]
            trigger.last_fired = time.time()
            trigger.fire_count += 1
        
        return ActionResult(
            success=True,
            message=f"Fired trigger '{trigger_id}'",
            data={'trigger': trigger.to_dict()}
        )
    
    def _list_triggers(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List all triggers."""
        trigger_type = params.get('trigger_type')
        state = params.get('state')
        
        with self._lock:
            triggers = list(self._triggers.values())
        
        if trigger_type:
            try:
                tt = TriggerType(trigger_type.lower())
                triggers = [t for t in triggers if t.trigger_type == tt]
            except ValueError:
                pass
        
        if state:
            try:
                ts = TriggerState(state.lower())
                triggers = [t for t in triggers if t.state == ts]
            except ValueError:
                pass
        
        return ActionResult(
            success=True,
            message=f"Found {len(triggers)} triggers",
            data={
                'triggers': [t.to_dict() for t in triggers],
                'count': len(triggers)
            }
        )
    
    def _pause_trigger(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pause a trigger."""
        trigger_id = params.get('trigger_id')
        
        if not trigger_id:
            return ActionResult(success=False, message="trigger_id is required")
        
        with self._lock:
            if trigger_id not in self._triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger '{trigger_id}' not found"
                )
            
            trigger = self._triggers[trigger_id]
            trigger.state = TriggerState.PAUSED
        
        return ActionResult(
            success=True,
            message=f"Paused trigger '{trigger_id}'",
            data={'trigger': trigger.to_dict()}
        )
    
    def _resume_trigger(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Resume a paused trigger."""
        trigger_id = params.get('trigger_id')
        
        if not trigger_id:
            return ActionResult(success=False, message="trigger_id is required")
        
        with self._lock:
            if trigger_id not in self._triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger '{trigger_id}' not found"
                )
            
            trigger = self._triggers[trigger_id]
            trigger.state = TriggerState.ACTIVE
        
        return ActionResult(
            success=True,
            message=f"Resumed trigger '{trigger_id}'",
            data={'trigger': trigger.to_dict()}
        )
    
    def _delete_trigger(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Delete a trigger."""
        trigger_id = params.get('trigger_id')
        
        if not trigger_id:
            return ActionResult(success=False, message="trigger_id is required")
        
        with self._lock:
            if trigger_id not in self._triggers:
                return ActionResult(
                    success=False,
                    message=f"Trigger '{trigger_id}' not found"
                )
            
            del self._triggers[trigger_id]
        
        return ActionResult(
            success=True,
            message=f"Deleted trigger '{trigger_id}'"
        )
    
    def _evaluate_condition(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate a condition against data."""
        conditions = params.get('conditions', [])
        data = params.get('data', {})
        
        if not conditions:
            return ActionResult(
                success=False,
                message="No conditions to evaluate"
            )
        
        match = self._check_conditions(conditions, data)
        
        return ActionResult(
            success=True,
            message=f"Condition evaluation: {'matched' if match else 'not matched'}",
            data={'matched': match}
        )
    
    def _check_conditions(
        self,
        conditions: List[Dict[str, Any]],
        data: Dict[str, Any]
    ) -> bool:
        """Check if conditions match data."""
        for condition in conditions:
            field = condition.get('field')
            operator = condition.get('operator', 'eq')
            value = condition.get('value')
            
            record_value = data.get(field)
            
            if operator == 'eq' and record_value != value:
                return False
            elif operator == 'ne' and record_value == value:
                return False
            elif operator == 'gt' and not (isinstance(record_value, (int, float)) and record_value > value):
                return False
            elif operator == 'gte' and not (isinstance(record_value, (int, float)) and record_value >= value):
                return False
            elif operator == 'lt' and not (isinstance(record_value, (int, float)) and record_value < value):
                return False
            elif operator == 'lte' and not (isinstance(record_value, (int, float)) and record_value <= value):
                return False
            elif operator == 'exists' and (value and field not in data):
                return False
        
        return True
    
    def _start_monitor(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Start the trigger monitor thread."""
        if self._running:
            return ActionResult(
                success=True,
                message="Monitor already running",
                data={'running': True}
            )
        
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        
        return ActionResult(
            success=True,
            message="Trigger monitor started",
            data={'running': True}
        )
    
    def _stop_monitor(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Stop the trigger monitor thread."""
        self._running = False
        
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None
        
        return ActionResult(
            success=True,
            message="Trigger monitor stopped",
            data={'running': False}
        )
    
    def _monitor_loop(self):
        """Main monitor loop for checking triggers."""
        while self._running:
            try:
                now = time.time()
                
                with self._lock:
                    for trigger in self._triggers.values():
                        if trigger.state != TriggerState.ACTIVE:
                            continue
                        
                        if trigger.trigger_type == TriggerType.COUNTDOWN:
                            interval = trigger.config.get('interval', 60)
                            if not trigger.last_fired or (now - trigger.last_fired) >= interval:
                                trigger.last_fired = now
                                trigger.fire_count += 1
                
                time.sleep(1)
                
            except Exception:
                time.sleep(1)
