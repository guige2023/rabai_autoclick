"""Data CDC action module for RabAI AutoClick.

Provides Change Data Capture for tracking changes,
event generation, and downstream propagation.
"""

import time
import sys
import os
import json
import hashlib
from typing import Any, Dict, List, Optional, Union, Callable
from enum import Enum
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CDCEventType(Enum):
    """CDC event types."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class CDCEvent:
    """Represents a CDC event."""
    
    def __init__(
        self,
        event_type: CDCEventType,
        entity_type: str,
        entity_id: str,
        old_data: Optional[Dict[str, Any]],
        new_data: Optional[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.event_id = f"cdc_{int(time.time() * 1000)}"
        self.event_type = event_type
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.old_data = old_data
        self.new_data = new_data
        self.metadata = metadata or {}
        self.timestamp = time.time()
        self.version = self.metadata.get('version', 1)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type.value,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'old_data': self.old_data,
            'new_data': self.new_data,
            'metadata': self.metadata,
            'timestamp': self.timestamp,
            'version': self.version
        }


class DataCDCAction(BaseAction):
    """Track data changes and generate CDC events.
    
    Supports change detection, event generation,
    event log management, and downstream propagation.
    """
    action_type = "data_cdc"
    display_name = "变更数据捕获"
    description = "数据变更追踪和CDC事件生成"
    
    def __init__(self):
        super().__init__()
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._event_log: List[CDCEvent] = []
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute CDC operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (capture, process_change,
                   get_events, subscribe), config.
        
        Returns:
            ActionResult with CDC result.
        """
        action = params.get('action', 'capture')
        
        if action == 'capture':
            return self._capture_change(params)
        elif action == 'process_change':
            return self._process_change(params)
        elif action == 'get_events':
            return self._get_events(params)
        elif action == 'get_entity_state':
            return self._get_entity_state(params)
        elif action == 'snapshot':
            return self._create_snapshot(params)
        elif action == 'replay':
            return self._replay_events(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _capture_change(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Capture a data change."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        old_data = params.get('old_data')
        new_data = params.get('new_data')
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        if old_data is None and new_data is None:
            return ActionResult(
                success=False,
                message="Either old_data or new_data is required"
            )
        
        with self._lock:
            old_snapshot = self._snapshots.get(f"{entity_type}:{entity_id}")
            
            if old_data is None and new_data is not None:
                event_type = CDCEventType.INSERT
            elif old_data is not None and new_data is None:
                event_type = CDCEventType.DELETE
            else:
                event_type = CDCEventType.UPDATE
            
            event = CDCEvent(
                event_type=event_type,
                entity_type=entity_type,
                entity_id=entity_id,
                old_data=old_data or old_snapshot,
                new_data=new_data,
                metadata={'source': 'capture'}
            )
            
            self._event_log.append(event)
            
            if new_data is not None:
                self._snapshots[f"{entity_type}:{entity_id}"] = new_data.copy()
            elif event_type == CDCEventType.DELETE:
                if f"{entity_type}:{entity_id}" in self._snapshots:
                    del self._snapshots[f"{entity_type}:{entity_id}"]
        
        return ActionResult(
            success=True,
            message=f"Captured {event_type.value} for {entity_type}:{entity_id}",
            data={'event': event.to_dict()}
        )
    
    def _process_change(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process a record change and generate CDC event."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        current_data = params.get('current_data', {})
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            key = f"{entity_type}:{entity_id}"
            old_data = self._snapshots.get(key)
            
            if old_data is None:
                event_type = CDCEventType.INSERT
                event = CDCEvent(
                    event_type=event_type,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    old_data=None,
                    new_data=current_data,
                    metadata={'source': 'process'}
                )
            elif current_data != old_data:
                event_type = CDCEventType.UPDATE
                event = CDCEvent(
                    event_type=event_type,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    old_data=old_data.copy(),
                    new_data=current_data,
                    metadata={'source': 'process'}
                )
            else:
                return ActionResult(
                    success=True,
                    message="No change detected",
                    data={'changed': False}
                )
            
            self._event_log.append(event)
            self._snapshots[key] = current_data.copy()
        
        return ActionResult(
            success=True,
            message=f"Processed {event_type.value} for {entity_type}:{entity_id}",
            data={
                'event': event.to_dict(),
                'changed': True
            }
        )
    
    def _get_events(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get CDC events from the log."""
        entity_type = params.get('entity_type')
        entity_id = params.get('entity_id')
        event_type = params.get('event_type')
        since = params.get('since')
        limit = params.get('limit', 100)
        
        with self._lock:
            events = self._event_log
            
            if entity_type:
                events = [e for e in events if e.entity_type == entity_type]
            if entity_id:
                events = [e for e in events if e.entity_id == entity_id]
            if event_type:
                try:
                    et = CDCEventType(event_type.lower())
                    events = [e for e in events if e.event_type == et]
                except ValueError:
                    pass
            if since:
                events = [e for e in events if e.timestamp >= since]
            
            events = events[-limit:]
        
        return ActionResult(
            success=True,
            message=f"Retrieved {len(events)} CDC events",
            data={
                'events': [e.to_dict() for e in events],
                'count': len(events)
            }
        )
    
    def _get_entity_state(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get current state of an entity."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            key = f"{entity_type}:{entity_id}"
            state = self._snapshots.get(key)
        
        if state:
            return ActionResult(
                success=True,
                message=f"Retrieved state for {entity_type}:{entity_id}",
                data={'state': state}
            )
        else:
            return ActionResult(
                success=False,
                message=f"No state found for {entity_type}:{entity_id}"
            )
    
    def _create_snapshot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a full snapshot of all tracked entities."""
        with self._lock:
            snapshot = {
                'timestamp': time.time(),
                'entities': {k: v.copy() for k, v in self._snapshots.items()},
                'event_count': len(self._event_log)
            }
        
        return ActionResult(
            success=True,
            message=f"Created snapshot with {len(snapshot['entities'])} entities",
            data=snapshot
        )
    
    def _replay_events(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replay events to reconstruct state."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            relevant_events = [
                e for e in self._event_log
                if e.entity_type == entity_type and e.entity_id == entity_id
            ]
            relevant_events.sort(key=lambda e: e.timestamp)
        
        state = {}
        for event in relevant_events:
            if event.event_type == CDCEventType.INSERT:
                state = event.new_data.copy() if event.new_data else {}
            elif event.event_type == CDCEventType.UPDATE:
                if event.new_data:
                    state.update(event.new_data)
            elif event.event_type == CDCEventType.DELETE:
                state = {}
        
        return ActionResult(
            success=True,
            message=f"Replayed {len(relevant_events)} events",
            data={
                'state': state,
                'event_count': len(relevant_events)
            }
        )
