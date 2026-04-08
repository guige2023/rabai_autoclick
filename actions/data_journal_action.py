"""Data journal action module for RabAI AutoClick.

Provides data journaling with audit trail, change logging,
event sourcing, and replay capabilities.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime
from enum import Enum
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JournalEntryType(Enum):
    """Journal entry types."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    READ = "read"
    CUSTOM = "custom"


class JournalEntry:
    """Represents a journal entry."""
    
    def __init__(
        self,
        entry_id: str,
        entry_type: JournalEntryType,
        entity_type: str,
        entity_id: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.entry_id = entry_id
        self.entry_type = entry_type
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.data = data
        self.metadata = metadata or {}
        self.timestamp = time.time()
        self.version = self.metadata.get('version', 1)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'entry_id': self.entry_id,
            'entry_type': self.entry_type.value,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'data': self.data,
            'metadata': self.metadata,
            'timestamp': self.timestamp,
            'version': self.version
        }


class DataJournalAction(BaseAction):
    """Journal data changes with full audit trail.
    
    Supports CRUD operations journaling, change tracking,
    event sourcing, and replay capabilities.
    """
    action_type = "data_journal"
    display_name = "数据日志"
    description = "数据变更日志和审计追踪"
    
    def __init__(self):
        super().__init__()
        self._journal: List[JournalEntry] = []
        self._entities: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute journal operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (journal, replay, query,
                   snapshot, restore), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'journal')
        
        if action == 'journal':
            return self._journal_operation(params)
        elif action == 'replay':
            return self._replay_events(params)
        elif action == 'query':
            return self._query_journal(params)
        elif action == 'snapshot':
            return self._create_snapshot(params)
        elif action == 'restore':
            return self._restore_snapshot(params)
        elif action == 'get_entity':
            return self._get_entity(params)
        elif action == 'create_entity':
            return self._create_entity(params)
        elif action == 'update_entity':
            return self._update_entity(params)
        elif action == 'delete_entity':
            return self._delete_entity(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _journal_operation(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Journal a data operation."""
        entry_type_str = params.get('entry_type', 'CUSTOM')
        try:
            entry_type = JournalEntryType(entry_type_str.lower())
        except ValueError:
            entry_type = JournalEntryType.CUSTOM
        
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        data = params.get('data')
        metadata = params.get('metadata', {})
        
        entry_id = metadata.get('entry_id', f"{entity_type}_{entity_id}_{int(time.time() * 1000)}")
        
        entry = JournalEntry(
            entry_id=entry_id,
            entry_type=entry_type,
            entity_type=entity_type,
            entity_id=entity_id,
            data=data,
            metadata=metadata
        )
        
        with self._lock:
            self._journal.append(entry)
        
        return ActionResult(
            success=True,
            message=f"Journaled {entry_type.value} for {entity_type}:{entity_id}",
            data=entry.to_dict()
        )
    
    def _create_entity(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create an entity with journal entry."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        data = params.get('data', {})
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            key = f"{entity_type}:{entity_id}"
            if key in self._entities:
                return ActionResult(
                    success=False,
                    message=f"Entity {key} already exists"
                )
            
            self._entities[key] = data.copy()
            
            entry = JournalEntry(
                entry_id=f"{key}_{int(time.time() * 1000)}",
                entry_type=JournalEntryType.CREATE,
                entity_type=entity_type,
                entity_id=entity_id,
                data=data,
                metadata={'version': 1}
            )
            self._journal.append(entry)
        
        return ActionResult(
            success=True,
            message=f"Created entity {key}",
            data={'entity': self._entities[key], 'entry': entry.to_dict()}
        )
    
    def _update_entity(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Update an entity with journal entry."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        data = params.get('data', {})
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            key = f"{entity_type}:{entity_id}"
            
            if key not in self._entities:
                return ActionResult(
                    success=False,
                    message=f"Entity {key} not found"
                )
            
            old_data = self._entities[key].copy()
            self._entities[key].update(data)
            
            entries = [e for e in self._journal 
                       if e.entity_type == entity_type and e.entity_id == entity_id]
            version = len(entries) + 1
            
            entry = JournalEntry(
                entry_id=f"{key}_{int(time.time() * 1000)}",
                entry_type=JournalEntryType.UPDATE,
                entity_type=entity_type,
                entity_id=entity_id,
                data={'old': old_data, 'new': data},
                metadata={'version': version}
            )
            self._journal.append(entry)
        
        return ActionResult(
            success=True,
            message=f"Updated entity {key} (version {version})",
            data={'entity': self._entities[key], 'entry': entry.to_dict()}
        )
    
    def _delete_entity(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Delete an entity with journal entry."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            key = f"{entity_type}:{entity_id}"
            
            if key not in self._entities:
                return ActionResult(
                    success=False,
                    message=f"Entity {key} not found"
                )
            
            deleted_data = self._entities.pop(key)
            
            entries = [e for e in self._journal 
                       if e.entity_type == entity_type and e.entity_id == entity_id]
            version = len(entries) + 1
            
            entry = JournalEntry(
                entry_id=f"{key}_{int(time.time() * 1000)}",
                entry_type=JournalEntryType.DELETE,
                entity_type=entity_type,
                entity_id=entity_id,
                data=deleted_data,
                metadata={'version': version}
            )
            self._journal.append(entry)
        
        return ActionResult(
            success=True,
            message=f"Deleted entity {key}",
            data={'entry': entry.to_dict()}
        )
    
    def _get_entity(
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
            
            if key not in self._entities:
                return ActionResult(
                    success=False,
                    message=f"Entity {key} not found"
                )
            
            return ActionResult(
                success=True,
                message=f"Retrieved entity {key}",
                data={'entity': self._entities[key]}
            )
    
    def _query_journal(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Query journal entries."""
        entity_type = params.get('entity_type')
        entity_id = params.get('entity_id')
        entry_type = params.get('entry_type')
        start_time = params.get('start_time')
        end_time = params.get('end_time')
        limit = params.get('limit', 100)
        
        with self._lock:
            results = self._journal
            
            if entity_type:
                results = [e for e in results if e.entity_type == entity_type]
            if entity_id:
                results = [e for e in results if e.entity_id == entity_id]
            if entry_type:
                try:
                    et = JournalEntryType(entry_type.lower())
                    results = [e for e in results if e.entry_type == et]
                except ValueError:
                    pass
            if start_time:
                results = [e for e in results if e.timestamp >= start_time]
            if end_time:
                results = [e for e in results if e.timestamp <= end_time]
            
            results = results[-limit:]
            
            return ActionResult(
                success=True,
                message=f"Found {len(results)} journal entries",
                data={
                    'entries': [e.to_dict() for e in results],
                    'count': len(results)
                }
            )
    
    def _replay_events(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replay journal events to reconstruct entity state."""
        entity_type = params.get('entity_type', 'unknown')
        entity_id = params.get('entity_id', '')
        
        if not entity_id:
            return ActionResult(success=False, message="entity_id is required")
        
        with self._lock:
            entries = [
                e for e in self._journal
                if e.entity_type == entity_type and e.entity_id == entity_id
            ]
            entries.sort(key=lambda e: e.timestamp)
        
        state = {}
        replay_log = []
        
        for entry in entries:
            if entry.entry_type == JournalEntryType.CREATE:
                state = entry.data.copy()
                replay_log.append({'action': 'create', 'data': entry.data})
            elif entry.entry_type == JournalEntryType.UPDATE:
                if 'new' in entry.data:
                    state.update(entry.data['new'])
                replay_log.append({'action': 'update', 'data': entry.data})
            elif entry.entry_type == JournalEntryType.DELETE:
                state = {}
                replay_log.append({'action': 'delete', 'data': entry.data})
        
        return ActionResult(
            success=True,
            message=f"Replayed {len(entries)} events",
            data={
                'state': state,
                'replay_log': replay_log,
                'event_count': len(entries)
            }
        )
    
    def _create_snapshot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a snapshot of current state."""
        with self._lock:
            snapshot = {
                'timestamp': time.time(),
                'entities': {k: v.copy() for k, v in self._entities.items()},
                'journal_count': len(self._journal)
            }
            
            return ActionResult(
                success=True,
                message=f"Snapshot created with {len(self._entities)} entities",
                data=snapshot
            )
    
    def _restore_snapshot(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Restore from a snapshot."""
        snapshot = params.get('snapshot')
        if not snapshot:
            return ActionResult(success=False, message="snapshot is required")
        
        with self._lock:
            self._entities = {k: v.copy() for k, v in snapshot.get('entities', {}).items()}
            self._journal = []
            
            return ActionResult(
                success=True,
                message=f"Restored {len(self._entities)} entities",
                data={'entity_count': len(self._entities)}
            )
