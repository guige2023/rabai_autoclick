"""
Automation context utilities for workflow state management.

Provides context isolation, state management, and
variable passing between automation steps.
"""

from __future__ import annotations

import time
import threading
from typing import Optional, Any, Dict, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import copy


class ContextScope(Enum):
    """Context scope levels."""
    GLOBAL = "global"
    WORKFLOW = "workflow"
    STEP = "step"
    ACTION = "action"


@dataclass
class ContextEntry:
    """Context entry with metadata."""
    key: str
    value: Any
    scope: ContextScope
    created_at: float
    updated_at: float
    readonly: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


class AutomationContext:
    """Manages automation workflow context."""
    
    def __init__(self, name: str = "default"):
        """
        Initialize automation context.
        
        Args:
            name: Context name.
        """
        self.name = name
        self._global: Dict[str, ContextEntry] = {}
        self._workflow: Dict[str, ContextEntry] = {}
        self._step: Dict[str, ContextEntry] = {}
        self._action: Dict[str, ContextEntry] = {}
        self._lock = threading.RLock()
        self._history: List[Dict[str, Any]] = []
        self._max_history = 1000
    
    def set(self, key: str, value: Any,
            scope: ContextScope = ContextScope.STEP,
            readonly: bool = False,
            tags: Optional[Dict[str, str]] = None) -> None:
        """
        Set context value.
        
        Args:
            key: Variable name.
            value: Value to store.
            scope: Scope level.
            readonly: Whether value is immutable.
            tags: Optional metadata tags.
        """
        with self._lock:
            entry = ContextEntry(
                key=key,
                value=value,
                scope=scope,
                created_at=time.time(),
                updated_at=time.time(),
                readonly=readonly,
                tags=tags or {}
            )
            
            store = self._get_store(scope)
            old_entry = store.get(key)
            
            if old_entry and old_entry.readonly:
                raise ValueError(f"Cannot overwrite readonly key '{key}'")
            
            store[key] = entry
            self._record_history('set', key, scope, value)
    
    def get(self, key: str,
            default: Any = None,
            scopes: Optional[List[ContextScope]] = None) -> Any:
        """
        Get context value.
        
        Args:
            key: Variable name.
            default: Default if not found.
            scopes: Scopes to search (ordered priority).
            
        Returns:
            Value or default.
        """
        if scopes is None:
            scopes = [ContextScope.ACTION, ContextScope.STEP,
                     ContextScope.WORKFLOW, ContextScope.GLOBAL]
        
        with self._lock:
            for scope in scopes:
                store = self._get_store(scope)
                entry = store.get(key)
                if entry:
                    return entry.value
        
        return default
    
    def get_entry(self, key: str) -> Optional[ContextEntry]:
        """
        Get context entry with metadata.
        
        Args:
            key: Variable name.
            
        Returns:
            ContextEntry or None.
        """
        with self._lock:
            for store in [self._action, self._step, self._workflow, self._global]:
                entry = store.get(key)
                if entry:
                    return entry
        return None
    
    def delete(self, key: str, scope: Optional[ContextScope] = None) -> bool:
        """
        Delete context variable.
        
        Args:
            key: Variable name.
            scope: Specific scope to delete from, or all if None.
            
        Returns:
            True if deleted, False if not found.
        """
        with self._lock:
            if scope:
                store = self._get_store(scope)
                if key in store:
                    del store[key]
                    self._record_history('delete', key, scope, None)
                    return True
                return False
            
            for s in ContextScope:
                store = self._get_store(s)
                if key in store:
                    del store[key]
                    self._record_history('delete', key, s, None)
                    return True
            return False
    
    def exists(self, key: str, scopes: Optional[List[ContextScope]] = None) -> bool:
        """
        Check if key exists.
        
        Args:
            key: Variable name.
            scopes: Scopes to check.
            
        Returns:
            True if exists.
        """
        return self.get(key, scopes=scopes) is not None
    
    def clear_scope(self, scope: ContextScope) -> None:
        """
        Clear all variables in scope.
        
        Args:
            scope: Scope to clear.
        """
        with self._lock:
            store = self._get_store(scope)
            store.clear()
            self._record_history('clear', '*', scope, None)
    
    def _get_store(self, scope: ContextScope) -> Dict[str, ContextEntry]:
        """Get store dict for scope."""
        return {
            ContextScope.GLOBAL: self._global,
            ContextScope.WORKFLOW: self._workflow,
            ContextScope.STEP: self._step,
            ContextScope.ACTION: self._action,
        }[scope]
    
    def _record_history(self, action: str, key: str, scope: ContextScope, value: Any) -> None:
        """Record history entry."""
        entry = {
            'action': action,
            'key': key,
            'scope': scope.value,
            'value': copy.deepcopy(value),
            'timestamp': time.time()
        }
        self._history.append(entry)
        
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]
    
    def get_history(self, key: Optional[str] = None,
                    limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get context history.
        
        Args:
            key: Optional filter by key.
            limit: Max entries.
            
        Returns:
            List of history entries.
        """
        with self._lock:
            history = self._history.copy()
        
        if key:
            history = [h for h in history if h['key'] == key]
        
        return history[-limit:]
    
    def get_all(self, scope: Optional[ContextScope] = None) -> Dict[str, Any]:
        """
        Get all variables.
        
        Args:
            scope: Optional scope filter.
            
        Returns:
            Dict of key->value.
        """
        with self._lock:
            if scope:
                return {k: v.value for k, v in self._get_store(scope).items()}
            
            result = {}
            for s in ContextScope:
                for k, v in self._get_store(s).items():
                    result[k] = v.value
            return result
    
    def push_scope(self, scope: ContextScope) -> None:
        """
        Push current scope values to parent scope.
        
        Args:
            scope: Target scope to push to.
        """
        with self._lock:
            if scope == ContextScope.GLOBAL:
                return
            
            current = self._step if scope == ContextScope.WORKFLOW else self._action
            parent = self._workflow if scope == ContextScope.WORKFLOW else self._step
            
            for key, entry in current.items():
                if key not in parent:
                    parent[key] = entry
    
    def get_snapshot(self) -> Dict[str, Any]:
        """
        Get full context snapshot.
        
        Returns:
            Dict with all scopes.
        """
        with self._lock:
            return {
                'name': self.name,
                'global': {k: v.value for k, v in self._global.items()},
                'workflow': {k: v.value for k, v in self._workflow.items()},
                'step': {k: v.value for k, v in self._step.items()},
                'action': {k: v.value for k, v in self._action.items()},
            }


class ContextManager:
    """Global context manager."""
    
    _instance: Optional['ContextManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._contexts: Dict[str, AutomationContext] = {}
        self._current: Optional[AutomationContext] = None
        self._initialized = True
    
    def create(self, name: str) -> AutomationContext:
        """Create new context."""
        ctx = AutomationContext(name)
        self._contexts[name] = ctx
        self._current = ctx
        return ctx
    
    def get(self, name: str = "default") -> Optional[AutomationContext]:
        """Get context by name."""
        return self._contexts.get(name)
    
    def set_current(self, name: str) -> bool:
        """Set current context."""
        if name in self._contexts:
            self._current = self._contexts[name]
            return True
        return False
    
    def delete(self, name: str) -> bool:
        """Delete context."""
        if name in self._contexts:
            del self._contexts[name]
            if self._current and self._current.name == name:
                self._current = None
            return True
        return False
