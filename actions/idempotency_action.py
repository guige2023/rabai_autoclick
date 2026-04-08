"""Idempotency action module for RabAI AutoClick.

Provides idempotency key management to ensure operations are
executed exactly once even with retries.
"""

import sys
import os
import time
import hashlib
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IdempotencyStatus(Enum):
    """Idempotency record status."""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class IdempotencyRecord:
    """Record of an idempotent operation."""
    key: str
    status: IdempotencyStatus
    result: Any = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    expires_at: float = 0
    attempts: int = 0
    error: Optional[str] = None


class IdempotencyAction(BaseAction):
    """Manage idempotency keys for safe retries.
    
    Ensures operations are executed exactly once by tracking
    operation status and returning cached results.
    """
    action_type = "idempotency"
    display_name = "幂等性"
    description = "幂等键管理确保操作只执行一次"
    
    def __init__(self):
        super().__init__()
        self._records: Dict[str, IdempotencyRecord] = {}
        self._lock = Lock()
        self._default_ttl = 3600  # 1 hour
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute idempotency operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'check', 'start', 'complete', 'fail', 'cleanup'
                - key: Idempotency key
                - ttl: Time-to-live in seconds
                - result: Operation result (for complete)
                - error: Error message (for fail)
                - handler: Operation handler function
        
        Returns:
            ActionResult with idempotency check result.
        """
        operation = params.get('operation', 'check').lower()
        
        if operation == 'check':
            return self._check(params)
        elif operation == 'start':
            return self._start(params)
        elif operation == 'complete':
            return self._complete(params)
        elif operation == 'fail':
            return self._fail(params)
        elif operation == 'cleanup':
            return self._cleanup(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _check(self, params: Dict[str, Any]) -> ActionResult:
        """Check if operation was already executed."""
        key = params.get('key')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            record = self._records.get(key)
            
            if not record:
                return ActionResult(
                    success=True,
                    message="New operation, proceed",
                    data={'key': key, 'status': 'new'}
                )
            
            # Check if expired
            if record.expires_at and time.time() > record.expires_at:
                record.status = IdempotencyStatus.EXPIRED
                return ActionResult(
                    success=True,
                    message="Record expired",
                    data={'key': key, 'status': 'expired'}
                )
            
            return ActionResult(
                success=True,
                message=f"Operation status: {record.status.value}",
                data={
                    'key': key,
                    'status': record.status.value,
                    'result': record.result if record.status == IdempotencyStatus.COMPLETED else None
                }
            )
    
    def _start(self, params: Dict[str, Any]) -> ActionResult:
        """Mark operation as started (claiming idempotency key)."""
        key = params.get('key')
        ttl = params.get('ttl', self._default_ttl)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            # Check if already exists
            if key in self._records:
                record = self._records[key]
                
                if record.status == IdempotencyStatus.PENDING:
                    return ActionResult(
                        success=False,
                        message="Operation already in progress",
                        data={'key': key, 'status': record.status.value}
                    )
                
                if record.status == IdempotencyStatus.COMPLETED:
                    return ActionResult(
                        success=True,
                        message="Operation already completed",
                        data={
                            'key': key,
                            'status': 'completed',
                            'result': record.result
                        }
                    )
            
            # Create new record
            record = IdempotencyRecord(
                key=key,
                status=IdempotencyStatus.PENDING,
                expires_at=time.time() + ttl
            )
            self._records[key] = record
        
        return ActionResult(
            success=True,
            message="Idempotency key claimed",
            data={'key': key, 'status': 'pending'}
        )
    
    def _complete(self, params: Dict[str, Any]) -> ActionResult:
        """Mark operation as completed."""
        key = params.get('key')
        result = params.get('result')
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            if key not in self._records:
                return ActionResult(
                    success=False,
                    message=f"No record for key '{key}'"
                )
            
            record = self._records[key]
            
            if record.status != IdempotencyStatus.PENDING:
                return ActionResult(
                    success=True,
                    message=f"Operation already {record.status.value}",
                    data={'key': key, 'status': record.status.value}
                )
            
            record.status = IdempotencyStatus.COMPLETED
            record.result = result
            record.completed_at = time.time()
        
        return ActionResult(
            success=True,
            message="Operation completed",
            data={'key': key, 'status': 'completed'}
        )
    
    def _fail(self, params: Dict[str, Any]) -> ActionResult:
        """Mark operation as failed."""
        key = params.get('key')
        error = params.get('error')
        retry = params.get('retry', True)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            if key not in self._records:
                return ActionResult(
                    success=False,
                    message=f"No record for key '{key}'"
                )
            
            record = self._records[key]
            
            if retry and record.attempts < 3:
                # Allow retry
                record.attempts += 1
                record.error = error
                return ActionResult(
                    success=False,
                    message=f"Operation failed, allow retry ({record.attempts}/3)",
                    data={'key': key, 'status': 'retry', 'attempts': record.attempts}
                )
            else:
                record.status = IdempotencyStatus.FAILED
                record.error = error
                record.completed_at = time.time()
                
                return ActionResult(
                    success=False,
                    message="Operation failed permanently",
                    data={'key': key, 'status': 'failed'}
                )
    
    def _cleanup(self, params: Dict[str, Any]) -> ActionResult:
        """Remove expired records."""
        max_age = params.get('max_age', 86400)  # Default 24 hours
        now = time.time()
        
        with self._lock:
            keys_to_remove = [
                k for k, r in self._records.items()
                if (now - r.created_at) > max_age or
                   (r.expires_at and now > r.expires_at)
            ]
            
            for key in keys_to_remove:
                del self._records[key]
        
        return ActionResult(
            success=True,
            message=f"Removed {len(keys_to_remove)} expired records",
            data={'removed': len(keys_to_remove)}
        )
    
    def generate_key(self, *parts: Any) -> str:
        """Generate idempotency key from parts."""
        content = ':'.join(str(p) for p in parts)
        return hashlib.sha256(content.encode()).hexdigest()[:32]


class IdempotentOperationAction(BaseAction):
    """Execute operation with automatic idempotency handling."""
    action_type = "idempotent_operation"
    display_name = "幂等操作"
    description = "自动幂等性处理的操作执行"
    
    def __init__(self):
        super().__init__()
        self._manager = IdempotencyAction()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute idempotent operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - key: Idempotency key
                - handler: Operation to execute
                - ttl: TTL in seconds
                - args: Handler arguments
                - kwargs: Handler keyword arguments
        
        Returns:
            ActionResult with operation result.
        """
        key = params.get('key')
        handler = params.get('handler')
        ttl = params.get('ttl', 3600)
        args = params.get('args', [])
        kwargs = params.get('kwargs', {})
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        # Check if already completed
        check = self._manager._check({'key': key})
        
        if check.data.get('status') == 'completed':
            return ActionResult(
                success=True,
                message="Returning cached result",
                data={'result': check.data.get('result'), 'cached': True}
            )
        
        # Try to claim the key
        start = self._manager._start({'key': key, 'ttl': ttl})
        
        if not start.success:
            return start
        
        # Execute handler
        try:
            if callable(handler):
                result = handler(*args, **kwargs)
            else:
                result = None
            
            # Mark as completed
            self._manager._complete({'key': key, 'result': result})
            
            return ActionResult(
                success=True,
                message="Operation completed",
                data={'result': result, 'cached': False}
            )
        except Exception as e:
            # Mark as failed
            self._manager._fail({'key': key, 'error': str(e)})
            
            return ActionResult(
                success=False,
                message=f"Operation failed: {e}",
                data={'error': str(e)}
            )
