"""Idempotency V2 Action Module.

Provides idempotency key management
for safe retries.
"""

import time
import hashlib
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IdempotencyStatus(Enum):
    """Idempotency record status."""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IdempotencyRecord:
    """Idempotency record."""
    key: str
    status: IdempotencyStatus
    result: Any = None
    created_at: float = field(default_factory=time.time)
    expires_at: float = 0


class IdempotencyV2Manager:
    """Manages idempotency."""

    def __init__(self):
        self._records: Dict[str, IdempotencyRecord] = {}
        self._default_ttl = 3600

    def generate_key(
        self,
        operation: str,
        *args,
        **kwargs
    ) -> str:
        """Generate idempotency key."""
        data = f"{operation}:{args}:{kwargs}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def check_and_create(
        self,
        key: str,
        ttl_seconds: float = 3600
    ) -> tuple[bool, Optional[Any]]:
        """Check if exists or create new."""
        if key in self._records:
            record = self._records[key]

            if record.status == IdempotencyStatus.COMPLETED:
                return False, record.result

            if record.status == IdempotencyStatus.FAILED:
                return False, None

            return True, None

        expires_at = time.time() + ttl_seconds

        self._records[key] = IdempotencyRecord(
            key=key,
            status=IdempotencyStatus.PENDING,
            expires_at=expires_at
        )

        return True, None

    def complete(self, key: str, result: Any) -> bool:
        """Mark as completed."""
        if key not in self._records:
            return False

        self._records[key].status = IdempotencyStatus.COMPLETED
        self._records[key].result = result
        return True

    def fail(self, key: str) -> bool:
        """Mark as failed."""
        if key not in self._records:
            return False

        self._records[key].status = IdempotencyStatus.FAILED
        return True

    def get_record(self, key: str) -> Optional[Dict]:
        """Get record."""
        if key not in self._records:
            return None

        record = self._records[key]
        return {
            "key": record.key,
            "status": record.status.value,
            "result": record.result,
            "created_at": record.created_at
        }


class IdempotencyV2Action(BaseAction):
    """Action for idempotency operations."""

    def __init__(self):
        super().__init__("idempotency_v2")
        self._manager = IdempotencyV2Manager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute idempotency action."""
        try:
            operation = params.get("operation", "check")

            if operation == "generate_key":
                return self._generate_key(params)
            elif operation == "check":
                return self._check(params)
            elif operation == "complete":
                return self._complete(params)
            elif operation == "fail":
                return self._fail(params)
            elif operation == "get":
                return self._get(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _generate_key(self, params: Dict) -> ActionResult:
        """Generate key."""
        key = self._manager.generate_key(
            params.get("operation", ""),
            params.get("args", []),
            params.get("kwargs", {})
        )
        return ActionResult(success=True, data={"key": key})

    def _check(self, params: Dict) -> ActionResult:
        """Check or create."""
        can_proceed, result = self._manager.check_and_create(
            key=params.get("key", ""),
            ttl_seconds=params.get("ttl_seconds", 3600)
        )
        return ActionResult(success=True, data={
            "can_proceed": can_proceed,
            "result": result
        })

    def _complete(self, params: Dict) -> ActionResult:
        """Complete."""
        success = self._manager.complete(
            key=params.get("key", ""),
            result=params.get("result")
        )
        return ActionResult(success=success)

    def _fail(self, params: Dict) -> ActionResult:
        """Fail."""
        success = self._manager.fail(params.get("key", ""))
        return ActionResult(success=success)

    def _get(self, params: Dict) -> ActionResult:
        """Get record."""
        record = self._manager.get_record(params.get("key", ""))
        if not record:
            return ActionResult(success=False, message="Record not found")
        return ActionResult(success=True, data=record)
