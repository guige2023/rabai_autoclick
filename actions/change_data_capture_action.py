"""Change Data Capture action module for RabAI AutoClick.

Provides CDC (Change Data Capture) for tracking database
changes with log-based, trigger-based, and timestamp-based approaches.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CDCMethod(Enum):
    """CDC capture methods."""
    LOG_BASED = "log_based"
    TRIGGER_BASED = "trigger_based"
    TIMESTAMP_BASED = "timestamp_based"
    POLLING = "polling"


class ChangeType(Enum):
    """Types of data changes."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    SNAPSHOT = "snapshot"


@dataclass
class ChangeEvent:
    """Represents a captured change event."""
    event_id: str
    table_name: str
    operation: ChangeType
    timestamp: float
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    keys: Dict[str, Any] = field(default_factory=dict)  # Primary key columns
    sequence: int = 0
    source_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableConfig:
    """Configuration for CDC on a table."""
    table_name: str
    primary_key: List[str]
    tracked_columns: Optional[List[str]] = None
    timestamp_column: str = "updated_at"
    cdc_enabled: bool = True


@dataclass
class CDCStream:
    """A CDC event stream for a table."""
    stream_id: str
    table_config: TableConfig
    method: CDCMethod
    last_sequence: int = 0
    last_timestamp: float = 0.0
    offset: Dict[str, Any] = field(default_factory=dict)  # For resuming


class ChangeDataCapture:
    """Change Data Capture engine."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._streams: Dict[str, CDCStream] = {}
        self._handlers: Dict[str, List] = defaultdict(list)
        self._change_log: List[ChangeEvent] = []
        self._max_log_size = 10000
        self._persistence_path = persistence_path
    
    def register_stream(self, stream: CDCStream) -> None:
        """Register a CDC stream for a table."""
        self._streams[stream.stream_id] = stream
    
    def unregister_stream(self, stream_id: str) -> bool:
        """Unregister a CDC stream."""
        if stream_id in self._streams:
            del self._streams[stream_id]
            return True
        return False
    
    def record_change(
        self,
        stream_id: str,
        operation: ChangeType,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        keys: Dict[str, Any]
    ) -> Optional[ChangeEvent]:
        """Record a data change event."""
        stream = self._streams.get(stream_id)
        if not stream:
            return None
        
        event = ChangeEvent(
            event_id=str(uuid.uuid4()),
            table_name=stream.table_config.table_name,
            operation=operation,
            timestamp=time.time(),
            before=before,
            after=after,
            keys=keys,
            sequence=stream.last_sequence + 1
        )
        
        stream.last_sequence = event.sequence
        stream.last_timestamp = event.timestamp
        
        # Store in log
        self._change_log.append(event)
        if len(self._change_log) > self._max_log_size:
            self._change_log = self._change_log[-self._max_log_size:]
        
        # Notify handlers
        for handler in self._handlers.get(stream_id, []):
            try:
                handler(event)
            except Exception:
                pass
        
        return event
    
    def get_changes(
        self,
        stream_id: str,
        since_sequence: Optional[int] = None,
        since_timestamp: Optional[float] = None,
        limit: int = 100
    ) -> List[ChangeEvent]:
        """Get changes from a stream."""
        stream = self._streams.get(stream_id)
        if not stream:
            return []
        
        changes = [
            c for c in self._change_log
            if c.table_name == stream.table_config.table_name
        ]
        
        if since_sequence is not None:
            changes = [c for c in changes if c.sequence > since_sequence]
        elif since_timestamp is not None:
            changes = [c for c in changes if c.timestamp > since_timestamp]
        
        return changes[-limit:]
    
    def get_latest_sequence(self, stream_id: str) -> int:
        """Get the latest sequence number for a stream."""
        stream = self._streams.get(stream_id)
        return stream.last_sequence if stream else 0
    
    def register_handler(self, stream_id: str, handler) -> None:
        """Register a change handler for a stream."""
        self._handlers[stream_id].append(handler)
    
    def get_offset(self, stream_id: str) -> Dict[str, Any]:
        """Get current offset for resuming a stream."""
        stream = self._streams.get(stream_id)
        if not stream:
            return {}
        return {
            "sequence": stream.last_sequence,
            "timestamp": stream.last_timestamp
        }
    
    def reset_offset(self, stream_id: str) -> bool:
        """Reset stream offset to beginning."""
        stream = self._streams.get(stream_id)
        if not stream:
            return False
        stream.last_sequence = 0
        stream.last_timestamp = 0.0
        return True
    
    def get_stream_info(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a CDC stream."""
        stream = self._streams.get(stream_id)
        if not stream:
            return None
        return {
            "stream_id": stream.stream_id,
            "table_name": stream.table_config.table_name,
            "method": stream.method.value,
            "last_sequence": stream.last_sequence,
            "last_timestamp": stream.last_timestamp
        }


class ChangeDataCaptureAction(BaseAction):
    """Change Data Capture for tracking database changes.
    
    Supports log-based, trigger-based, and timestamp-based CDC
    with event streaming and handlers.
    """
    action_type = "change_data_capture"
    display_name = "变更数据捕获"
    description = "CDC变更数据捕获，支持日志和触发器方式"
    
    def __init__(self):
        super().__init__()
        self._cdc = ChangeDataCapture()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute CDC operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "register_stream":
                return self._register_stream(params)
            elif operation == "unregister_stream":
                return self._unregister_stream(params)
            elif operation == "record_change":
                return self._record_change(params)
            elif operation == "get_changes":
                return self._get_changes(params)
            elif operation == "get_offset":
                return self._get_offset(params)
            elif operation == "reset_offset":
                return self._reset_offset(params)
            elif operation == "get_stream_info":
                return self._get_stream_info(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _register_stream(self, params: Dict[str, Any]) -> ActionResult:
        """Register a CDC stream."""
        stream_id = params.get("stream_id", str(uuid.uuid4()))
        table_config = TableConfig(
            table_name=params.get("table_name", ""),
            primary_key=params.get("primary_key", []),
            timestamp_column=params.get("timestamp_column", "updated_at")
        )
        stream = CDCStream(
            stream_id=stream_id,
            table_config=table_config,
            method=CDCMethod(params.get("method", "timestamp_based"))
        )
        self._cdc.register_stream(stream)
        return ActionResult(success=True, message=f"Stream '{stream_id}' registered",
                         data={"stream_id": stream_id})
    
    def _unregister_stream(self, params: Dict[str, Any]) -> ActionResult:
        """Unregister a CDC stream."""
        stream_id = params.get("stream_id", "")
        unregistered = self._cdc.unregister_stream(stream_id)
        return ActionResult(success=unregistered, message="Unregistered" if unregistered else "Not found")
    
    def _record_change(self, params: Dict[str, Any]) -> ActionResult:
        """Record a data change."""
        stream_id = params.get("stream_id", "")
        operation = ChangeType(params.get("operation", "insert"))
        
        event = self._cdc.record_change(
            stream_id=stream_id,
            operation=operation,
            before=params.get("before"),
            after=params.get("after"),
            keys=params.get("keys", {})
        )
        
        if not event:
            return ActionResult(success=False, message="Stream not found")
        
        return ActionResult(success=True, message=f"Change recorded: {event.event_id}",
                         data={"event_id": event.event_id, "sequence": event.sequence})
    
    def _get_changes(self, params: Dict[str, Any]) -> ActionResult:
        """Get changes from a stream."""
        stream_id = params.get("stream_id", "")
        limit = params.get("limit", 100)
        since_seq = params.get("since_sequence")
        since_ts = params.get("since_timestamp")
        
        changes = self._cdc.get_changes(stream_id, since_seq, since_ts, limit)
        return ActionResult(success=True, message=f"Found {len(changes)} changes",
                         data={"changes": [
                             {"event_id": c.event_id, "operation": c.operation.value,
                              "sequence": c.sequence, "timestamp": c.timestamp}
                             for c in changes
                         ]})
    
    def _get_offset(self, params: Dict[str, Any]) -> ActionResult:
        """Get stream offset."""
        stream_id = params.get("stream_id", "")
        offset = self._cdc.get_offset(stream_id)
        return ActionResult(success=True, message="Offset retrieved", data={"offset": offset})
    
    def _reset_offset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset stream offset."""
        stream_id = params.get("stream_id", "")
        reset = self._cdc.reset_offset(stream_id)
        return ActionResult(success=reset, message="Offset reset" if reset else "Stream not found")
    
    def _get_stream_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get stream info."""
        stream_id = params.get("stream_id", "")
        info = self._cdc.get_stream_info(stream_id)
        if not info:
            return ActionResult(success=False, message="Stream not found")
        return ActionResult(success=True, message="Info retrieved", data=info)
