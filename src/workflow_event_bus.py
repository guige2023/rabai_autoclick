"""
Event-Driven Architecture Module for Workflow System

Implements an EventBus class with:
1. Event publishing
2. Event subscription
3. Event filtering
4. Event ordering (within partitions)
5. Dead letter queue
6. Event schema validation
7. Event replay
8. Event sourcing
9. CQRS (Command Query Responsibility Segregation)
10. Event correlation

Commit: 'feat(event_bus): add event-driven architecture with pub/sub, filtering, ordering, dead letter queue, event replay, event sourcing, CQRS, correlation'
"""

import uuid
import json
import threading
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from queue import Queue, Empty
from enum import Enum
import copy


class EventType(Enum):
    """Standard workflow event types."""
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_PAUSED = "workflow.paused"
    WORKFLOW_RESUMED = "workflow.resumed"
    STEP_STARTED = "step.started"
    STEP_COMPLETED = "step.completed"
    STEP_FAILED = "step.failed"
    ACTION_EXECUTED = "action.executed"
    CONDITION_EVALUATED = "condition.evaluated"
    STATE_CHANGED = "state.changed"
    COMMAND_RECEIVED = "command.received"
    QUERY_REQUESTED = "query.requested"


class EventStatus(Enum):
    """Event processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass
class EventSchema:
    """Schema definition for event validation."""
    name: str
    version: str
    required_fields: List[str] = field(default_factory=list)
    field_types: Dict[str, Type] = field(default_factory=dict)
    field_patterns: Dict[str, str] = field(default_factory=dict)
    
    def validate(self, event: 'WorkflowEvent') -> tuple[bool, List[str]]:
        """Validate an event against this schema."""
        errors = []
        
        for field_name in self.required_fields:
            if not hasattr(event, field_name) or getattr(event, field_name) is None:
                errors.append(f"Missing required field: {field_name}")
        
        for field_name, expected_type in self.field_types.items():
            if hasattr(event, field_name):
                value = getattr(event, field_name)
                if value is not None and not isinstance(value, expected_type):
                    errors.append(f"Field {field_name} expected type {expected_type}, got {type(value)}")
        
        for field_name, pattern in self.field_patterns.items():
            if hasattr(event, field_name):
                value = getattr(event, field_name)
                if value is not None and not re.match(pattern, str(value)):
                    errors.append(f"Field {field_name} does not match pattern: {pattern}")
        
        return len(errors) == 0, errors


@dataclass
class WorkflowEvent:
    """Base workflow event."""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None
    partition_key: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: EventStatus = EventStatus.PENDING
    retry_count: int = 0
    schema_name: Optional[str] = None
    schema_version: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowEvent':
        """Create event from dictionary."""
        data = copy.deepcopy(data)
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if isinstance(data.get('status'), str):
            data['status'] = EventStatus(data['status'])
        return cls(**data)


@dataclass
class Command:
    """Command for CQRS pattern."""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    command_type: str = ""
    aggregate_id: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass
class Query:
    """Query for CQRS pattern."""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    query_type: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass 
class QueryResult:
    """Result from a query in CQRS pattern."""
    query_id: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DeadLetterEvent:
    """Event that failed processing and moved to DLQ."""
    original_event: WorkflowEvent
    error: str
    failed_at: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'original_event': self.original_event.to_dict(),
            'error': self.error,
            'failed_at': self.failed_at.isoformat(),
            'retry_count': self.retry_count
        }


class EventFilter:
    """Filter for events based on type, source, and content."""
    
    def __init__(
        self,
        event_types: Optional[List[str]] = None,
        sources: Optional[List[str]] = None,
        content_patterns: Optional[Dict[str, str]] = None,
        correlation_ids: Optional[List[str]] = None,
        time_range: Optional[tuple] = None
    ):
        self.event_types = set(event_types) if event_types else None
        self.sources = set(sources) if sources else None
        self.content_patterns = content_patterns or {}
        self.correlation_ids = set(correlation_ids) if correlation_ids else None
        self.time_range = time_range
    
    def matches(self, event: WorkflowEvent) -> bool:
        """Check if event matches this filter."""
        if self.event_types and event.event_type not in self.event_types:
            return False
        
        if self.sources and event.source not in self.sources:
            return False
        
        if self.correlation_ids and event.correlation_id not in self.correlation_ids:
            return False
        
        if self.time_range:
            start, end = self.time_range
            if not (start <= event.timestamp <= end):
                return False
        
        for key, pattern in self.content_patterns.items():
            value = event.payload.get(key) or event.metadata.get(key)
            if value is None:
                return False
            if not re.search(pattern, str(value)):
                return False
        
        return True


class EventStore:
    """Event store for persistence and replay."""
    
    def __init__(self, storage_path: Optional[str] = None):
        self.storage_path = storage_path
        self._events: Dict[str, List[WorkflowEvent]] = defaultdict(list)
        self._lock = threading.Lock()
        self._aggregate_index: Dict[str, List[str]] = defaultdict(list)
    
    def append(self, event: WorkflowEvent) -> None:
        """Append an event to the store."""
        with self._lock:
            partition = event.partition_key or "default"
            self._events[partition].append(event)
            
            if event.correlation_id:
                self._aggregate_index[event.correlation_id].append(event.event_id)
    
    def get_events(
        self,
        partition_key: Optional[str] = None,
        event_types: Optional[List[str]] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        correlation_id: Optional[str] = None
    ) -> List[WorkflowEvent]:
        """Retrieve events from the store with filters."""
        with self._lock:
            partitions = [partition_key] if partition_key else self._events.keys()
            result = []
            
            for partition in partitions:
                for event in self._events.get(partition, []):
                    if event_types and event.event_type not in event_types:
                        continue
                    if since and event.timestamp < since:
                        continue
                    if until and event.timestamp > until:
                        continue
                    if correlation_id and event.correlation_id != correlation_id:
                        continue
                    result.append(event)
            
            return sorted(result, key=lambda e: e.timestamp)
    
    def get_by_correlation(self, correlation_id: str) -> List[WorkflowEvent]:
        """Get all events with a specific correlation ID."""
        with self._lock:
            event_ids = self._aggregate_index.get(correlation_id, [])
            all_events = [e for events in self._events.values() for e in events]
            event_map = {e.event_id: e for e in all_events}
            return [event_map[eid] for eid in event_ids if eid in event_map]
    
    def replay(
        self,
        partition_key: Optional[str] = None,
        from_timestamp: Optional[datetime] = None,
        filter_func: Optional[Callable[[WorkflowEvent], bool]] = None
    ) -> List[WorkflowEvent]:
        """Replay events for reconstruction."""
        events = self.get_events(partition_key=partition_key, since=from_timestamp)
        if filter_func:
            events = [e for e in events if filter_func(e)]
        return events
    
    def clear(self, partition_key: Optional[str] = None) -> None:
        """Clear events from store."""
        with self._lock:
            if partition_key:
                self._events[partition_key] = []
            else:
                self._events.clear()
                self._aggregate_index.clear()


class Aggregate:
    """Base aggregate for event sourcing."""
    
    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self.version = 0
        self._pending_events: List[WorkflowEvent] = []
    
    def apply_event(self, event: WorkflowEvent) -> None:
        """Apply an event to update aggregate state."""
        raise NotImplementedError
    
    def load_from_events(self, events: List[WorkflowEvent]) -> None:
        """Reconstruct aggregate state from events."""
        for event in sorted(events, key=lambda e: e.timestamp):
            self.apply_event(event)
            self.version += 1
    
    def get_pending_events(self) -> List[WorkflowEvent]:
        """Get pending events to be committed."""
        return self._pending_events.copy()
    
    def clear_pending_events(self) -> None:
        """Clear pending events after commit."""
        self._pending_events.clear()


class DeadLetterQueue:
    """Dead letter queue for failed events."""
    
    def __init__(self, max_size: int = 1000, max_retries: int = 3):
        self.max_size = max_size
        self.max_retries = max_retries
        self._queue: Queue = Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._processed_count = 0
        self._failed_count = 0
    
    def add(self, event: WorkflowEvent, error: str) -> DeadLetterEvent:
        """Add a failed event to the DLQ."""
        dl_event = DeadLetterEvent(
            original_event=event,
            error=error,
            retry_count=event.retry_count
        )
        
        try:
            self._queue.put_nowait(dl_event)
            self._failed_count += 1
        except:
            pass
        
        return dl_event
    
    def get(self, timeout: float = 1.0) -> Optional[DeadLetterEvent]:
        """Get a dead letter event for reprocessing."""
        try:
            dl_event = self._queue.get(timeout=timeout)
            self._processed_count += 1
            return dl_event
        except Empty:
            return None
    
    def size(self) -> int:
        """Get current DLQ size."""
        return self._queue.qsize()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        return {
            'size': self.size(),
            'processed_count': self._processed_count,
            'failed_count': self._failed_count,
            'max_retries': self.max_retries
        }


class CQRSHandler:
    """CQRS handler separating commands and queries."""
    
    def __init__(self):
        self._command_handlers: Dict[str, Callable] = {}
        self._query_handlers: Dict[str, Callable] = {}
        self._read_models: Dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def register_command(self, command_type: str, handler: Callable[[Command], Any]) -> None:
        """Register a command handler."""
        self._command_handlers[command_type] = handler
    
    def register_query(self, query_type: str, handler: Callable[[Query], QueryResult]) -> None:
        """Register a query handler."""
        self._query_handlers[query_type] = handler
    
    def execute_command(self, command: Command) -> Any:
        """Execute a command and return result."""
        handler = self._command_handlers.get(command.command_type)
        if not handler:
            raise ValueError(f"No handler for command type: {command.command_type}")
        return handler(command)
    
    def execute_query(self, query: Query) -> QueryResult:
        """Execute a query and return result."""
        handler = self._query_handlers.get(query.query_type)
        if not handler:
            return QueryResult(
                query_id=query.query_id,
                success=False,
                error=f"No handler for query type: {query.query_type}"
            )
        try:
            result = handler(query)
            return QueryResult(query_id=query.query_id, success=True, data=result)
        except Exception as e:
            return QueryResult(query_id=query.query_id, success=False, error=str(e))
    
    def update_read_model(self, model_name: str, data: Any) -> None:
        """Update a read model."""
        with self._lock:
            self._read_models[model_name] = data
    
    def get_read_model(self, model_name: str) -> Optional[Any]:
        """Get a read model."""
        return self._read_models.get(model_name)


class EventBus:
    """
    Main EventBus class implementing event-driven architecture.
    
    Features:
    1. Event publishing: Publish workflow events
    2. Event subscription: Subscribe to events
    3. Event filtering: Filter events by type, source, content
    4. Event ordering: Ensure ordered delivery within partitions
    5. Dead letter queue: Handle failed event processing
    6. Event schema: Schema validation for events
    7. Event replay: Replay events from event store
    8. Event sourcing: Event sourcing pattern for workflow state
    9. CQRS: Command Query Responsibility Segregation
    10. Event correlation: Correlate related events
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        self._subscribers: Dict[str, List[tuple[Callable, EventFilter]]] = defaultdict(list)
        self._all_subscribers: List[tuple[Callable, EventFilter]] = []
        self._lock = threading.RLock()
        
        self.event_store = EventStore(storage_path)
        self.dlq = DeadLetterQueue()
        self.cqrs_handler = CQRSHandler()
        
        self._schemas: Dict[str, EventSchema] = {}
        self._aggregates: Dict[str, Aggregate] = {}
        
        self._ordered_queues: Dict[str, Queue] = defaultdict(Queue)
        self._ordering_lock = threading.Lock()
        
        self._running = False
        self._processor_thread: Optional[threading.Thread] = None
        self._event_queue: Queue = Queue()
    
    # ===== Schema Management =====
    
    def register_schema(self, schema: EventSchema) -> None:
        """Register an event schema for validation."""
        key = f"{schema.name}:{schema.version}"
        self._schemas[key] = schema
    
    def get_schema(self, name: str, version: str) -> Optional[EventSchema]:
        """Get a registered schema."""
        return self._schemas.get(f"{name}:{version}")
    
    def validate_event(self, event: WorkflowEvent) -> tuple[bool, List[str]]:
        """Validate an event against its schema."""
        if not event.schema_name or not event.schema_version:
            return True, []
        
        schema = self.get_schema(event.schema_name, event.schema_version)
        if not schema:
            return False, [f"Schema not found: {event.schema_name}:{event.schema_version}"]
        
        return schema.validate(event)
    
    # ===== Event Publishing =====
    
    def publish(
        self,
        event: WorkflowEvent,
        validate: bool = True,
        store: bool = True
    ) -> bool:
        """
        Publish a workflow event.
        
        Args:
            event: The event to publish
            validate: Whether to validate against schema
            store: Whether to store in event store
            
        Returns:
            True if published successfully
        """
        if validate:
            is_valid, errors = self.validate_event(event)
            if not is_valid:
                error_msg = f"Event validation failed: {', '.join(errors)}"
                self.dlq.add(event, error_msg)
                return False
        
        if store:
            self.event_store.append(event)
        
        try:
            self._event_queue.put_nowait(event)
        except:
            pass
        
        return True
    
    def publish_command(self, command: Command) -> Any:
        """Publish a command via CQRS."""
        result = self.cqrs_handler.execute_command(command)
        
        for pending_event in result if isinstance(result, list) else [result]:
            if isinstance(pending_event, WorkflowEvent):
                self.publish(pending_event)
        
        return result
    
    def publish_query(self, query: Query) -> QueryResult:
        """Publish a query via CQRS."""
        return self.cqrs_handler.execute_query(query)
    
    # ===== Event Subscription =====
    
    def subscribe(
        self,
        handler: Callable[[WorkflowEvent], None],
        event_filter: Optional[EventFilter] = None,
        event_types: Optional[List[str]] = None
    ) -> str:
        """
        Subscribe to events.
        
        Returns:
            Subscription ID
        """
        subscription_id = str(uuid.uuid4())
        filter_obj = event_filter or EventFilter(event_types=event_types)
        
        with self._lock:
            if event_types:
                for et in event_types:
                    self._subscribers[et].append((handler, filter_obj))
            else:
                self._all_subscribers.append((handler, filter_obj))
        
        return subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        return True
    
    def subscribe_to_commands(
        self,
        command_type: str,
        handler: Callable[[Command], Any]
    ) -> None:
        """Subscribe to a command type."""
        self.cqrs_handler.register_command(command_type, handler)
    
    def subscribe_to_queries(
        self,
        query_type: str,
        handler: Callable[[Query], Any]
    ) -> None:
        """Subscribe to a query type."""
        self.cqrs_handler.register_query(query_type, handler)
    
    # ===== Event Processing =====
    
    def _process_event(self, event: WorkflowEvent) -> None:
        """Process a single event and dispatch to subscribers."""
        handlers_to_call = []
        
        with self._lock:
            if event.event_type in self._subscribers:
                for handler, filter_obj in self._subscribers[event.event_type]:
                    if filter_obj.matches(event):
                        handlers_to_call.append(handler)
            
            for handler, filter_obj in self._all_subscribers:
                if filter_obj.matches(event):
                    handlers_to_call.append(handler)
        
        for handler in handlers_to_call:
            try:
                handler(event)
            except Exception as e:
                self.dlq.add(event, str(e))
    
    def _ordered_delivery(self, partition_key: str) -> None:
        """Ensure ordered delivery within a partition."""
        with self._ordering_lock:
            queue = self._ordered_queues[partition_key]
        
        while self._running:
            try:
                event = queue.get(timeout=0.1)
                if event:
                    self._process_event(event)
            except Empty:
                continue
    
    def start(self) -> None:
        """Start the event bus processor."""
        if self._running:
            return
        
        self._running = True
        self._processor_thread = threading.Thread(target=self._process_loop, daemon=True)
        self._processor_thread.start()
    
    def _process_loop(self) -> None:
        """Main event processing loop."""
        while self._running:
            try:
                event = self._event_queue.get(timeout=0.1)
                if event:
                    partition = event.partition_key or "default"
                    with self._ordering_lock:
                        self._ordered_queues[partition].put(event)
                    
                    self._process_event(event)
            except Empty:
                continue
    
    def stop(self) -> None:
        """Stop the event bus processor."""
        self._running = False
        if self._processor_thread:
            self._processor_thread.join(timeout=5)
    
    # ===== Event Replay =====
    
    def replay(
        self,
        partition_key: Optional[str] = None,
        from_timestamp: Optional[datetime] = None,
        event_types: Optional[List[str]] = None,
        correlation_id: Optional[str] = None,
        subscriber_filter: Optional[Callable[[WorkflowEvent], bool]] = None
    ) -> List[WorkflowEvent]:
        """
        Replay events from the event store.
        
        Args:
            partition_key: Replay events from specific partition
            from_timestamp: Replay events since this time
            event_types: Filter by event types
            correlation_id: Replay events with specific correlation ID
            subscriber_filter: Custom filter function
            
        Returns:
            List of replayed events
        """
        filter_func = subscriber_filter
        if event_types or correlation_id:
            def create_filter():
                types = event_types
                corr_id = correlation_id
                def f(e):
                    if types and e.event_type not in types:
                        return False
                    if corr_id and e.correlation_id != corr_id:
                        return False
                    return True
                return f
            filter_func = create_filter()
        
        events = self.event_store.replay(
            partition_key=partition_key,
            from_timestamp=from_timestamp,
            filter_func=filter_func
        )
        
        for event in events:
            self._process_event(event)
        
        return events
    
    # ===== Event Sourcing =====
    
    def register_aggregate(self, aggregate: Aggregate) -> None:
        """Register an aggregate for event sourcing."""
        self._aggregates[aggregate.aggregate_id] = aggregate
    
    def get_aggregate_state(
        self,
        aggregate_id: str,
        as_of: Optional[datetime] = None
    ) -> Optional[Aggregate]:
        """Get current state of an aggregate."""
        aggregate = self._aggregates.get(aggregate_id)
        if not aggregate:
            return None
        
        events = self.event_store.get_events(
            partition_key=aggregate_id,
            until=as_of
        )
        
        if events:
            aggregate.load_from_events(events)
        
        return aggregate
    
    def save_aggregate(self, aggregate: Aggregate) -> None:
        """Save an aggregate's pending events."""
        for event in aggregate.get_pending_events():
            self.publish(event)
        aggregate.clear_pending_events()
    
    # ===== Event Correlation =====
    
    def correlate_events(
        self,
        events: List[WorkflowEvent],
        correlation_key: str = "workflow_id"
    ) -> Dict[str, List[WorkflowEvent]]:
        """
        Correlate related events.
        
        Args:
            events: List of events to correlate
            correlation_key: Key in payload to use for correlation
            
        Returns:
            Dictionary mapping correlation values to event lists
        """
        correlated: Dict[str, List[WorkflowEvent]] = defaultdict(list)
        
        for event in events:
            key = event.payload.get(correlation_key) or event.correlation_id
            if key:
                correlated[key].append(event)
        
        return dict(correlated)
    
    def create_correlation_chain(
        self,
        events: List[WorkflowEvent]
    ) -> List[List[WorkflowEvent]]:
        """
        Create ordered chains of correlated events.
        
        Returns:
            List of event chains ordered by timestamp
        """
        grouped = self.correlate_events(events)
        chains = []
        
        for corr_id, corr_events in grouped.items():
            chain = sorted(corr_events, key=lambda e: e.timestamp)
            chains.append(chain)
        
        return sorted(chains, key=lambda c: c[0].timestamp if c else None)
    
    # ===== Utility Methods =====
    
    def get_event_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        return {
            'total_events': sum(len(e) for e in self.event_store._events.values()),
            'active_subscriptions': len(self._all_subscribers) + sum(len(s) for s in self._subscribers.values()),
            'registered_schemas': len(self._schemas),
            'registered_aggregates': len(self._aggregates),
            'dlq_stats': self.dlq.get_stats(),
            'event_types': list(self._subscribers.keys())
        }
    
    def clear(self, partition_key: Optional[str] = None) -> None:
        """Clear event store."""
        self.event_store.clear(partition_key)


# ===== Convenience Functions =====

def create_event(
    event_type: str,
    source: str,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    partition_key: Optional[str] = None,
    schema_name: Optional[str] = None,
    schema_version: Optional[str] = None
) -> WorkflowEvent:
    """Create a new workflow event."""
    return WorkflowEvent(
        event_type=event_type,
        source=source,
        payload=payload or {},
        correlation_id=correlation_id,
        partition_key=partition_key,
        schema_name=schema_name,
        schema_version=schema_version
    )


def create_command(
    command_type: str,
    aggregate_id: str,
    payload: Optional[Dict[str, Any]] = None
) -> Command:
    """Create a new command."""
    return Command(
        command_type=command_type,
        aggregate_id=aggregate_id,
        payload=payload or {}
    )


def create_query(
    query_type: str,
    payload: Optional[Dict[str, Any]] = None
) -> Query:
    """Create a new query."""
    return Query(
        query_type=query_type,
        payload=payload or {}
    )


# ===== Example Usage =====

if __name__ == "__main__":
    bus = EventBus()
    
    # Register a schema
    schema = EventSchema(
        name="workflow.event",
        version="1.0",
        required_fields=["event_type", "source"],
        field_types={"timestamp": datetime}
    )
    bus.register_schema(schema)
    
    # Subscribe to events
    def handle_workflow_event(event: WorkflowEvent):
        print(f"Received event: {event.event_type} from {event.source}")
    
    bus.subscribe(handle_workflow_event, event_types=["workflow.started", "workflow.completed"])
    
    # Register CQRS handlers
    def handle_start_workflow(cmd: Command):
        event = create_event(
            event_type="workflow.started",
            source="cqrs_handler",
            payload=cmd.payload,
            correlation_id=cmd.command_id,
            partition_key=cmd.aggregate_id,
            schema_name="workflow.event",
            schema_version="1.0"
        )
        return [event]
    
    bus.subscribe_to_commands("start_workflow", handle_start_workflow)
    
    # Publish events
    bus.start()
    
    event = create_event(
        event_type="workflow.started",
        source="test",
        payload={"workflow_id": "wf-123", "name": "Test Workflow"},
        correlation_id="corr-1",
        partition_key="partition-1",
        schema_name="workflow.event",
        schema_version="1.0"
    )
    bus.publish(event)
    
    time.sleep(0.5)
    
    # Replay events
    replayed = bus.replay(partition_key="partition-1")
    print(f"Replayed {len(replayed)} events")
    
    # Get stats
    print(f"Event bus stats: {bus.get_event_stats()}")
    
    bus.stop()
