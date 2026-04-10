"""
Tests for Workflow Event Bus Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_event_bus import (
    EventType,
    EventStatus,
    EventSchema,
    WorkflowEvent,
    Command,
    Query,
    QueryResult,
    DeadLetterEvent,
    EventFilter,
    EventStore,
    Aggregate,
    DeadLetterQueue,
    CQRSHandler,
    EventBus,
    create_event,
    create_command,
    create_query
)


class TestEventEnums(unittest.TestCase):
    """Test event enums"""

    def test_event_type_values(self):
        """Test EventType enum values"""
        self.assertEqual(EventType.WORKFLOW_STARTED.value, "workflow.started")
        self.assertEqual(EventType.WORKFLOW_COMPLETED.value, "workflow.completed")
        self.assertEqual(EventType.WORKFLOW_FAILED.value, "workflow.failed")
        self.assertEqual(EventType.STEP_STARTED.value, "step.started")
        self.assertEqual(EventType.ACTION_EXECUTED.value, "action.executed")

    def test_event_status_values(self):
        """Test EventStatus enum values"""
        self.assertEqual(EventStatus.PENDING.value, "pending")
        self.assertEqual(EventStatus.PROCESSING.value, "processing")
        self.assertEqual(EventStatus.COMPLETED.value, "completed")
        self.assertEqual(EventStatus.FAILED.value, "failed")
        self.assertEqual(EventStatus.DEAD_LETTER.value, "dead_letter")


class TestEventSchema(unittest.TestCase):
    """Test EventSchema class"""

    def test_create_schema(self):
        """Test creating an event schema"""
        schema = EventSchema(
            name="workflow.event",
            version="1.0",
            required_fields=["event_type", "source"],
            field_types={"timestamp": datetime}
        )
        
        self.assertEqual(schema.name, "workflow.event")
        self.assertEqual(schema.version, "1.0")
        self.assertEqual(len(schema.required_fields), 2)

    def test_validate_valid_event(self):
        """Test validating a valid event"""
        schema = EventSchema(
            name="workflow.event",
            version="1.0",
            required_fields=["event_type", "source"],
            field_types={}
        )
        
        event = WorkflowEvent(
            event_type="test",
            source="test_source"
        )
        
        is_valid, errors = schema.validate(event)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_validate_missing_required_field(self):
        """Test validation fails when required field is missing"""
        schema = EventSchema(
            name="workflow.event",
            version="1.0",
            required_fields=["event_type", "source", "payload"],
            field_types={}
        )
        
        event = WorkflowEvent(
            event_type="test",
            source="test_source",
            payload=None  # Explicitly set to None to trigger validation error
        )
        
        is_valid, errors = schema.validate(event)
        self.assertFalse(is_valid)
        self.assertIn("Missing required field: payload", errors)

    def test_validate_field_type_mismatch(self):
        """Test validation fails on type mismatch"""
        schema = EventSchema(
            name="workflow.event",
            version="1.0",
            required_fields=[],
            field_types={"retry_count": int}
        )
        
        event = WorkflowEvent(
            event_type="test",
            source="test_source",
            retry_count="not_an_int"
        )
        
        is_valid, errors = schema.validate(event)
        self.assertFalse(is_valid)
        self.assertTrue(any("retry_count" in e and "int" in e for e in errors))

    def test_validate_field_pattern(self):
        """Test validation with regex pattern"""
        schema = EventSchema(
            name="workflow.event",
            version="1.0",
            required_fields=[],
            field_patterns={"event_type": r"^\w+\.\w+$"}
        )
        
        event = WorkflowEvent(event_type="valid.type", source="test")
        is_valid, errors = schema.validate(event)
        self.assertTrue(is_valid)
        
        event_bad = WorkflowEvent(event_type="invalid-type!", source="test")
        is_valid, errors = schema.validate(event_bad)
        self.assertFalse(is_valid)


class TestWorkflowEvent(unittest.TestCase):
    """Test WorkflowEvent dataclass"""

    def test_create_event(self):
        """Test creating a workflow event"""
        event = WorkflowEvent(
            event_type="workflow.started",
            source="test",
            payload={"workflow_id": "wf-001"},
            correlation_id="corr-001"
        )
        
        self.assertIsNotNone(event.event_id)
        self.assertEqual(event.event_type, "workflow.started")
        self.assertEqual(event.payload["workflow_id"], "wf-001")
        self.assertEqual(event.status, EventStatus.PENDING)

    def test_to_dict(self):
        """Test converting event to dictionary"""
        event = WorkflowEvent(
            event_type="test",
            source="test_source"
        )
        
        data = event.to_dict()
        
        self.assertIn("event_id", data)
        self.assertIn("timestamp", data)
        self.assertEqual(data["status"], "pending")

    def test_from_dict(self):
        """Test creating event from dictionary"""
        original = WorkflowEvent(
            event_type="test",
            source="test_source",
            payload={"key": "value"},
            correlation_id="corr-001"
        )
        
        data = original.to_dict()
        restored = WorkflowEvent.from_dict(data)
        
        self.assertEqual(restored.event_type, original.event_type)
        self.assertEqual(restored.source, original.source)
        self.assertEqual(restored.correlation_id, original.correlation_id)


class TestCommand(unittest.TestCase):
    """Test Command dataclass"""

    def test_create_command(self):
        """Test creating a command"""
        cmd = Command(
            command_type="start_workflow",
            aggregate_id="wf-001",
            payload={"name": "Test Workflow"}
        )
        
        self.assertIsNotNone(cmd.command_id)
        self.assertEqual(cmd.command_type, "start_workflow")
        self.assertEqual(cmd.aggregate_id, "wf-001")

    def test_command_to_dict(self):
        """Test command to dictionary"""
        cmd = Command(
            command_type="test",
            aggregate_id="agg-001"
        )
        
        data = cmd.to_dict()
        
        self.assertIn("command_id", data)
        self.assertIn("timestamp", data)


class TestQuery(unittest.TestCase):
    """Test Query dataclass"""

    def test_create_query(self):
        """Test creating a query"""
        query = Query(
            query_type="get_workflow_status",
            payload={"workflow_id": "wf-001"}
        )
        
        self.assertIsNotNone(query.query_id)
        self.assertEqual(query.query_type, "get_workflow_status")


class TestQueryResult(unittest.TestCase):
    """Test QueryResult dataclass"""

    def test_create_query_result(self):
        """Test creating a query result"""
        result = QueryResult(
            query_id="q-001",
            success=True,
            data={"status": "running"}
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.data["status"], "running")


class TestDeadLetterEvent(unittest.TestCase):
    """Test DeadLetterEvent dataclass"""

    def test_create_dlq_event(self):
        """Test creating a dead letter event"""
        event = WorkflowEvent(
            event_type="test",
            source="test"
        )
        dl_event = DeadLetterEvent(
            original_event=event,
            error="Processing failed",
            retry_count=3
        )
        
        self.assertEqual(dl_event.original_event.event_type, "test")
        self.assertEqual(dl_event.error, "Processing failed")
        self.assertEqual(dl_event.retry_count, 3)

    def test_dlq_event_to_dict(self):
        """Test DLQ event to dictionary"""
        event = WorkflowEvent(event_type="test", source="test")
        dl_event = DeadLetterEvent(
            original_event=event,
            error="Failed"
        )
        
        data = dl_event.to_dict()
        
        self.assertIn("original_event", data)
        self.assertIn("error", data)
        self.assertIn("failed_at", data)


class TestEventFilter(unittest.TestCase):
    """Test EventFilter class"""

    def test_create_filter(self):
        """Test creating an event filter"""
        filter_obj = EventFilter(
            event_types=["workflow.started", "workflow.completed"],
            sources=["test_source"],
            content_patterns={"workflow_id": "^wf-"},
            correlation_ids=["corr-001"],
            time_range=(datetime.utcnow() - timedelta(hours=1), datetime.utcnow())
        )
        
        self.assertEqual(len(filter_obj.event_types), 2)
        self.assertIn("workflow_id", filter_obj.content_patterns)

    def test_filter_matches_event_type(self):
        """Test filter matching by event type"""
        filter_obj = EventFilter(event_types=["workflow.started"])
        
        event1 = WorkflowEvent(event_type="workflow.started", source="test")
        event2 = WorkflowEvent(event_type="workflow.completed", source="test")
        
        self.assertTrue(filter_obj.matches(event1))
        self.assertFalse(filter_obj.matches(event2))

    def test_filter_matches_source(self):
        """Test filter matching by source"""
        filter_obj = EventFilter(sources=["source_a", "source_b"])
        
        event1 = WorkflowEvent(event_type="test", source="source_a")
        event2 = WorkflowEvent(event_type="test", source="source_c")
        
        self.assertTrue(filter_obj.matches(event1))
        self.assertFalse(filter_obj.matches(event2))

    def test_filter_matches_correlation_id(self):
        """Test filter matching by correlation ID"""
        filter_obj = EventFilter(correlation_ids=["corr-001", "corr-002"])
        
        event1 = WorkflowEvent(event_type="test", source="test", correlation_id="corr-001")
        event2 = WorkflowEvent(event_type="test", source="test", correlation_id="corr-003")
        
        self.assertTrue(filter_obj.matches(event1))
        self.assertFalse(filter_obj.matches(event2))

    def test_filter_matches_content_pattern(self):
        """Test filter matching by content pattern"""
        filter_obj = EventFilter(content_patterns={"workflow_id": r"^wf-\d+$"})
        
        event1 = WorkflowEvent(event_type="test", source="test", payload={"workflow_id": "wf-123"})
        event2 = WorkflowEvent(event_type="test", source="test", payload={"workflow_id": "invalid"})
        
        self.assertTrue(filter_obj.matches(event1))
        self.assertFalse(filter_obj.matches(event2))


class TestEventStore(unittest.TestCase):
    """Test EventStore class"""

    def setUp(self):
        """Set up test fixtures"""
        self.store = EventStore(storage_path="/tmp/test_events")

    def test_append_event(self):
        """Test appending an event to store"""
        event = WorkflowEvent(
            event_type="workflow.started",
            source="test",
            partition_key="partition-1"
        )
        
        self.store.append(event)
        
        events = self.store.get_events(partition_key="partition-1")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "workflow.started")

    def test_get_events_with_filters(self):
        """Test getting events with various filters"""
        old_event = WorkflowEvent(
            event_type="workflow.started",
            source="test",
            timestamp=datetime.utcnow() - timedelta(days=2)
        )
        new_event = WorkflowEvent(
            event_type="workflow.completed",
            source="test",
            timestamp=datetime.utcnow()
        )
        
        self.store.append(old_event)
        self.store.append(new_event)
        
        events = self.store.get_events(
            event_types=["workflow.completed"],
            since=datetime.utcnow() - timedelta(days=1)
        )
        
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "workflow.completed")

    def test_get_by_correlation(self):
        """Test getting events by correlation ID"""
        event1 = WorkflowEvent(
            event_type="step.started",
            source="test",
            correlation_id="corr-001"
        )
        event2 = WorkflowEvent(
            event_type="step.completed",
            source="test",
            correlation_id="corr-001"
        )
        event3 = WorkflowEvent(
            event_type="step.started",
            source="test",
            correlation_id="corr-002"
        )
        
        self.store.append(event1)
        self.store.append(event2)
        self.store.append(event3)
        
        events = self.store.get_by_correlation("corr-001")
        
        self.assertEqual(len(events), 2)

    def test_replay(self):
        """Test replaying events"""
        event1 = WorkflowEvent(
            event_type="workflow.started",
            source="test",
            timestamp=datetime.utcnow() - timedelta(hours=2)
        )
        event2 = WorkflowEvent(
            event_type="workflow.completed",
            source="test",
            timestamp=datetime.utcnow() - timedelta(hours=1)
        )
        
        self.store.append(event1)
        self.store.append(event2)
        
        # Replay from 90 minutes ago should only get the recent event
        replayed = self.store.replay(from_timestamp=datetime.utcnow() - timedelta(minutes=90))
        
        self.assertEqual(len(replayed), 1)
        self.assertEqual(replayed[0].event_type, "workflow.completed")

    def test_clear_partition(self):
        """Test clearing events for a partition"""
        event1 = WorkflowEvent(event_type="test", source="test", partition_key="p1")
        event2 = WorkflowEvent(event_type="test", source="test", partition_key="p2")
        
        self.store.append(event1)
        self.store.append(event2)
        
        self.store.clear(partition_key="p1")
        
        events_p1 = self.store.get_events(partition_key="p1")
        events_p2 = self.store.get_events(partition_key="p2")
        
        self.assertEqual(len(events_p1), 0)
        self.assertEqual(len(events_p2), 1)


class TestAggregate(unittest.TestCase):
    """Test Aggregate base class"""

    def test_aggregate_initialization(self):
        """Test aggregate initialization"""
        class TestAggregate(Aggregate):
            def apply_event(self, event):
                pass
        
        agg = TestAggregate(aggregate_id="agg-001")
        
        self.assertEqual(agg.aggregate_id, "agg-001")
        self.assertEqual(agg.version, 0)

    def test_load_from_events(self):
        """Test loading aggregate from events"""
        class TestAggregate(Aggregate):
            def __init__(self, aggregate_id):
                super().__init__(aggregate_id)
                self.events_applied = 0
                
            def apply_event(self, event):
                self.events_applied += 1
        
        agg = TestAggregate("agg-001")
        
        events = [
            WorkflowEvent(event_type="e1", source="test", timestamp=datetime.utcnow() - timedelta(seconds=1)),
            WorkflowEvent(event_type="e2", source="test", timestamp=datetime.utcnow())
        ]
        
        agg.load_from_events(events)
        
        self.assertEqual(agg.events_applied, 2)
        self.assertEqual(agg.version, 2)

    def test_pending_events(self):
        """Test getting pending events"""
        class TestAggregate(Aggregate):
            def apply_event(self, event):
                pass
        
        agg = TestAggregate("agg-001")
        event = WorkflowEvent(event_type="test", source="test")
        agg._pending_events.append(event)
        
        pending = agg.get_pending_events()
        
        self.assertEqual(len(pending), 1)
        
        agg.clear_pending_events()
        self.assertEqual(len(agg._pending_events), 0)


class TestDeadLetterQueue(unittest.TestCase):
    """Test DeadLetterQueue class"""

    def test_add_to_dlq(self):
        """Test adding an event to DLQ"""
        dlq = DeadLetterQueue(max_size=100, max_retries=3)
        event = WorkflowEvent(event_type="test", source="test")
        
        dl_event = dlq.add(event, "Processing failed")
        
        self.assertEqual(dl_event.error, "Processing failed")
        self.assertEqual(dlq.size(), 1)

    def test_get_from_dlq(self):
        """Test getting an event from DLQ"""
        dlq = DeadLetterQueue()
        event = WorkflowEvent(event_type="test", source="test")
        dlq.add(event, "Failed")
        
        dl_event = dlq.get(timeout=1.0)
        
        self.assertIsNotNone(dl_event)
        self.assertEqual(dl_event.original_event.event_type, "test")

    def test_dlq_get_stats(self):
        """Test getting DLQ statistics"""
        dlq = DeadLetterQueue(max_retries=5)
        event = WorkflowEvent(event_type="test", source="test")
        dlq.add(event, "Failed")
        
        stats = dlq.get_stats()
        
        self.assertIn("size", stats)
        self.assertIn("failed_count", stats)
        self.assertIn("max_retries", stats)
        self.assertEqual(stats["max_retries"], 5)


class TestCQRSHandler(unittest.TestCase):
    """Test CQRSHandler class"""

    def setUp(self):
        """Set up test fixtures"""
        self.handler = CQRSHandler()

    def test_register_command(self):
        """Test registering a command handler"""
        def handler(cmd):
            return "handled"
        
        self.handler.register_command("test_command", handler)
        
        self.assertIn("test_command", self.handler._command_handlers)

    def test_execute_command(self):
        """Test executing a command"""
        def handler(cmd):
            return f"handled: {cmd.command_type}"
        
        self.handler.register_command("test_cmd", handler)
        cmd = Command(command_type="test_cmd", aggregate_id="agg-001")
        
        result = self.handler.execute_command(cmd)
        
        self.assertEqual(result, "handled: test_cmd")

    def test_execute_command_no_handler(self):
        """Test executing command with no handler"""
        cmd = Command(command_type="unknown", aggregate_id="agg-001")
        
        with self.assertRaises(ValueError):
            self.handler.execute_command(cmd)

    def test_register_query(self):
        """Test registering a query handler"""
        def handler(query):
            return {"result": "data"}
        
        self.handler.register_query("test_query", handler)
        
        self.assertIn("test_query", self.handler._query_handlers)

    def test_execute_query_success(self):
        """Test executing a query successfully"""
        def handler(query):
            return {"status": "ok"}
        
        self.handler.register_query("test_q", handler)
        query = Query(query_type="test_q")
        
        result = self.handler.execute_query(query)
        
        self.assertTrue(result.success)
        self.assertEqual(result.data["status"], "ok")

    def test_execute_query_no_handler(self):
        """Test executing query with no handler"""
        query = Query(query_type="unknown")
        
        result = self.handler.execute_query(query)
        
        self.assertFalse(result.success)
        self.assertIn("No handler", result.error)

    def test_read_model(self):
        """Test updating and getting read model"""
        self.handler.update_read_model("workflow_stats", {"total": 100})
        
        model = self.handler.get_read_model("workflow_stats")
        
        self.assertEqual(model["total"], 100)


class TestEventBus(unittest.TestCase):
    """Test EventBus class"""

    def setUp(self):
        """Set up test fixtures"""
        self.bus = EventBus(storage_path="/tmp/test_bus")

    def tearDown(self):
        """Tear down test fixtures"""
        self.bus.stop()

    def test_init(self):
        """Test EventBus initialization"""
        self.assertIsNotNone(self.bus.event_store)
        self.assertIsNotNone(self.bus.dlq)
        self.assertIsNotNone(self.bus.cqrs_handler)

    def test_register_schema(self):
        """Test registering an event schema"""
        schema = EventSchema(
            name="workflow.event",
            version="1.0",
            required_fields=["event_type"]
        )
        
        self.bus.register_schema(schema)
        
        retrieved = self.bus.get_schema("workflow.event", "1.0")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "workflow.event")

    def test_validate_event_with_schema(self):
        """Test event validation with registered schema"""
        schema = EventSchema(
            name="test.event",
            version="1.0",
            required_fields=["event_type", "source"]
        )
        self.bus.register_schema(schema)
        
        valid_event = WorkflowEvent(
            event_type="test",
            source="test",
            schema_name="test.event",
            schema_version="1.0"
        )
        
        is_valid, errors = self.bus.validate_event(valid_event)
        self.assertTrue(is_valid)

    def test_validate_event_missing_schema(self):
        """Test event validation with missing schema"""
        event = WorkflowEvent(
            event_type="test",
            source="test",
            schema_name="unknown",
            schema_version="1.0"
        )
        
        is_valid, errors = self.bus.validate_event(event)
        self.assertFalse(is_valid)
        self.assertIn("Schema not found", errors[0])

    def test_publish_event(self):
        """Test publishing an event"""
        event = WorkflowEvent(
            event_type="workflow.started",
            source="test",
            payload={"workflow_id": "wf-001"}
        )
        
        result = self.bus.publish(event, validate=False)
        
        self.assertTrue(result)
        events = self.bus.event_store.get_events()
        self.assertEqual(len(events), 1)

    def test_publish_event_with_validation(self):
        """Test publishing an event with validation"""
        schema = EventSchema(
            name="test.event",
            version="1.0",
            required_fields=["event_type", "source", "payload"]
        )
        self.bus.register_schema(schema)
        
        valid_event = WorkflowEvent(
            event_type="test",
            source="test",
            payload={"data": "value"},
            schema_name="test.event",
            schema_version="1.0"
        )
        
        result = self.bus.publish(valid_event)
        self.assertTrue(result)

    def test_publish_invalid_event(self):
        """Test publishing an invalid event goes to DLQ"""
        schema = EventSchema(
            name="test.event",
            version="1.0",
            required_fields=["payload"]
        )
        self.bus.register_schema(schema)
        
        invalid_event = WorkflowEvent(
            event_type="test",
            source="test",
            payload=None,  # Explicitly set to None to fail validation
            schema_name="test.event",
            schema_version="1.0"
        )
        
        result = self.bus.publish(invalid_event)
        
        self.assertFalse(result)
        self.assertGreater(self.bus.dlq.size(), 0)

    def test_subscribe(self):
        """Test subscribing to events"""
        received_events = []
        
        def handler(event):
            received_events.append(event)
        
        sub_id = self.bus.subscribe(handler, event_types=["workflow.started"])
        
        self.assertIsNotNone(sub_id)

    def test_publish_and_receive(self):
        """Test publishing and receiving events"""
        received = []
        
        def handler(event):
            received.append(event)
        
        self.bus.subscribe(handler, event_types=["workflow.started"])
        self.bus.start()
        
        event = WorkflowEvent(event_type="workflow.started", source="test")
        self.bus.publish(event, validate=False)
        
        time.sleep(0.2)
        
        self.assertEqual(len(received), 1)
        self.bus.stop()

    def test_publish_command(self):
        """Test publishing a command"""
        def handler(cmd):
            event = WorkflowEvent(
                event_type="workflow.started",
                source="cqrs",
                payload=cmd.payload
            )
            return [event]
        
        self.bus.subscribe_to_commands("start_workflow", handler)
        
        cmd = create_command("start_workflow", "wf-001", {"name": "Test"})
        result = self.bus.publish_command(cmd)
        
        self.assertIsNotNone(result)

    def test_publish_query(self):
        """Test publishing a query"""
        def handler(query):
            return {"status": "running"}
        
        self.bus.subscribe_to_queries("get_status", handler)
        
        query = create_query("get_status", {"workflow_id": "wf-001"})
        result = self.bus.publish_query(query)
        
        self.assertTrue(result.success)
        self.assertEqual(result.data["status"], "running")

    def test_replay(self):
        """Test replaying events"""
        event1 = WorkflowEvent(
            event_type="workflow.started",
            source="test",
            timestamp=datetime.utcnow() - timedelta(minutes=5)
        )
        event2 = WorkflowEvent(
            event_type="workflow.completed",
            source="test",
            timestamp=datetime.utcnow() - timedelta(minutes=4)
        )
        
        self.bus.publish(event1, validate=False)
        self.bus.publish(event2, validate=False)
        
        received = []
        def handler(e):
            received.append(e)
        
        self.bus.subscribe(handler)
        self.bus.start()
        
        replayed = self.bus.replay(
            from_timestamp=datetime.utcnow() - timedelta(minutes=10),
            event_types=["workflow.started"]
        )
        
        time.sleep(0.2)
        
        self.assertGreater(len(replayed), 0)
        self.bus.stop()

    def test_register_aggregate(self):
        """Test registering an aggregate"""
        class TestAggregate(Aggregate):
            def apply_event(self, event):
                pass
        
        agg = TestAggregate("agg-001")
        
        self.bus.register_aggregate(agg)
        
        retrieved = self.bus.get_aggregate_state("agg-001")
        self.assertIsNotNone(retrieved)

    def test_correlate_events(self):
        """Test correlating events"""
        events = [
            WorkflowEvent(event_type="e1", source="test", payload={"workflow_id": "wf-001"}),
            WorkflowEvent(event_type="e2", source="test", payload={"workflow_id": "wf-001"}),
            WorkflowEvent(event_type="e3", source="test", payload={"workflow_id": "wf-002"})
        ]
        
        correlated = self.bus.correlate_events(events, correlation_key="workflow_id")
        
        self.assertEqual(len(correlated["wf-001"]), 2)
        self.assertEqual(len(correlated["wf-002"]), 1)

    def test_create_correlation_chain(self):
        """Test creating correlation chains"""
        events = [
            WorkflowEvent(
                event_type="step.completed",
                source="test",
                correlation_id="chain-1",
                timestamp=datetime.utcnow() - timedelta(seconds=2)
            ),
            WorkflowEvent(
                event_type="step.started",
                source="test",
                correlation_id="chain-1",
                timestamp=datetime.utcnow() - timedelta(seconds=1)
            )
        ]
        
        chains = self.bus.create_correlation_chain(events)
        
        self.assertEqual(len(chains), 1)
        self.assertEqual(len(chains[0]), 2)

    def test_get_event_stats(self):
        """Test getting event bus statistics"""
        event = WorkflowEvent(event_type="test", source="test")
        self.bus.publish(event, validate=False)
        
        stats = self.bus.get_event_stats()
        
        self.assertIn("total_events", stats)
        self.assertIn("active_subscriptions", stats)
        self.assertIn("registered_schemas", stats)
        self.assertIn("dlq_stats", stats)

    def test_clear(self):
        """Test clearing event store"""
        event = WorkflowEvent(event_type="test", source="test")
        self.bus.publish(event, validate=False)
        
        self.bus.clear()
        
        stats = self.bus.get_event_stats()
        self.assertEqual(stats["total_events"], 0)


class TestCreateFunctions(unittest.TestCase):
    """Test convenience create functions"""

    def test_create_event(self):
        """Test create_event convenience function"""
        event = create_event(
            event_type="workflow.started",
            source="test",
            payload={"key": "value"},
            correlation_id="corr-001",
            partition_key="partition-1",
            schema_name="workflow.event",
            schema_version="1.0"
        )
        
        self.assertEqual(event.event_type, "workflow.started")
        self.assertEqual(event.correlation_id, "corr-001")
        self.assertEqual(event.partition_key, "partition-1")

    def test_create_command(self):
        """Test create_command convenience function"""
        cmd = create_command(
            command_type="start",
            aggregate_id="agg-001",
            payload={"data": "value"}
        )
        
        self.assertEqual(cmd.command_type, "start")
        self.assertEqual(cmd.aggregate_id, "agg-001")

    def test_create_query(self):
        """Test create_query convenience function"""
        query = create_query(
            query_type="get_status",
            payload={"workflow_id": "wf-001"}
        )
        
        self.assertEqual(query.query_type, "get_status")
        self.assertEqual(query.payload["workflow_id"], "wf-001")


if __name__ == '__main__':
    unittest.main()
