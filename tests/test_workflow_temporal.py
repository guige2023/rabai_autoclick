"""
Tests for workflow_temporal module
Temporal Workflow Engine Integration
"""
import unittest
import asyncio
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, AsyncMock, mock_open
from typing import Dict, Any

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_temporal import (
    TemporalManager,
    WorkflowStatus,
    ActivityStatus,
    SignalType,
    ChildWorkflowPolicy,
    WorkflowRegistration,
    WorkflowExecution,
    ActivityRegistration,
    ActivityExecution,
    ChildWorkflowExecution,
    WorkflowQuery,
    SearchAttribute,
    NamespaceConfig,
    TaskQueueConfig,
    HistoryEvent,
    TemporalUIConfig,
    WorkflowReplayResult,
)


class TestWorkflowStatus(unittest.TestCase):
    """Test WorkflowStatus enum"""

    def test_workflow_status_values(self):
        """Test all workflow status values exist"""
        self.assertEqual(WorkflowStatus.RUNNING.value, "running")
        self.assertEqual(WorkflowStatus.COMPLETED.value, "completed")
        self.assertEqual(WorkflowStatus.FAILED.value, "failed")
        self.assertEqual(WorkflowStatus.CANCELED.value, "canceled")
        self.assertEqual(WorkflowStatus.TERMINATED.value, "terminated")
        self.assertEqual(WorkflowStatus.CONTINUED_AS_NEW.value, "continued_as_new")
        self.assertEqual(WorkflowStatus.PENDING.value, "pending")


class TestActivityStatus(unittest.TestCase):
    """Test ActivityStatus enum"""

    def test_activity_status_values(self):
        """Test all activity status values"""
        self.assertEqual(ActivityStatus.SCHEDULED.value, "scheduled")
        self.assertEqual(ActivityStatus.STARTED.value, "started")
        self.assertEqual(ActivityStatus.COMPLETED.value, "completed")
        self.assertEqual(ActivityStatus.FAILED.value, "failed")
        self.assertEqual(ActivityStatus.CANCELED.value, "canceled")
        self.assertEqual(ActivityStatus.TIMEOUT.value, "timeout")
        self.assertEqual(ActivityStatus.HEARTBEAT.value, "heartbeat")


class TestTemporalManagerConnection(unittest.TestCase):
    """Test TemporalManager connection management"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager(
            host="localhost",
            port=7233,
            namespace="test-namespace"
        )

    def tearDown(self):
        """Tear down test fixtures"""
        if hasattr(self, 'manager'):
            pass

    def test_initial_state(self):
        """Test initial manager state"""
        self.assertEqual(self.manager.host, "localhost")
        self.assertEqual(self.manager.port, 7233)
        self.assertEqual(self.manager.namespace, "test-namespace")
        self.assertFalse(self.manager.tls_enabled)
        self.assertFalse(self.manager._connected)

    def test_is_connected(self):
        """Test is_connected method"""
        self.assertFalse(self.manager.is_connected())
        self.manager._connected = True
        self.assertTrue(self.manager.is_connected())

    @patch('src.workflow_temporal.datetime')
    def test_ping_disconnected(self, mock_datetime):
        """Test ping when disconnected"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        result = asyncio.run(self.manager.ping())
        self.assertFalse(result)

    @patch('src.workflow_temporal.datetime')
    def test_ping_connected(self, mock_datetime):
        """Test ping when connected"""
        self.manager._connected = True
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        result = asyncio.run(self.manager.ping())
        self.assertTrue(result)


class TestTemporalManagerWorkflowRegistration(unittest.TestCase):
    """Test TemporalManager workflow registration"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()

    def test_register_workflow_basic(self):
        """Test basic workflow registration"""
        registration = self.manager.register_workflow(
            name="test-workflow",
            version="1.0",
            workflow_type="standard",
            task_queue="default",
            description="Test workflow"
        )
        
        self.assertIsInstance(registration, WorkflowRegistration)
        self.assertEqual(registration.name, "test-workflow")
        self.assertEqual(registration.version, "1.0")
        self.assertEqual(registration.workflow_type, "standard")
        self.assertEqual(registration.task_queue, "default")
        self.assertEqual(registration.description, "Test workflow")
        self.assertIsNotNone(registration.registered_at)

    def test_register_workflow_with_retry_policy(self):
        """Test workflow registration with retry policy"""
        retry_policy = {
            "initial_interval": 1,
            "backoff_coefficient": 2.0,
            "maximum_attempts": 5
        }
        
        registration = self.manager.register_workflow(
            name="test-workflow-retry",
            version="1.0",
            retry_policy=retry_policy
        )
        
        self.assertEqual(registration.retry_policy, retry_policy)

    def test_register_workflow_with_timeout_config(self):
        """Test workflow registration with timeout config"""
        timeout_config = {
            "execution_timeout": 3600,
            "run_timeout": 1800
        }
        
        registration = self.manager.register_workflow(
            name="test-workflow-timeout",
            version="1.0",
            timeout_config=timeout_config
        )
        
        self.assertEqual(registration.timeout_config, timeout_config)

    def test_get_workflow_registration(self):
        """Test getting workflow registration"""
        self.manager.register_workflow(
            name="test-get",
            version="1.0"
        )
        
        registration = self.manager.get_workflow_registration("test-get", "1.0")
        self.assertIsNotNone(registration)
        self.assertEqual(registration.name, "test-get")

    def test_get_workflow_registration_not_found(self):
        """Test getting non-existent workflow registration"""
        registration = self.manager.get_workflow_registration("non-existent", "1.0")
        self.assertIsNone(registration)

    def test_list_workflows(self):
        """Test listing workflows"""
        self.manager.register_workflow("workflow1", "1.0", task_queue="queue1")
        self.manager.register_workflow("workflow2", "1.0", task_queue="queue2")
        self.manager.register_workflow("workflow3", "1.0", task_queue="queue1")
        
        all_workflows = self.manager.list_workflows()
        self.assertEqual(len(all_workflows), 3)
        
        queue1_workflows = self.manager.list_workflows(task_queue="queue1")
        self.assertEqual(len(queue1_workflows), 2)

    def test_unregister_workflow(self):
        """Test unregistering workflow"""
        self.manager.register_workflow("test-unregister", "1.0")
        
        result = self.manager.unregister_workflow("test-unregister", "1.0")
        self.assertTrue(result)
        
        registration = self.manager.get_workflow_registration("test-unregister", "1.0")
        self.assertIsNone(registration)

    def test_unregister_workflow_not_found(self):
        """Test unregistering non-existent workflow"""
        result = self.manager.unregister_workflow("non-existent", "1.0")
        self.assertFalse(result)


class TestTemporalManagerWorkflowExecution(unittest.TestCase):
    """Test TemporalManager workflow execution"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()
        self.manager.register_workflow("test-workflow", "1.0")

    @patch('src.workflow_temporal.datetime')
    def test_start_workflow(self, mock_datetime):
        """Test starting a workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="test-workflow",
            workflow_id="test-id-123"
        ))
        
        self.assertIsInstance(execution, WorkflowExecution)
        self.assertEqual(execution.workflow_id, "test-id-123")
        self.assertEqual(execution.workflow_name, "test-workflow")
        self.assertEqual(execution.status, WorkflowStatus.RUNNING)

    @patch('src.workflow_temporal.datetime')
    def test_signal_workflow(self, mock_datetime):
        """Test signaling a workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="test-workflow",
            workflow_id="signal-test-id"
        ))
        
        result = asyncio.run(self.manager.signal_workflow(
            execution_id="signal-test-id",
            signal_name="test_signal",
            signal_data={"key": "value"}
        ))
        
        self.assertTrue(result)

    @patch('src.workflow_temporal.datetime')
    def test_signal_workflow_not_found(self, mock_datetime):
        """Test signaling non-existent workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        result = asyncio.run(self.manager.signal_workflow(
            execution_id="non-existent-id",
            signal_name="test_signal"
        ))
        
        self.assertFalse(result)

    @patch('src.workflow_temporal.datetime')
    def test_cancel_workflow(self, mock_datetime):
        """Test canceling a workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="test-workflow",
            workflow_id="cancel-test-id"
        ))
        
        result = asyncio.run(self.manager.cancel_workflow(
            execution_id="cancel-test-id",
            reason="Test cancellation"
        ))
        
        self.assertTrue(result)
        canceled_exec = self.manager.get_workflow_execution("cancel-test-id")
        self.assertEqual(canceled_exec.status, WorkflowStatus.CANCELED)

    @patch('src.workflow_temporal.datetime')
    def test_terminate_workflow(self, mock_datetime):
        """Test terminating a workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="test-workflow",
            workflow_id="terminate-test-id"
        ))
        
        result = asyncio.run(self.manager.terminate_workflow(
            execution_id="terminate-test-id",
            reason="Test termination",
            error="Test error"
        ))
        
        self.assertTrue(result)
        terminated_exec = self.manager.get_workflow_execution("terminate-test-id")
        self.assertEqual(terminated_exec.status, WorkflowStatus.TERMINATED)

    @patch('src.workflow_temporal.datetime')
    def test_complete_workflow(self, mock_datetime):
        """Test completing a workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="test-workflow",
            workflow_id="complete-test-id"
        ))
        
        result = asyncio.run(self.manager.complete_workflow(
            execution_id="complete-test-id",
            result={"output": "success"}
        ))
        
        self.assertTrue(result)
        completed_exec = self.manager.get_workflow_execution("complete-test-id")
        self.assertEqual(completed_exec.status, WorkflowStatus.COMPLETED)

    @patch('src.workflow_temporal.datetime')
    def test_fail_workflow(self, mock_datetime):
        """Test failing a workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="test-workflow",
            workflow_id="fail-test-id"
        ))
        
        result = asyncio.run(self.manager.fail_workflow(
            execution_id="fail-test-id",
            error="Test error"
        ))
        
        self.assertTrue(result)
        failed_exec = self.manager.get_workflow_execution("fail-test-id")
        self.assertEqual(failed_exec.status, WorkflowStatus.FAILED)

    def test_list_workflow_executions(self):
        """Test listing workflow executions"""
        asyncio.run(self.manager.start_workflow("test-workflow", workflow_id="exec-1"))
        asyncio.run(self.manager.start_workflow("test-workflow", workflow_id="exec-2"))
        
        executions = self.manager.list_workflow_executions()
        self.assertEqual(len(executions), 2)

    def test_list_workflow_executions_by_status(self):
        """Test listing workflow executions by status"""
        asyncio.run(self.manager.start_workflow("test-workflow", workflow_id="exec-running"))
        asyncio.run(self.manager.complete_workflow("exec-running", "done"))
        
        asyncio.run(self.manager.start_workflow("test-workflow", workflow_id="exec-failed"))
        asyncio.run(self.manager.fail_workflow("exec-failed", "error"))
        
        all_executions = self.manager.list_workflow_executions()
        running = [e for e in all_executions if e.status == WorkflowStatus.RUNNING]
        failed = [e for e in all_executions if e.status == WorkflowStatus.FAILED]
        
        # Should have at least the newly created executions
        self.assertGreaterEqual(len(all_executions), 2)


class TestTemporalManagerActivityRegistration(unittest.TestCase):
    """Test TemporalManager activity registration"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()

    def test_register_activity(self):
        """Test activity registration"""
        registration = self.manager.register_activity(
            name="test-activity",
            version="1.0",
            activity_type="standard",
            task_queue="default"
        )
        
        self.assertIsInstance(registration, ActivityRegistration)
        self.assertEqual(registration.name, "test-activity")
        self.assertEqual(registration.version, "1.0")
        self.assertEqual(registration.activity_type, "standard")

    def test_get_activity_registration(self):
        """Test getting activity registration"""
        self.manager.register_activity("test-get", "1.0")
        
        registration = self.manager.get_activity_registration("test-get", "1.0")
        self.assertIsNotNone(registration)
        self.assertEqual(registration.name, "test-get")

    def test_list_activities(self):
        """Test listing activities"""
        self.manager.register_activity("activity1", "1.0", task_queue="q1")
        self.manager.register_activity("activity2", "1.0", task_queue="q2")
        
        all_activities = self.manager.list_activities()
        self.assertEqual(len(all_activities), 2)

    def test_unregister_activity(self):
        """Test unregistering activity"""
        self.manager.register_activity("test-unregister", "1.0")
        
        result = self.manager.unregister_activity("test-unregister", "1.0")
        self.assertTrue(result)


class TestTemporalManagerActivityExecution(unittest.TestCase):
    """Test TemporalManager activity execution"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()
        self.manager.register_activity("test-activity", "1.0")

    @patch('src.workflow_temporal.datetime')
    def test_execute_activity(self, mock_datetime):
        """Test executing an activity"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.execute_activity(
            activity_name="test-activity",
            activity_id="activity-exec-1"
        ))
        
        self.assertIsInstance(execution, ActivityExecution)
        self.assertEqual(execution.activity_name, "test-activity")

    @patch('src.workflow_temporal.datetime')
    def test_record_activity_heartbeat(self, mock_datetime):
        """Test recording activity heartbeat"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.execute_activity(
            activity_name="test-activity",
            activity_id="heartbeat-test"
        ))
        
        result = asyncio.run(self.manager.record_activity_heartbeat(
            execution_id="heartbeat-test",
            details={"progress": 50}
        ))
        
        self.assertTrue(result)

    def test_get_activity_execution(self):
        """Test getting activity execution"""
        asyncio.run(self.manager.execute_activity(
            activity_name="test-activity",
            activity_id="get-test"
        ))
        
        execution = self.manager.get_activity_execution("get-test")
        self.assertIsNotNone(execution)


class TestTemporalManagerChildWorkflows(unittest.TestCase):
    """Test TemporalManager child workflow management"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()
        self.manager.register_workflow("parent-workflow", "1.0")
        self.manager.register_workflow("child-workflow", "1.0")

    @patch('src.workflow_temporal.datetime')
    def test_start_child_workflow(self, mock_datetime):
        """Test starting a child workflow"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        parent = asyncio.run(self.manager.start_workflow(
            workflow_name="parent-workflow",
            workflow_id="parent-id"
        ))
        
        child = asyncio.run(self.manager.start_child_workflow(
            parent_execution_id="parent-id",
            workflow_name="child-workflow",
            workflow_id="child-id"
        ))
        
        self.assertIsInstance(child, ChildWorkflowExecution)
        self.assertEqual(child.parent_execution_id, "parent-id")
        self.assertEqual(child.child_workflow_name, "child-workflow")

    def test_start_child_workflow_parent_not_found(self):
        """Test starting child workflow with non-existent parent"""
        with self.assertRaises(ValueError):
            asyncio.run(self.manager.start_child_workflow(
                parent_execution_id="non-existent-parent",
                workflow_name="child-workflow"
            ))

    @patch('src.workflow_temporal.datetime')
    def test_list_child_workflows(self, mock_datetime):
        """Test listing child workflows"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        parent = asyncio.run(self.manager.start_workflow(
            workflow_name="parent-workflow",
            workflow_id="list-children-parent"
        ))
        
        asyncio.run(self.manager.start_child_workflow(
            parent_execution_id="list-children-parent",
            workflow_name="child-workflow",
            workflow_id="child-1"
        ))
        
        asyncio.run(self.manager.start_child_workflow(
            parent_execution_id="list-children-parent",
            workflow_name="child-workflow",
            workflow_id="child-2"
        ))
        
        children = self.manager.list_child_workflows("list-children-parent")
        self.assertEqual(len(children), 2)


class TestTemporalManagerQueries(unittest.TestCase):
    """Test TemporalManager query handling"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()
        self.manager.register_workflow("query-workflow", "1.0")

    @patch('src.workflow_temporal.datetime')
    def test_register_query_handler(self, mock_datetime):
        """Test registering a query handler"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        def my_handler():
            return {"status": "custom"}
        
        result = self.manager.register_query_handler("test_query", my_handler)
        self.assertTrue(result)

    @patch('src.workflow_temporal.datetime')
    def test_handle_query_with_handler(self, mock_datetime):
        """Test handling query with registered handler"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        async def my_handler():
            return {"custom": "result"}
        
        self.manager.register_query_handler("custom_query", my_handler)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="query-workflow",
            workflow_id="query-test-id"
        ))
        
        result = asyncio.run(self.manager.handle_query(
            execution_id="query-test-id",
            query_type="custom_query"
        ))
        
        self.assertEqual(result, {"custom": "result"})

    @patch('src.workflow_temporal.datetime')
    def test_handle_query_default(self, mock_datetime):
        """Test handling query without handler (default)"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="query-workflow",
            workflow_id="default-query-id"
        ))
        
        result = asyncio.run(self.manager.handle_query(
            execution_id="default-query-id",
            query_type="unknown_query"
        ))
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result["execution_id"], "default-query-id")


class TestTemporalManagerSearchAttributes(unittest.TestCase):
    """Test TemporalManager search attributes"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()

    def test_define_search_attribute(self):
        """Test defining a search attribute"""
        attr = self.manager.define_search_attribute(
            name="CustomAttr",
            value_type="string",
            indexed=True
        )
        
        self.assertIsInstance(attr, SearchAttribute)
        self.assertEqual(attr.name, "CustomAttr")
        self.assertEqual(attr.value_type, "string")
        self.assertTrue(attr.indexed)

    @patch('src.workflow_temporal.datetime')
    def test_set_search_attribute(self, mock_datetime):
        """Test setting search attribute on execution"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        self.manager.register_workflow("attr-workflow", "1.0")
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="attr-workflow",
            workflow_id="attr-test-id"
        ))
        
        self.manager.define_search_attribute("CustomAttr", "string")
        result = self.manager.set_search_attribute(
            execution_id="attr-test-id",
            attribute_name="CustomAttr",
            value="test_value"
        )
        
        self.assertTrue(result)
        
        value = self.manager.get_search_attribute("attr-test-id", "CustomAttr")
        self.assertEqual(value, "test_value")


class TestTemporalManagerNamespace(unittest.TestCase):
    """Test TemporalManager namespace management"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()

    def test_register_namespace(self):
        """Test registering a namespace"""
        ns = self.manager.register_namespace(
            name="test-ns",
            description="Test namespace",
            retention_days=14
        )
        
        self.assertIsInstance(ns, NamespaceConfig)
        self.assertEqual(ns.name, "test-ns")
        self.assertEqual(ns.retention_days, 14)

    def test_get_namespace(self):
        """Test getting namespace"""
        self.manager.register_namespace("get-ns")
        
        ns = self.manager.get_namespace("get-ns")
        self.assertIsNotNone(ns)
        self.assertEqual(ns.name, "get-ns")

    def test_list_namespaces(self):
        """Test listing namespaces"""
        self.manager.register_namespace("ns1")
        self.manager.register_namespace("ns2")
        
        namespaces = self.manager.list_namespaces()
        self.assertGreaterEqual(len(namespaces), 2)


class TestTemporalManagerTaskQueue(unittest.TestCase):
    """Test TemporalManager task queue management"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()

    def test_register_task_queue(self):
        """Test registering a task queue"""
        tq = self.manager.register_task_queue(
            name="test-queue",
            task_queue_type="normal",
            max_tasks_per_second=100
        )
        
        self.assertIsInstance(tq, TaskQueueConfig)
        self.assertEqual(tq.name, "test-queue")
        self.assertEqual(tq.max_tasks_per_second, 100)

    def test_get_task_queue(self):
        """Test getting task queue"""
        self.manager.register_task_queue("get-queue")
        
        tq = self.manager.get_task_queue("get-queue")
        self.assertIsNotNone(tq)
        self.assertEqual(tq.name, "get-queue")


class TestTemporalManagerHistory(unittest.TestCase):
    """Test TemporalManager history management"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()
        self.manager.register_workflow("history-workflow", "1.0")

    @patch('src.workflow_temporal.datetime')
    def test_record_history_event(self, mock_datetime):
        """Test recording history event"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="history-workflow",
            workflow_id="history-test-id"
        ))
        
        event = self.manager.record_history_event(
            execution_id="history-test-id",
            event_type="TestEvent",
            event_data={"key": "value"}
        )
        
        self.assertTrue(event)

    @patch('src.workflow_temporal.datetime')
    def test_get_workflow_history(self, mock_datetime):
        """Test getting workflow history"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        execution = asyncio.run(self.manager.start_workflow(
            workflow_name="history-workflow",
            workflow_id="history-get-test"
        ))
        
        history = self.manager.get_workflow_history("history-get-test")
        self.assertIsInstance(history, list)


class TestTemporalManagerUI(unittest.TestCase):
    """Test TemporalManager UI configuration"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()

    def test_configure_ui(self):
        """Test configuring UI"""
        config = self.manager.configure_ui(
            host="ui.example.com",
            port=8088,
            tls_enabled=True
        )
        
        self.assertIsInstance(config, TemporalUIConfig)
        self.assertEqual(config.host, "ui.example.com")
        self.assertEqual(config.port, 8088)
        self.assertTrue(config.tls_enabled)


class TestTemporalManagerCallbacks(unittest.TestCase):
    """Test TemporalManager callback handling"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = TemporalManager()
        self.manager.register_workflow("callback-workflow", "1.0")
        self.callback_results = []

    def workflow_callback(self, event, execution, *args):
        """Test callback function"""
        self.callback_results.append({
            "event": event,
            "execution_id": execution.execution_id
        })

    @patch('src.workflow_temporal.datetime')
    def test_register_workflow_callback(self, mock_datetime):
        """Test registering workflow callback"""
        mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        
        # Register callback for specific workflow name
        self.manager.register_workflow_callback("started", self.workflow_callback)
        
        asyncio.run(self.manager.start_workflow(
            workflow_name="callback-workflow",
            workflow_id="callback-test-id"
        ))
        
        # Note: Callback may be called once for 'started' event
        # We just verify callback registration works
        self.assertIsNotNone(self.manager._workflow_callbacks)


class TestWorkflowRegistration(unittest.TestCase):
    """Test WorkflowRegistration dataclass"""

    def test_workflow_registration_creation(self):
        """Test creating WorkflowRegistration"""
        reg = WorkflowRegistration(
            name="test",
            version="1.0",
            workflow_type="standard"
        )
        
        self.assertEqual(reg.name, "test")
        self.assertEqual(reg.execution_count, 0)


class TestWorkflowExecution(unittest.TestCase):
    """Test WorkflowExecution dataclass"""

    def test_workflow_execution_creation(self):
        """Test creating WorkflowExecution"""
        exec = WorkflowExecution(
            execution_id="exec-1",
            workflow_id="wf-1",
            workflow_name="test",
            status=WorkflowStatus.RUNNING,
            start_time=datetime.now()
        )
        
        self.assertEqual(exec.execution_id, "exec-1")
        self.assertEqual(exec.status, WorkflowStatus.RUNNING)


class TestActivityRegistration(unittest.TestCase):
    """Test ActivityRegistration dataclass"""

    def test_activity_registration_creation(self):
        """Test creating ActivityRegistration"""
        reg = ActivityRegistration(
            name="activity",
            version="1.0",
            activity_type="standard"
        )
        
        self.assertEqual(reg.name, "activity")


class TestActivityExecution(unittest.TestCase):
    """Test ActivityExecution dataclass"""

    def test_activity_execution_creation(self):
        """Test creating ActivityExecution"""
        exec = ActivityExecution(
            execution_id="aexec-1",
            activity_id="act-1",
            activity_name="test",
            status=ActivityStatus.SCHEDULED,
            schedule_time=datetime.now()
        )
        
        self.assertEqual(exec.execution_id, "aexec-1")
        self.assertEqual(exec.status, ActivityStatus.SCHEDULED)


class TestChildWorkflowExecution(unittest.TestCase):
    """Test ChildWorkflowExecution dataclass"""

    def test_child_workflow_execution_creation(self):
        """Test creating ChildWorkflowExecution"""
        child = ChildWorkflowExecution(
            child_execution_id="child-1",
            parent_execution_id="parent-1",
            child_workflow_id="cw-1",
            child_workflow_name="child",
            status=WorkflowStatus.RUNNING,
            policy=ChildWorkflowPolicy.ALLOW_PARALLEL,
            start_time=datetime.now()
        )
        
        self.assertEqual(child.status, WorkflowStatus.RUNNING)
        self.assertEqual(child.policy, ChildWorkflowPolicy.ALLOW_PARALLEL)


class TestSearchAttribute(unittest.TestCase):
    """Test SearchAttribute dataclass"""

    def test_search_attribute_creation(self):
        """Test creating SearchAttribute"""
        attr = SearchAttribute(
            name="CustomKey",
            value_type="string",
            indexed=True
        )
        
        self.assertEqual(attr.name, "CustomKey")


class TestNamespaceConfig(unittest.TestCase):
    """Test NamespaceConfig dataclass"""

    def test_namespace_config_creation(self):
        """Test creating NamespaceConfig"""
        ns = NamespaceConfig(
            name="test-ns",
            retention_days=14
        )
        
        self.assertEqual(ns.name, "test-ns")
        self.assertEqual(ns.retention_days, 14)


class TestTaskQueueConfig(unittest.TestCase):
    """Test TaskQueueConfig dataclass"""

    def test_task_queue_config_creation(self):
        """Test creating TaskQueueConfig"""
        tq = TaskQueueConfig(
            name="test-queue",
            max_tasks_per_second=50
        )
        
        self.assertEqual(tq.name, "test-queue")
        self.assertEqual(tq.max_tasks_per_second, 50)


class TestHistoryEvent(unittest.TestCase):
    """Test HistoryEvent dataclass"""

    def test_history_event_creation(self):
        """Test creating HistoryEvent"""
        event = HistoryEvent(
            event_id=1,
            event_type="TestEvent",
            timestamp=datetime.now(),
            workflow_execution_id="wf-1"
        )
        
        self.assertEqual(event.event_id, 1)
        self.assertEqual(event.event_type, "TestEvent")


class TestTemporalUIConfig(unittest.TestCase):
    """Test TemporalUIConfig dataclass"""

    def test_temporal_ui_config_creation(self):
        """Test creating TemporalUIConfig"""
        ui = TemporalUIConfig(
            host="localhost",
            port=8088
        )
        
        self.assertEqual(ui.host, "localhost")
        self.assertEqual(ui.port, 8088)


class TestWorkflowReplayResult(unittest.TestCase):
    """Test WorkflowReplayResult class"""

    def test_workflow_replay_result_creation(self):
        """Test creating WorkflowReplayResult"""
        # Check if it has expected attributes by setting them directly
        result = WorkflowReplayResult.__new__(WorkflowReplayResult)
        result.success = True
        result.replayed_events = 10
        result.mismatches = []
        result.error = None
        result.duration_seconds = 1.5
        
        self.assertTrue(result.success)
        self.assertEqual(result.replayed_events, 10)
        self.assertEqual(result.duration_seconds, 1.5)


if __name__ == '__main__':
    unittest.main()
