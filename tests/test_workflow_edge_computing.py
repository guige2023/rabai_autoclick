"""
Tests for workflow_edge_computing module - Edge Computing for Distributed Workflow Execution.
Covers edge node registration, orchestration, task distribution, data locality,
offline execution, edge-to-edge sync, latency and bandwidth optimization,
edge caching, and security at edge nodes.
"""

import sys
import os
import json
import time
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from collections import defaultdict

sys.path.insert(0, '/Users/guige/my_project')

# Import workflow_edge_computing module
from src.workflow_edge_computing import (
    NodeStatus,
    TaskStatus,
    EncryptionMode,
    EdgeNode,
    Task,
    SyncState,
    CachedWorkflow,
    EdgeComputing,
)


class TestEdgeNode(unittest.TestCase):
    """Test EdgeNode dataclass and methods."""

    def test_edge_node_creation(self):
        """Test creating an edge node."""
        node = EdgeNode(
            node_id="edge_001",
            name="Edge Node 1",
            host="192.168.1.100",
            port=8080,
            status=NodeStatus.ONLINE,
            capabilities=["workflow_execution", "data_processing"],
            location={"lat": 37.7749, "lon": -122.4194}
        )
        self.assertEqual(node.node_id, "edge_001")
        self.assertEqual(node.status, NodeStatus.ONLINE)
        self.assertIn("workflow_execution", node.capabilities)

    def test_edge_node_is_available(self):
        """Test checking node availability."""
        node = EdgeNode(
            node_id="edge_001",
            name="Test Node",
            host="localhost",
            port=8080,
            status=NodeStatus.ONLINE,
            current_load=50.0,
            max_load=100.0
        )
        self.assertTrue(node.is_available())

    def test_edge_node_unavailable_when_overloaded(self):
        """Test node unavailable when overloaded."""
        node = EdgeNode(
            node_id="edge_001",
            name="Test Node",
            host="localhost",
            port=8080,
            status=NodeStatus.ONLINE,
            current_load=100.0,
            max_load=100.0
        )
        self.assertFalse(node.is_available())

    def test_edge_node_unavailable_when_offline(self):
        """Test node unavailable when offline."""
        node = EdgeNode(
            node_id="edge_001",
            name="Test Node",
            host="localhost",
            port=8080,
            status=NodeStatus.OFFLINE,
            current_load=10.0,
            max_load=100.0
        )
        self.assertFalse(node.is_available())

    def test_edge_node_distance_calculation(self):
        """Test distance calculation between nodes."""
        node1 = EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            location={"lat": 37.7749, "lon": -122.4194}
        )
        node2 = EdgeNode(
            node_id="n2", name="n2", host="h2", port=2,
            location={"lat": 40.7128, "lon": -74.0060}
        )
        distance = node1.distance_to(node2)
        self.assertGreater(distance, 0)

    def test_edge_node_distance_no_location(self):
        """Test distance when location is missing."""
        node1 = EdgeNode(node_id="n1", name="n1", host="h1", port=1)
        node2 = EdgeNode(node_id="n2", name="n2", host="h2", port=2)
        distance = node1.distance_to(node2)
        self.assertEqual(distance, float('inf'))


class TestTask(unittest.TestCase):
    """Test Task dataclass and methods."""

    def test_task_creation(self):
        """Test creating a task."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={"action": "click", "selector": "#button"},
            priority=5
        )
        self.assertEqual(task.task_id, "task_001")
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertEqual(task.priority, 5)

    def test_task_can_execute_no_dependencies(self):
        """Test task with no dependencies can execute."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            dependencies=[]
        )
        completed = set()
        self.assertTrue(task.can_execute(completed))

    def test_task_can_execute_with_dependencies(self):
        """Test task with satisfied dependencies can execute."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            dependencies=["task_dep_001", "task_dep_002"]
        )
        completed = {"task_dep_001", "task_dep_002"}
        self.assertTrue(task.can_execute(completed))

    def test_task_cannot_execute_missing_dependencies(self):
        """Test task with unsatisfied dependencies cannot execute."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            dependencies=["task_dep_001", "task_dep_002"]
        )
        completed = {"task_dep_001"}  # Missing task_dep_002
        self.assertFalse(task.can_execute(completed))


class TestEdgeComputing(unittest.TestCase):
    """Test EdgeComputing main class."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="test_edge_node")

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_initialization(self):
        """Test edge computing initialization."""
        self.assertEqual(self.edge.local_node_id, "test_edge")
        self.assertIn("test_edge", self.edge.nodes)
        self.assertEqual(self.edge.offline_mode, False)

    def test_local_node_registered(self):
        """Test local node is registered on init."""
        node = self.edge.get_node(self.edge.local_node_id)
        self.assertIsNotNone(node)
        self.assertEqual(node.name, "test_edge_node")
        self.assertEqual(node.status, NodeStatus.ONLINE)


class TestEdgeNodeRegistration(unittest.TestCase):
    """Test edge node registration operations."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_register_node(self):
        """Test registering a new edge node."""
        node = EdgeNode(
            node_id="edge_001",
            name="Edge 1",
            host="192.168.1.100",
            port=8080,
            status=NodeStatus.ONLINE,
            capabilities=["data_processing"]
        )
        result = self.edge.register_node(node)
        self.assertTrue(result)
        self.assertIn("edge_001", self.edge.nodes)

    def test_register_duplicate_node_updates(self):
        """Test re-registering existing node updates its properties."""
        node1 = EdgeNode(
            node_id="edge_001",
            name="Edge 1",
            host="192.168.1.100",
            port=8080,
            status=NodeStatus.ONLINE
        )
        self.edge.register_node(node1)

        node1_updated = EdgeNode(
            node_id="edge_001",
            name="Edge 1 Updated",
            host="192.168.1.101",
            port=9090,
            status=NodeStatus.BUSY
        )
        result = self.edge.register_node(node1_updated)
        self.assertTrue(result)
        self.assertEqual(self.edge.nodes["edge_001"].name, "Edge 1 Updated")
        self.assertEqual(self.edge.nodes["edge_001"].status, NodeStatus.BUSY)

    def test_unregister_node(self):
        """Test unregistering an edge node."""
        node = EdgeNode(
            node_id="edge_001",
            name="Edge 1",
            host="192.168.1.100",
            port=8080
        )
        self.edge.register_node(node)
        result = self.edge.unregister_node("edge_001")
        self.assertTrue(result)
        self.assertNotIn("edge_001", self.edge.nodes)

    def test_unregister_nonexistent_node(self):
        """Test unregistering non-existent node returns False."""
        result = self.edge.unregister_node("nonexistent")
        self.assertFalse(result)

    def test_get_node(self):
        """Test getting node details."""
        node = EdgeNode(
            node_id="edge_001",
            name="Edge 1",
            host="192.168.1.100",
            port=8080
        )
        self.edge.register_node(node)
        retrieved = self.edge.get_node("edge_001")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.node_id, "edge_001")

    def test_get_all_nodes(self):
        """Test getting all registered nodes."""
        self.edge.register_node(EdgeNode(node_id="n1", name="n1", host="h1", port=1))
        self.edge.register_node(EdgeNode(node_id="n2", name="n2", host="h2", port=2))
        nodes = self.edge.get_all_nodes()
        self.assertGreaterEqual(len(nodes), 2)

    def test_get_available_nodes(self):
        """Test getting available nodes."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE, current_load=10, max_load=100
        ))
        self.edge.register_node(EdgeNode(
            node_id="n2", name="n2", host="h2", port=2,
            status=NodeStatus.OFFLINE, current_load=10, max_load=100
        ))
        available = self.edge.get_available_nodes()
        self.assertEqual(len(available), 1)
        self.assertEqual(available[0].node_id, "n1")

    def test_update_node_status(self):
        """Test updating node status."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE
        ))
        result = self.edge.update_node_status("n1", NodeStatus.BUSY)
        self.assertTrue(result)
        self.assertEqual(self.edge.nodes["n1"].status, NodeStatus.BUSY)

    def test_update_node_load(self):
        """Test updating node load."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            current_load=10, max_load=100
        ))
        result = self.edge.update_node_load("n1", 50.0)
        self.assertTrue(result)
        self.assertEqual(self.edge.nodes["n1"].current_load, 50.0)


class TestTaskDistribution(unittest.TestCase):
    """Test task distribution operations."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_submit_task(self):
        """Test submitting a task."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={"action": "click"}
        )
        task_id = self.edge.submit_task(task)
        self.assertEqual(task_id, "task_001")
        self.assertIn("task_001", self.edge.tasks)

    def test_submit_tasks(self):
        """Test submitting multiple tasks."""
        tasks = [
            Task(task_id=f"task_{i:03d}", workflow_id="wf_001", task_definition={})
            for i in range(3)
        ]
        task_ids = self.edge.submit_tasks(tasks)
        self.assertEqual(len(task_ids), 3)

    def test_get_task_status(self):
        """Test getting task status."""
        task = Task(task_id="task_001", workflow_id="wf_001", task_definition={})
        self.edge.submit_task(task)
        status = self.edge.get_task_status("task_001")
        self.assertEqual(status, TaskStatus.PENDING)

    def test_get_task_status_nonexistent(self):
        """Test getting status of non-existent task."""
        status = self.edge.get_task_status("nonexistent")
        self.assertIsNone(status)

    def test_update_task_status(self):
        """Test updating task status."""
        task = Task(task_id="task_001", workflow_id="wf_001", task_definition={})
        self.edge.submit_task(task)
        result = self.edge.update_task_status("task_001", TaskStatus.RUNNING)
        self.assertTrue(result)
        self.assertEqual(self.edge.tasks["task_001"].status, TaskStatus.RUNNING)
        self.assertIsNotNone(self.edge.tasks["task_001"].started_at)

    def test_update_task_status_completed(self):
        """Test updating task to completed releases node load."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            current_load=10, max_load=100
        ))
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            priority=5,
            assigned_node="n1"
        )
        self.edge.submit_task(task)
        self.edge.update_task_status("task_001", TaskStatus.COMPLETED, result={"success": True})
        self.assertEqual(self.edge.tasks["task_001"].result["success"], True)
        self.assertIsNotNone(self.edge.tasks["task_001"].completed_at)

    def test_retry_task(self):
        """Test retrying a failed task."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            retry_count=0,
            max_retries=3
        )
        self.edge.submit_task(task)
        self.edge.update_task_status("task_001", TaskStatus.FAILED, error="Test error")
        result = self.edge.retry_task("task_001")
        self.assertTrue(result)
        self.assertEqual(self.edge.tasks["task_001"].retry_count, 1)
        self.assertEqual(self.edge.tasks["task_001"].status, TaskStatus.PENDING)

    def test_retry_task_exceeds_max(self):
        """Test retry fails when max retries exceeded."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            retry_count=3,
            max_retries=3
        )
        self.edge.submit_task(task)
        result = self.edge.retry_task("task_001")
        self.assertFalse(result)

    def test_register_task_callback(self):
        """Test registering task callback."""
        callback_called = []
        def callback(task):
            callback_called.append(task.task_id)

        self.edge.register_task_callback("task_001", callback)
        self.assertIn("task_001", self.edge._task_callbacks)


class TestOrchestration(unittest.TestCase):
    """Test workflow orchestration."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_orchestrate_workflow(self):
        """Test orchestrating a workflow."""
        tasks = [
            Task(
                task_id="task_001",
                workflow_id="wf_001",
                task_definition={},
                dependencies=[]
            ),
            Task(
                task_id="task_002",
                workflow_id="wf_001",
                task_definition={},
                dependencies=["task_001"]
            )
        ]
        self.edge.register_node(EdgeNode(
            node_id="edge_001", name="e1", host="h1", port=1,
            status=NodeStatus.ONLINE, current_load=0, max_load=100
        ))
        result = self.edge.orchestrate_workflow("wf_001", tasks)
        self.assertEqual(result["workflow_id"], "wf_001")
        self.assertEqual(result["status"], "orchestrated")
        self.assertIn("execution_order", result)
        self.assertIn("task_assignments", result)

    def test_resolve_execution_order(self):
        """Test resolving task execution order by dependencies."""
        task1 = Task(task_id="t1", workflow_id="wf_001", task_definition={}, dependencies=[])
        task2 = Task(task_id="t2", workflow_id="wf_001", task_definition={}, dependencies=["t1"])
        task3 = Task(task_id="t3", workflow_id="wf_001", task_definition={}, dependencies=["t2"])

        self.edge.tasks = {"t1": task1, "t2": task2, "t3": task3}
        order = self.edge._resolve_execution_order("wf_001")
        self.assertEqual(order, ["t1", "t2", "t3"])


class TestDataLocality(unittest.TestCase):
    """Test data locality operations."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_register_data_source(self):
        """Test registering a data source."""
        self.edge.register_node(EdgeNode(node_id="n1", name="n1", host="h1", port=1))
        self.edge.register_data_source("ds_001", "n1", location={"lat": 37.0, "lon": -122.0})
        self.assertEqual(self.edge.data_source_map["ds_001"], "n1")

    def test_get_optimal_node_for_data(self):
        """Test getting optimal node for data source."""
        self.edge.register_node(EdgeNode(node_id="n1", name="n1", host="h1", port=1))
        self.edge.register_data_source("ds_001", "n1")
        node = self.edge.get_optimal_node_for_data("ds_001")
        self.assertIsNotNone(node)
        self.assertEqual(node.node_id, "n1")

    def test_get_optimal_node_fallback(self):
        """Test fallback when no data source mapping exists."""
        self.edge.register_node(EdgeNode(node_id="n1", name="n1", host="h1", port=1, status=NodeStatus.ONLINE))
        node = self.edge.get_optimal_node_for_data("nonexistent")
        self.assertIsNotNone(node)


class TestOfflineExecution(unittest.TestCase):
    """Test offline execution functionality."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_enable_offline_mode(self):
        """Test enabling offline mode."""
        self.edge.enable_offline_mode()
        self.assertTrue(self.edge.offline_mode)

    def test_disable_offline_mode(self):
        """Test disabling offline mode."""
        self.edge.enable_offline_mode()
        self.edge.disable_offline_mode()
        self.assertFalse(self.edge.offline_mode)

    def test_offline_queue_submission(self):
        """Test tasks are queued when in offline mode."""
        self.edge.enable_offline_mode()
        task = Task(task_id="task_001", workflow_id="wf_001", task_definition={})
        self.edge.submit_task(task)
        self.assertEqual(self.edge.get_offline_queue_size(), 1)
        self.assertEqual(self.edge.tasks["task_001"].status, TaskStatus.OFFLINE_READY)

    def test_get_offline_queue_size(self):
        """Test getting offline queue size."""
        self.edge.enable_offline_mode()
        self.assertEqual(self.edge.get_offline_queue_size(), 0)
        task = Task(task_id="task_001", workflow_id="wf_001", task_definition={})
        self.edge.submit_task(task)
        self.assertEqual(self.edge.get_offline_queue_size(), 1)

    def test_cache_for_offline(self):
        """Test caching workflow for offline execution."""
        task = Task(task_id="task_001", workflow_id="wf_001", task_definition={})
        self.edge.cache_for_offline("wf_001", [task])
        self.assertTrue(self.edge.is_workflow_available_offline("wf_001"))

    def test_is_workflow_available_offline(self):
        """Test checking if workflow is cached offline."""
        self.assertFalse(self.edge.is_workflow_available_offline("wf_001"))
        task = Task(task_id="task_001", workflow_id="wf_001", task_definition={})
        self.edge.cache_for_offline("wf_001", [task])
        self.assertTrue(self.edge.is_workflow_available_offline("wf_001"))


class TestEdgeToEdgeSync(unittest.TestCase):
    """Test edge-to-edge synchronization."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_create_sync_state(self):
        """Test creating sync state."""
        sync_state = self.edge.create_sync_state("wf_001")
        self.assertEqual(sync_state.workflow_id, "wf_001")
        self.assertEqual(sync_state.node_id, self.edge.local_node_id)
        self.assertIn(sync_state.state_id, self.edge.sync_states)

    def test_update_sync_state(self):
        """Test updating sync state."""
        sync_state = self.edge.create_sync_state("wf_001")
        self.edge.update_sync_state(sync_state.state_id, "task_001", "completed")
        updated = self.edge.sync_states[sync_state.state_id]
        self.assertEqual(updated.task_states["task_001"], "completed")
        self.assertGreater(updated.version, 0)

    def test_sync_with_node(self):
        """Test syncing with another node."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE
        ))
        result = self.edge.sync_with_node("n1")
        self.assertTrue(result)

    def test_sync_with_offline_node_fails(self):
        """Test syncing with offline node fails."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.OFFLINE
        ))
        result = self.edge.sync_with_node("n1")
        self.assertFalse(result)

    def test_merge_sync_state(self):
        """Test merging sync states."""
        local_state = SyncState(
            state_id="local_001",
            workflow_id="wf_001",
            node_id="local",
            task_states={"task_001": "completed"},
            version=1
        )
        remote_state = SyncState(
            state_id="remote_001",
            workflow_id="wf_001",
            node_id="remote",
            task_states={"task_002": "running", "task_003": "pending"},
            version=2
        )
        result = self.edge.merge_sync_state(remote_state)
        self.assertTrue(result)

    def test_get_sync_state(self):
        """Test getting sync state for workflow."""
        sync_state = self.edge.create_sync_state("wf_001")
        retrieved = self.edge.get_sync_state("wf_001")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.state_id, sync_state.state_id)

    def test_register_sync_callback(self):
        """Test registering sync callback."""
        self.edge.register_sync_callback(lambda state: None)
        self.assertEqual(len(self.edge._sync_callbacks), 1)


class TestLatencyOptimization(unittest.TestCase):
    """Test latency optimization operations."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_find_nearest_node(self):
        """Test finding nearest node."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE, current_load=0, max_load=100,
            location={"lat": 37.7749, "lon": -122.4194}
        ))
        self.edge.register_node(EdgeNode(
            node_id="n2", name="n2", host="h2", port=2,
            status=NodeStatus.ONLINE, current_load=0, max_load=100,
            location={"lat": 40.7128, "lon": -74.0060}
        ))
        nearest = self.edge.find_nearest_node({"lat": 37.7749, "lon": -122.4194})
        self.assertIsNotNone(nearest)
        self.assertEqual(nearest.node_id, "n1")

    def test_find_lowest_latency_node(self):
        """Test finding lowest latency node."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE, current_load=0, max_load=100,
            latency_ms=50
        ))
        self.edge.register_node(EdgeNode(
            node_id="n2", name="n2", host="h2", port=2,
            status=NodeStatus.ONLINE, current_load=0, max_load=100,
            latency_ms=10
        ))
        lowest = self.edge.find_lowest_latency_node()
        self.assertEqual(lowest.node_id, "n2")

    def test_optimize_route(self):
        """Test optimizing task route."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE, current_load=0, max_load=100,
            latency_ms=100
        ))
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            data_requirements={"source_id": "ds_001"}
        )
        self.edge.register_data_source("ds_001", "n1")
        optimized = self.edge.optimize_route(task)
        self.assertIsNotNone(optimized)
        self.assertEqual(optimized.node_id, "n1")

    def test_measure_latency(self):
        """Test measuring latency to node."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            latency_ms=25.0
        ))
        latency = self.edge.measure_latency("n1")
        self.assertEqual(latency, 25.0)


class TestBandwidthOptimization(unittest.TestCase):
    """Test bandwidth optimization operations."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_estimate_transfer_size(self):
        """Test estimating data transfer size."""
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={"action": "click", "selector": "#btn"},
            data_requirements={"input": "data"}
        )
        size = self.edge.estimate_transfer_size(task)
        self.assertGreater(size, 0)

    def test_should_process_remote(self):
        """Test determining if task should be processed remotely."""
        source = EdgeNode(
            node_id="source", name="source", host="h1", port=1,
            bandwidth_mbps=1000
        )
        target = EdgeNode(
            node_id="target", name="target", host="h2", port=2,
            bandwidth_mbps=100
        )
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={},
            estimated_execution_time_ms=1000
        )
        should_remote = self.edge.should_process_remote(task, source, target)
        # Transfer time depends on size estimation
        self.assertIsInstance(should_remote, bool)

    def test_compress_for_transfer(self):
        """Test compressing data for transfer."""
        data = {"workflow": "test", "tasks": [{"id": i} for i in range(100)]}
        compressed = self.edge.compress_for_transfer(data)
        self.assertIsInstance(compressed, bytes)
        self.assertLess(len(compressed), len(json.dumps(data)))

    def test_decompress_from_transfer(self):
        """Test decompressing received data."""
        original = {"key": "value", "number": 42}
        compressed = self.edge.compress_for_transfer(original)
        decompressed = self.edge.decompress_from_transfer(compressed)
        self.assertEqual(decompressed, original)

    def test_optimize_bandwidth_usage(self):
        """Test optimizing bandwidth by grouping tasks."""
        tasks = [
            Task(task_id="t1", workflow_id="wf_001", task_definition={},
                 data_requirements={"source_id": "ds_001"}),
            Task(task_id="t2", workflow_id="wf_001", task_definition={},
                 data_requirements={"source_id": "ds_002"}),
            Task(task_id="t3", workflow_id="wf_001", task_definition={},
                 data_requirements={"source_id": "ds_001"}),
        ]
        optimized = self.edge.optimize_bandwidth_usage(tasks)
        self.assertEqual(len(optimized), 3)


class TestEdgeCaching(unittest.TestCase):
    """Test edge caching operations."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_cache_workflow(self):
        """Test caching a workflow."""
        definition = {"name": "test_workflow", "steps": [{"action": "click"}]}
        self.edge.cache_workflow("wf_001", definition, assets={"asset_1": "path/to/asset"})
        cached = self.edge.get_cached_workflow("wf_001")
        self.assertIsNotNone(cached)
        self.assertEqual(cached.workflow_id, "wf_001")

    def test_get_cached_workflow(self):
        """Test retrieving cached workflow."""
        definition = {"name": "test"}
        self.edge.cache_workflow("wf_001", definition)
        cached = self.edge.get_cached_workflow("wf_001")
        self.assertIsNotNone(cached)
        # Check last_used is updated
        time.sleep(0.01)
        cached_again = self.edge.get_cached_workflow("wf_001")
        self.assertGreaterEqual(cached_again.last_used, cached.last_used)

    def test_get_nonexistent_cached_workflow(self):
        """Test getting non-existent cached workflow returns None."""
        cached = self.edge.get_cached_workflow("nonexistent")
        self.assertIsNone(cached)

    def test_cache_asset(self):
        """Test caching an asset at edge node."""
        content = b"asset content"
        self.edge.cache_asset("asset_001", content)
        self.assertTrue(self.edge.is_asset_cached("asset_001"))

    def test_is_asset_cached(self):
        """Test checking if asset is cached."""
        self.assertFalse(self.edge.is_asset_cached("nonexistent"))
        self.edge.cache_asset("asset_001", b"content")
        self.assertTrue(self.edge.is_asset_cached("asset_001"))

    def test_evict_cache(self):
        """Test evicting cached workflows."""
        self.edge.cache_workflow("wf_001", {"name": "old"})
        self.edge.cache_workflow("wf_002", {"name": "new"})
        # Evict workflows older than 0 hours (all)
        self.edge.evict_cache(max_age_hours=0)
        self.assertFalse(self.edge.is_workflow_available_offline("wf_001"))

    def test_evict_specific_workflow(self):
        """Test evicting specific cached workflow."""
        self.edge.cache_workflow("wf_001", {"name": "test1"})
        self.edge.cache_workflow("wf_002", {"name": "test2"})
        self.edge.evict_cache(workflow_id="wf_001")
        self.assertFalse(self.edge.is_workflow_available_offline("wf_001"))
        self.assertTrue(self.edge.is_workflow_available_offline("wf_002"))

    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        self.edge.cache_workflow("wf_001", {"name": "test1", "data": "x" * 1000})
        stats = self.edge.get_cache_stats()
        self.assertIn("cached_workflows", stats)
        self.assertIn("total_size_mb", stats)
        self.assertGreaterEqual(stats["cached_workflows"], 1)


class TestSecurityAtEdge(unittest.TestCase):
    """Test security operations at edge nodes."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_set_encryption_key(self):
        """Test setting encryption key."""
        key = b"a" * 32
        self.edge.set_encryption_key(key)
        self.assertEqual(self.edge.encryption_key, key)

    def test_set_encryption_key_too_short(self):
        """Test setting key that's too short raises error."""
        with self.assertRaises(ValueError) as context:
            self.edge.set_encryption_key(b"short")
        self.assertIn("32 bytes", str(context.exception))

    def test_set_encryption_mode(self):
        """Test setting encryption mode."""
        self.edge.set_encryption_mode(EncryptionMode.END_TO_END)
        self.assertEqual(self.edge.encryption_mode, EncryptionMode.END_TO_END)

    def test_encrypt_decrypt_data(self):
        """Test encrypting and decrypting data."""
        self.edge.set_encryption_key(b"a" * 32)
        data = {"secret": "value", "number": 42}
        encrypted = self.edge.encrypt_data(data)
        self.assertIsInstance(encrypted, bytes)
        decrypted = self.edge.decrypt_data(encrypted)
        self.assertEqual(decrypted, data)

    def test_encrypt_without_key(self):
        """Test encryption without key returns plain data."""
        data = {"test": "data"}
        encrypted = self.edge.encrypt_data(data)
        # Without key, returns JSON encoded
        self.assertIsInstance(encrypted, bytes)

    def test_encrypt_decrypt_task_data(self):
        """Test encrypting and decrypting task data."""
        self.edge.set_encryption_key(b"a" * 32)
        self.edge.set_encryption_mode(EncryptionMode.AES_256)
        task = Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={"sensitive": {"password": "secret123"}}
        )
        encrypted_task = self.edge.encrypt_task_data(task)
        self.assertIn("_encrypted_data", encrypted_task.task_definition)

        decrypted_task = self.edge.decrypt_task_data(encrypted_task)
        self.assertEqual(decrypted_task.task_definition, {"sensitive": {"password": "secret123"}})

    def test_secure_transfer(self):
        """Test preparing data for secure transfer."""
        self.edge.set_encryption_key(b"a" * 32)
        self.edge.register_node(EdgeNode(node_id="n1", name="n1", host="h1", port=1))
        data = {"task_id": "task_001", "result": "success"}
        transferred = self.edge.secure_transfer(data, "n1")
        self.assertIsInstance(transferred, bytes)

    def test_verify_node_identity(self):
        """Test verifying node identity."""
        node = EdgeNode(node_id="n1", name="n1", host="h1", port=1)
        self.edge.register_node(node)

        # Generate expected fingerprint
        import hashlib
        fingerprint_input = f"{node.node_id}:{node.name}:{node.host}:{node.port}"
        expected_fingerprint = hashlib.sha256(fingerprint_input.encode()).hexdigest()[:16]

        result = self.edge.verify_node_identity("n1", expected_fingerprint)
        self.assertTrue(result)

    def test_verify_node_identity_fails(self):
        """Test node identity verification fails with wrong fingerprint."""
        node = EdgeNode(node_id="n1", name="n1", host="h1", port=1)
        self.edge.register_node(node)
        result = self.edge.verify_node_identity("n1", "wrong_fingerprint")
        self.assertFalse(result)


class TestStatistics(unittest.TestCase):
    """Test statistics and utility methods."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self        self.edge.stop_orchestration()

    def test_get_statistics(self):
        """Test getting comprehensive statistics."""
        self.edge.register_node(EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE, current_load=10, max_load=100
        ))
        self.edge.submit_task(Task(
            task_id="task_001",
            workflow_id="wf_001",
            task_definition={}
        ))
        stats = self.edge.get_statistics()
        self.assertIn("nodes", stats)
        self.assertIn("tasks", stats)
        self.assertIn("cache", stats)
        self.assertIn("offline_mode", stats)

    def test_count_nodes_by_status(self):
        """Test counting nodes by status."""
        self.edge.register_node(EdgeNode(node_id="n1", name="n1", host="h1", port=1, status=NodeStatus.ONLINE))
        self.edge.register_node(EdgeNode(node_id="n2", name="n2", host="h2", port=2, status=NodeStatus.BUSY))
        counts = self.edge._count_nodes_by_status()
        self.assertIn("online", counts)
        self.assertIn("busy", counts)


class TestNodeScoring(unittest.TestCase):
    """Test node scoring for task assignment."""

    def setUp(self):
        self.edge = EdgeComputing(node_id="test_edge", node_name="main")
        self.edge.stop_orchestration()

    def tearDown(self):
        self.edge.stop_orchestration()

    def test_calculate_node_score_basic(self):
        """Test basic node scoring."""
        node = EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE,
            current_load=50.0,
            max_load=100.0,
            latency_ms=20.0,
            bandwidth_mbps=500.0
        )
        task = Task(task_id="t1", workflow_id="wf_001", task_definition={})
        score = self.edge._calculate_node_score(node, task)
        self.assertGreater(score, 0)

    def test_calculate_node_score_capability_bonus(self):
        """Test capability matching gives score bonus."""
        node = EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE,
            current_load=50.0,
            max_load=100.0,
            capabilities=["data_processing"]
        )
        task = Task(
            task_id="t1",
            workflow_id="wf_001",
            task_definition={"required_capability": "data_processing"}
        )
        score_with = self.edge._calculate_node_score(node, task)

        node_no_cap = EdgeNode(
            node_id="n2", name="n2", host="h2", port=2,
            status=NodeStatus.ONLINE,
            current_load=50.0,
            max_load=100.0,
            capabilities=[]
        )
        score_without = self.edge._calculate_node_score(node_no_cap, task)
        self.assertGreater(score_with, score_without)

    def test_calculate_node_score_data_locality(self):
        """Test data locality gives score bonus."""
        node = EdgeNode(
            node_id="n1", name="n1", host="h1", port=1,
            status=NodeStatus.ONLINE,
            current_load=50.0,
            max_load=100.0
        )
        task = Task(
            task_id="t1",
            workflow_id="wf_001",
            task_definition={},
            data_requirements={"source_id": "ds_001"}
        )
        self.edge.register_data_source("ds_001", "n1")
        score = self.edge._calculate_node_score(node, task)
        self.assertGreater(score, 100.0)


if __name__ == '__main__':
    unittest.main()
