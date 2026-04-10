"""
Tests for workflow_prefect module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import json
import time
import tempfile
import shutil
from datetime import datetime

from src.workflow_prefect import (
    PrefectManager,
    FlowState,
    DeploymentState,
    WorkQueueState,
    FlowDefinition,
    FlowRun,
    Deployment,
    TaskDefinition,
    WorkQueue,
    Block,
    NotificationHook,
    ResultStorage
)


class TestFlowState(unittest.TestCase):
    """Test FlowState enum"""

    def test_flow_state_values(self):
        self.assertEqual(FlowState.REGISTERED.value, "registered")
        self.assertEqual(FlowState.RUNNING.value, "running")
        self.assertEqual(FlowState.COMPLETED.value, "completed")
        self.assertEqual(FlowState.FAILED.value, "failed")
        self.assertEqual(FlowState.CANCELLED.value, "cancelled")


class TestDeploymentState(unittest.TestCase):
    """Test DeploymentState enum"""

    def test_deployment_state_values(self):
        self.assertEqual(DeploymentState.ACTIVE.value, "active")
        self.assertEqual(DeploymentState.INACTIVE.value, "inactive")
        self.assertEqual(DeploymentState.PAUSED.value, "paused")


class TestWorkQueueState(unittest.TestCase):
    """Test WorkQueueState enum"""

    def test_work_queue_state_values(self):
        self.assertEqual(WorkQueueState.READY.value, "ready")
        self.assertEqual(WorkQueueState.PAUSED.value, "paused")
        self.assertEqual(WorkQueueState.FULL.value, "full")


class TestPrefectManagerInit(unittest.TestCase):
    """Test PrefectManager initialization"""

    @patch('src.workflow_prefect.os.makedirs')
    @patch('src.workflow_prefect.os.path.exists', return_value=False)
    @patch('builtins.open', new_callable=mock_open)
    def test_init_creates_data_dir(self, mock_file, mock_exists, mock_makedirs):
        with patch('src.workflow_prefect.json.load', return_value={}):
            manager = PrefectManager(data_dir="/tmp/test_prefect")
            mock_makedirs.assert_called_once_with("/tmp/test_prefect", exist_ok=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_init_loads_data(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir="/tmp/test")
                self.assertEqual(manager.data_dir, "/tmp/test")
                self.assertIsNotNone(manager.flows)
                self.assertIsNotNone(manager.deployments)
                self.assertIsNotNone(manager.tasks)


class TestFlowRegistration(unittest.TestCase):
    """Test flow registration functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mock_open = mock_open()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_register_flow(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": [{"action": "test"}]},
                    parameters={"param1": "value1"},
                    description="Test flow",
                    version="1.0.0",
                    tags=["test", "unit"]
                )

                self.assertIsNotNone(flow_id)
                self.assertEqual(len(flow_id), 12)
                self.assertIn(flow_id, manager.flows)
                flow = manager.flows[flow_id]
                self.assertEqual(flow.flow_name, "test_flow")
                self.assertEqual(flow.parameters, {"param1": "value1"})
                self.assertEqual(flow.description, "Test flow")
                self.assertEqual(flow.version, "1.0.0")
                self.assertEqual(flow.tags, ["test", "unit"])
                self.assertEqual(flow.state, FlowState.REGISTERED)

    @patch('builtins.open', new_callable=mock_open)
    def test_get_flow(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": []}
                )

                flow = manager.get_flow(flow_id)
                self.assertIsNotNone(flow)
                self.assertEqual(flow.flow_name, "test_flow")

    @patch('builtins.open', new_callable=mock_open)
    def test_get_flow_not_found(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                flow = manager.get_flow("nonexistent")
                self.assertIsNone(flow)

    @patch('builtins.open', new_callable=mock_open)
    def test_list_flows(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                manager.register_flow("flow1", {"steps": []}, tags=["tag1"])
                manager.register_flow("flow2", {"steps": []}, tags=["tag2"])
                manager.register_flow("flow3", {"steps": []}, tags=["tag1", "tag2"])

                flows = manager.list_flows()
                self.assertEqual(len(flows), 3)

                flows_tag1 = manager.list_flows(tags=["tag1"])
                self.assertEqual(len(flows_tag1), 2)

    @patch('builtins.open', new_callable=mock_open)
    def test_update_flow(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": []}
                )

                result = manager.update_flow(flow_id, description="updated", version="2.0.0")
                self.assertTrue(result)

                flow = manager.get_flow(flow_id)
                self.assertEqual(flow.description, "updated")
                self.assertEqual(flow.version, "2.0.0")

    @patch('builtins.open', new_callable=mock_open)
    def test_delete_flow(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": []}
                )

                result = manager.delete_flow(flow_id)
                self.assertTrue(result)
                self.assertIsNone(manager.get_flow(flow_id))

                result = manager.delete_flow("nonexistent")
                self.assertFalse(result)


class TestFlowExecution(unittest.TestCase):
    """Test flow execution functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_run_flow(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": [{"action": "step1", "target": "task1"}]}
                )

                run_id = manager.run_flow(flow_id, parameters={"param1": "value1"}, wait=False)
                self.assertIsNotNone(run_id)
                self.assertEqual(len(run_id), 12)

                run = manager.get_flow_run(run_id)
                self.assertIsNotNone(run)
                self.assertEqual(run.flow_id, flow_id)
                self.assertEqual(run.state, FlowState.RUNNING)

    @patch('builtins.open', new_callable=mock_open)
    def test_run_flow_not_found(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)

                with self.assertRaises(ValueError):
                    manager.run_flow("nonexistent")

    @patch('builtins.open', new_callable=mock_open)
    def test_list_flow_runs(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": []}
                )

                run_id1 = manager.run_flow(flow_id, wait=False)
                run_id2 = manager.run_flow(flow_id, wait=False)

                runs = manager.list_flow_runs(flow_id=flow_id)
                self.assertEqual(len(runs), 2)

                runs = manager.list_flow_runs(flow_id="nonexistent")
                self.assertEqual(len(runs), 0)

    @patch('builtins.open', new_callable=mock_open)
    def test_cancel_flow_run(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": []}
                )

                # Manually create a running flow run for cancellation test
                run_id = "test_run_id"
                from src.workflow_prefect import FlowRun, FlowState
                run = FlowRun(
                    run_id=run_id,
                    flow_id=flow_id,
                    flow_name="test_flow",
                    parameters={},
                    state=FlowState.RUNNING,
                    run_start_time=time.time()
                )
                manager.flow_runs[run_id] = run

                result = manager.cancel_flow_run(run_id)
                self.assertTrue(result)

                run = manager.get_flow_run(run_id)
                self.assertEqual(run.state, FlowState.CANCELLED)


class TestDeploymentManagement(unittest.TestCase):
    """Test deployment management functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_create_deployment(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow(
                    flow_name="test_flow",
                    flow_data={"steps": []}
                )

                deployment_id = manager.create_deployment(
                    flow_id=flow_id,
                    deployment_name="test_deployment",
                    work_queue_name="default",
                    schedule="0 * * * *",
                    tags=["prod"],
                    parameters={"env": "production"}
                )

                self.assertIsNotNone(deployment_id)
                deployment = manager.get_deployment(deployment_id)
                self.assertIsNotNone(deployment)
                self.assertEqual(deployment.deployment_name, "test_deployment")
                self.assertEqual(deployment.work_queue_name, "default")
                self.assertEqual(deployment.schedule, "0 * * * *")
                self.assertEqual(deployment.tags, ["prod"])

    @patch('builtins.open', new_callable=mock_open)
    def test_create_deployment_flow_not_found(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)

                with self.assertRaises(ValueError):
                    manager.create_deployment(
                        flow_id="nonexistent",
                        deployment_name="test_deployment"
                    )

    @patch('builtins.open', new_callable=mock_open)
    def test_list_deployments(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id1 = manager.register_flow("flow1", {"steps": []})
                flow_id2 = manager.register_flow("flow2", {"steps": []})

                dep1 = manager.create_deployment(flow_id1, "dep1", tags=["tag1"])
                dep2 = manager.create_deployment(flow_id1, "dep2", tags=["tag2"])
                dep3 = manager.create_deployment(flow_id2, "dep3", tags=["tag1"])

                all_deps = manager.list_deployments()
                self.assertEqual(len(all_deps), 3)

                flow1_deps = manager.list_deployments(flow_id=flow_id1)
                self.assertEqual(len(flow1_deps), 2)

                tag1_deps = manager.list_deployments(tags=["tag1"])
                self.assertEqual(len(tag1_deps), 2)

    @patch('builtins.open', new_callable=mock_open)
    def test_set_deployment_schedule(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow("flow", {"steps": []})
                dep_id = manager.create_deployment(flow_id, "dep")

                result = manager.set_deployment_schedule(dep_id, "0 0 * * *", enabled=True)
                self.assertTrue(result)

                deployment = manager.get_deployment(dep_id)
                self.assertEqual(deployment.schedule, "0 0 * * *")
                self.assertTrue(deployment.schedule_enabled)


class TestTaskManagement(unittest.TestCase):
    """Test task management functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_register_task(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                task_id = manager.register_task(
                    task_name="test_task",
                    task_func="my_module.my_func",
                    parameters={"arg1": "value1"},
                    description="Test task",
                    tags=["important"],
                    retry_policy={"max_retries": 3},
                    timeout=60
                )

                self.assertIsNotNone(task_id)
                task = manager.get_task(task_id)
                self.assertEqual(task.task_name, "test_task")
                self.assertEqual(task.task_func, "my_module.my_func")
                self.assertEqual(task.timeout, 60)

    @patch('builtins.open', new_callable=mock_open)
    def test_list_tasks(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                manager.register_task("task1", "func1", tags=["tag1"])
                manager.register_task("task2", "func2", tags=["tag2"])
                manager.register_task("task3", "func3", tags=["tag1", "tag2"])

                tasks = manager.list_tasks()
                self.assertEqual(len(tasks), 3)

                filtered = manager.list_tasks(tags=["tag1"])
                self.assertEqual(len(filtered), 2)


class TestBlockManagement(unittest.TestCase):
    """Test block management functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_create_block(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                block_id = manager.create_block(
                    block_name="test_block",
                    block_type="s3",
                    data={"bucket": "my-bucket"},
                    description="Test block"
                )

                self.assertIsNotNone(block_id)
                block = manager.get_block(block_id)
                self.assertEqual(block.block_name, "test_block")
                self.assertEqual(block.block_type, "s3")
                self.assertEqual(block.data["bucket"], "my-bucket")

    @patch('builtins.open', new_callable=mock_open)
    def test_get_block_by_name(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                block_id = manager.create_block(
                    block_name="my_block",
                    block_type="s3",
                    data={}
                )

                block = manager.get_block_by_name("my_block", "s3")
                self.assertIsNotNone(block)
                self.assertEqual(block.block_id, block_id)

                block = manager.get_block_by_name("my_block", "gcs")
                self.assertIsNone(block)


class TestWorkQueueManagement(unittest.TestCase):
    """Test work queue management functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_create_work_queue(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                queue_id = manager.create_work_queue(
                    queue_name="test_queue",
                    concurrency=20,
                    priority=10,
                    description="Test queue"
                )

                self.assertIsNotNone(queue_id)
                queue = manager.get_work_queue(queue_id)
                self.assertEqual(queue.queue_name, "test_queue")
                self.assertEqual(queue.concurrency, 20)
                self.assertEqual(queue.priority, 10)

    @patch('builtins.open', new_callable=mock_open)
    def test_pause_work_queue(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                queue_id = manager.create_work_queue("queue1")

                result = manager.pause_work_queue(queue_id, paused=True)
                self.assertTrue(result)

                queue = manager.get_work_queue(queue_id)
                self.assertEqual(queue.state, WorkQueueState.PAUSED)

                result = manager.pause_work_queue(queue_id, paused=False)
                self.assertTrue(result)

                queue = manager.get_work_queue(queue_id)
                self.assertEqual(queue.state, WorkQueueState.READY)


class TestCloudIntegration(unittest.TestCase):
    """Test cloud integration functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_configure_cloud(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                manager.configure_cloud(
                    api_url="https://api.prefect.cloud",
                    api_key="test_key",
                    workspace="my-workspace",
                    tenant="my-tenant"
                )

                config = manager.get_cloud_config()
                self.assertEqual(config["api_url"], "https://api.prefect.cloud")
                self.assertEqual(config["api_key"], "test_key")
                self.assertEqual(config["workspace"], "my-workspace")
                self.assertEqual(config["tenant"], "my-tenant")
                self.assertTrue(manager.is_cloud_configured())

    @patch('builtins.open', new_callable=mock_open)
    def test_sync_to_cloud_not_configured(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                result = manager.sync_to_cloud()
                self.assertFalse(result)


class TestNotificationHooks(unittest.TestCase):
    """Test notification hook functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_create_notification_hook(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                hook_id = manager.create_notification_hook(
                    name="test_hook",
                    event_type="flow_completed",
                    channel_type="webhook",
                    config={"url": "https://example.com/webhook"},
                    enabled=True
                )

                self.assertIsNotNone(hook_id)
                hook = manager.get_notification_hook(hook_id)
                self.assertEqual(hook.name, "test_hook")
                self.assertEqual(hook.event_type, "flow_completed")
                self.assertEqual(hook.channel_type, "webhook")

    @patch('builtins.open', new_callable=mock_open)
    def test_list_notification_hooks_by_event_type(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                manager.create_notification_hook("hook1", "flow_completed", "webhook", {})
                manager.create_notification_hook("hook2", "flow_failed", "webhook", {})
                manager.create_notification_hook("hook3", "flow_completed", "email", {})

                hooks = manager.list_notification_hooks(event_type="flow_completed")
                self.assertEqual(len(hooks), 2)


class TestResultStorage(unittest.TestCase):
    """Test result storage functionality"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_configure_result_storage(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                storage_id = manager.configure_result_storage(
                    storage_type="s3",
                    location="s3://my-bucket/results",
                    config={"region": "us-east-1"}
                )

                self.assertIsNotNone(storage_id)
                storage = manager.get_result_storage(storage_id)
                self.assertEqual(storage.storage_type, "s3")
                self.assertEqual(storage.location, "s3://my-bucket/results")

    @patch('builtins.open', new_callable=mock_open)
    def test_store_flow_result(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow("flow", {"steps": []})
                run_id = manager.run_flow(flow_id, wait=False)

                result = manager.store_flow_result(run_id, {"output": "test_result"})
                self.assertTrue(result)


class TestUtilityMethods(unittest.TestCase):
    """Test utility methods"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('builtins.open', new_callable=mock_open)
    def test_get_status(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                manager.register_flow("flow1", {"steps": []})
                manager.create_work_queue("queue1")

                status = manager.get_status()
                self.assertEqual(status["flows"], 1)
                self.assertEqual(status["work_queues"], 1)
                self.assertFalse(status["cloud_configured"])

    @patch('builtins.open', new_callable=mock_open)
    def test_cleanup_old_runs(self, mock_file):
        with patch('src.workflow_prefect.os.makedirs'):
            with patch('src.workflow_prefect.json.load', return_value={}):
                manager = PrefectManager(data_dir=self.temp_dir)
                manager._save_data = Mock()

                flow_id = manager.register_flow("flow", {"steps": []})
                run_id = manager.run_flow(flow_id, wait=False)

                removed = manager.cleanup_old_runs(max_age_seconds=0)
                self.assertGreaterEqual(removed, 0)


if __name__ == '__main__':
    unittest.main()
