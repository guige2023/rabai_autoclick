"""
Tests for workflow_airflow module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import time
from datetime import datetime, timedelta
import hashlib

from src.workflow_airflow import (
    AirflowManager,
    AirflowAPIException,
    DAGState,
    TaskState,
    TriggerType,
    PoolState,
    DAG,
    DAGRun,
    Task,
    Variable,
    Connection,
    XComMessage,
    SLA,
    Trigger,
    Pool,
    AirflowPlugin
)


class TestDAGState(unittest.TestCase):
    """Test DAGState enum"""

    def test_dag_state_values(self):
        self.assertEqual(DAGState.SUCCESS.value, "success")
        self.assertEqual(DAGState.FAILED.value, "failed")
        self.assertEqual(DAGState.RUNNING.value, "running")
        self.assertEqual(DAGState.QUEUED.value, "queued")
        self.assertEqual(DAGState.UP_FOR_RETRY.value, "up_for_retry")
        self.assertEqual(DAGState.SKIPPED.value, "skipped")


class TestTaskState(unittest.TestCase):
    """Test TaskState enum"""

    def test_task_state_values(self):
        self.assertEqual(TaskState.SUCCESS.value, "success")
        self.assertEqual(TaskState.FAILED.value, "failed")
        self.assertEqual(TaskState.RUNNING.value, "running")
        self.assertEqual(TaskState.QUEUED.value, "queued")


class TestTriggerType(unittest.TestCase):
    """Test TriggerType enum"""

    def test_trigger_type_values(self):
        self.assertEqual(TriggerType.MANUAL.value, "manual")
        self.assertEqual(TriggerType.SCHEDULED.value, "scheduled")
        self.assertEqual(TriggerType.CLI.value, "cli")
        self.assertEqual(TriggerType.REST_API.value, "rest_api")
        self.assertEqual(TriggerType.Callback.value, "callback")


class TestPoolState(unittest.TestCase):
    """Test PoolState enum"""

    def test_pool_state_values(self):
        self.assertEqual(PoolState.OPEN.value, "open")
        self.assertEqual(PoolState.FULL.value, "full")


class TestAirflowManagerInit(unittest.TestCase):
    """Test AirflowManager initialization"""

    def test_init_with_defaults(self):
        manager = AirflowManager()
        self.assertEqual(manager.base_url, "http://localhost:8080")
        self.assertEqual(manager.auth, ("airflow", "airflow"))
        self.assertTrue(manager.verify_ssl)

    def test_init_with_custom_params(self):
        manager = AirflowManager(
            base_url="https://airflow.example.com",
            username="admin",
            password="secret",
            verify_ssl=False
        )
        self.assertEqual(manager.base_url, "https://airflow.example.com")
        self.assertEqual(manager.auth, ("admin", "secret"))
        self.assertFalse(manager.verify_ssl)

    def test_stats_initialized(self):
        manager = AirflowManager()
        self.assertEqual(manager._stats["dags_created"], 0)
        self.assertEqual(manager._stats["dag_runs_triggered"], 0)
        self.assertEqual(manager._stats["tasks_executed"], 0)


class TestDAGManagement(unittest.TestCase):
    """Test DAG management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_dag(self):
        dag = self.manager.create_dag(
            dag_id="test_dag",
            fileloc="/path/to/dag.py",
            schedule_interval="0 * * * *",
            description="Test DAG",
            owners="test_owner",
            max_active_runs=10,
            max_active_tasks=20,
            depends_on_past=True,
            retry_delay=600
        )

        self.assertEqual(dag.dag_id, "test_dag")
        self.assertEqual(dag.fileloc, "/path/to/dag.py")
        self.assertEqual(dag.schedule_interval, "0 * * * *")
        self.assertEqual(dag.owners, "test_owner")
        self.assertEqual(dag.max_active_runs, 10)
        self.assertEqual(dag.max_active_tasks, 20)
        self.assertTrue(dag.depends_on_past)
        self.assertFalse(dag.is_paused)

    def test_create_dag_duplicate_raises(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        with self.assertRaises(AirflowAPIException) as context:
            self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag2.py")
        self.assertIn("already exists", str(context.exception))

    def test_get_dag(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        dag = self.manager.get_dag("test_dag")
        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "test_dag")

    def test_get_dag_not_found(self):
        dag = self.manager.get_dag("nonexistent")
        self.assertIsNone(dag)

    def test_list_dags(self):
        self.manager.create_dag(dag_id="dag1", fileloc="/path/dag1.py")
        self.manager.create_dag(dag_id="dag2", fileloc="/path/dag2.py")
        dags = self.manager.list_dags()
        self.assertEqual(len(dags), 2)

    def test_update_dag(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        dag = self.manager.update_dag("test_dag", description="updated", owners="new_owner")
        self.assertIsNotNone(dag)
        self.assertEqual(dag.description, "updated")
        self.assertEqual(dag.owners, "new_owner")

    def test_delete_dag(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        result = self.manager.delete_dag("test_dag")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_dag("test_dag"))

    def test_delete_dag_not_found(self):
        result = self.manager.delete_dag("nonexistent")
        self.assertFalse(result)

    def test_pause_dag(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        result = self.manager.pause_dag("test_dag")
        self.assertTrue(result)
        dag = self.manager.get_dag("test_dag")
        self.assertTrue(dag.is_paused)

    def test_unpause_dag(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.pause_dag("test_dag")
        result = self.manager.unpause_dag("test_dag")
        self.assertTrue(result)
        dag = self.manager.get_dag("test_dag")
        self.assertFalse(dag.is_paused)

    def test_get_dag_details(self):
        self.manager.create_dag(
            dag_id="test_dag",
            fileloc="/path/to/dag.py",
            description="Test",
            owners="test",
            schedule_interval="0 * * * *"
        )
        self.manager.create_task("task1", "test_dag")

        details = self.manager.get_dag_details("test_dag")
        self.assertIsNotNone(details)
        self.assertEqual(details["dag_id"], "test_dag")
        self.assertEqual(details["description"], "Test")
        self.assertEqual(details["task_count"], 1)


class TestDAGExecution(unittest.TestCase):
    """Test DAG execution functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_trigger_dag(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id = self.manager.trigger_dag("test_dag")
        self.assertIsNotNone(run_id)
        self.assertIn("manual__test_dag__", run_id)

    def test_trigger_dag_with_conf(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id = self.manager.trigger_dag("test_dag", conf={"key": "value"})
        run = self.manager.get_dag_run("test_dag", run_id)
        self.assertEqual(run.conf["key"], "value")

    def test_trigger_dag_not_found(self):
        with self.assertRaises(AirflowAPIException):
            self.manager.trigger_dag("nonexistent")

    def test_trigger_dag_paused(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.pause_dag("test_dag")
        with self.assertRaises(AirflowAPIException) as context:
            self.manager.trigger_dag("test_dag")
        self.assertIn("paused", str(context.exception))

    def test_get_dag_run(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id = self.manager.trigger_dag("test_dag")
        run = self.manager.get_dag_run("test_dag", run_id)
        self.assertIsNotNone(run)
        self.assertEqual(run.dag_id, "test_dag")

    def test_list_dag_runs(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id1 = self.manager.trigger_dag("test_dag")
        run_id2 = self.manager.trigger_dag("test_dag")
        runs = self.manager.list_dag_runs("test_dag")
        self.assertEqual(len(runs), 2)

    def test_list_dag_runs_filtered_by_state(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id1 = self.manager.trigger_dag("test_dag")
        runs = self.manager.list_dag_runs("test_dag", state="queued")
        self.assertEqual(len(runs), 1)

    def test_update_dag_run_state(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id = self.manager.trigger_dag("test_dag")
        result = self.manager.update_dag_run_state("test_dag", run_id, "running")
        self.assertTrue(result)
        run = self.manager.get_dag_run("test_dag", run_id)
        self.assertEqual(run.state, "running")
        self.assertIsNotNone(run.start_date)

    def test_clear_dag_run(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        run_id = self.manager.trigger_dag("test_dag")
        result = self.manager.clear_dag_run("test_dag", run_id)
        self.assertTrue(result)
        run = self.manager.get_dag_run("test_dag", run_id)
        self.assertEqual(run.state, "cleared")


class TestTaskManagement(unittest.TestCase):
    """Test task management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_task(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        task = self.manager.create_task(
            task_id="task1",
            dag_id="test_dag",
            task_type="python",
            owner="test",
            retries=3,
            retry_delay=600,
            pool="test_pool",
            queue="test_queue"
        )

        self.assertEqual(task.task_id, "task1")
        self.assertEqual(task.dag_id, "test_dag")
        self.assertEqual(task.task_type, "python")
        self.assertEqual(task.retries, 3)
        self.assertEqual(task.pool, "test_pool")

    def test_create_task_dag_not_found(self):
        with self.assertRaises(AirflowAPIException):
            self.manager.create_task("task1", "nonexistent")

    def test_create_task_duplicate(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        with self.assertRaises(AirflowAPIException):
            self.manager.create_task("task1", "test_dag")

    def test_get_task(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        task = self.manager.get_task("test_dag", "task1")
        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, "task1")

    def test_list_tasks(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        self.manager.create_task("task2", "test_dag")
        tasks = self.manager.list_tasks("test_dag")
        self.assertEqual(len(tasks), 2)

    def test_update_task(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        task = self.manager.update_task("test_dag", "task1", retries=5, owner="new_owner")
        self.assertIsNotNone(task)
        self.assertEqual(task.retries, 5)
        self.assertEqual(task.owner, "new_owner")

    def test_delete_task(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        result = self.manager.delete_task("test_dag", "task1")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_task("test_dag", "task1"))

    def test_set_task_upstream(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        self.manager.create_task("task2", "test_dag")
        result = self.manager.set_task_upstream("test_dag", "task2", "task1")
        self.assertTrue(result)
        deps = self.manager.get_task_dependencies("test_dag", "task2")
        self.assertIn("task1", deps["upstream"])

    def test_set_task_downstream(self):
        self.manager.create_dag(dag_id="test_dag", fileloc="/path/to/dag.py")
        self.manager.create_task("task1", "test_dag")
        self.manager.create_task("task2", "test_dag")
        result = self.manager.set_task_downstream("test_dag", "task1", "task2")
        self.assertTrue(result)
        deps = self.manager.get_task_dependencies("test_dag", "task1")
        self.assertIn("task2", deps["downstream"])


class TestVariableManagement(unittest.TestCase):
    """Test variable management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_set_variable(self):
        var = self.manager.set_variable(
            key="test_var",
            value="test_value",
            description="Test variable",
            encrypt=False
        )
        self.assertEqual(var.key, "test_var")
        self.assertEqual(var.value, "test_value")
        self.assertEqual(var.description, "Test variable")
        self.assertFalse(var.is_encrypted)

    def test_get_variable(self):
        self.manager.set_variable("test_var", "test_value")
        value = self.manager.get_variable("test_var")
        self.assertEqual(value, "test_value")

    def test_get_variable_not_found(self):
        value = self.manager.get_variable("nonexistent", default="default_value")
        self.assertEqual(value, "default_value")

    def test_get_variable_full(self):
        self.manager.set_variable("test_var", "test_value", description="desc")
        var = self.manager.get_variable_full("test_var")
        self.assertIsNotNone(var)
        self.assertEqual(var.key, "test_var")
        self.assertEqual(var.description, "desc")

    def test_list_variables(self):
        self.manager.set_variable("var1", "value1")
        self.manager.set_variable("var2", "value2")
        vars = self.manager.list_variables()
        self.assertEqual(len(vars), 2)

    def test_delete_variable(self):
        self.manager.set_variable("test_var", "value")
        result = self.manager.delete_variable("test_var")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_variable("test_var"))

    def test_import_variables(self):
        variables = {"var1": "value1", "var2": "value2", "var3": "value3"}
        count = self.manager.import_variables(variables)
        self.assertEqual(count, 3)
        self.assertEqual(self.manager.get_variable("var1"), "value1")
        self.assertEqual(self.manager.get_variable("var2"), "value2")

    def test_export_variables(self):
        self.manager.set_variable("var1", "value1")
        self.manager.set_variable("var2", "value2")
        exported = self.manager.export_variables()
        self.assertEqual(exported["var1"], "value1")
        self.assertEqual(exported["var2"], "value2")


class TestConnectionManagement(unittest.TestCase):
    """Test connection management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_connection(self):
        conn = self.manager.create_connection(
            conn_id="test_conn",
            conn_type="mysql",
            host="localhost",
            login="user",
            password="pass",
            port=3306,
            extra={"charset": "utf8"},
            description="Test connection"
        )
        self.assertEqual(conn.conn_id, "test_conn")
        self.assertEqual(conn.conn_type, "mysql")
        self.assertEqual(conn.host, "localhost")
        self.assertEqual(conn.port, 3306)

    def test_create_connection_duplicate(self):
        self.manager.create_connection(conn_id="test_conn", conn_type="mysql")
        with self.assertRaises(AirflowAPIException):
            self.manager.create_connection(conn_id="test_conn", conn_type="postgres")

    def test_get_connection(self):
        self.manager.create_connection(conn_id="test_conn", conn_type="mysql")
        conn = self.manager.get_connection("test_conn")
        self.assertIsNotNone(conn)
        self.assertEqual(conn.conn_id, "test_conn")

    def test_list_connections(self):
        self.manager.create_connection(conn_id="conn1", conn_type="mysql")
        self.manager.create_connection(conn_id="conn2", conn_type="postgres")
        conns = self.manager.list_connections()
        self.assertEqual(len(conns), 2)

    def test_list_connections_filtered(self):
        self.manager.create_connection(conn_id="conn1", conn_type="mysql")
        self.manager.create_connection(conn_id="conn2", conn_type="postgres")
        conns = self.manager.list_connections(conn_type="mysql")
        self.assertEqual(len(conns), 1)
        self.assertEqual(conns[0].conn_type, "mysql")

    def test_update_connection(self):
        self.manager.create_connection(conn_id="test_conn", conn_type="mysql", host="old_host")
        conn = self.manager.update_connection("test_conn", host="new_host")
        self.assertIsNotNone(conn)
        self.assertEqual(conn.host, "new_host")

    def test_delete_connection(self):
        self.manager.create_connection(conn_id="test_conn", conn_type="mysql")
        result = self.manager.delete_connection("test_conn")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_connection("test_conn"))

    def test_test_connection(self):
        self.manager.create_connection(
            conn_id="test_conn",
            conn_type="mysql",
            host="localhost",
            port=3306
        )
        result = self.manager.test_connection("test_conn")
        self.assertTrue(result["success"])
        self.assertEqual(result["conn_type"], "mysql")


class TestXComManagement(unittest.TestCase):
    """Test XCom management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_xcom_push(self):
        msg = self.manager.xcom_push(
            key="result",
            value={"data": "test"},
            task_id="task1",
            dag_id="test_dag",
            execution_date=datetime.now()
        )
        self.assertEqual(msg.key, "result")
        self.assertEqual(msg.value["data"], "test")
        self.assertEqual(msg.task_id, "task1")

    def test_xcom_pull(self):
        exec_date = datetime(2024, 1, 15, 10, 30, 0)
        self.manager.xcom_push("result", "value1", "task1", "dag1", exec_date)
        self.manager.xcom_push("result", "value2", "task2", "dag1", exec_date)
        results = self.manager.xcom_pull(task_ids=["task1"], dag_id="dag1", execution_date=exec_date, key="result")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "value1")

    def test_xcom_get(self):
        exec_date = datetime(2024, 1, 15, 10, 30, 0)
        self.manager.xcom_push("result", "test_value", "task1", "dag1", exec_date)
        value = self.manager.xcom_get("task1", "dag1", exec_date, "result")
        self.assertEqual(value, "test_value")

    def test_xcom_delete(self):
        exec_date = datetime(2024, 1, 15, 10, 30, 0)
        self.manager.xcom_push("result", "value", "task1", "dag1", exec_date)
        result = self.manager.xcom_delete("task1", "dag1", exec_date, "result")
        self.assertTrue(result)
        value = self.manager.xcom_get("task1", "dag1", exec_date, "result")
        self.assertIsNone(value)

    def test_list_xcoms(self):
        exec_date = datetime(2024, 1, 15, 10, 30, 0)
        self.manager.xcom_push("key1", "value1", "task1", "dag1", exec_date)
        self.manager.xcom_push("key2", "value2", "task2", "dag1", exec_date)
        xcoms = self.manager.list_xcoms(dag_id="dag1")
        self.assertEqual(len(xcoms), 2)


class TestSLAMonitoring(unittest.TestCase):
    """Test SLA monitoring functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_sla(self):
        deadline = datetime.now() + timedelta(hours=1)
        sla = self.manager.create_sla(
            task_id="task1",
            dag_id="test_dag",
            deadline=deadline,
            email="test@example.com",
            description="Test SLA"
        )
        self.assertEqual(sla.task_id, "task1")
        self.assertEqual(sla.dag_id, "test_dag")
        self.assertEqual(sla.email, "test@example.com")
        self.assertTrue(sla.enabled)

    def test_get_sla(self):
        deadline = datetime.now() + timedelta(hours=1)
        self.manager.create_sla("task1", "test_dag", deadline)
        sla = self.manager.get_sla("test_dag", "task1")
        self.assertIsNotNone(sla)
        self.assertEqual(sla.task_id, "task1")

    def test_check_sla_not_missed(self):
        deadline = datetime.now() + timedelta(hours=1)
        self.manager.create_sla("task1", "test_dag", deadline)
        result = self.manager.check_sla("test_dag", "task1")
        self.assertTrue(result["has_sla"])
        self.assertFalse(result["is_missed"])

    def test_check_sla_missed(self):
        deadline = datetime.now() - timedelta(hours=1)
        self.manager.create_sla("task1", "test_dag", deadline)
        result = self.manager.check_sla("test_dag", "task1")
        self.assertTrue(result["has_sla"])
        self.assertTrue(result["is_missed"])

    def test_check_sla_not_found(self):
        result = self.manager.check_sla("test_dag", "nonexistent")
        self.assertFalse(result["has_sla"])

    def test_get_missed_slas(self):
        deadline1 = datetime.now() - timedelta(hours=1)
        deadline2 = datetime.now() + timedelta(hours=1)
        self.manager.create_sla("task1", "dag1", deadline1)
        self.manager.create_sla("task2", "dag2", deadline2)
        missed = self.manager.get_missed_slas()
        self.assertEqual(len(missed), 1)
        self.assertEqual(missed[0]["task_id"], "task1")

    def test_delete_sla(self):
        deadline = datetime.now() + timedelta(hours=1)
        self.manager.create_sla("task1", "test_dag", deadline)
        result = self.manager.delete_sla("test_dag", "task1")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_sla("test_dag", "task1"))


class TestTriggerManagement(unittest.TestCase):
    """Test trigger management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_trigger(self):
        trigger = self.manager.create_trigger(
            dag_id="test_dag",
            trigger_type=TriggerType.MANUAL,
            execution_date=datetime.now()
        )
        self.assertEqual(trigger.dag_id, "test_dag")
        self.assertEqual(trigger.trigger_type, TriggerType.MANUAL)
        self.assertEqual(trigger.status, "pending")

    def test_list_triggers(self):
        self.manager.create_trigger("dag1", TriggerType.MANUAL)
        self.manager.create_trigger("dag2", TriggerType.SCHEDULED)
        triggers = self.manager.list_triggers()
        self.assertEqual(len(triggers), 2)

    def test_list_triggers_filtered_by_status(self):
        self.manager.create_trigger("dag1", TriggerType.MANUAL)
        triggers = self.manager.list_triggers(status="pending")
        self.assertEqual(len(triggers), 1)

    def test_update_trigger_status(self):
        trigger = self.manager.create_trigger("dag1", TriggerType.MANUAL)
        result = self.manager.update_trigger_status(
            "dag1",
            trigger.trigger_id,
            "completed",
            {"output": "success"}
        )
        self.assertTrue(result)
        updated = self.manager.get_trigger("dag1", trigger.trigger_id)
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.result["output"], "success")

    def test_delete_trigger(self):
        trigger = self.manager.create_trigger("dag1", TriggerType.MANUAL)
        result = self.manager.delete_trigger("dag1", trigger.trigger_id)
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_trigger("dag1", trigger.trigger_id))


class TestPoolManagement(unittest.TestCase):
    """Test pool management functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_pool(self):
        pool = self.manager.create_pool(
            name="test_pool",
            slots=10,
            description="Test pool"
        )
        self.assertEqual(pool.name, "test_pool")
        self.assertEqual(pool.slots, 10)
        self.assertEqual(pool.used_slots, 0)
        self.assertEqual(pool.queued_slots, 0)

    def test_create_pool_duplicate(self):
        self.manager.create_pool("test_pool", 10)
        with self.assertRaises(AirflowAPIException):
            self.manager.create_pool("test_pool", 20)

    def test_get_pool(self):
        self.manager.create_pool("test_pool", 10)
        pool = self.manager.get_pool("test_pool")
        self.assertIsNotNone(pool)
        self.assertEqual(pool.name, "test_pool")

    def test_list_pools(self):
        self.manager.create_pool("pool1", 10)
        self.manager.create_pool("pool2", 20)
        pools = self.manager.list_pools()
        self.assertEqual(len(pools), 2)

    def test_allocate_pool_slots(self):
        self.manager.create_pool("test_pool", 10)
        result = self.manager.allocate_pool_slots("test_pool", 5)
        self.assertTrue(result)
        pool = self.manager.get_pool("test_pool")
        self.assertEqual(pool.used_slots, 5)

    def test_allocate_pool_slots_exceed(self):
        self.manager.create_pool("test_pool", 10)
        result = self.manager.allocate_pool_slots("test_pool", 15)
        self.assertFalse(result)

    def test_release_pool_slots(self):
        self.manager.create_pool("test_pool", 10)
        self.manager.allocate_pool_slots("test_pool", 5)
        result = self.manager.release_pool_slots("test_pool", 3)
        self.assertTrue(result)
        pool = self.manager.get_pool("test_pool")
        self.assertEqual(pool.used_slots, 2)

    def test_get_pool_stats(self):
        self.manager.create_pool("test_pool", 10)
        self.manager.allocate_pool_slots("test_pool", 3)
        stats = self.manager.get_pool_stats("test_pool")
        self.assertEqual(stats["total_slots"], 10)
        self.assertEqual(stats["used_slots"], 3)
        self.assertEqual(stats["available_slots"], 7)
        self.assertAlmostEqual(stats["utilization"], 0.3)


class TestPluginIntegration(unittest.TestCase):
    """Test plugin integration functionality"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_create_plugin(self):
        plugin = self.manager.create_plugin(
            name="test_plugin",
            version="1.0.0",
            description="Test plugin",
            hooks=["hook1", "hook2"],
            operators=["operator1"],
            sensors=["sensor1"],
            macros=["macro1"]
        )
        self.assertEqual(plugin.name, "test_plugin")
        self.assertEqual(plugin.version, "1.0.0")
        self.assertEqual(plugin.hooks, ["hook1", "hook2"])
        self.assertEqual(plugin.operators, ["operator1"])

    def test_create_plugin_duplicate(self):
        self.manager.create_plugin("test_plugin", "1.0.0")
        with self.assertRaises(AirflowAPIException):
            self.manager.create_plugin("test_plugin", "2.0.0")

    def test_get_plugin(self):
        self.manager.create_plugin("test_plugin", "1.0.0")
        plugin = self.manager.get_plugin("test_plugin")
        self.assertIsNotNone(plugin)
        self.assertEqual(plugin.name, "test_plugin")

    def test_list_plugins(self):
        self.manager.create_plugin("plugin1", "1.0.0")
        self.manager.create_plugin("plugin2", "2.0.0")
        plugins = self.manager.list_plugins()
        self.assertEqual(len(plugins), 2)

    def test_update_plugin(self):
        self.manager.create_plugin("test_plugin", "1.0.0", description="old")
        plugin = self.manager.update_plugin("test_plugin", description="new", version="1.1.0")
        self.assertEqual(plugin.description, "new")
        self.assertEqual(plugin.version, "1.1.0")

    def test_delete_plugin(self):
        self.manager.create_plugin("test_plugin", "1.0.0")
        result = self.manager.delete_plugin("test_plugin")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_plugin("test_plugin"))

    def test_register_operator(self):
        self.manager.create_plugin("test_plugin", "1.0.0")
        result = self.manager.register_operator("test_plugin", "CustomOperator")
        self.assertTrue(result)
        plugin = self.manager.get_plugin("test_plugin")
        self.assertIn("CustomOperator", plugin.operators)

    def test_register_sensor(self):
        self.manager.create_plugin("test_plugin", "1.0.0")
        result = self.manager.register_sensor("test_plugin", "CustomSensor")
        self.assertTrue(result)
        plugin = self.manager.get_plugin("test_plugin")
        self.assertIn("CustomSensor", plugin.sensors)


class TestGenerateRunId(unittest.TestCase):
    """Test run ID generation"""

    def setUp(self):
        self.manager = AirflowManager()

    def test_generate_run_id_format(self):
        run_id = self.manager._generate_run_id("test_dag")
        self.assertTrue(run_id.startswith("manual__test_dag__"))
        self.assertEqual(len(run_id), len("manual__test_dag__") + 8)

    def test_generate_run_id_with_execution_date(self):
        exec_date = datetime(2024, 1, 1, 12, 0, 0)
        run_id = self.manager._generate_run_id("test_dag", exec_date)
        self.assertTrue(run_id.startswith("manual__test_dag__"))

    def test_generate_run_id_consistent(self):
        exec_date = datetime(2024, 1, 1, 12, 0, 0)
        run_id1 = self.manager._generate_run_id("test_dag", exec_date)
        run_id2 = self.manager._generate_run_id("test_dag", exec_date)
        self.assertEqual(run_id1, run_id2)


if __name__ == '__main__':
    unittest.main()
