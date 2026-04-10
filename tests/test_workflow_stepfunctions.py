"""
Tests for workflow_stepfunctions module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import time
from datetime import datetime, timedelta
import hashlib

from src.workflow_stepfunctions import (
    StepFunctionsManager,
    StepFunctionsAPIException,
    ExecutionType,
    ExecutionStatus,
    StateMachineStatus,
    ActivityStatus,
    StateMachine,
    Execution,
    Activity,
    StateMachineDefinition,
    StateBuilder,
    ChoiceRuleBuilder,
    ErrorHandlerBuilder,
    CloudWatchBuilder,
    IAMRoleBuilder
)


class TestStepFunctionsEnums(unittest.TestCase):
    """Test Step Functions enums"""

    def test_execution_type_values(self):
        self.assertEqual(ExecutionType.EXPRESS.value, "EXPRESS")
        self.assertEqual(ExecutionType.STANDARD.value, "STANDARD")

    def test_execution_status_values(self):
        self.assertEqual(ExecutionStatus.RUNNING.value, "RUNNING")
        self.assertEqual(ExecutionStatus.SUCCEEDED.value, "SUCCEEDED")
        self.assertEqual(ExecutionStatus.FAILED.value, "FAILED")
        self.assertEqual(ExecutionStatus.TIMED_OUT.value, "TIMED_OUT")
        self.assertEqual(ExecutionStatus.ABORTED.value, "ABORTED")

    def test_state_machine_status_values(self):
        self.assertEqual(StateMachineStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(StateMachineStatus.DELETING.value, "DELETING")
        self.assertEqual(StateMachineStatus.PENDING.value, "PENDING")

    def test_activity_status_values(self):
        self.assertEqual(ActivityStatus.STATUS.value, "ACTIVE")
        self.assertEqual(ActivityStatus.DORMANT.value, "DORMANT")


class TestStateMachineDefinition(unittest.TestCase):
    """Test StateMachineDefinition class"""

    def test_to_dict(self):
        definition = StateMachineDefinition(
            Comment="Test workflow",
            StartAt="PassState",
            States={"PassState": {"Type": "Pass"}},
            TimeoutSeconds=300
        )
        result = definition.to_dict()
        self.assertEqual(result["Comment"], "Test workflow")
        self.assertEqual(result["StartAt"], "PassState")
        self.assertIn("PassState", result["States"])
        self.assertEqual(result["TimeoutSeconds"], 300)

    def test_to_json(self):
        definition = StateMachineDefinition(
            Comment="Test",
            StartAt="Start",
            States={"Start": {"Type": "Pass"}}
        )
        json_str = definition.to_json()
        parsed = json.loads(json_str)
        self.assertEqual(parsed["Comment"], "Test")


class TestStateBuilder(unittest.TestCase):
    """Test StateBuilder class"""

    def test_pass_state(self):
        state = StateBuilder.pass_state("PassState", comment="Test pass", result={"key": "value"}, next_state="NextState", end=True)
        self.assertEqual(state["Type"], "Pass")
        self.assertEqual(state["Comment"], "Test pass")
        self.assertEqual(state["Result"], {"key": "value"})
        self.assertEqual(state["Next"], "NextState")
        self.assertTrue(state["End"])

    def test_task_state(self):
        state = StateBuilder.task_state(
            "TaskState",
            resource="arn:aws:lambda:us-east-1:123456789012:function:MyFunction",
            comment="Invoke Lambda",
            timeout_seconds=120,
            heartbeat_seconds=30,
            parameters={"input": "$"},
            result_path="$.result",
            next_state="NextState",
            end=True
        )
        self.assertEqual(state["Type"], "Task")
        self.assertEqual(state["Resource"], "arn:aws:lambda:us-east-1:123456789012:function:MyFunction")
        self.assertEqual(state["TimeoutSeconds"], 120)
        self.assertEqual(state["HeartbeatSeconds"], 30)
        self.assertEqual(state["Parameters"], {"input": "$"})
        self.assertEqual(state["ResultPath"], "$.result")
        self.assertEqual(state["Next"], "NextState")
        self.assertTrue(state["End"])

    def test_choice_state(self):
        choices = [
            {"Variable": "$.status", "StringEquals": "success", "Next": "SuccessState"},
            {"Variable": "$.status", "StringEquals": "failure", "Next": "FailureState"}
        ]
        state = StateBuilder.choice_state("ChoiceState", comment="Choose path", choices=choices, default_state="DefaultState")
        self.assertEqual(state["Type"], "Choice")
        self.assertEqual(state["Comment"], "Choose path")
        self.assertEqual(len(state["Choices"]), 2)
        self.assertEqual(state["Default"], "DefaultState")

    def test_wait_state_seconds(self):
        state = StateBuilder.wait_state("WaitState", seconds=10, next_state="NextState")
        self.assertEqual(state["Type"], "Wait")
        self.assertEqual(state["Seconds"], 10)
        self.assertEqual(state["Next"], "NextState")

    def test_wait_state_timestamp(self):
        state = StateBuilder.wait_state("WaitState", timestamp="2024-01-01T00:00:00Z", next_state="NextState")
        self.assertEqual(state["Type"], "Wait")
        self.assertEqual(state["Timestamp"], "2024-01-01T00:00:00Z")
        self.assertEqual(state["Next"], "NextState")

    def test_succeed_state(self):
        state = StateBuilder.succeed_state("SucceedState", comment="Success", output={"result": "done"})
        self.assertEqual(state["Type"], "Succeed")
        self.assertEqual(state["Comment"], "Success")
        self.assertEqual(state["Output"], {"result": "done"})

    def test_fail_state(self):
        state = StateBuilder.fail_state("FailState", error="MyError", cause="Something went wrong")
        self.assertEqual(state["Type"], "Fail")
        self.assertEqual(state["Error"], "MyError")
        self.assertEqual(state["Cause"], "Something went wrong")

    def test_parallel_state(self):
        branches = [
            {"StartAt": "Branch1", "States": {"Branch1": {"Type": "Pass", "End": True}}}
        ]
        state = StateBuilder.parallel_state("ParallelState", branches=branches, result_path="$.parallel", next_state="NextState", comment="Parallel execution")
        self.assertEqual(state["Type"], "Parallel")
        self.assertEqual(len(state["Branches"]), 1)
        self.assertEqual(state["ResultPath"], "$.parallel")
        self.assertEqual(state["Next"], "NextState")
        self.assertEqual(state["Comment"], "Parallel execution")

    def test_map_state(self):
        iterator = {"StartAt": "MapIter", "States": {"MapIter": {"Type": "Pass", "End": True}}}
        state = StateBuilder.map_state("MapState", items_path="$.items", max_concurrency=10, iterator=iterator, result_path="$.map_result")
        self.assertEqual(state["Type"], "Map")
        self.assertEqual(state["ItemsPath"], "$.items")
        self.assertEqual(state["MaxConcurrency"], 10)
        self.assertIn("Iterator", state)
        self.assertEqual(state["ResultPath"], "$.map_result")


class TestChoiceRuleBuilder(unittest.TestCase):
    """Test ChoiceRuleBuilder class"""

    def test_string_equals(self):
        rule = ChoiceRuleBuilder.string_equals("$.status", "success")
        self.assertEqual(rule["Variable"], "$.status")
        self.assertEqual(rule["StringEquals"], "success")

    def test_numeric_equals(self):
        rule = ChoiceRuleBuilder.numeric_equals("$.count", 5)
        self.assertEqual(rule["Variable"], "$.count")
        self.assertEqual(rule["NumericEquals"], 5)

    def test_numeric_greater_than(self):
        rule = ChoiceRuleBuilder.numeric_greater_than("$.count", 10)
        self.assertEqual(rule["Variable"], "$.count")
        self.assertEqual(rule["NumericGreaterThan"], 10)

    def test_numeric_less_than(self):
        rule = ChoiceRuleBuilder.numeric_less_than("$.count", 10)
        self.assertEqual(rule["Variable"], "$.count")
        self.assertEqual(rule["NumericLessThan"], 10)

    def test_boolean_equals(self):
        rule = ChoiceRuleBuilder.boolean_equals("$.flag", True)
        self.assertEqual(rule["Variable"], "$.flag")
        self.assertEqual(rule["BooleanEquals"], True)

    def test_timestamp_equals(self):
        rule = ChoiceRuleBuilder.timestamp_equals("$.time", "2024-01-01T00:00:00Z")
        self.assertEqual(rule["Variable"], "$.time")
        self.assertEqual(rule["TimestampEquals"], "2024-01-01T00:00:00Z")

    def test_is_present(self):
        rule = ChoiceRuleBuilder.is_present("$.optional")
        self.assertEqual(rule["Variable"], "$.optional")
        self.assertTrue(rule["IsPresent"])

    def test_and_(self):
        cond1 = ChoiceRuleBuilder.string_equals("$.status", "success")
        cond2 = ChoiceRuleBuilder.numeric_greater_than("$.count", 0)
        rule = ChoiceRuleBuilder.and_(cond1, cond2)
        self.assertIn("And", rule)
        self.assertEqual(len(rule["And"]), 2)

    def test_or_(self):
        cond1 = ChoiceRuleBuilder.string_equals("$.status", "success")
        cond2 = ChoiceRuleBuilder.string_equals("$.status", "warning")
        rule = ChoiceRuleBuilder.or_(cond1, cond2)
        self.assertIn("Or", rule)
        self.assertEqual(len(rule["Or"]), 2)

    def test_not_(self):
        cond = ChoiceRuleBuilder.string_equals("$.status", "success")
        rule = ChoiceRuleBuilder.not_(cond)
        self.assertIn("Not", rule)


class TestErrorHandlerBuilder(unittest.TestCase):
    """Test ErrorHandlerBuilder class"""

    def test_retry(self):
        retry = ErrorHandlerBuilder.retry(
            error_equals=["States.Timeout", "States.TaskFailed"],
            max_attempts=5,
            interval_seconds=2,
            backoff_rate=2.0,
            max_interval_seconds=100
        )
        self.assertEqual(retry["ErrorEquals"], ["States.Timeout", "States.TaskFailed"])
        self.assertEqual(retry["MaxAttempts"], 5)
        self.assertEqual(retry["IntervalSeconds"], 2)
        self.assertEqual(retry["BackoffRate"], 2.0)
        self.assertEqual(retry["MaxIntervalSeconds"], 100)

    def test_catch_default(self):
        catch = ErrorHandlerBuilder.catch(reason="Error", next_state="FallbackState")
        self.assertEqual(catch["ErrorEquals"], ["States.ALL"])
        self.assertEqual(catch["Next"], "FallbackState")

    def test_catch_custom_errors(self):
        catch = ErrorHandlerBuilder.catch(reason="Error", next_state="FallbackState", error_equals=["CustomError"])
        self.assertEqual(catch["ErrorEquals"], ["CustomError"])


class TestCloudWatchBuilder(unittest.TestCase):
    """Test CloudWatchBuilder class"""

    def test_logging_configuration(self):
        config = CloudWatchBuilder.logging_configuration(
            level="ALL",
            include_execution_data=True,
            log_group_arn="arn:aws:logs:us-east-1:123456789012:log-group:/aws/states:*"
        )
        self.assertEqual(config["level"], "ALL")
        self.assertTrue(config["includeExecutionData"])
        self.assertIn("logGroupArn", config)

    def test_logging_configuration_minimal(self):
        config = CloudWatchBuilder.logging_configuration()
        self.assertEqual(config["level"], "ALL")
        self.assertTrue(config["includeExecutionData"])

    def test_tracing_configuration(self):
        config = CloudWatchBuilder.tracing_configuration(enabled=True)
        self.assertTrue(config["enabled"])


class TestIAMRoleBuilder(unittest.TestCase):
    """Test IAMRoleBuilder class"""

    def test_basic_execution_role(self):
        policy = IAMRoleBuilder.basic_execution_role()
        self.assertEqual(policy["Version"], "2012-10-17")
        self.assertEqual(len(policy["Statement"]), 1)
        self.assertEqual(policy["Statement"][0]["Effect"], "Allow")

    def test_activity_task_role(self):
        policy = IAMRoleBuilder.activity_task_role()
        self.assertEqual(policy["Version"], "2012-10-17")
        actions = policy["Statement"][0]["Action"]
        self.assertIn("states:GetActivityTask", actions)
        self.assertIn("states:SendTaskHeartbeat", actions)

    def test_lambda_invoke_role(self):
        policy = IAMRoleBuilder.lambda_invoke_role()
        self.assertEqual(policy["Version"], "2012-10-17")
        self.assertEqual(policy["Statement"][0]["Action"], ["lambda:InvokeFunction"])
        self.assertEqual(policy["Statement"][0]["Resource"], "*")

    def test_lambda_invoke_role_with_arn(self):
        policy = IAMRoleBuilder.lambda_invoke_role("arn:aws:lambda:us-east-1:123456789012:function:MyFunction")
        self.assertEqual(policy["Statement"][0]["Resource"], "arn:aws:lambda:us-east-1:123456789012:function:MyFunction")


class TestStepFunctionsManagerInit(unittest.TestCase):
    """Test StepFunctionsManager initialization"""

    def test_init_with_defaults(self):
        manager = StepFunctionsManager()
        self.assertEqual(manager.region, "us-east-1")
        self.assertIsNone(manager.aws_access_key_id)
        self.assertIsNone(manager.aws_secret_access_key)

    def test_init_with_custom_params(self):
        manager = StepFunctionsManager(
            region="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="secret",
            endpoint_url="https://states.us-west-2.amazonaws.com"
        )
        self.assertEqual(manager.region, "us-west-2")
        self.assertEqual(manager.aws_access_key_id, "AKIAIOSFODNN7EXAMPLE")
        self.assertEqual(manager.endpoint_url, "https://states.us-west-2.amazonaws.com")


class TestStateMachineManagement(unittest.TestCase):
    """Test State Machine management functionality"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_create_state_machine(self):
        definition = {
            "Comment": "Test state machine",
            "StartAt": "PassState",
            "States": {
                "PassState": {"Type": "Pass", "End": True}
            }
        }
        sm = self.manager.create_state_machine(
            name="test_state_machine",
            definition=definition,
            role_arn="arn:aws:iam::123456789012:role/test-role",
            execution_type=ExecutionType.STANDARD,
            description="Test"
        )
        self.assertEqual(sm.name, "test_state_machine")
        self.assertEqual(sm.type, "STANDARD")
        self.assertEqual(sm.status, "ACTIVE")

    def test_get_state_machine(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        sm = self.manager.get_state_machine("test_sm")
        self.assertIsNotNone(sm)
        self.assertEqual(sm.name, "test_sm")

    def test_get_state_machine_not_found(self):
        sm = self.manager.get_state_machine("nonexistent")
        self.assertIsNone(sm)

    def test_list_state_machines(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("sm1", definition, "arn:aws:iam::123456789012:role/test")
        self.manager.create_state_machine("sm2", definition, "arn:aws:iam::123456789012:role/test")
        machines = self.manager.list_state_machines()
        self.assertEqual(len(machines), 2)

    def test_update_state_machine(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.update_state_machine("test_sm", role_arn="arn:aws:iam::123456789012:role/new-role")
        self.assertTrue(result)
        sm = self.manager.get_state_machine("test_sm")
        self.assertEqual(sm.role_arn, "arn:aws:iam::123456789012:role/new-role")

    def test_delete_state_machine(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.delete_state_machine("test_sm")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_state_machine("test_sm"))

    def test_delete_state_machine_not_found(self):
        result = self.manager.delete_state_machine("nonexistent")
        self.assertFalse(result)


class TestExecutionManagement(unittest.TestCase):
    """Test Execution management functionality"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_start_execution(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        execution = self.manager.start_execution("test_sm", input_data={"key": "value"})
        self.assertIsNotNone(execution)
        self.assertEqual(execution.status, "RUNNING")
        self.assertEqual(execution.input, {"key": "value"})

    def test_start_execution_not_found(self):
        with self.assertRaises(StepFunctionsAPIException) as context:
            self.manager.start_execution("nonexistent")
        self.assertIn("not found", str(context.exception))

    def test_get_execution(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        execution = self.manager.start_execution("test_sm")
        retrieved = self.manager.get_execution(execution.execution_arn)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.execution_arn, execution.execution_arn)

    def test_list_executions(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        # Use unique names to avoid execution name collision
        self.manager.start_execution("test_sm", name="exec1")
        self.manager.start_execution("test_sm", name="exec2")
        # All executions across all state machines
        executions = self.manager.list_executions()
        self.assertEqual(len(executions), 2)

    def test_stop_execution(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        execution = self.manager.start_execution("test_sm")
        result = self.manager.stop_execution(execution.execution_arn, error="CustomError", cause="Test error")
        self.assertTrue(result)
        stopped = self.manager.get_execution(execution.execution_arn)
        self.assertEqual(stopped.status, "ABORTED")
        self.assertEqual(stopped.error, "CustomError")

    def test_describe_execution(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        execution = self.manager.start_execution("test_sm", input_data={"test": "data"})
        desc = self.manager.describe_execution(execution.execution_arn)
        self.assertEqual(desc["status"], "RUNNING")
        self.assertIn("executionArn", desc)
        self.assertIn("startDate", desc)

    def test_get_execution_history(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        execution = self.manager.start_execution("test_sm")
        history = self.manager.get_execution_history(execution.execution_arn)
        self.assertIsInstance(history, list)
        self.assertTrue(len(history) > 0)


class TestActivityManagement(unittest.TestCase):
    """Test Activity management functionality"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_create_activity(self):
        activity = self.manager.create_activity("test_activity", tags={"env": "test"})
        self.assertEqual(activity.name, "test_activity")
        self.assertEqual(activity.status, "ACTIVE")
        self.assertIsNotNone(activity.activity_arn)

    def test_get_activity(self):
        self.manager.create_activity("test_activity")
        activity = self.manager.get_activity("test_activity")
        self.assertIsNotNone(activity)
        self.assertEqual(activity.name, "test_activity")

    def test_list_activities(self):
        self.manager.create_activity("activity1")
        self.manager.create_activity("activity2")
        activities = self.manager.list_activities()
        self.assertEqual(len(activities), 2)

    def test_delete_activity(self):
        self.manager.create_activity("test_activity")
        result = self.manager.delete_activity("test_activity")
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_activity("test_activity"))

    def test_get_activity_task(self):
        self.manager.create_activity("test_activity")
        activity = self.manager.get_activity("test_activity")
        # get_activity_task requires activity_arn, use the actual activity_arn
        task = self.manager.get_activity_task(activity.activity_arn, timeout_seconds=60)
        self.assertIsNotNone(task)
        self.assertIn("taskArn", task)
        self.assertIn("input", task)

    def test_send_task_success(self):
        result = self.manager.send_task_success("token123", {"result": "success"})
        self.assertTrue(result)

    def test_send_task_failure(self):
        result = self.manager.send_task_failure("token123", "CustomError", "Test cause")
        self.assertTrue(result)

    def test_send_task_heartbeat(self):
        result = self.manager.send_task_heartbeat("token123")
        self.assertTrue(result)


class TestWorkflowBuilders(unittest.TestCase):
    """Test workflow builder methods"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_build_definition(self):
        definition = self.manager.build_definition()
        self.assertIsInstance(definition, StateMachineDefinition)
        self.assertEqual(definition.Comment, "")
        self.assertEqual(definition.StartAt, "")

    def test_create_simple_workflow(self):
        states = {
            "PassState": {"Type": "Pass", "End": True}
        }
        sm = self.manager.create_simple_workflow(
            name="simple_workflow",
            start_state="PassState",
            end_state="PassState",
            role_arn="arn:aws:iam::123456789012:role/test",
            states=states
        )
        self.assertEqual(sm.name, "simple_workflow")
        self.assertIn("PassState", sm.definition["States"])

    def test_create_parallel_workflow(self):
        branches = [
            {"StartAt": "Branch1", "States": {"Branch1": {"Type": "Pass", "End": True}}}
        ]
        sm = self.manager.create_parallel_workflow(
            name="parallel_workflow",
            branches=branches,
            role_arn="arn:aws:iam::123456789012:role/test",
            result_path="$.parallel"
        )
        self.assertEqual(sm.name, "parallel_workflow")
        self.assertIn("Parallel", sm.definition["States"])

    def test_create_map_workflow(self):
        iterator = {"StartAt": "Iter", "States": {"Iter": {"Type": "Pass", "End": True}}}
        sm = self.manager.create_map_workflow(
            name="map_workflow",
            iterator=iterator,
            items_path="$.items",
            max_concurrency=5,
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        self.assertEqual(sm.name, "map_workflow")
        self.assertIn("MapState", sm.definition["States"])

    def test_create_choice_workflow(self):
        choice_state = {
            "Type": "Choice",
            "Choices": [
                {"Variable": "$.status", "StringEquals": "ok", "Next": "SuccessState"}
            ]
        }
        sm = self.manager.create_choice_workflow(
            name="choice_workflow",
            choice_state=choice_state,
            default_state="DefaultState",
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        self.assertEqual(sm.name, "choice_workflow")


class TestCloudWatchIntegration(unittest.TestCase):
    """Test CloudWatch integration methods"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_enable_logging(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.enable_logging(
            "test_sm",
            "arn:aws:logs:us-east-1:123456789012:log-group:/aws/states:*",
            log_level="ALL"
        )
        self.assertTrue(result)
        sm = self.manager.get_state_machine("test_sm")
        self.assertIsNotNone(sm.logging_configuration)

    def test_enable_logging_not_found(self):
        result = self.manager.enable_logging("nonexistent", "arn:aws:logs:example:*")
        self.assertFalse(result)

    def test_enable_tracing(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.enable_tracing("test_sm")
        self.assertTrue(result)
        sm = self.manager.get_state_machine("test_sm")
        self.assertIsNotNone(sm.tracing_configuration)

    def test_get_execution_metrics(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        execution = self.manager.start_execution("test_sm")
        metrics = self.manager.get_execution_metrics(execution.execution_arn)
        self.assertIn("executionArn", metrics)
        self.assertIn("status", metrics)
        self.assertIn("durationSeconds", metrics)


class TestIAMRoleManagement(unittest.TestCase):
    """Test IAM role management methods"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_create_execution_role(self):
        policy = self.manager.create_execution_role("test_role")
        self.assertIn("Version", policy)
        self.assertIn("Statement", policy)

    def test_create_activity_role(self):
        policy = self.manager.create_activity_role("test_role")
        self.assertIn("Version", policy)
        self.assertIn("Statement", policy)

    def test_create_lambda_role(self):
        policy = self.manager.create_lambda_role("test_role", "arn:aws:lambda:us-east-1:123456789012:function:MyFunc")
        self.assertIn("Version", policy)

    def test_validate_role_valid(self):
        result = self.manager.validate_role("arn:aws:iam::123456789012:role/test-role")
        self.assertTrue(result)

    def test_validate_role_invalid(self):
        result = self.manager.validate_role("invalid-role")
        self.assertFalse(result)

    def test_validate_role_empty(self):
        result = self.manager.validate_role("")
        self.assertFalse(result)


class TestErrorHandling(unittest.TestCase):
    """Test error handling configuration methods"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_add_retry_to_state(self):
        state = {"Type": "Task", "Resource": "arn:aws:lambda:func"}
        updated = self.manager.add_retry_to_state(
            state,
            error_equals=["States.Timeout"],
            max_attempts=3
        )
        self.assertIn("Retry", updated)
        self.assertEqual(len(updated["Retry"]), 1)

    def test_add_retry_multiple(self):
        state = {"Type": "Task", "Resource": "arn:aws:lambda:func"}
        updated = self.manager.add_retry_to_state(state, ["Error1"])
        updated = self.manager.add_retry_to_state(updated, ["Error2"])
        self.assertEqual(len(updated["Retry"]), 2)

    def test_add_catch_to_state(self):
        state = {"Type": "Task", "Resource": "arn:aws:lambda:func"}
        updated = self.manager.add_catch_to_state(state, next_state="Fallback")
        self.assertIn("Catch", updated)
        self.assertEqual(updated["Catch"][0]["Next"], "Fallback")


class TestSimulation(unittest.TestCase):
    """Test execution simulation"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_simulate_pass_workflow(self):
        definition = {
            "Comment": "Test Pass workflow",
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"status": "done"},
                    "End": True
                }
            }
        }
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.simulate_execution("test_sm", {"input": "data"})
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertIn("history", result)

    def test_simulate_task_workflow(self):
        definition = {
            "StartAt": "TaskState",
            "States": {
                "TaskState": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:func",
                    "End": True
                }
            }
        }
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.simulate_execution("test_sm")
        self.assertEqual(result["status"], "SUCCEEDED")

    def test_simulate_fail_workflow(self):
        definition = {
            "StartAt": "FailState",
            "States": {
                "FailState": {
                    "Type": "Fail",
                    "Error": "MyError",
                    "Cause": "Test failure"
                }
            }
        }
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.simulate_execution("test_sm")
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["error"], "MyError")

    def test_simulate_succeed_workflow(self):
        definition = {
            "StartAt": "SucceedState",
            "States": {
                "SucceedState": {
                    "Type": "Succeed",
                    "Output": {"result": "success"}
                }
            }
        }
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        result = self.manager.simulate_execution("test_sm")
        self.assertEqual(result["status"], "SUCCEEDED")

    def test_simulate_not_found(self):
        with self.assertRaises(StepFunctionsAPIException) as context:
            self.manager.simulate_execution("nonexistent")
        self.assertIn("not found", str(context.exception))


class TestValidationAndExport(unittest.TestCase):
    """Test validation and export methods"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_export_definition(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        exported = self.manager.export_definition("test_sm")
        parsed = json.loads(exported)
        self.assertEqual(parsed["StartAt"], "Pass")

    def test_export_not_found(self):
        with self.assertRaises(StepFunctionsAPIException) as context:
            self.manager.export_definition("nonexistent")
        self.assertIn("not found", str(context.exception))

    def test_import_definition(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        new_def = '{"StartAt": "NewPass", "States": {"NewPass": {"Type": "Pass", "End": true}}}'
        result = self.manager.import_definition("test_sm", new_def)
        self.assertTrue(result)
        sm = self.manager.get_state_machine("test_sm")
        self.assertEqual(sm.definition["StartAt"], "NewPass")

    def test_import_invalid_json(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        with self.assertRaises(StepFunctionsAPIException) as context:
            self.manager.import_definition("test_sm", "invalid json")
        self.assertIn("Invalid JSON", str(context.exception))

    def test_validate_definition_valid(self):
        definition = {
            "StartAt": "Pass",
            "States": {
                "Pass": {"Type": "Pass", "End": True}
            }
        }
        errors = self.manager.validate_definition(definition)
        self.assertEqual(len(errors), 0)

    def test_validate_definition_missing_startat(self):
        definition = {"States": {"Pass": {"Type": "Pass", "End": True}}}
        errors = self.manager.validate_definition(definition)
        self.assertIn("Missing required field: StartAt", errors)

    def test_validate_definition_missing_states(self):
        definition = {"StartAt": "Pass"}
        errors = self.manager.validate_definition(definition)
        self.assertIn("Missing required field: States", errors)

    def test_validate_definition_invalid_state_type(self):
        definition = {
            "StartAt": "BadState",
            "States": {
                "BadState": {"Type": "InvalidType", "End": True}
            }
        }
        errors = self.manager.validate_definition(definition)
        self.assertTrue(any("Invalid state type" in e for e in errors))

    def test_validate_definition_terminal_with_next(self):
        definition = {
            "StartAt": "SucceedState",
            "States": {
                "SucceedState": {"Type": "Succeed", "Next": "OtherState"}
            }
        }
        errors = self.manager.validate_definition(definition)
        self.assertTrue(any("should not have 'Next'" in e for e in errors))


class TestStatsAndCleanup(unittest.TestCase):
    """Test stats and cleanup methods"""

    def setUp(self):
        self.manager = StepFunctionsManager()

    def test_get_state_machine_types(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("sm1", definition, "arn:aws:iam::123456789012:role/test", ExecutionType.EXPRESS)
        self.manager.create_state_machine("sm2", definition, "arn:aws:iam::123456789012:role/test", ExecutionType.STANDARD)
        types = self.manager.get_state_machine_types()
        self.assertEqual(types["EXPRESS"], 1)
        self.assertEqual(types["STANDARD"], 1)

    def test_get_execution_stats(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        # Use unique names to avoid execution name collision
        self.manager.start_execution("test_sm", name="exec1")
        self.manager.start_execution("test_sm", name="exec2")
        stats = self.manager.get_execution_stats()
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["running"], 2)

    def test_cleanup_old_executions(self):
        definition = {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        self.manager.create_state_machine("test_sm", definition, "arn:aws:iam::123456789012:role/test")
        self.manager.start_execution("test_sm")
        cleaned = self.manager.cleanup_old_executions(days=0)
        self.assertEqual(cleaned, 0)


if __name__ == '__main__':
    unittest.main()
