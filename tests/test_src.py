"""Tests for src modules of RabAI AutoClick.

Tests SelfHealingSystem, PredictiveAutomationEngine,
WorkflowDiagnostics, and PipelineMode.
"""

import sys
import os
import json
import tempfile
import time
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass

sys.path.insert(0, '/Users/guige/my_project')


class TestSelfHealingSystem(unittest.TestCase):
    """Tests for SelfHealingSystem attempt_recovery."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        # Create data directory
        self.data_dir = os.path.join(self.temp_dir, "data")
        os.makedirs(self.data_dir)
        
    def tearDown(self):
        """Clean up."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_analyze_error_element_not_found(self):
        """Test error analysis for element not found."""
        from src.self_healing_system import SelfHealingSystem, ErrorType
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        
        error = ValueError("Element not found at position")
        record = system.analyze_error(
            error, "test_workflow", "click_step", 0
        )
        
        self.assertEqual(record.error_type, ErrorType.ELEMENT_NOT_FOUND)

    def test_analyze_error_timeout(self):
        """Test error analysis for timeout."""
        from src.self_healing_system import SelfHealingSystem, ErrorType
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        
        error = TimeoutError("Operation timed out after 30 seconds")
        record = system.analyze_error(
            error, "test_workflow", "wait_step", 1
        )
        
        self.assertEqual(record.error_type, ErrorType.TIMEOUT)

    def test_analyze_error_permission_denied(self):
        """Test error analysis for permission denied."""
        from src.self_healing_system import SelfHealingSystem, ErrorType
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        
        error = PermissionError("Access denied to resource")
        record = system.analyze_error(
            error, "test_workflow", "access_step", 2
        )
        
        self.assertEqual(record.error_type, ErrorType.PERMISSION_DENIED)

    def test_get_fix_suggestions(self):
        """Test getting fix suggestions for error type."""
        from src.self_healing_system import SelfHealingSystem, ErrorType
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        
        record = Mock()
        record.error_type = ErrorType.ELEMENT_NOT_FOUND
        record.workflow_name = "test"
        record.step_name = "step1"
        record.error_message = "Element not found at position"
        
        suggestions = system.get_fix_suggestions(record)
        
        self.assertIsInstance(suggestions, list)
        # Should have suggestions for element not found
        self.assertTrue(len(suggestions) > 0)

    def test_attempt_recovery_no_strategy(self):
        """Test attempt recovery when no strategy available."""
        from src.self_healing_system import SelfHealingSystem, ErrorType, RecoveryStrategy
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        system.recovery_patterns = {}  # Clear patterns
        
        record = Mock()
        record.error_type = ErrorType.UNKNOWN
        record.error_message = "Unknown error"
        record.workflow_name = "test"
        record.step_name = "step1"
        record.step_index = 0
        record.recovery_attempted = False
        record.recovery_result = "none"
        record.recovery_details = ""
        
        callback = Mock()
        result = system.attempt_recovery(record, {}, callback)
        
        self.assertFalse(result.success)
        self.assertEqual(result.strategy, RecoveryStrategy.NOTIFY_USER)

    def test_health_score_no_history(self):
        """Test health score with no history returns 100."""
        from src.self_healing_system import SelfHealingSystem
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        system.healing_history = []
        
        score = system.health_score()
        
        self.assertEqual(score, 100.0)

    def test_health_score_with_failures(self):
        """Test health score decreases with failures."""
        from src.self_healing_system import SelfHealingSystem, HealingAction, ErrorType, RecoveryStrategy
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        
        # Add some healing actions
        for i in range(10):
            action = HealingAction(
                timestamp=time.time(),
                workflow_name="test",
                step_name="step1",
                step_index=0,
                error_type=ErrorType.ELEMENT_NOT_FOUND,
                error_message="Not found",
                strategy_used=RecoveryStrategy.RETRY,
                success=(i % 2 == 0),  # Half succeed
                recovery_time=1.0,
                details=""
            )
            system.healing_history.append(action)
        
        score = system.health_score()
        
        # Score should be less than 100 with failures
        self.assertLess(score, 100.0)

    def test_get_healing_metrics(self):
        """Test getting healing metrics."""
        from src.self_healing_system import SelfHealingSystem
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        system.metrics.total_healing_attempts = 10
        system.metrics.successful_healings = 7
        
        metrics = system.get_healing_metrics()
        
        self.assertEqual(metrics["total_healing_attempts"], 10)
        self.assertEqual(metrics["successful_healings"], 7)
        self.assertIn("success_rate", metrics)

    def test_register_flow_engine_callback(self):
        """Test registering flow engine callback."""
        from src.self_healing_system import SelfHealingSystem
        
        system = SelfHealingSystem(data_dir=self.data_dir)
        
        callback = Mock()
        system.register_flow_engine_callback(callback)
        
        self.assertEqual(system._flow_engine_callback, callback)


class TestPredictiveAutomationEngine(unittest.TestCase):
    """Tests for PredictiveAutomationEngine predictions."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.temp_dir, "data")
        os.makedirs(self.data_dir)

    def tearDown(self):
        """Clean up."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_record_action(self):
        """Test recording a user action."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        
        engine.record_action(
            action_type="click",
            target="button_submit",
            context={"active_app": "Chrome"},
            result="success",
            duration=0.5
        )
        
        self.assertEqual(len(engine.action_history), 1)
        self.assertEqual(engine.action_history[0].target, "button_submit")

    def test_predict_next_action_no_history(self):
        """Test prediction with no history returns None."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        engine.action_history = []
        
        prediction = engine.predict_next_action()
        
        # With no history, prediction might be None or based on patterns
        # The exact behavior depends on implementation
        self.assertIsNone(prediction)

    def test_record_user_correction(self):
        """Test recording user correction."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        
        engine.record_user_correction(
            predicted_action="click_button_a",
            user_action="click_button_b",
            context={"screen": "main"}
        )
        
        self.assertEqual(engine.user_corrections["click_button_b"], 1)

    def test_get_action_confidence(self):
        """Test getting action confidence based on corrections."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        engine.user_corrections["action1"] = 5
        engine.user_corrections["action2"] = 1
        
        conf1 = engine.get_action_confidence("action1")
        conf2 = engine.get_action_confidence("action2")
        
        # More corrections = lower confidence
        self.assertLess(conf1, conf2)
        # Both should be between 0 and 1
        self.assertGreaterEqual(conf1, 0.0)
        self.assertLessEqual(conf1, 1.0)

    def test_export_learned_patterns(self):
        """Test exporting learned patterns."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        engine.time_patterns["morning"].append("coffee_app")
        
        export_path = engine.export_learned_patterns()
        
        self.assertTrue(os.path.exists(export_path))
        
        with open(export_path) as f:
            data = json.load(f)
        
        self.assertIn("time_patterns", data)

    def test_set_flow_engine_callback(self):
        """Test setting flow engine callback."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        
        callback = Mock()
        engine.set_flow_engine_callback(callback)
        
        self.assertEqual(engine.flow_engine_callback, callback)

    def test_analyze_user_behavior_no_data(self):
        """Test analyzing behavior with no data."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        engine.action_history = []
        
        result = engine.analyze_user_behavior()
        
        self.assertEqual(result["status"], "no_data")

    def test_get_time_based_suggestions(self):
        """Test getting time-based suggestions."""
        from src.predictive_engine import PredictiveAutomationEngine
        
        engine = PredictiveAutomationEngine(data_dir=self.data_dir)
        
        suggestions = engine.get_time_based_suggestions()
        
        self.assertIsInstance(suggestions, list)


class TestWorkflowDiagnostics(unittest.TestCase):
    """Tests for WorkflowDiagnostics health_score."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.temp_dir, "data")
        os.makedirs(self.data_dir)

    def tearDown(self):
        """Clean up."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_diagnose_no_history(self):
        """Test diagnosing workflow with no history."""
        from src.workflow_diagnostics import WorkflowDiagnosticsV2, HealthLevel
        
        diag = WorkflowDiagnosticsV2(data_dir=self.data_dir)
        
        report = diag.diagnose("nonexistent_workflow")
        
        # Should return empty report with default values
        self.assertEqual(report.workflow_id, "nonexistent_workflow")
        self.assertEqual(report.health_score, 50)
        self.assertEqual(report.overall_health, HealthLevel.FAIR)

    def test_record_execution(self):
        """Test recording execution."""
        from src.workflow_diagnostics import WorkflowDiagnosticsV2
        
        diag = WorkflowDiagnosticsV2(data_dir=self.data_dir)
        
        diag.record_execution(
            workflow_id="test_workflow",
            workflow_name="Test Workflow",
            step_results=[],
            duration=5.0,
            success=True
        )
        
        history = diag.execution_history.get("test_workflow", [])
        self.assertEqual(len(history), 1)
        self.assertTrue(history[0]["success"])

    def test_health_score_trend_no_data(self):
        """Test health score trend with no data."""
        from src.workflow_diagnostics import WorkflowDiagnosticsV2
        
        diag = WorkflowDiagnosticsV2(data_dir=self.data_dir)
        
        trend = diag.get_health_score_trend("nonexistent")
        
        self.assertEqual(trend["status"], "no_data")

    def test_health_score_trend_with_data(self):
        """Test health score trend calculation."""
        from src.workflow_diagnostics import WorkflowDiagnosticsV2
        
        diag = WorkflowDiagnosticsV2(data_dir=self.data_dir)
        
        # Add health score history
        now = time.time()
        diag.health_score_history["test_workflow"] = [
            {"timestamp": now - 86400, "score": 90, "success": True},
            {"timestamp": now - 43200, "score": 85, "success": True},
            {"timestamp": now, "score": 95, "success": True},
        ]
        
        trend = diag.get_health_score_trend("test_workflow")
        
        self.assertEqual(trend["status"], "ok")
        self.assertIn("trend", trend)

    def test_perform_root_cause_analysis_no_failures(self):
        """Test root cause analysis with no failures."""
        from src.workflow_diagnostics import WorkflowDiagnosticsV2
        
        diag = WorkflowDiagnosticsV2(data_dir=self.data_dir)
        
        # Add successful executions
        diag.record_execution("wf1", "Workflow 1", [], 1.0, True)
        
        result = diag.perform_root_cause_analysis("wf1")
        
        self.assertEqual(result["status"], "healthy")

    def test_set_flow_engine_callback(self):
        """Test setting flow engine callback."""
        from src.workflow_diagnostics import WorkflowDiagnosticsV2
        
        diag = WorkflowDiagnosticsV2(data_dir=self.data_dir)
        
        callback = Mock()
        diag.set_flow_engine_callback(callback)
        
        self.assertEqual(diag.flow_engine_callback, callback)


class TestPipelineMode(unittest.TestCase):
    """Tests for PipelineMode validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.temp_dir, "data")
        os.makedirs(self.data_dir)

    def tearDown(self):
        """Clean up."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_pipe_mode_enum_values(self):
        """Test PipeMode enum has expected values."""
        from src.pipeline_mode import PipeMode
        
        self.assertEqual(PipeMode.LINEAR.value, "linear")
        self.assertEqual(PipeMode.BRANCH.value, "branch")
        self.assertEqual(PipeMode.MERGE.value, "merge")
        self.assertEqual(PipeMode.CONDITIONAL.value, "conditional")
        self.assertEqual(PipeMode.PARALLEL.value, "parallel")

    def test_data_format_enum_values(self):
        """Test DataFormat enum has expected values."""
        from src.pipeline_mode import DataFormat
        
        self.assertEqual(DataFormat.JSON.value, "json")
        self.assertEqual(DataFormat.TEXT.value, "text")
        self.assertEqual(DataFormat.CSV.value, "csv")
        self.assertEqual(DataFormat.YAML.value, "yaml")

    def test_pipe_step_dataclass(self):
        """Test PipeStep dataclass creation."""
        from src.pipeline_mode import PipeStep
        
        step = PipeStep(
            step_id="step1",
            name="Test Step",
            command="echo hello"
        )
        
        self.assertEqual(step.step_id, "step1")
        self.assertEqual(step.name, "Test Step")
        self.assertEqual(step.command, "echo hello")
        self.assertTrue(step.enabled)
        self.assertEqual(step.timeout, 300)

    def test_pipe_chain_dataclass(self):
        """Test PipeChain dataclass creation."""
        from src.pipeline_mode import PipeChain, PipeMode, PipeStep
        
        chain = PipeChain(
            chain_id="chain1",
            name="Test Chain",
            mode=PipeMode.LINEAR,
            steps=[]
        )
        
        self.assertEqual(chain.chain_id, "chain1")
        self.assertEqual(chain.mode, PipeMode.LINEAR)

    def test_pipe_result_dataclass(self):
        """Test PipeResult dataclass creation."""
        from src.pipeline_mode import PipeResult
        
        result = PipeResult(
            success=True,
            step_id="step1",
            step_name="Test Step",
            output={"result": "ok"}
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.step_id, "step1")
        self.assertEqual(result.output["result"], "ok")

    def test_validate_chain_empty_steps(self):
        """Test validating chain with no steps."""
        from src.pipeline_mode import PipeRunner, PipeChain, PipeMode
        
        runner = PipeRunner(data_dir=self.temp_dir if hasattr(self, 'temp_dir') else "./data")
        
        # Create empty chain
        runner.chains["empty_chain"] = PipeChain(
            chain_id="empty_chain",
            name="Empty",
            mode=PipeMode.LINEAR,
            steps=[]
        )
        
        valid, errors = runner.validate_chain("empty_chain")
        
        self.assertFalse(valid)
        self.assertTrue(any("没有定义任何步骤" in e for e in errors))

    def test_validate_chain_missing_step_id(self):
        """Test validating chain with missing step_id."""
        from src.pipeline_mode import PipeRunner, PipeChain, PipeMode, PipeStep
        
        runner = PipeRunner(data_dir=self.temp_dir)
        
        # Create chain with step missing step_id
        runner.chains["test_chain"] = PipeChain(
            chain_id="test_chain",
            name="Test",
            mode=PipeMode.LINEAR,
            steps=[
                PipeStep(step_id="", name="No ID Step", command="echo test")
            ]
        )
        
        valid, errors = runner.validate_chain("test_chain")
        
        self.assertFalse(valid)
        self.assertTrue(any("缺少 step_id" in e for e in errors))

    def test_validate_chain_duplicate_step_id(self):
        """Test validating chain with duplicate step_id."""
        from src.pipeline_mode import PipeRunner, PipeChain, PipeMode, PipeStep
        
        runner = PipeRunner(data_dir=self.temp_dir)
        
        runner.chains["dup_chain"] = PipeChain(
            chain_id="dup_chain",
            name="Duplicate",
            mode=PipeMode.LINEAR,
            steps=[
                PipeStep(step_id="same_id", name="Step 1", command="cmd1"),
                PipeStep(step_id="same_id", name="Step 2", command="cmd2")
            ]
        )
        
        valid, errors = runner.validate_chain("dup_chain")
        
        self.assertFalse(valid)
        self.assertTrue(any("重复" in e or "duplicate" in e.lower() for e in errors))

    def test_validate_chain_parallel_requires_two_steps(self):
        """Test parallel mode requires at least 2 steps."""
        from src.pipeline_mode import PipeRunner, PipeChain, PipeMode, PipeStep
        
        runner = PipeRunner(data_dir=self.temp_dir)
        
        runner.chains["parallel_chain"] = PipeChain(
            chain_id="parallel_chain",
            name="Parallel",
            mode=PipeMode.PARALLEL,
            steps=[
                PipeStep(step_id="only_one", name="Solo", command="echo solo")
            ]
        )
        
        valid, errors = runner.validate_chain("parallel_chain")
        
        self.assertFalse(valid)
        self.assertTrue(any("至少2个步骤" in e or "at least 2" in e.lower() for e in errors))

    def test_validate_chain_nonexistent(self):
        """Test validating nonexistent chain."""
        from src.pipeline_mode import PipeRunner
        
        runner = PipeRunner(data_dir=self.temp_dir)
        
        valid, errors = runner.validate_chain("nonexistent")
        
        self.assertFalse(valid)
        self.assertTrue(any("不存在" in e for e in errors))

    def test_create_chain(self):
        """Test creating a new chain."""
        from src.pipeline_mode import PipeRunner, PipeMode
        
        runner = PipeRunner(data_dir=self.temp_dir)
        
        chain = runner.create_chain("New Chain", PipeMode.LINEAR, "Description")
        
        self.assertIsNotNone(chain)
        self.assertIn(chain.chain_id, runner.chains)

    def test_dry_run_property(self):
        """Test dry run property."""
        from src.pipeline_mode import PipeRunner
        
        runner = PipeRunner(data_dir=self.temp_dir, dry_run=True)
        
        self.assertTrue(runner.dry_run)
        
        runner.dry_run = False
        self.assertFalse(runner.dry_run)

    def test_step_by_step_property(self):
        """Test step by step property."""
        from src.pipeline_mode import PipeRunner
        
        runner = PipeRunner(data_dir=self.temp_dir, step_by_step=False)
        
        self.assertFalse(runner.step_by_step)
        
        runner.step_by_step = True
        self.assertTrue(runner.step_by_step)

    def test_pipeline_validation_error(self):
        """Test PipelineValidationError."""
        from src.pipeline_mode import PipelineValidationError
        
        error = PipelineValidationError("Test error", ["detail1", "detail2"])
        
        self.assertEqual(error.message, "Test error")
        self.assertEqual(error.errors, ["detail1", "detail2"])

    def test_pipeline_execute_error(self):
        """Test PipelineExecuteError."""
        from src.pipeline_mode import PipelineExecuteError
        
        error = PipelineExecuteError("Execution failed", step_id="step1", context={"key": "value"})
        
        self.assertEqual(error.message, "Execution failed")
        self.assertEqual(error.step_id, "step1")
        self.assertEqual(error.context["key"], "value")


if __name__ == '__main__':
    unittest.main()
