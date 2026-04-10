"""
Tests for Error Handler Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
import traceback
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.error_handler import (
    ErrorCode,
    ErrorContext,
    ErrorRecord,
    RecoveryAttempt,
    AggregatedError,
    ErrorPattern,
    ErrorStats,
    ErrorCategory,
    ErrorSeverity,
    ErrorStatus,
    ErrorCodeCatalog,
    UserMessageGenerator,
    AutoRecoveryEngine,
    ErrorPatternDetector,
    ErrorDashboardGenerator,
    WorkflowErrorHandler
)


class TestErrorCodeCatalog(unittest.TestCase):
    """Test ErrorCodeCatalog class"""

    def test_init_creates_standard_codes(self):
        """Test initialization creates standard error codes"""
        catalog = ErrorCodeCatalog()
        self.assertGreater(len(catalog._codes), 0)

    def test_get_code(self):
        """Test getting error code by ID"""
        catalog = ErrorCodeCatalog()
        
        code = catalog.get_code("NET001")
        self.assertIsNotNone(code)
        self.assertEqual(code.code, "NET001")
        self.assertEqual(code.category, ErrorCategory.NETWORK)

    def test_get_code_not_found(self):
        """Test getting non-existent error code"""
        catalog = ErrorCodeCatalog()
        
        code = catalog.get_code("NONEXISTENT")
        self.assertIsNone(code)

    def test_generate_code(self):
        """Test generating new error code"""
        catalog = ErrorCodeCatalog()
        
        new_code = catalog.generate_code(ErrorCategory.NETWORK, "test_error")
        
        self.assertTrue(new_code.startswith("NET"))
        self.assertTrue(new_code[3:].isdigit())

    def test_find_matching_code(self):
        """Test finding matching error code"""
        catalog = ErrorCodeCatalog()
        
        matched, confidence = catalog.find_matching_code(
            "Connection refused",
            "ConnectionError"
        )
        
        self.assertIsNotNone(matched)
        self.assertGreater(confidence, 0.0)

    def test_find_matching_code_low_confidence(self):
        """Test finding match with low confidence"""
        catalog = ErrorCodeCatalog()
        
        matched, confidence = catalog.find_matching_code(
            "Some random error that doesn't match anything",
            "UnknownError"
        )
        
        # May or may not find a match depending on implementation
        self.assertIsInstance(confidence, float)

    def test_get_all_codes(self):
        """Test getting all error codes"""
        catalog = ErrorCodeCatalog()
        
        codes = catalog.get_all_codes()
        
        self.assertGreater(len(codes), 0)
        self.assertIsInstance(codes[0], ErrorCode)

    def test_get_codes_by_category(self):
        """Test getting error codes by category"""
        catalog = ErrorCodeCatalog()
        
        network_codes = catalog.get_codes_by_category(ErrorCategory.NETWORK)
        
        self.assertGreater(len(network_codes), 0)
        for code in network_codes:
            self.assertEqual(code.category, ErrorCategory.NETWORK)

    def test_get_codes_by_severity(self):
        """Test getting error codes by severity"""
        catalog = ErrorCodeCatalog()
        
        critical_codes = catalog.get_codes_by_severity(ErrorSeverity.CRITICAL)
        
        for code in critical_codes:
            self.assertEqual(code.severity, ErrorSeverity.CRITICAL)


class TestUserMessageGenerator(unittest.TestCase):
    """Test UserMessageGenerator class"""

    def test_generate_with_error_code(self):
        """Test generating message with error code"""
        generator = UserMessageGenerator()
        code = ErrorCode(
            code="NET001",
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.HIGH,
            title="Connection Refused",
            description="Connection refused error",
            technical_details="TCP connection refused",
            user_message="Could not connect to the server.",
            recovery_suggestions=["Check server", "Try again"],
            auto_recoverable=True,
            known_causes=["Server down"],
            related_codes=[]
        )
        
        message = generator.generate("Connection refused", code)
        
        self.assertEqual(message, "Could not connect to the server.")

    def test_generate_without_error_code(self):
        """Test generating message without error code"""
        generator = UserMessageGenerator()
        
        message = generator.generate("Connection refused")
        
        self.assertIn("refused", message.lower())

    def test_generate_with_context(self):
        """Test generating message with context"""
        generator = UserMessageGenerator()
        
        message = generator.generate(
            "Error occurred",
            context={"workflow_name": "TestWF", "step_name": "Step1"}
        )
        
        self.assertIsInstance(message, str)

    def test_generate_recovery_hint(self):
        """Test generating recovery hint"""
        generator = UserMessageGenerator()
        code = ErrorCode(
            code="NET001",
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.HIGH,
            title="Connection Refused",
            description="Connection refused error",
            technical_details="TCP connection refused",
            user_message="Could not connect to the server.",
            recovery_suggestions=["Check server", "Try again"],
            auto_recoverable=True,
            known_causes=["Server down"],
            related_codes=[]
        )
        
        hint = generator.generate_recovery_hint(code)
        
        self.assertEqual(hint, "Check server")


class TestAutoRecoveryEngine(unittest.TestCase):
    """Test AutoRecoveryEngine class"""

    def test_init_default_retries(self):
        """Test initialization with default retries"""
        engine = AutoRecoveryEngine()
        self.assertEqual(engine.max_retries, 3)

    def test_init_custom_retries(self):
        """Test initialization with custom retries"""
        engine = AutoRecoveryEngine(max_retries=5)
        self.assertEqual(engine.max_retries, 5)

    def test_register_handler(self):
        """Test registering recovery handler"""
        engine = AutoRecoveryEngine()
        
        def custom_handler(error, context):
            return {"success": True}
        
        engine.register_handler("TEST001", custom_handler)
        self.assertIn("TEST001", engine._recovery_handlers)

    def test_can_recover(self):
        """Test checking if error is recoverable"""
        engine = AutoRecoveryEngine()
        
        # Standard handlers are registered
        self.assertTrue(engine.can_recover("NET001"))
        self.assertFalse(engine.can_recover("RANDOM_999"))

    def test_attempt_recovery_no_handler(self):
        """Test recovery attempt with no handler"""
        engine = AutoRecoveryEngine()
        record = ErrorRecord(
            error_id="err_001",
            error_code="UNKNOWN",
            category=ErrorCategory.UNKNOWN,
            severity=ErrorSeverity.MEDIUM,
            message="Unknown error",
            user_message="An error occurred",
            context=ErrorContext(
                timestamp=time.time(),
                workflow_name="Test",
                workflow_id="wf_001",
                step_name="Step1",
                step_index=0,
                action_type="test",
                action_params={},
                environment={},
                system_state={},
                user_data={},
                stack_trace="",
                raw_exception=None,
                previous_errors=[]
            ),
            timestamp=time.time(),
            status=ErrorStatus.NEW,
            recovery_attempts=[],
            resolved=False,
            resolved_at=None,
            resolved_by=None
        )
        
        attempt = engine.attempt_recovery(record, {})
        
        self.assertFalse(attempt.success)
        self.assertEqual(attempt.strategy, "none")

    def test_attempt_recovery_with_handler(self):
        """Test recovery attempt with handler"""
        engine = AutoRecoveryEngine()
        
        def custom_handler(error, context):
            return {"success": True, "strategy": "custom", "action": "Fixed"}
        
        engine.register_handler("TEST001", custom_handler)
        
        record = ErrorRecord(
            error_id="err_001",
            error_code="TEST001",
            category=ErrorCategory.UNKNOWN,
            severity=ErrorSeverity.MEDIUM,
            message="Test error",
            user_message="Test error occurred",
            context=ErrorContext(
                timestamp=time.time(),
                workflow_name="Test",
                workflow_id="wf_001",
                step_name="Step1",
                step_index=0,
                action_type="test",
                action_params={},
                environment={},
                system_state={},
                user_data={},
                stack_trace="",
                raw_exception=None,
                previous_errors=[]
            ),
            timestamp=time.time(),
            status=ErrorStatus.NEW,
            recovery_attempts=[],
            resolved=False,
            resolved_at=None,
            resolved_by=None
        )
        
        attempt = engine.attempt_recovery(record, {})
        
        self.assertTrue(attempt.success)


class TestErrorPatternDetector(unittest.TestCase):
    """Test ErrorPatternDetector class"""

    def test_register_pattern(self):
        """Test registering a pattern"""
        detector = ErrorPatternDetector()
        
        pattern_id = detector.register_pattern("connection_error", threshold=5)
        
        self.assertIsInstance(pattern_id, str)
        self.assertEqual(len(detector._patterns), 1)

    def test_record_occurrence_new_pattern(self):
        """Test recording occurrence for new pattern"""
        detector = ErrorPatternDetector()
        
        should_alert, pattern = detector.record_occurrence("new_error")
        
        self.assertFalse(should_alert)
        self.assertIsNotNone(pattern)
        self.assertEqual(pattern.occurrences, 1)

    def test_record_occurrence_existing_pattern(self):
        """Test recording occurrence for existing pattern"""
        detector = ErrorPatternDetector()
        detector.register_pattern("test_error", threshold=3)
        
        for _ in range(3):
            should_alert, pattern = detector.record_occurrence("test_error")
        
        self.assertTrue(should_alert)

    def test_get_active_alerts(self):
        """Test getting active alerts"""
        detector = ErrorPatternDetector()
        detector.register_pattern("alert_error", threshold=1)
        detector.record_occurrence("alert_error")
        
        alerts = detector.get_active_alerts()
        
        self.assertEqual(len(alerts), 1)

    def test_reset_alert(self):
        """Test resetting alert"""
        detector = ErrorPatternDetector()
        pattern_id = detector.register_pattern("reset_error", threshold=1)
        detector.record_occurrence("reset_error")
        
        result = detector.reset_alert(pattern_id)
        
        self.assertTrue(result)
        self.assertFalse(detector._patterns[pattern_id].alerted)

    def test_reset_alert_not_found(self):
        """Test resetting non-existent alert"""
        detector = ErrorPatternDetector()
        
        result = detector.reset_alert("nonexistent")
        
        self.assertFalse(result)


class TestWorkflowErrorHandler(unittest.TestCase):
    """Test WorkflowErrorHandler main class"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.handler = WorkflowErrorHandler(data_dir=self.temp_dir)

    def tearDown(self):
        """Clean up temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_init_creates_components(self):
        """Test initialization creates required components"""
        self.assertIsNotNone(self.handler.catalog)
        self.assertIsNotNone(self.handler.user_messages)
        self.assertIsNotNone(self.handler.auto_recovery)
        self.assertIsNotNone(self.handler.pattern_detector)

    def test_record_error(self):
        """Test recording an error"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            record = self.handler.record_error(
                exception=e,
                context={},
                workflow_name="TestWorkflow",
                step_name="Connect",
                step_index=0,
                action_type="network"
            )
        
        self.assertIsNotNone(record)
        self.assertEqual(record.error_code[:3], "NET")
        self.assertEqual(record.category, ErrorCategory.NETWORK)

    def test_record_error_aggregation(self):
        """Test that similar errors are aggregated"""
        for _ in range(3):
            try:
                raise ConnectionError("Connection refused")
            except ConnectionError as e:
                self.handler.record_error(
                    exception=e,
                    context={},
                    workflow_name="TestWorkflow"
                )
        
        aggregated = self.handler.get_aggregated_errors(min_count=2)
        self.assertGreater(len(aggregated), 0)

    def test_classify_error_network(self):
        """Test error classification as network"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            code, category, severity = self.handler._classify_error(e)
        
        self.assertEqual(category, ErrorCategory.NETWORK)

    def test_classify_error_file_system(self):
        """Test error classification as file system"""
        try:
            raise FileNotFoundError("File not found")
        except FileNotFoundError as e:
            code, category, severity = self.handler._classify_error(e)
        
        self.assertEqual(category, ErrorCategory.FILE_SYSTEM)

    def test_classify_error_permission(self):
        """Test error classification as permission"""
        try:
            raise PermissionError("Access denied")
        except PermissionError as e:
            code, category, severity = self.handler._classify_error(e)
        
        self.assertEqual(category, ErrorCategory.PERMISSION)

    def test_classify_error_timeout(self):
        """Test error classification as timeout"""
        try:
            raise TimeoutError("Operation timed out")
        except TimeoutError as e:
            code, category, severity = self.handler._classify_error(e)
        
        self.assertEqual(category, ErrorCategory.TIMEOUT)

    def test_classify_error_validation(self):
        """Test error classification as validation"""
        try:
            raise ValueError("Invalid value")
        except ValueError as e:
            code, category, severity = self.handler._classify_error(e)
        
        self.assertEqual(category, ErrorCategory.VALIDATION)

    def test_get_recovery_suggestions(self):
        """Test getting recovery suggestions"""
        suggestions = self.handler.get_recovery_suggestions("NET001")
        
        self.assertIsInstance(suggestions, list)
        self.assertGreater(len(suggestions), 0)

    def test_get_recovery_suggestions_unknown_code(self):
        """Test getting suggestions for unknown error code"""
        suggestions = self.handler.get_recovery_suggestions("UNKNOWN999")
        
        self.assertIsInstance(suggestions, list)
        # Should return default suggestions
        self.assertGreater(len(suggestions), 0)

    def test_attempt_manual_recovery(self):
        """Test manual recovery attempt"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            record = self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        attempt = self.handler.attempt_manual_recovery(
            record.error_id,
            "retry"
        )
        
        self.assertIsNotNone(attempt)
        self.assertEqual(attempt.strategy, "retry")

    def test_attempt_manual_recovery_not_found(self):
        """Test manual recovery for non-existent error"""
        attempt = self.handler.attempt_manual_recovery("nonexistent_id", "retry")
        
        self.assertFalse(attempt.success)

    def test_resolve_error(self):
        """Test resolving an error"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            record = self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        result = self.handler.resolve_error(record.error_id, "test_user")
        
        self.assertTrue(result)
        updated_record = self.handler.get_error_by_id(record.error_id)
        self.assertTrue(updated_record.resolved)

    def test_resolve_error_not_found(self):
        """Test resolving non-existent error"""
        result = self.handler.resolve_error("nonexistent_id")
        self.assertFalse(result)

    def test_acknowledge_error(self):
        """Test acknowledging an error"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            record = self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        result = self.handler.acknowledge_error(record.error_id)
        
        self.assertTrue(result)
        updated_record = self.handler.get_error_by_id(record.error_id)
        self.assertEqual(updated_record.status, ErrorStatus.ACKNOWLEDGED)

    def test_escalate_error(self):
        """Test escalating an error"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            record = self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        result = self.handler.escalate_error(record.error_id)
        
        self.assertTrue(result)

    def test_get_error_stats(self):
        """Test getting error statistics"""
        for _ in range(5):
            try:
                raise ConnectionError("Connection refused")
            except ConnectionError as e:
                self.handler.record_error(
                    exception=e,
                    context={},
                    workflow_name="TestWorkflow"
                )
        
        stats = self.handler.get_error_stats()
        
        self.assertIsInstance(stats, ErrorStats)
        self.assertGreater(stats.total_errors, 0)

    def test_get_aggregated_errors(self):
        """Test getting aggregated errors"""
        for _ in range(3):
            try:
                raise ConnectionError("Connection refused")
            except ConnectionError as e:
                self.handler.record_error(
                    exception=e,
                    context={},
                    workflow_name="Test"
                )
        
        aggregated = self.handler.get_aggregated_errors(min_count=2)
        
        self.assertIsInstance(aggregated, list)

    def test_get_error_history(self):
        """Test getting error history"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            self.handler.record_error(
                exception=e,
                context={},
                workflow_name="TestWorkflow"
            )
        
        history = self.handler.get_error_history()
        
        self.assertIsInstance(history, list)
        self.assertGreater(len(history), 0)

    def test_get_error_history_filtered_by_workflow(self):
        """Test getting error history filtered by workflow"""
        for i in range(3):
            try:
                raise ConnectionError("Connection refused")
            except ConnectionError as e:
                self.handler.record_error(
                    exception=e,
                    context={},
                    workflow_name=f"Workflow{i}"
                )
        
        history = self.handler.get_error_history(workflow_name="Workflow0")
        
        self.assertIsInstance(history, list)

    def test_get_active_alerts(self):
        """Test getting active alerts"""
        # Register a pattern and trigger it
        self.handler.pattern_detector.register_pattern("test_alert", threshold=1)
        self.handler.pattern_detector.record_occurrence("test_alert")
        
        alerts = self.handler.get_active_alerts()
        
        self.assertIsInstance(alerts, list)

    def test_get_error_by_id(self):
        """Test getting error by ID"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            record = self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        found = self.handler.get_error_by_id(record.error_id)
        
        self.assertIsNotNone(found)
        self.assertEqual(found.error_id, record.error_id)

    def test_get_error_by_id_not_found(self):
        """Test getting non-existent error by ID"""
        found = self.handler.get_error_by_id("nonexistent_id")
        self.assertIsNone(found)

    def test_get_error_code_info(self):
        """Test getting error code info"""
        info = self.handler.get_error_code_info("NET001")
        
        self.assertIsNotNone(info)
        self.assertEqual(info.code, "NET001")

    def test_generate_dashboard(self):
        """Test generating HTML dashboard"""
        # Create some errors first
        for _ in range(3):
            try:
                raise ConnectionError("Connection refused")
            except ConnectionError as e:
                self.handler.record_error(
                    exception=e,
                    context={},
                    workflow_name="Test"
                )
        
        html = self.handler.generate_dashboard()
        
        self.assertIsInstance(html, str)
        self.assertIn("<html", html.lower())

    def test_get_dashboard_path(self):
        """Test getting dashboard path"""
        path = self.handler.get_dashboard_path()
        
        self.assertIsInstance(path, str)
        self.assertTrue(path.endswith(".html"))

    def test_set_notification_callback(self):
        """Test setting notification callback"""
        callback_called = []
        
        def test_callback(data):
            callback_called.append(data)
        
        self.handler.set_notification_callback(test_callback)
        self.assertEqual(self.handler.notification_callback, test_callback)

    def test_clear_history(self):
        """Test clearing error history"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        self.handler.clear_history()
        
        self.assertEqual(len(self.handler._errors), 0)

    def test_clear_history_before_timestamp(self):
        """Test clearing history before timestamp"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        # Clear errors older than now (should keep them all)
        self.handler.clear_history(before_timestamp=time.time() + 1000)
        
        # History should be empty since we only have one recent error
        # and we're clearing before it happened

    def test_export_errors(self):
        """Test exporting errors"""
        for _ in range(2):
            try:
                raise ConnectionError("Connection refused")
            except ConnectionError as e:
                self.handler.record_error(
                    exception=e,
                    context={},
                    workflow_name="Test"
                )
        
        exported = self.handler.export_errors(format="json")
        
        self.assertIsInstance(exported, str)
        data = json.loads(exported)
        self.assertIsInstance(data, list)

    def test_get_error_code_catalog(self):
        """Test getting error code catalog"""
        catalog = self.handler.get_error_code_catalog()
        
        self.assertIsInstance(catalog, list)
        self.assertGreater(len(catalog), 0)

    def test_search_errors(self):
        """Test searching errors"""
        try:
            raise ConnectionError("Connection refused")
        except ConnectionError as e:
            self.handler.record_error(
                exception=e,
                context={},
                workflow_name="Test"
            )
        
        results = self.handler.search_errors("connection")
        
        self.assertIsInstance(results, list)

    def test_search_errors_no_results(self):
        """Test searching errors with no results"""
        results = self.handler.search_errors("xyznonexistent123")
        
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 0)


class TestErrorDataclasses(unittest.TestCase):
    """Test error handler dataclasses"""

    def test_error_context_creation(self):
        """Test creating ErrorContext"""
        ctx = ErrorContext(
            timestamp=time.time(),
            workflow_name="Test",
            workflow_id="wf_001",
            step_name="Step1",
            step_index=0,
            action_type="click",
            action_params={"target": "button"},
            environment={"os": "macos"},
            system_state={"running": True},
            user_data={"user": "test"},
            stack_trace="",
            raw_exception=None,
            previous_errors=[]
        )
        
        self.assertEqual(ctx.workflow_name, "Test")
        self.assertEqual(ctx.step_index, 0)

    def test_error_record_creation(self):
        """Test creating ErrorRecord"""
        ctx = ErrorContext(
            timestamp=time.time(),
            workflow_name="Test",
            workflow_id="wf_001",
            step_name="Step1",
            step_index=0,
            action_type="click",
            action_params={},
            environment={},
            system_state={},
            user_data={},
            stack_trace="",
            raw_exception=None,
            previous_errors=[]
        )
        
        record = ErrorRecord(
            error_id="err_001",
            error_code="TEST001",
            category=ErrorCategory.UNKNOWN,
            severity=ErrorSeverity.MEDIUM,
            message="Test error",
            user_message="Test error occurred",
            context=ctx,
            timestamp=time.time(),
            status=ErrorStatus.NEW,
            recovery_attempts=[],
            resolved=False,
            resolved_at=None,
            resolved_by=None
        )
        
        self.assertEqual(record.error_id, "err_001")
        self.assertFalse(record.resolved)

    def test_recovery_attempt_creation(self):
        """Test creating RecoveryAttempt"""
        attempt = RecoveryAttempt(
            timestamp=time.time(),
            strategy="retry",
            action_taken="Retrying operation",
            success=True,
            duration=1.5,
            details="Succeeded on second attempt"
        )
        
        self.assertEqual(attempt.strategy, "retry")
        self.assertTrue(attempt.success)

    def test_aggregated_error_creation(self):
        """Test creating AggregatedError"""
        agg = AggregatedError(
            error_signature="sig123",
            error_code="TEST001",
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.HIGH,
            message="Network error",
            user_message="Network issue occurred",
            count=5,
            first_occurrence=time.time() - 3600,
            last_occurrence=time.time(),
            recent_timestamps=[time.time()],
            success_rate=0.8,
            workflow_names={"WF1", "WF2"},
            step_names={"Step1"}
        )
        
        self.assertEqual(agg.count, 5)
        self.assertEqual(len(agg.workflow_names), 2)

    def test_error_pattern_creation(self):
        """Test creating ErrorPattern"""
        pattern = ErrorPattern(
            pattern_id="pat_001",
            error_signature="sig123",
            threshold=5,
            time_window=300.0,
            occurrences=3,
            first_detected=time.time() - 100,
            last_detected=time.time(),
            alerted=False,
            alert_count=0
        )
        
        self.assertEqual(pattern.threshold, 5)
        self.assertFalse(pattern.alerted)

    def test_error_stats_creation(self):
        """Test creating ErrorStats"""
        stats = ErrorStats(
            total_errors=100,
            errors_by_category={"network": 30, "file": 20},
            errors_by_severity={"high": 10, "medium": 50},
            errors_by_workflow={"WF1": 40},
            top_errors=[("NET001", 15)],
            error_rate=4.2,
            resolution_rate=75.0,
            avg_resolution_time=120.5,
            critical_errors_active=2
        )
        
        self.assertEqual(stats.total_errors, 100)
        self.assertEqual(stats.error_rate, 4.2)


if __name__ == '__main__':
    unittest.main()
