"""
Tests for workflow_testing module - Comprehensive testing framework
for workflow unit tests, integration tests, performance tests,
load tests, screenshot comparison, CI/CD integration, and coverage tracking.
"""

import sys
import os
import json
import time
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime
from io import BytesIO

sys.path.insert(0, '/Users/guige/my_project')

# Import the workflow_testing module
import src.workflow_testing as wt_module
from src.workflow_testing import (
    TestType,
    TestStatus,
    TestResult,
    TestFixture,
    WorkflowPath,
    MockHTTPService,
    MockResponse,
    MockFileSystem,
    ScreenshotComparator,
    ResourceMonitor,
    LoadTestRunner,
    CoverageTracker,
    JUnitReporter,
    CoberturaReporter,
    RegressionBaseline,
    WorkflowTestingFramework,
    quick_unit_test,
    quick_performance_test
)


class TestTestTypeEnum(unittest.TestCase):
    """Test TestType enum values."""

    def test_test_type_values(self):
        """Test all TestType enum values exist."""
        self.assertEqual(TestType.UNIT.value, "unit")
        self.assertEqual(TestType.INTEGRATION.value, "integration")
        self.assertEqual(TestType.PERFORMANCE.value, "performance")
        self.assertEqual(TestType.LOAD.value, "load")
        self.assertEqual(TestType.REGRESSION.value, "regression")
        self.assertEqual(TestType.SCREENSHOT.value, "screenshot")


class TestTestStatusEnum(unittest.TestCase):
    """Test TestStatus enum values."""

    def test_test_status_values(self):
        """Test all TestStatus enum values exist."""
        self.assertEqual(TestStatus.PASSED.value, "passed")
        self.assertEqual(TestStatus.FAILED.value, "failed")
        self.assertEqual(TestStatus.SKIPPED.value, "skipped")
        self.assertEqual(TestStatus.ERROR.value, "error")


class TestTestResult(unittest.TestCase):
    """Test TestResult dataclass."""

    def test_test_result_creation(self):
        """Test creating a TestResult."""
        result = TestResult(
            test_name="test_example",
            test_type=TestType.UNIT,
            status=TestStatus.PASSED,
            duration_ms=100.5
        )
        self.assertEqual(result.test_name, "test_example")
        self.assertEqual(result.test_type, TestType.UNIT)
        self.assertEqual(result.status, TestStatus.PASSED)
        self.assertEqual(result.duration_ms, 100.5)
        self.assertEqual(result.message, "")
        self.assertEqual(result.error_trace, "")

    def test_test_result_with_metadata(self):
        """Test TestResult with metadata."""
        metadata = {"key": "value", "count": 42}
        result = TestResult(
            test_name="test_with_meta",
            test_type=TestType.PERFORMANCE,
            status=TestStatus.FAILED,
            duration_ms=200.0,
            message="Performance degraded",
            metadata=metadata
        )
        self.assertEqual(result.metadata["key"], "value")
        self.assertEqual(result.metadata["count"], 42)


class TestMockHTTPService(unittest.TestCase):
    """Test MockHTTPService for HTTP mocking."""

    def setUp(self):
        self.mock_http = MockHTTPService()

    def test_mock_get(self):
        """Test mocking GET request."""
        self.mock_http.mock_get("/api/test", {"data": "value"}, status_code=200)
        response = self.mock_http.get_response("/api/test")
        self.assertIsNotNone(response)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {"data": "value"})

    def test_mock_post(self):
        """Test mocking POST request."""
        self.mock_http.mock_post("/api/create", {"id": 1}, status_code=201)
        response = self.mock_http.get_response("/api/create")
        self.assertEqual(response.status_code, 201)

    def test_mock_put(self):
        """Test mocking PUT request."""
        self.mock_http.mock_put("/api/update/1", {"updated": True}, status_code=200)
        response = self.mock_http.get_response("/api/update/1")
        self.assertEqual(response.status_code, 200)

    def test_mock_delete(self):
        """Test mocking DELETE request."""
        self.mock_http.mock_delete("/api/delete/1", status_code=204)
        response = self.mock_http.get_response("/api/delete/1")
        self.assertEqual(response.status_code, 204)

    def test_record_call(self):
        """Test recording call history."""
        self.mock_http.record_call("GET", "/api/test", {"param": "value"})
        self.assertEqual(len(self.mock_http.call_history), 1)
        self.assertEqual(self.mock_http.call_history[0]["method"], "GET")
        self.assertEqual(self.mock_http.call_history[0]["url"], "/api/test")

    def test_reset(self):
        """Test resetting mock service."""
        self.mock_http.mock_get("/api/test", {"data": "value"})
        self.mock_http.record_call("GET", "/api/test")
        self.mock_http.reset()
        self.assertEqual(len(self.mock_http.responses), 0)
        self.assertEqual(len(self.mock_http.call_history), 0)


class TestMockResponse(unittest.TestCase):
    """Test MockResponse class."""

    def test_response_json(self):
        """Test JSON response."""
        data = {"key": "value", "number": 42}
        response = MockResponse(200, data)
        self.assertEqual(response.json(), data)

    def test_response_text(self):
        """Test text response."""
        data = {"key": "value"}
        response = MockResponse(200, data)
        self.assertEqual(response.text(), json.dumps(data))

    def test_response_headers(self):
        """Test response headers."""
        response = MockResponse(200, {"data": "value"})
        self.assertEqual(response.headers['Content-Type'], 'application/json')


class TestMockFileSystem(unittest.TestCase):
    """Test MockFileSystem for file operation mocking."""

    def setUp(self):
        self.mock_fs = MockFileSystem()

    def test_mock_file_creation(self):
        """Test creating a mock file."""
        self.mock_fs.mock_file("/tmp/test.txt", "content")
        self.assertIn("/tmp/test.txt", self.mock_fs.files)

    def test_mock_read(self):
        """Test reading from mock file."""
        self.mock_fs.mock_file("/tmp/test.txt", "test content")
        content = self.mock_fs.mock_read("/tmp/test.txt")
        self.assertEqual(content, "test content")

    def test_mock_read_nonexistent(self):
        """Test reading nonexistent file returns empty string."""
        content = self.mock_fs.mock_read("/nonexistent.txt")
        self.assertEqual(content, "")

    def test_mock_exists(self):
        """Test file existence check."""
        self.mock_fs.mock_file("/tmp/test.txt", "content")
        self.assertTrue(self.mock_fs.mock_exists("/tmp/test.txt"))
        self.assertFalse(self.mock_fs.mock_exists("/nonexistent.txt"))

    def test_mock_mkdir(self):
        """Test directory creation."""
        self.mock_fs.mock_mkdir("/new_dir")
        self.assertIn("/new_dir", self.mock_fs.dirs)

    def test_mock_remove(self):
        """Test file removal."""
        self.mock_fs.mock_file("/tmp/test.txt", "content")
        self.mock_fs.mock_remove("/tmp/test.txt")
        self.assertNotIn("/tmp/test.txt", self.mock_fs.files)

    def test_call_history(self):
        """Test operation call history tracking."""
        self.mock_fs.mock_file("/tmp/test.txt", "content")
        self.mock_fs.mock_read("/tmp/test.txt")
        self.assertEqual(len(self.mock_fs.call_history), 2)
        self.assertEqual(self.mock_fs.call_history[0]["operation"], "create_file")
        self.assertEqual(self.mock_fs.call_history[1]["operation"], "read_file")

    def test_reset(self):
        """Test resetting mock filesystem."""
        self.mock_fs.mock_file("/tmp/test.txt", "content")
        self.mock_fs.mock_mkdir("/new_dir")
        self.mock_fs.reset()
        self.assertEqual(len(self.mock_fs.files), 0)
        self.assertEqual(len(self.mock_fs.dirs), 0)


class TestScreenshotComparator(unittest.TestCase):
    """Test ScreenshotComparator for image comparison."""

    def setUp(self):
        self.comparator = ScreenshotComparator(threshold=0.95)

    def test_identical_images(self):
        """Test comparing identical images returns 1.0 similarity."""
        img_data = b'fake_image_data'
        passed, similarity, result = self.comparator.compare(img_data, img_data)
        self.assertTrue(passed)
        self.assertEqual(similarity, 1.0)

    def test_different_images(self):
        """Test comparing different images returns lower similarity."""
        img1 = b'fake_image_data_1'
        img2 = b'fake_image_data_2'
        passed, similarity, result = self.comparator.compare(img1, img2)
        self.assertLess(similarity, 1.0)

    def test_threshold_check(self):
        """Test threshold comparison logic."""
        img1 = b'short'
        img2 = b'much_longer_image_data'
        passed, similarity, result = self.comparator.compare(img1, img2)
        expected_pass = similarity >= self.comparator.threshold
        self.assertEqual(passed, expected_pass)

    def test_comparison_history(self):
        """Test that comparison results are recorded."""
        self.comparator.compare(b'img1', b'img2')
        self.assertEqual(len(self.comparator.comparison_history), 1)


class TestResourceMonitor(unittest.TestCase):
    """Test ResourceMonitor for tracking CPU/memory usage."""

    @patch('psutil.Process')
    def test_resource_monitor_initialization(self, mock_process):
        """Test resource monitor initializes correctly."""
        mock_process.return_value = MagicMock()
        monitor = ResourceMonitor()
        self.assertFalse(monitor.monitoring)
        self.assertEqual(len(monitor.samples), 0)

    @patch('psutil.Process')
    def test_resource_monitor_start_stop(self, mock_process):
        """Test starting and stopping resource monitor."""
        mock_process.return_value = MagicMock()
        mock_process.return_value.cpu_percent.return_value = 10.0
        mock_process.return_value.memory_info.return_value = MagicMock(rss=1024*1024)
        mock_process.return_value.num_threads.return_value = 2
        
        monitor = ResourceMonitor()
        monitor.start()
        self.assertTrue(monitor.monitoring)
        
        summary = monitor.stop()
        self.assertFalse(monitor.monitoring)
        self.assertIn('avg_cpu', summary)
        self.assertIn('avg_memory', summary)

    @patch('psutil.Process')
    def test_get_summary_empty(self, mock_process):
        """Test getting summary with no samples."""
        mock_process.return_value = MagicMock()
        monitor = ResourceMonitor()
        summary = monitor.get_summary()
        self.assertEqual(summary['avg_cpu'], 0)
        self.assertEqual(summary['avg_memory'], 0)


class TestLoadTestRunner(unittest.TestCase):
    """Test LoadTestRunner for concurrent load testing."""

    def test_load_test_runner_init(self):
        """Test LoadTestRunner initialization."""
        runner = LoadTestRunner(num_workers=5)
        self.assertEqual(runner.num_workers, 5)
        self.assertEqual(len(runner.results), 0)
        self.assertEqual(len(runner.errors), 0)

    def test_simple_concurrent_test(self):
        """Test running a simple concurrent test."""
        def simple_test(i):
            return TestResult(
                test_name=f"test_{i}",
                test_type=TestType.LOAD,
                status=TestStatus.PASSED,
                duration_ms=10.0
            )

        runner = LoadTestRunner(num_workers=2)
        # Use small iterations for fast test
        result = runner.run_concurrent(simple_test, iterations=5)
        
        self.assertEqual(result['total_iterations'], 5)
        self.assertIn('successful', result)
        self.assertIn('failed', result)
        self.assertIn('total_duration', result)


class TestCoverageTracker(unittest.TestCase):
    """Test CoverageTracker for test coverage tracking."""

    def setUp(self):
        self.tracker = CoverageTracker()

    def test_register_path(self):
        """Test registering a workflow path."""
        self.tracker.register_path("path1", "Test Workflow", ["action1", "action2"])
        self.assertIn("path1", self.tracker.workflow_paths)
        path = self.tracker.workflow_paths["path1"]
        self.assertEqual(path.path_name, "Test Workflow")
        self.assertEqual(path.actions, ["action1", "action2"])
        self.assertFalse(path.covered)

    def test_mark_path_tested(self):
        """Test marking a path as tested."""
        self.tracker.register_path("path1", "Test Workflow", ["action1"])
        self.tracker.mark_path_tested("path1")
        self.assertTrue(self.tracker.workflow_paths["path1"].covered)
        self.assertIsNotNone(self.tracker.workflow_paths["path1"].last_tested)

    def test_mark_action_tested(self):
        """Test marking an action as tested."""
        self.tracker.mark_action_tested("action1")
        self.assertEqual(self.tracker.action_coverage["action1"], 1)
        self.tracker.mark_action_tested("action1")
        self.assertEqual(self.tracker.action_coverage["action1"], 2)

    def test_mark_branch(self):
        """Test marking branch coverage."""
        self.tracker.mark_branch("branch1", True)
        self.assertTrue(self.tracker.branch_coverage["branch1"])
        self.tracker.mark_branch("branch1", False)
        self.assertTrue(self.tracker.branch_coverage["branch1"])  # Should still be True

    def test_get_coverage_report(self):
        """Test generating coverage report."""
        self.tracker.register_path("path1", "Workflow1", ["a1", "a2"])
        self.tracker.register_path("path2", "Workflow2", ["a3"])
        self.tracker.mark_path_tested("path1")
        self.tracker.mark_action_tested("a1")
        self.tracker.mark_action_tested("a2")
        self.tracker.mark_branch("b1", True)
        
        report = self.tracker.get_coverage_report()
        self.assertEqual(report['total_paths'], 2)
        self.assertEqual(report['covered_paths'], 1)
        self.assertEqual(report['path_coverage'], 0.5)


class TestJUnitReporter(unittest.TestCase):
    """Test JUnitReporter for JUnit XML report generation."""

    def setUp(self):
        self.reporter = JUnitReporter()

    def test_add_test_suite(self):
        """Test adding a test suite."""
        results = [
            TestResult("test1", TestType.UNIT, TestStatus.PASSED, 100.0),
            TestResult("test2", TestType.UNIT, TestStatus.FAILED, 50.0),
            TestResult("test3", TestType.INTEGRATION, TestStatus.ERROR, 200.0)
        ]
        self.reporter.add_test_suite("TestSuite", results, 350.0)
        
        self.assertEqual(len(self.reporter.test_suites), 1)
        suite = self.reporter.test_suites[0]
        self.assertEqual(suite['name'], "TestSuite")
        self.assertEqual(suite['tests'], 3)
        self.assertEqual(suite['failures'], 1)
        self.assertEqual(suite['errors'], 1)

    def test_generate_xml(self):
        """Test generating JUnit XML output."""
        results = [
            TestResult("test_pass", TestType.UNIT, TestStatus.PASSED, 100.0),
            TestResult("test_fail", TestType.UNIT, TestStatus.FAILED, 50.0, message="Failed")
        ]
        self.reporter.add_test_suite("TestSuite", results, 150.0)
        
        xml = self.reporter.generate_xml()
        self.assertIn("<testsuite", xml)
        self.assertIn('tests="2"', xml)
        self.assertIn('failures="1"', xml)


class TestCoberturaReporter(unittest.TestCase):
    """Test CoberturaReporter for coverage report generation."""

    def setUp(self):
        self.reporter = CoberturaReporter()

    def test_add_package_coverage(self):
        """Test adding package coverage data."""
        classes = {
            'WorkflowTestingFramework': {
                'total_lines': 100,
                'covered_lines': 80,
                'total_branches': 20,
                'covered_branches': 15
            }
        }
        self.reporter.add_package_coverage('workflow_testing', classes)
        self.assertIn('workflow_testing', self.reporter.packages)

    def test_calculate_rates(self):
        """Test calculating coverage rates."""
        self.reporter.add_package_coverage('test_pkg', {
            'classes': {
                'Class1': {'total_lines': 100, 'covered_lines': 50, 'total_branches': 20, 'covered_branches': 10}
            }
        })
        self.reporter.calculate_rates()
        self.assertEqual(self.reporter.line_rate, 0.5)
        self.assertEqual(self.reporter.branch_rate, 0.5)

    def test_generate_xml(self):
        """Test generating Cobertura XML."""
        self.reporter.add_package_coverage('test_pkg', {
            'classes': {
                'Class1': {'total_lines': 100, 'covered_lines': 100, 'total_branches': 20, 'covered_branches': 20}
            }
        })
        xml = self.reporter.generate_xml()
        self.assertIn("<coverage", xml)
        self.assertIn("<package", xml)


class TestRegressionBaseline(unittest.TestCase):
    """Test RegressionBaseline for baseline comparison."""

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_baseline(self, mock_open_file, mock_makedirs):
        """Test saving baseline data."""
        baseline = RegressionBaseline('/tmp/baselines')
        data = {"key": "value", "count": 42}
        baseline.save_baseline("test_baseline", data)
        self.assertIn("test_baseline", baseline.baseline_data)

    @patch('builtins.open', new_callable=mock_open, read_data='{"key": "value"}')
    @patch('os.path.exists', return_value=True)
    def test_load_baseline(self, mock_exists, mock_open_file):
        """Test loading baseline data."""
        baseline = RegressionBaseline('/tmp/baselines')
        loaded = baseline.load_baseline("test_baseline")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["key"], "value")

    @patch('builtins.open', new_callable=mock_open, read_data='{"key": "old_value"}')
    @patch('os.path.exists', return_value=True)
    def test_compare_no_regression(self, mock_exists, mock_open_file):
        """Test comparing identical data shows no regression."""
        baseline = RegressionBaseline('/tmp/baselines')
        current_data = {"key": "old_value"}
        is_regression, result = baseline.compare("test_baseline", current_data)
        self.assertFalse(is_regression)

    @patch('builtins.open', new_callable=mock_open, read_data='{"key": "old_value"}')
    @patch('os.path.exists', return_value=True)
    def test_compare_with_regression(self, mock_exists, mock_open_file):
        """Test comparing different data shows regression."""
        baseline = RegressionBaseline('/tmp/baselines')
        current_data = {"key": "new_value"}
        is_regression, result = baseline.compare("test_baseline", current_data)
        self.assertTrue(is_regression)
        self.assertEqual(result['differences']['key']['status'], 'changed')


class TestWorkflowTestingFramework(unittest.TestCase):
    """Test WorkflowTestingFramework main class."""

    def setUp(self):
        self.patcher1 = patch('os.makedirs')
        self.patcher2 = patch('os.path.exists', return_value=True)
        self.patcher1.start()
        self.patcher2.start()
        self.framework = WorkflowTestingFramework(project_root='/tmp/test_project')

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()

    def test_framework_initialization(self):
        """Test framework initializes with correct defaults."""
        self.assertEqual(self.framework.project_root, '/tmp/test_project')
        self.assertEqual(len(self.framework.test_results), 0)
        self.assertIsNotNone(self.framework.mock_http)
        self.assertIsNotNone(self.framework.mock_fs)
        self.assertIsNotNone(self.framework.screenshot_comparator)
        self.assertIsNotNone(self.framework.coverage_tracker)

    def test_register_fixture(self):
        """Test registering a test fixture."""
        fixture = TestFixture(name="test_fixture", description="Test fixture")
        self.framework.register_fixture(fixture)
        self.assertIn("test_fixture", self.framework.fixtures)

    def test_setup_fixtures(self):
        """Test setting up fixtures."""
        setup_called = []
        
        def setup_func():
            setup_called.append(True)
        
        fixture = TestFixture(name="test_fixture", setup_func=setup_func)
        self.framework.register_fixture(fixture)
        self.framework.setup_fixtures(["test_fixture"])
        self.assertEqual(len(setup_called), 1)

    def test_teardown_fixtures(self):
        """Test tearing down fixtures."""
        teardown_called = []
        
        def teardown_func():
            teardown_called.append(True)
        
        fixture = TestFixture(name="test_fixture", teardown_func=teardown_func)
        self.framework.register_fixture(fixture)
        self.framework.teardown_fixtures(["test_fixture"])
        self.assertEqual(len(teardown_called), 1)

    def test_create_unit_test_passed(self):
        """Test creating a passing unit test."""
        def test_func():
            return True
        
        result = self.framework.create_unit_test("test_pass", test_func)
        self.assertEqual(result.status, TestStatus.PASSED)
        self.assertEqual(result.test_name, "test_pass")
        self.assertGreater(result.duration_ms, 0)

    def test_create_unit_test_failed(self):
        """Test creating a failing unit test."""
        def test_func():
            return False
        
        result = self.framework.create_unit_test("test_fail", test_func)
        self.assertEqual(result.status, TestStatus.FAILED)

    def test_create_unit_test_error(self):
        """Test creating a unit test that raises exception."""
        def test_func():
            raise ValueError("Test error")
        
        result = self.framework.create_unit_test("test_error", test_func)
        self.assertEqual(result.status, TestStatus.ERROR)
        self.assertIn("Test error", result.message)

    def test_create_integration_test(self):
        """Test creating an integration test."""
        def workflow():
            return {"status": "completed", "steps": 5}
        
        result = self.framework.create_integration_test("integration_test", workflow)
        self.assertEqual(result.status, TestStatus.PASSED)
        self.assertEqual(result.test_type, TestType.INTEGRATION)

    def test_run_screenshot_test_passed(self):
        """Test running a passing screenshot test."""
        img_data = b'identical_image_data'
        result = self.framework.run_screenshot_test("screenshot_test", img_data, img_data)
        self.assertEqual(result.status, TestStatus.PASSED)

    def test_run_screenshot_test_failed(self):
        """Test running a failing screenshot test."""
        result = self.framework.run_screenshot_test(
            "screenshot_test", 
            b'short', 
            b'much_longer_image_data_here',
            threshold=0.95
        )
        self.assertEqual(result.status, TestStatus.FAILED)

    def test_run_performance_test_passed(self):
        """Test running a passing performance test."""
        def fast_operation():
            time.sleep(0.01)
            return True
        
        result = self.framework.run_performance_test("perf_test", fast_operation, max_duration_ms=100)
        self.assertEqual(result.status, TestStatus.PASSED)

    def test_run_performance_test_failed(self):
        """Test running a failing performance test."""
        def slow_operation():
            time.sleep(0.1)
            return True
        
        result = self.framework.run_performance_test("perf_test", slow_operation, max_duration_ms=10)
        self.assertEqual(result.status, TestStatus.FAILED)

    def test_run_regression_test_no_regression(self):
        """Test running a regression test with no regression."""
        self.framework.regression_baseline.baseline_data["baseline1"] = {"key": "value"}
        
        with patch.object(self.framework.regression_baseline, 'load_baseline', return_value={"key": "value"}):
            result = self.framework.run_regression_test("baseline1", {"key": "value"})
            self.assertEqual(result.status, TestStatus.PASSED)

    def test_run_regression_test_with_regression(self):
        """Test running a regression test with regression detected."""
        with patch.object(self.framework.regression_baseline, 'load_baseline', return_value={"key": "old"}):
            result = self.framework.run_regression_test("baseline1", {"key": "new"})
            self.assertEqual(result.status, TestStatus.FAILED)

    def test_track_coverage(self):
        """Test tracking coverage."""
        self.framework.track_coverage("path1", "Test Workflow", ["a1", "a2"])
        self.assertIn("path1", self.framework.coverage_tracker.workflow_paths)

    def test_mark_path_covered(self):
        """Test marking path as covered."""
        self.framework.track_coverage("path1", "Test Workflow", ["a1"])
        self.framework.mark_path_covered("path1")
        self.assertTrue(self.framework.coverage_tracker.workflow_paths["path1"].covered)

    def test_get_test_summary(self):
        """Test getting test summary."""
        # Add some test results
        self.framework.test_results = [
            TestResult("test1", TestType.UNIT, TestStatus.PASSED, 100.0),
            TestResult("test2", TestType.UNIT, TestStatus.FAILED, 50.0),
            TestResult("test3", TestType.UNIT, TestStatus.ERROR, 75.0),
        ]
        
        summary = self.framework.get_test_summary()
        self.assertEqual(summary['total_tests'], 3)
        self.assertEqual(summary['passed'], 1)
        self.assertEqual(summary['failed'], 1)
        self.assertEqual(summary['errors'], 1)
        self.assertAlmostEqual(summary['pass_rate'], 1/3)

    def test_create_test_fixture(self):
        """Test creating a test fixture."""
        fixture = self.framework.create_test_fixture(
            name="my_fixture",
            setup=lambda: None,
            teardown=lambda: None,
            data={"key": "value"}
        )
        self.assertEqual(fixture.name, "my_fixture")
        self.assertIn("my_fixture", self.framework.fixtures)


class TestQuickTestFunctions(unittest.TestCase):
    """Test quick test convenience functions."""

    def test_quick_unit_test(self):
        """Test quick_unit_test function."""
        def my_test():
            return True
        
        with patch('os.makedirs'), patch('os.path.exists', return_value=True):
            result = quick_unit_test(my_test, "my_quick_test")
            self.assertEqual(result.test_name, "my_quick_test")
            self.assertEqual(result.status, TestStatus.PASSED)

    def test_quick_performance_test(self):
        """Test quick_performance_test function."""
        def my_perf_test():
            time.sleep(0.01)
            return "done"
        
        with patch('os.makedirs'), patch('os.path.exists', return_value=True):
            result = quick_performance_test(my_perf_test, max_ms=100)
            self.assertEqual(result.test_type, TestType.PERFORMANCE)


if __name__ == '__main__':
    unittest.main()
