"""
工作流测试框架 v1.0
支持单元测试、集成测试、性能测试、负载测试、截图对比、CI/CD集成
"""
import unittest
import time
import json
import os
import tempfile
import shutil
import threading
import multiprocessing
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime
from io import BytesIO
import hashlib
import logging
import xml.etree.ElementTree as ET
from xml.dom import minidom
from unittest.mock import Mock, patch, MagicMock, mock_open
import psutil
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestType(Enum):
    """测试类型"""
    UNIT = "unit"
    INTEGRATION = "integration"
    PERFORMANCE = "performance"
    LOAD = "load"
    REGRESSION = "regression"
    SCREENSHOT = "screenshot"


class TestStatus(Enum):
    """测试状态"""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class TestResult:
    """测试结果"""
    test_name: str
    test_type: TestType
    status: TestStatus
    duration_ms: float
    message: str = ""
    error_trace: str = ""
    screenshot_before: Optional[bytes] = None
    screenshot_after: Optional[bytes] = None
    cpu_usage: float = 0.0
    memory_usage_mb: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TestFixture:
    """测试夹具"""
    name: str
    setup_func: Optional[Callable] = None
    teardown_func: Optional[Callable] = None
    data: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class WorkflowPath:
    """工作流路径"""
    path_id: str
    path_name: str
    actions: List[str]
    covered: bool = False
    last_tested: Optional[datetime] = None


class MockHTTPService:
    """模拟HTTP服务"""

    def __init__(self):
        self.responses: Dict[str, MockResponse] = {}
        self.call_history: List[Dict[str, Any]] = []

    def mock_get(self, url: str, response_data: Any, status_code: int = 200):
        self.responses[url] = MockResponse(status_code, response_data)

    def mock_post(self, url: str, response_data: Any, status_code: int = 200):
        self.responses[url] = MockResponse(status_code, response_data)

    def mock_put(self, url: str, response_data: Any, status_code: int = 200):
        self.responses[url] = MockResponse(status_code, response_data)

    def mock_delete(self, url: str, status_code: int = 204):
        self.responses[url] = MockResponse(status_code, None)

    def get_response(self, url: str) -> Optional['MockResponse']:
        return self.responses.get(url)

    def record_call(self, method: str, url: str, data: Any = None):
        self.call_history.append({
            'method': method,
            'url': url,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })

    def reset(self):
        self.responses.clear()
        self.call_history.clear()


class MockResponse:
    """模拟HTTP响应"""

    def __init__(self, status_code: int, data: Any):
        self.status_code = status_code
        self.data = data
        self.headers = {'Content-Type': 'application/json'}

    def json(self):
        return self.data

    def text(self):
        return json.dumps(self.data) if self.data else ''


class MockFileSystem:
    """模拟文件系统"""

    def __init__(self):
        self.files: Dict[str, str] = {}
        self.dirs: set = {'/tmp', '/home', '/workspace'}
        self.call_history: List[Dict[str, Any]] = []

    def mock_file(self, path: str, content: str = ""):
        self.files[path] = content
        self.call_history.append({
            'operation': 'create_file',
            'path': path,
            'timestamp': datetime.now().isoformat()
        })

    def mock_read(self, path: str) -> str:
        self.call_history.append({
            'operation': 'read_file',
            'path': path,
            'timestamp': datetime.now().isoformat()
        })
        return self.files.get(path, '')

    def mock_exists(self, path: str) -> bool:
        self.call_history.append({
            'operation': 'exists',
            'path': path,
            'timestamp': datetime.now().isoformat()
        })
        return path in self.files or path in self.dirs

    def mock_mkdir(self, path: str):
        self.dirs.add(path)
        self.call_history.append({
            'operation': 'mkdir',
            'path': path,
            'timestamp': datetime.now().isoformat()
        })

    def mock_remove(self, path: str):
        if path in self.files:
            del self.files[path]
        self.call_history.append({
            'operation': 'remove',
            'path': path,
            'timestamp': datetime.now().isoformat()
        })

    def reset(self):
        self.files.clear()
        self.dirs.clear()
        self.call_history.clear()


class ScreenshotComparator:
    """截图对比器"""

    def __init__(self, threshold: float = 0.95):
        self.threshold = threshold
        self.comparison_history: List[Dict[str, Any]] = []

    def compute_diff(self, img1: bytes, img2: bytes) -> float:
        """计算两张图片的相似度"""
        if img1 == img2:
            return 1.0

        hash1 = hashlib.md5(img1).hexdigest()
        hash2 = hashlib.md5(img2).hexdigest()

        if hash1 == hash2:
            return 1.0

        len1, len2 = len(img1), len(img2)
        max_len = max(len1, len2)
        min_len = min(len1, len2)

        similarity = min_len / max_len if max_len > 0 else 0

        return similarity

    def compare(self, before: bytes, after: bytes) -> Tuple[bool, float, Dict]:
        """对比两张截图"""
        similarity = self.compute_diff(before, after)
        passed = similarity >= self.threshold

        result = {
            'similarity': similarity,
            'threshold': self.threshold,
            'passed': passed,
            'before_size': len(before),
            'after_size': len(after),
            'timestamp': datetime.now().isoformat()
        }

        self.comparison_history.append(result)

        return passed, similarity, result


class ResourceMonitor:
    """资源监控器"""

    def __init__(self):
        self.process = psutil.Process()
        self.samples: List[Dict[str, float]] = []
        self.monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None

    def start(self):
        """开始监控"""
        self.monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()

    def stop(self) -> Dict[str, Any]:
        """停止监控"""
        self.monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=2.0)
        return self.get_summary()

    def _monitor_loop(self):
        """监控循环"""
        while self.monitoring:
            try:
                sample = {
                    'timestamp': time.time(),
                    'cpu_percent': self.process.cpu_percent(interval=0.1),
                    'memory_mb': self.process.memory_info().rss / 1024 / 1024,
                    'num_threads': self.process.num_threads()
                }
                self.samples.append(sample)
            except Exception:
                pass
            time.sleep(0.5)

    def get_summary(self) -> Dict[str, Any]:
        """获取监控摘要"""
        if not self.samples:
            return {'avg_cpu': 0, 'avg_memory': 0, 'peak_memory': 0}

        cpu_values = [s['cpu_percent'] for s in self.samples]
        mem_values = [s['memory_mb'] for s in self.samples]

        return {
            'avg_cpu': sum(cpu_values) / len(cpu_values),
            'peak_cpu': max(cpu_values),
            'avg_memory': sum(mem_values) / len(mem_values),
            'peak_memory': max(mem_values),
            'num_samples': len(self.samples)
        }


class LoadTestRunner:
    """负载测试运行器"""

    def __init__(self, num_workers: int = 10):
        self.num_workers = num_workers
        self.results: List[TestResult] = []
        self.errors: List[str] = []

    def run_concurrent(self, test_func: Callable, iterations: int = 100) -> Dict[str, Any]:
        """运行并发测试"""
        with multiprocessing.Pool(self.num_workers) as pool:
            start_time = time.time()

            async_results = []
            for i in range(iterations):
                result = pool.apply_async(test_func, args=(i,))
                async_results.append(result)

            pool.close()
            pool.join()

            for result in async_results:
                try:
                    self.results.append(result.get(timeout=30))
                except Exception as e:
                    self.errors.append(str(e))

            duration = time.time() - start_time

        return {
            'total_iterations': iterations,
            'successful': len(self.results),
            'failed': len(self.errors),
            'total_duration': duration,
            'throughput': iterations / duration if duration > 0 else 0,
            'errors': self.errors[:10]
        }


class CoverageTracker:
    """覆盖率追踪器"""

    def __init__(self):
        self.workflow_paths: Dict[str, WorkflowPath] = {}
        self.action_coverage: Dict[str, int] = {}
        self.branch_coverage: Dict[str, bool] = {}

    def register_path(self, path_id: str, path_name: str, actions: List[str]):
        """注册工作流路径"""
        self.workflow_paths[path_id] = WorkflowPath(
            path_id=path_id,
            path_name=path_name,
            actions=actions
        )

    def mark_path_tested(self, path_id: str):
        """标记路径已测试"""
        if path_id in self.workflow_paths:
            self.workflow_paths[path_id].covered = True
            self.workflow_paths[path_id].last_tested = datetime.now()

    def mark_action_tested(self, action_id: str):
        """标记动作已测试"""
        self.action_coverage[action_id] = self.action_coverage.get(action_id, 0) + 1

    def mark_branch(self, branch_id: str, taken: bool):
        """标记分支"""
        if branch_id not in self.branch_coverage:
            self.branch_coverage[branch_id] = False
        self.branch_coverage[branch_id] = self.branch_coverage[branch_id] or taken

    def get_coverage_report(self) -> Dict[str, Any]:
        """获取覆盖率报告"""
        total_paths = len(self.workflow_paths)
        covered_paths = sum(1 for p in self.workflow_paths.values() if p.covered)
        total_actions = len(self.action_coverage)
        tested_actions = sum(1 for c in self.action_coverage.values() if c > 0)
        total_branches = len(self.branch_coverage)
        taken_branches = sum(1 for t in self.branch_coverage.values() if t)

        return {
            'path_coverage': covered_paths / total_paths if total_paths > 0 else 0,
            'action_coverage': tested_actions / total_actions if total_actions > 0 else 0,
            'branch_coverage': taken_branches / total_branches if total_branches > 0 else 0,
            'total_paths': total_paths,
            'covered_paths': covered_paths,
            'total_actions': total_actions,
            'tested_actions': tested_actions,
            'total_branches': total_branches,
            'taken_branches': taken_branches
        }


class JUnitReporter:
    """JUnit XML报告生成器"""

    def __init__(self):
        self.test_suites: List[Dict[str, Any]] = []

    def add_test_suite(self, name: str, results: List[TestResult], duration: float):
        """添加测试套件"""
        suite = {
            'name': name,
            'tests': len(results),
            'failures': sum(1 for r in results if r.status == TestStatus.FAILED),
            'errors': sum(1 for r in results if r.status == TestStatus.ERROR),
            'skipped': sum(1 for r in results if r.status == TestStatus.SKIPPED),
            'time': duration / 1000,
            'test_cases': results
        }
        self.test_suites.append(suite)

    def generate_xml(self) -> str:
        """生成JUnit XML"""
        root = ET.Element('testsuites')

        for suite in self.test_suites:
            ts = ET.SubElement(root, 'testsuite')
            ts.set('name', suite['name'])
            ts.set('tests', str(suite['tests']))
            ts.set('failures', str(suite['failures']))
            ts.set('errors', str(suite['errors']))
            ts.set('skipped', str(suite['skipped']))
            ts.set('time', f"{suite['time']:.3f}")

            for result in suite['test_cases']:
                tc = ET.SubElement(ts, 'testcase')
                tc.set('name', result.test_name)
                tc.set('classname', result.test_type.value)
                tc.set('time', f"{result.duration_ms / 1000:.3f}")

                if result.status == TestStatus.FAILED:
                    failure = ET.SubElement(tc, 'failure')
                    failure.set('message', result.message)
                    failure.text = result.error_trace
                elif result.status == TestStatus.ERROR:
                    error = ET.SubElement(tc, 'error')
                    error.set('message', result.message)
                    error.text = result.error_trace
                elif result.status == TestStatus.SKIPPED:
                    skipped = ET.SubElement(tc, 'skipped')

        xml_str = ET.tostring(root, encoding='unicode')
        return minidom.parseString(xml_str).toprettyxml(indent='  ')


class CoberturaReporter:
    """Cobertura覆盖率报告生成器"""

    def __init__(self):
        self.packages: Dict[str, Dict] = {}
        self.line_rate = 0.0
        self.branch_rate = 0.0

    def add_package_coverage(self, package_name: str, classes: Dict[str, Any]):
        """添加包覆盖率"""
        self.packages[package_name] = classes

    def calculate_rates(self):
        """计算覆盖率"""
        total_lines = 0
        covered_lines = 0
        total_branches = 0
        covered_branches = 0

        for pkg in self.packages.values():
            for cls in pkg.get('classes', []):
                total_lines += cls.get('total_lines', 0)
                covered_lines += cls.get('covered_lines', 0)
                total_branches += cls.get('total_branches', 0)
                covered_branches += cls.get('covered_branches', 0)

        self.line_rate = covered_lines / total_lines if total_lines > 0 else 0
        self.branch_rate = covered_branches / total_branches if total_branches > 0 else 0

    def generate_xml(self) -> str:
        """生成Cobertura XML"""
        self.calculate_rates()

        root = ET.Element('coverage')
        root.set('line-rate', f"{self.line_rate:.2f}")
        root.set('branch-rate', f"{self.branch_rate:.2f}")
        root.set('version', '1.0')

        sources = ET.SubElement(root, 'sources')
        source = ET.SubElement(sources, 'source')
        source.text = '/Users/guige/my_project/rabai_autoclick/src'

        packages = ET.SubElement(root, 'packages')

        for pkg_name, pkg_data in self.packages.items():
            package = ET.SubElement(packages, 'package')
            package.set('name', pkg_name)
            package.set('line-rate', f"{pkg_data.get('line_rate', 0):.2f}")
            package.set('branch-rate', f"{pkg_data.get('branch_rate', 0):.2f}")

            classes = ET.SubElement(package, 'classes')
            for cls_name, cls_data in pkg_data.get('classes', {}).items():
                class_elem = ET.SubElement(classes, 'class')
                class_elem.set('name', cls_name)
                class_elem.set('filename', f"{cls_name}.py")
                class_elem.set('line-rate', f"{cls_data.get('line_rate', 0):.2f}")
                class_elem.set('branch-rate', f"{cls_data.get('branch_rate', 0):.2f}")

        xml_str = ET.tostring(root, encoding='unicode')
        return minidom.parseString(xml_str).toprettyxml(indent='  ')


class RegressionBaseline:
    """回归测试基线"""

    def __init__(self, baseline_dir: str):
        self.baseline_dir = baseline_dir
        os.makedirs(baseline_dir, exist_ok=True)
        self.baseline_data: Dict[str, Any] = {}

    def save_baseline(self, name: str, data: Dict[str, Any]):
        """保存基线数据"""
        path = os.path.join(self.baseline_dir, f"{name}.json")
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        self.baseline_data[name] = data

    def load_baseline(self, name: str) -> Optional[Dict[str, Any]]:
        """加载基线数据"""
        path = os.path.join(self.baseline_dir, f"{name}.json")
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
        return None

    def compare(self, name: str, current_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """对比基线"""
        baseline = self.load_baseline(name)
        if baseline is None:
            return True, {'message': 'No baseline found', 'is_regression': False}

        differences = {}
        for key in current_data:
            if key not in baseline:
                differences[key] = {'status': 'added', 'value': current_data[key]}
            elif current_data[key] != baseline[key]:
                differences[key] = {
                    'status': 'changed',
                    'baseline': baseline[key],
                    'current': current_data[key]
                }

        for key in baseline:
            if key not in current_data:
                differences[key] = {'status': 'removed', 'value': baseline[key]}

        is_regression = len(differences) > 0

        return not is_regression, {
            'is_regression': is_regression,
            'differences': differences,
            'baseline': baseline,
            'current': current_data
        }


class WorkflowTestingFramework:
    """工作流测试框架主类"""

    def __init__(self, project_root: str = '/Users/guige/my_project/rabai_autoclick'):
        self.project_root = project_root
        self.test_results: List[TestResult] = []
        self.fixtures: Dict[str, TestFixture] = {}
        self.mock_http = MockHTTPService()
        self.mock_fs = MockFileSystem()
        self.screenshot_comparator = ScreenshotComparator()
        self.coverage_tracker = CoverageTracker()
        self.junit_reporter = JUnitReporter()
        self.cobertura_reporter = CoberturaReporter()
        self.baseline_dir = os.path.join(project_root, 'test_baseline')
        self.regression_baseline = RegressionBaseline(self.baseline_dir)
        self._current_test_name: Optional[str] = None
        self._resource_monitor = ResourceMonitor()

    def register_fixture(self, fixture: TestFixture):
        """注册测试夹具"""
        self.fixtures[fixture.name] = fixture
        logger.info(f"Registered fixture: {fixture.name}")

    def setup_fixtures(self, fixture_names: List[str]):
        """设置测试夹具"""
        for name in fixture_names:
            if name in self.fixtures:
                fixture = self.fixtures[name]
                if fixture.setup_func:
                    fixture.setup_func()

    def teardown_fixtures(self, fixture_names: List[str]):
        """清理测试夹具"""
        for name in fixture_names:
            if name in self.fixtures:
                fixture = self.fixtures[name]
                if fixture.teardown_func:
                    fixture.teardown_func()

    def create_unit_test(self, test_name: str, test_func: Callable,
                         fixtures: List[str] = None) -> TestResult:
        """创建单元测试"""
        self._current_test_name = test_name
        start_time = time.time()

        resource_monitor = ResourceMonitor()
        resource_monitor.start()

        try:
            if fixtures:
                self.setup_fixtures(fixtures)

            result = test_func()

            if fixtures:
                self.teardown_fixtures(fixtures)

            duration = (time.time() - start_time) * 1000
            resource_stats = resource_monitor.stop()

            status = TestStatus.PASSED if result else TestStatus.FAILED

            test_result = TestResult(
                test_name=test_name,
                test_type=TestType.UNIT,
                status=status,
                duration_ms=duration,
                message="Unit test completed" if result else "Unit test failed",
                cpu_usage=resource_stats.get('avg_cpu', 0),
                memory_usage_mb=resource_stats.get('avg_memory', 0)
            )

        except Exception as e:
            duration = (time.time() - start_time) * 1000
            resource_stats = resource_monitor.stop()

            test_result = TestResult(
                test_name=test_name,
                test_type=TestType.UNIT,
                status=TestStatus.ERROR,
                duration_ms=duration,
                message=str(e),
                error_trace=traceback.format_exc(),
                cpu_usage=resource_stats.get('avg_cpu', 0),
                memory_usage_mb=resource_stats.get('avg_memory', 0)
            )

        self.test_results.append(test_result)
        self._current_test_name = None
        return test_result

    def create_integration_test(self, test_name: str, workflow: Callable,
                                fixtures: List[str] = None) -> TestResult:
        """创建集成测试"""
        self._current_test_name = test_name
        start_time = time.time()

        resource_monitor = ResourceMonitor()
        resource_monitor.start()

        try:
            if fixtures:
                self.setup_fixtures(fixtures)

            result = workflow()

            if fixtures:
                self.teardown_fixtures(fixtures)

            duration = (time.time() - start_time) * 1000
            resource_stats = resource_monitor.stop()

            status = TestStatus.PASSED if result else TestStatus.FAILED

            test_result = TestResult(
                test_name=test_name,
                test_type=TestType.INTEGRATION,
                status=status,
                duration_ms=duration,
                message="Integration test completed" if result else "Integration test failed",
                cpu_usage=resource_stats.get('avg_cpu', 0),
                memory_usage_mb=resource_stats.get('avg_memory', 0),
                metadata={'workflow_result': result} if result else {}
            )

        except Exception as e:
            duration = (time.time() - start_time) * 1000
            resource_stats = resource_monitor.stop()

            test_result = TestResult(
                test_name=test_name,
                test_type=TestType.INTEGRATION,
                status=TestStatus.ERROR,
                duration_ms=duration,
                message=str(e),
                error_trace=traceback.format_exc(),
                cpu_usage=resource_stats.get('avg_cpu', 0),
                memory_usage_mb=resource_stats.get('avg_memory', 0)
            )

        self.test_results.append(test_result)
        self._current_test_name = None
        return test_result

    def run_screenshot_test(self, test_name: str, before: bytes, after: bytes,
                             threshold: float = 0.95) -> TestResult:
        """运行截图对比测试"""
        self._current_test_name = test_name
        start_time = time.time()

        self.screenshot_comparator.threshold = threshold
        passed, similarity, comparison_result = self.screenshot_comparator.compare(before, after)

        duration = (time.time() - start_time) * 1000

        status = TestStatus.PASSED if passed else TestStatus.FAILED

        test_result = TestResult(
            test_name=test_name,
            test_type=TestType.SCREENSHOT,
            status=status,
            duration_ms=duration,
            message=f"Screenshot similarity: {similarity:.2%}",
            screenshot_before=before,
            screenshot_after=after,
            metadata=comparison_result
        )

        self.test_results.append(test_result)
        self._current_test_name = None
        return test_result

    def run_performance_test(self, test_name: str, test_func: Callable,
                             max_duration_ms: float = 1000) -> TestResult:
        """运行性能测试"""
        self._current_test_name = test_name
        start_time = time.time()

        resource_monitor = ResourceMonitor()
        resource_monitor.start()

        try:
            result = test_func()

            duration = (time.time() - start_time) * 1000
            resource_stats = resource_monitor.stop()

            passed = duration <= max_duration_ms
            status = TestStatus.PASSED if passed else TestStatus.FAILED

            test_result = TestResult(
                test_name=test_name,
                test_type=TestType.PERFORMANCE,
                status=status,
                duration_ms=duration,
                message=f"Execution time: {duration:.2f}ms (max: {max_duration_ms}ms)",
                cpu_usage=resource_stats.get('avg_cpu', 0),
                memory_usage_mb=resource_stats.get('avg_memory', 0),
                metadata={
                    'max_duration_ms': max_duration_ms,
                    'actual_duration_ms': duration,
                    'result': result
                }
            )

        except Exception as e:
            duration = (time.time() - start_time) * 1000
            resource_stats = resource_monitor.stop()

            test_result = TestResult(
                test_name=test_name,
                test_type=TestType.PERFORMANCE,
                status=TestStatus.ERROR,
                duration_ms=duration,
                message=str(e),
                error_trace=traceback.format_exc(),
                cpu_usage=resource_stats.get('avg_cpu', 0),
                memory_usage_mb=resource_stats.get('avg_memory', 0)
            )

        resource_monitor.stop()
        self.test_results.append(test_result)
        self._current_test_name = None
        return test_result

    def run_load_test(self, test_name: str, test_func: Callable,
                      num_workers: int = 10, iterations: int = 100) -> TestResult:
        """运行负载测试"""
        self._current_test_name = test_name
        start_time = time.time()

        runner = LoadTestRunner(num_workers)
        load_result = runner.run_concurrent(test_func, iterations)

        duration = (time.time() - start_time) * 1000

        status = TestStatus.PASSED if load_result['failed'] == 0 else TestStatus.FAILED

        test_result = TestResult(
            test_name=test_name,
            test_type=TestType.LOAD,
            status=status,
            duration_ms=duration,
            message=f"Load test: {load_result['successful']}/{iterations} successful",
            metadata=load_result
        )

        self.test_results.append(test_result)
        self._current_test_name = None
        return test_result

    def run_regression_test(self, baseline_name: str, current_data: Dict[str, Any]) -> TestResult:
        """运行回归测试"""
        self._current_test_name = baseline_name
        start_time = time.time()

        is_regression, comparison = self.regression_baseline.compare(baseline_name, current_data)

        duration = (time.time() - start_time) * 1000

        status = TestStatus.PASSED if not is_regression else TestStatus.FAILED

        test_result = TestResult(
            test_name=f"regression_{baseline_name}",
            test_type=TestType.REGRESSION,
            status=status,
            duration_ms=duration,
            message="No regression detected" if not is_regression else "Regression detected",
            metadata=comparison
        )

        self.test_results.append(test_result)
        self._current_test_name = None
        return test_result

    def save_baseline(self, baseline_name: str, data: Dict[str, Any]):
        """保存回归测试基线"""
        self.regression_baseline.save_baseline(baseline_name, data)

    @patch('urllib.request.urlopen')
    @patch('urllib.request.Request')
    def mock_http_call(self, mock_request, mock_urlopen, method: str, url: str,
                       response_data: Any = None, status_code: int = 200):
        """模拟HTTP调用"""
        mock_response = MockResponse(status_code, response_data)
        mock_urlopen.return_value = MockResponse(status_code, response_data)
        self.mock_http.record_call(method, url, response_data)

    def mock_file_operations(self):
        """模拟文件操作"""
        return {
            'open': mock_open(),
            'exists': lambda path: self.mock_fs.mock_exists(path),
            'read': lambda path: self.mock_fs.mock_read(path),
            'write': lambda path, data: self.mock_fs.mock_file(path, data),
            'remove': lambda path: self.mock_fs.mock_remove(path)
        }

    def track_coverage(self, path_id: str, path_name: str, actions: List[str]):
        """追踪覆盖率"""
        self.coverage_tracker.register_path(path_id, path_name, actions)

    def mark_path_covered(self, path_id: str):
        """标记路径已覆盖"""
        self.coverage_tracker.mark_path_tested(path_id)

    def generate_junit_report(self, suite_name: str = "WorkflowTestSuite") -> str:
        """生成JUnit XML报告"""
        self.junit_reporter.add_test_suite(
            suite_name,
            self.test_results,
            sum(r.duration_ms for r in self.test_results)
        )
        return self.junit_reporter.generate_xml()

    def generate_cobertura_report(self) -> str:
        """生成Cobertura覆盖率报告"""
        self.cobertura_reporter.add_package_coverage('workflow_testing', {
            'classes': {
                'WorkflowTestingFramework': {
                    'total_lines': 100,
                    'covered_lines': len([r for r in self.test_results if r.status == TestStatus.PASSED]),
                    'total_branches': 20,
                    'covered_branches': len([r for r in self.test_results if r.status == TestStatus.PASSED])
                }
            },
            'line_rate': self.coverage_tracker.get_coverage_report().get('line_coverage', 0),
            'branch_rate': self.coverage_tracker.get_coverage_report().get('branch_coverage', 0)
        })
        return self.cobertura_reporter.generate_xml()

    def save_reports(self, output_dir: str = None):
        """保存测试报告"""
        if output_dir is None:
            output_dir = os.path.join(self.project_root, 'test_reports')

        os.makedirs(output_dir, exist_ok=True)

        junit_path = os.path.join(output_dir, 'junit-results.xml')
        with open(junit_path, 'w') as f:
            f.write(self.generate_junit_report())

        cobertura_path = os.path.join(output_dir, 'cobertura-coverage.xml')
        with open(cobertura_path, 'w') as f:
            f.write(self.generate_cobertura_report())

        summary_path = os.path.join(output_dir, 'test-summary.json')
        with open(summary_path, 'w') as f:
            json.dump(self.get_test_summary(), f, indent=2, default=str)

        logger.info(f"Reports saved to {output_dir}")
        return {
            'junit': junit_path,
            'cobertura': cobertura_path,
            'summary': summary_path
        }

    def get_test_summary(self) -> Dict[str, Any]:
        """获取测试摘要"""
        total = len(self.test_results)
        passed = sum(1 for r in self.test_results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in self.test_results if r.status == TestStatus.FAILED)
        skipped = sum(1 for r in self.test_results if r.status == TestStatus.SKIPPED)
        errors = sum(1 for r in self.test_results if r.status == TestStatus.ERROR)

        total_duration = sum(r.duration_ms for r in self.test_results)
        avg_duration = total_duration / total if total > 0 else 0

        return {
            'total_tests': total,
            'passed': passed,
            'failed': failed,
            'skipped': skipped,
            'errors': errors,
            'pass_rate': passed / total if total > 0 else 0,
            'total_duration_ms': total_duration,
            'avg_duration_ms': avg_duration,
            'coverage': self.coverage_tracker.get_coverage_report(),
            'timestamp': datetime.now().isoformat()
        }

    def create_test_fixture(self, name: str, setup: Callable = None,
                            teardown: Callable = None, data: Dict = None) -> TestFixture:
        """创建测试夹具"""
        fixture = TestFixture(
            name=name,
            setup_func=setup,
            teardown_func=teardown,
            data=data or {}
        )
        self.register_fixture(fixture)
        return fixture


# Convenience functions for quick testing
def quick_unit_test(test_func: Callable, test_name: str = None) -> TestResult:
    """快速单元测试"""
    framework = WorkflowTestingFramework()
    name = test_name or getattr(test_func, '__name__', 'unnamed_test')
    return framework.create_unit_test(name, test_func)


def quick_performance_test(test_func: Callable, max_ms: float = 1000) -> TestResult:
    """快速性能测试"""
    framework = WorkflowTestingFramework()
    name = getattr(test_func, '__name__', 'unnamed_test')
    return framework.run_performance_test(name, test_func, max_ms)


# Example usage and tests
if __name__ == '__main__':
    print("WorkflowTestingFramework - Example Usage")

    framework = WorkflowTestingFramework()

    # Example 1: Unit Test
    def test_example_action():
        return True

    result = framework.create_unit_test("test_example_action", test_example_action)
    print(f"Unit Test Result: {result.status.value}")

    # Example 2: Performance Test
    def slow_operation():
        time.sleep(0.1)
        return "completed"

    result = framework.run_performance_test("test_slow_operation", slow_operation, max_duration_ms=500)
    print(f"Performance Test Result: {result.status.value}, Duration: {result.duration_ms:.2f}ms")

    # Example 3: Screenshot Comparison
    dummy_screenshot_before = b'fake_image_data_before'
    dummy_screenshot_after = b'fake_image_data_after'

    result = framework.run_screenshot_test("test_screenshot_comparison",
                                           dummy_screenshot_before,
                                           dummy_screenshot_after)
    print(f"Screenshot Test Result: {result.status.value}, Similarity: {result.metadata.get('similarity', 0):.2%}")

    # Example 4: Coverage Tracking
    framework.track_coverage("path_1", "click_workflow", ["mouse_click", "keyboard_press"])
    framework.mark_path_covered("path_1")
    coverage = framework.coverage_tracker.get_coverage_report()
    print(f"Coverage Report: {coverage}")

    # Example 5: Test Summary
    summary = framework.get_test_summary()
    print(f"Test Summary: {json.dumps(summary, indent=2, default=str)}")
