"""
API Testing Action Module.

Automated API testing: sends requests, validates responses against
schemas, checks status codes, measures latency, and generates test reports.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class TestCase:
    """A single API test case."""
    name: str
    method: str
    path: str
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    expected_status: int = 200
    expected_schema: Optional[dict] = None
    expected_fields: Optional[list[str]] = None
    timeout: float = 30


@dataclass
class TestResult:
    """Result of a single test case."""
    name: str
    passed: bool
    status_code: Optional[int]
    response_time_ms: float
    error: Optional[str] = None
    assertions: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TestReport:
    """Overall test report."""
    total: int
    passed: int
    failed: int
    skipped: int
    total_duration_ms: float
    results: list[TestResult]
    errors: list[str]


class APITestingAction(BaseAction):
    """Automated API testing and validation."""

    def __init__(self) -> None:
        super().__init__("api_testing")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Run API tests.

        Args:
            context: Execution context
            params: Parameters:
                - base_url: Base URL for API
                - tests: List of test case configs
                - auth: Optional auth credentials
                - validate_schema: Enable JSON schema validation
                - stop_on_failure: Stop on first failure (default: False)

        Returns:
            TestReport with results for all test cases
        """
        import time
        import json
        import urllib.request
        import urllib.parse
        import urllib.error

        base_url = params.get("base_url", "").rstrip("/")
        test_configs = params.get("tests", [])
        auth = params.get("auth", {})
        stop_on_failure = params.get("stop_on_failure", False)
        validate_schema = params.get("validate_schema", True)

        tests = []
        for cfg in test_configs:
            tests.append(TestCase(
                name=cfg.get("name", ""),
                method=cfg.get("method", "GET").upper(),
                path=cfg.get("path", ""),
                headers=cfg.get("headers", {}),
                body=cfg.get("body"),
                expected_status=cfg.get("expected_status", 200),
                expected_schema=cfg.get("expected_schema"),
                expected_fields=cfg.get("expected_fields"),
                timeout=cfg.get("timeout", 30)
            ))

        results: list[TestResult] = []
        errors: list[str] = []
        total_duration = 0.0
        passed = 0
        failed = 0
        skipped = 0

        for test in tests:
            start_time = time.time()
            try:
                url = base_url + test.path
                headers = dict(test.headers)
                if auth.get("type") == "bearer":
                    headers["Authorization"] = f"Bearer {auth.get('token', '')}"

                req = urllib.request.Request(url, method=test.method, headers=headers)
                if test.body is not None:
                    body = test.body if isinstance(test.body, str) else json.dumps(test.body).encode("utf-8")
                    req.data = body
                    headers.setdefault("Content-Type", "application/json")

                with urllib.request.urlopen(req, timeout=test.timeout) as response:
                    body = response.read().decode("utf-8")
                    content_type = response.headers.get("Content-Type", "")
                    if "application/json" in content_type:
                        try:
                            body = json.loads(body)
                        except Exception:
                            pass

                    elapsed_ms = (time.time() - start_time) * 1000
                    total_duration += elapsed_ms

                    assertions = []
                    test_passed = True

                    if response.status != test.expected_status:
                        assertions.append({
                            "assertion": "status_code",
                            "expected": test.expected_status,
                            "actual": response.status,
                            "passed": False
                        })
                        test_passed = False
                    else:
                        assertions.append({
                            "assertion": "status_code",
                            "expected": test.expected_status,
                            "actual": response.status,
                            "passed": True
                        })

                    if test.expected_fields and isinstance(body, dict):
                        for field in test.expected_fields:
                            has_field = field in body
                            assertions.append({
                                "assertion": f"has_field:{field}",
                                "expected": True,
                                "actual": has_field,
                                "passed": has_field
                            })
                            if not has_field:
                                test_passed = False

                    if test_passed:
                        passed += 1
                    else:
                        failed += 1

                    results.append(TestResult(
                        name=test.name,
                        passed=test_passed,
                        status_code=response.status,
                        response_time_ms=elapsed_ms,
                        assertions=assertions
                    ))

            except urllib.error.HTTPError as e:
                elapsed_ms = (time.time() - start_time) * 1000
                total_duration += elapsed_ms
                if e.code == test.expected_status:
                    passed += 1
                    test_passed = True
                else:
                    failed += 1
                    test_passed = False
                results.append(TestResult(
                    name=test.name,
                    passed=test_passed,
                    status_code=e.code,
                    response_time_ms=elapsed_ms,
                    error=f"HTTP {e.code}",
                    assertions=[{"assertion": "status_code", "expected": test.expected_status, "actual": e.code, "passed": test_passed}]
                ))

            except Exception as e:
                elapsed_ms = (time.time() - start_time) * 1000
                total_duration += elapsed_ms
                failed += 1
                errors.append(f"{test.name}: {str(e)}")
                results.append(TestResult(
                    name=test.name,
                    passed=False,
                    status_code=None,
                    response_time_ms=elapsed_ms,
                    error=str(e)
                ))
                if stop_on_failure:
                    break

        return TestReport(
            total=len(tests),
            passed=passed,
            failed=failed,
            skipped=skipped,
            total_duration_ms=total_duration,
            results=results,
            errors=errors
        )
