"""
API Load Test Scenario Action Module

Defines and executes load testing scenarios for API endpoints.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import time
import random
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed


class LoadPattern(Enum):
    """Load pattern types."""
    CONSTANT = "constant"
    RAMP_UP = "ramp_up"
    RAMP_DOWN = "ramp_down"
    SPIKE = "spike"
    STEADY = "steady"
    POISSON = "poisson"


class HttpMethod(Enum):
    """HTTP methods supported."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


@dataclass
class RequestStep:
    """Single request in a load test scenario."""
    name: str
    method: HttpMethod
    url: str
    headers: Optional[Dict[str, str]] = None
    body: Optional[Dict[str, Any]] = None
    expected_status: int = 200
    timeout_ms: int = 30000
    weight: float = 1.0  # For weighted distribution


@dataclass
class LoadConfig:
    """Load test configuration."""
    virtual_users: int = 10
    duration_seconds: int = 60
    warmup_seconds: int = 5
    cooldown_seconds: int = 5
    pattern: LoadPattern = LoadPattern.STEADY
    ramp_steps: int = 5
    think_time_ms: Tuple[int, int] = (100, 500)
    max_errors: int = 100


@dataclass
class RequestResult:
    """Result of a single request."""
    step_name: str
    success: bool
    status_code: Optional[int] = None
    response_time_ms: Optional[float] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class LoadTestReport:
    """Aggregated load test report."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    error_rate: float = 0.0
    avg_response_time_ms: float = 0.0
    median_response_time_ms: float = 0.0
    p95_response_time_ms: float = 0.0
    p99_response_time_ms: float = 0.0
    min_response_time_ms: float = 0.0
    max_response_time_ms: float = 0.0
    requests_per_second: float = 0.0
    step_results: Dict[str, List[RequestResult]] = field(default_factory=dict)


class LoadTestScenarioAction:
    """
    Defines and executes load testing scenarios for API endpoints.

    Supports various load patterns including constant, ramp-up, spike,
    and steady state loads with detailed performance reporting.

    Example:
        config = LoadConfig(virtual_users=50, duration_seconds=120)
        scenario = LoadTestScenarioAction(config)
        scenario.add_step(RequestStep("get_users", HttpMethod.GET, "/api/users"))
        result = await scenario.execute()
    """

    def __init__(
        self,
        config: Optional[LoadConfig] = None,
        request_handler: Optional[Callable] = None
    ):
        """
        Initialize load test scenario.

        Args:
            config: Load test configuration
            request_handler: Custom async function(url, method, headers, body, timeout)
        """
        self.config = config or LoadConfig()
        self.request_handler = request_handler
        self.steps: List[RequestStep] = []
        self.results: List[RequestResult] = []
        self._running = False
        self._cancelled = False

    def add_step(self, step: RequestStep) -> "LoadTestScenarioAction":
        """Add a request step to the scenario."""
        self.steps.append(step)
        return self

    def set_request_handler(self, handler: Callable) -> None:
        """Set custom request handler function."""
        self.request_handler = handler

    async def _execute_request(self, step: RequestStep) -> RequestResult:
        """Execute a single request step."""
        start = time.time()
        try:
            if self.request_handler:
                status, body = await self.request_handler(
                    step.url, step.method, step.headers, step.body, step.timeout_ms
                )
            else:
                # Default mock handler
                await asyncio.sleep(random.uniform(0.01, 0.1))
                status = step.expected_status
                body = {"status": "ok"}

            elapsed = (time.time() - start) * 1000
            success = status == step.expected_status
            return RequestResult(
                step_name=step.name,
                success=success,
                status_code=status,
                response_time_ms=elapsed
            )
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            return RequestResult(
                step_name=step.name,
                success=False,
                error=str(e),
                response_time_ms=elapsed
            )

    def _select_step(self) -> RequestStep:
        """Select a step based on weighted distribution."""
        if not self.steps:
            raise ValueError("No steps defined in scenario")
        weights = [s.weight for s in self.steps]
        total = sum(weights)
        probs = [w / total for w in weights]
        return random.choices(self.steps, weights=probs, k=1)[0]

    async def _user_simulation(self, user_id: int) -> List[RequestResult]:
        """Simulate a single virtual user's behavior."""
        results = []
        pattern = self.config.pattern
        ramp_steps = self.config.ramp_steps

        # Calculate effective VU based on pattern
        if pattern == LoadPattern.RAMP_UP:
            # Users gradually increase
            effective_vu = min(user_id + 1, self.config.virtual_users)
        elif pattern == LoadPattern.RAMP_DOWN:
            effective_vu = max(1, self.config.virtual_users - user_id)
        elif pattern == LoadPattern.SPIKE:
            # Spike in the middle
            mid = self.config.duration_seconds // 2
        else:
            effective_vu = self.config.virtual_users

        end_time = time.time() + self.config.duration_seconds
        while time.time() < end_time and not self._cancelled:
            step = self._select_step()
            result = await self._execute_request(step)
            results.append(result)

            # Think time between requests
            think_time = random.randint(*self.config.think_time_ms) / 1000
            await asyncio.sleep(think_time)

        return results

    async def execute(self) -> LoadTestReport:
        """
        Execute the load test scenario.

        Returns:
            LoadTestReport with aggregated results
        """
        self._running = True
        self._cancelled = False
        self.results = []

        # Warmup phase
        if self.config.warmup_seconds > 0:
            await asyncio.sleep(self.config.warmup_seconds)

        # Execute load test with virtual users
        tasks = [
            self._user_simulation(i)
            for i in range(self.config.virtual_users)
        ]

        all_results = []
        for coro in asyncio.as_completed(tasks):
            try:
                user_results = await coro
                all_results.extend(user_results)
            except Exception as e:
                pass  # Log error in production

        self.results = all_results

        # Cooldown
        await asyncio.sleep(self.config.cooldown_seconds)
        self._running = False

        return self._generate_report()

    def _generate_report(self) -> LoadTestReport:
        """Generate aggregated report from results."""
        if not self.results:
            return LoadTestReport()

        response_times = [r.response_time_ms for r in self.results if r.response_time_ms]
        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]

        report = LoadTestReport()
        report.total_requests = len(self.results)
        report.successful_requests = len(successful)
        report.failed_requests = len(failed)
        report.error_rate = len(failed) / len(self.results) if self.results else 0

        if response_times:
            report.avg_response_time_ms = statistics.mean(response_times)
            report.median_response_time_ms = statistics.median(response_times)
            sorted_times = sorted(response_times)
            report.p95_response_time_ms = sorted_times[int(len(sorted_times) * 0.95)]
            report.p99_response_time_ms = sorted_times[int(len(sorted_times) * 0.99)]
            report.min_response_time_ms = min(response_times)
            report.max_response_time_ms = max(response_times)

        total_time = self.config.duration_seconds
        report.requests_per_second = len(self.results) / total_time if total_time > 0 else 0

        # Step-level results
        for result in self.results:
            if result.step_name not in report.step_results:
                report.step_results[result.step_name] = []
            report.step_results[result.step_name].append(result)

        return report

    def cancel(self) -> None:
        """Cancel the running load test."""
        self._cancelled = True

    def get_progress(self) -> float:
        """Get current progress as percentage."""
        if not self._running:
            return 0.0
        # Approximate progress
        return min(1.0, len(self.results) / (self.config.virtual_users * 10))
