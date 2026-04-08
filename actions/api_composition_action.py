"""
API Composition Action Module.

Provides API composition, chaining, and orchestration
for building complex workflows from multiple service calls.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import uuid

logger = logging.getLogger(__name__)


class CompositionType(Enum):
    """Type of API composition."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    BRANCH = "branch"
    LOOP = "loop"
    RACE = "race"


@dataclass
class APICall:
    """Single API call in composition."""
    call_id: str
    name: str
    service: str
    endpoint: str
    method: str
    params: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0


@dataclass
class CallResult:
    """Result of an API call."""
    call_id: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    response_time: float = 0.0
    status_code: Optional[int] = None


@dataclass
class CompositionStep:
    """Step in API composition."""
    step_id: str
    composition_type: CompositionType
    calls: List[APICall] = field(default_factory=list)
    condition: Optional[Callable[[Any], bool]] = None
    iterations: int = 1
    max_duration: Optional[float] = None


@dataclass
class CompositionResult:
    """Result of composition execution."""
    composition_id: str
    success: bool
    step_results: Dict[str, List[CallResult]]
    total_time: float
    error: Optional[str] = None


class APIComposer:
    """Composes multiple API calls into workflows."""

    def __init__(self):
        self.services: Dict[str, Callable] = {}
        self.compositions: Dict[str, List[CompositionStep]] = {}

    def register_service(self, name: str, handler: Callable):
        """Register API service handler."""
        self.services[name] = handler

    def add_composition(
        self,
        composition_id: str,
        steps: List[CompositionStep]
    ):
        """Add composition definition."""
        self.compositions[composition_id] = steps

    async def execute_call(self, call: APICall) -> CallResult:
        """Execute single API call."""
        start_time = datetime.now()

        try:
            service_handler = self.services.get(call.service)
            if not service_handler:
                return CallResult(
                    call_id=call.call_id,
                    success=False,
                    error=f"Service not found: {call.service}",
                    response_time=0.0
                )

            result = await asyncio.wait_for(
                service_handler(call),
                timeout=call.timeout
            )

            response_time = (datetime.now() - start_time).total_seconds()

            return CallResult(
                call_id=call.call_id,
                success=True,
                data=result,
                response_time=response_time,
                status_code=200
            )

        except asyncio.TimeoutError:
            return CallResult(
                call_id=call.call_id,
                success=False,
                error="Request timeout",
                response_time=call.timeout,
                status_code=408
            )

        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds()
            return CallResult(
                call_id=call.call_id,
                success=False,
                error=str(e),
                response_time=response_time,
                status_code=500
            )

    async def execute_sequential(
        self,
        calls: List[APICall],
        context: Dict[str, Any]
    ) -> List[CallResult]:
        """Execute calls sequentially."""
        results = []
        current_context = context.copy()

        for call in calls:
            enriched_call = self._enrich_call(call, current_context)
            result = await self.execute_call(enriched_call)
            results.append(result)

            if result.success:
                current_context[call.call_id] = result.data
            elif not self._should_continue(call, result):
                break

        return results

    async def execute_parallel(
        self,
        calls: List[APICall],
        context: Dict[str, Any]
    ) -> List[CallResult]:
        """Execute calls in parallel."""
        enriched_calls = [self._enrich_call(call, context) for call in calls]
        tasks = [self.execute_call(call) for call in enriched_calls]
        return await asyncio.gather(*tasks)

    async def execute_race(
        self,
        calls: List[APICall],
        context: Dict[str, Any],
        timeout: float
    ) -> Tuple[CallResult, List[CallResult]]:
        """Execute calls in race, return first to complete."""
        enriched_calls = [self._enrich_call(call, context) for call in calls]
        tasks = [self.execute_call(call) for call in enriched_calls]

        done, pending = await asyncio.wait(
            tasks,
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED
        )

        results = [t.result() for t in done]
        for task in pending:
            task.cancel()

        return results[0] if results else None, results

    async def execute_composition(
        self,
        composition_id: str,
        initial_context: Dict[str, Any]
    ) -> CompositionResult:
        """Execute complete composition."""
        if composition_id not in self.compositions:
            return CompositionResult(
                composition_id=composition_id,
                success=False,
                step_results={},
                total_time=0.0,
                error="Composition not found"
            )

        start_time = datetime.now()
        step_results = {}
        context = initial_context.copy()
        success = True

        for step in self.compositions[composition_id]:
            if step.condition and not step.condition(context):
                continue

            if step.composition_type == CompositionType.SEQUENTIAL:
                results = await self.execute_sequential(step.calls, context)

            elif step.composition_type == CompositionType.PARALLEL:
                results = await self.execute_parallel(step.calls, context)

            elif step.composition_type == CompositionType.RACE:
                first_result, results = await self.execute_race(
                    step.calls, context, step.max_duration or 30.0
                )
                if first_result:
                    context["race_winner"] = first_result.call_id

            else:
                results = await self.execute_sequential(step.calls, context)

            step_results[step.step_id] = results

            for result in results:
                if result.success:
                    context[result.call_id] = result.data
                else:
                    success = False

        total_time = (datetime.now() - start_time).total_seconds()

        return CompositionResult(
            composition_id=composition_id,
            success=success,
            step_results=step_results,
            total_time=total_time
        )

    def _enrich_call(self, call: APICall, context: Dict[str, Any]) -> APICall:
        """Enrich call with context data."""
        enriched_params = call.params.copy()
        for key, value in call.params.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                context_key = value[2:-1]
                if context_key in context:
                    enriched_params[key] = context[context_key]

        return APICall(
            call_id=call.call_id,
            name=call.name,
            service=call.service,
            endpoint=call.endpoint,
            method=call.method,
            params=enriched_params,
            headers=call.headers,
            timeout=call.timeout
        )

    def _should_continue(self, call: APICall, result: CallResult) -> bool:
        """Check if execution should continue after failure."""
        return False


async def demo_service_handler(call: APICall) -> Dict[str, Any]:
    """Demo service handler."""
    await asyncio.sleep(0.1)
    return {"result": f"Called {call.endpoint}", "input": call.params}


async def main():
    """Demonstrate API composition."""
    composer = APIComposer()
    composer.register_service("demo", demo_service_handler)

    call1 = APICall(
        call_id="call1",
        name="Get User",
        service="demo",
        endpoint="/users/1",
        method="GET"
    )

    call2 = APICall(
        call_id="call2",
        name="Get Orders",
        service="demo",
        endpoint="/orders",
        method="GET",
        params={"user_id": "${call1.data.result}"}
    )

    composer.add_composition("user_workflow", [
        CompositionStep(
            step_id="step1",
            composition_type=CompositionType.SEQUENTIAL,
            calls=[call1, call2]
        )
    ])

    result = await composer.execute_composition("user_workflow", {})
    print(f"Success: {result.success}, Time: {result.total_time:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
