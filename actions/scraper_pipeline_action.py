"""
Scraper Pipeline Action Module.

Builds multi-stage scraping pipelines with extraction,
transformation, validation, and storage stages.

Example:
    >>> from scraper_pipeline_action import ScraperPipeline
    >>> pipeline = ScraperPipeline()
    >>> pipeline.add_stage("extract", extractor.extract_article)
    >>> pipeline.add_stage("transform", transformer.normalize)
    >>> results = await pipeline.run(urls)
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class PipelineStage:
    """A single stage in the scraping pipeline."""
    name: str
    func: Callable
    config: dict[str, Any] = field(default_factory=dict)
    continue_on_error: bool = True


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    item: Any
    stage_results: dict[str, Any]
    errors: list[str]
    duration_ms: float
    success: bool


@dataclass
class PipelineStats:
    """Aggregate pipeline statistics."""
    total_items: int = 0
    successful: int = 0
    failed: int = 0
    total_duration_ms: float = 0
    stage_stats: dict[str, dict[str, Any]] = field(default_factory=dict)


class ScraperPipeline:
    """Multi-stage scraping pipeline executor."""

    def __init__(self):
        self._stages: list[PipelineStage] = []
        self._stats = PipelineStats()

    def add_stage(
        self,
        name: str,
        func: Callable,
        continue_on_error: bool = True,
        **config,
    ) -> "ScraperPipeline":
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            func=func,
            continue_on_error=continue_on_error,
            config=config,
        )
        self._stages.append(stage)
        return self

    async def run(
        self,
        items: list[Any],
        concurrency: int = 5,
    ) -> list[PipelineResult]:
        """
        Run pipeline on list of items.

        Args:
            items: List of input items (URLs, etc.)
            concurrency: Max concurrent executions

        Returns:
            List of PipelineResult
        """
        semaphore = asyncio.Semaphore(concurrency)
        tasks = []

        for item in items:
            task = self._process_item(item, semaphore)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed: list[PipelineResult] = []
        for result in results:
            if isinstance(result, Exception):
                processed.append(PipelineResult(
                    item=None,
                    stage_results={},
                    errors=[str(result)],
                    duration_ms=0,
                    success=False,
                ))
            else:
                processed.append(result)

        self._update_stats(processed)
        return processed

    async def run_single(self, item: Any) -> PipelineResult:
        """Run pipeline on a single item."""
        return await self._process_item(item, None)

    async def _process_item(
        self,
        item: Any,
        semaphore: Optional[asyncio.Semaphore],
    ) -> PipelineResult:
        start = time.monotonic()
        stage_results: dict[str, Any] = {}
        errors: list[str] = []
        current = item

        for stage in self._stages:
            if semaphore:
                async with semaphore:
                    result = await self._execute_stage(stage, current)
            else:
                result = await self._execute_stage(stage, current)

            if isinstance(result, tuple) and len(result) == 2:
                success, output = result
            elif isinstance(result, Exception):
                success = stage.continue_on_error
                output = None
                errors.append(f"{stage.name}: {result}")
            else:
                success = True
                output = result

            stage_results[stage.name] = output
            if not success:
                if not stage.continue_on_error:
                    break
            else:
                current = output

        duration_ms = (time.monotonic() - start) * 1000
        return PipelineResult(
            item=item,
            stage_results=stage_results,
            errors=errors,
            duration_ms=duration_ms,
            success=len(errors) == 0,
        )

    async def _execute_stage(
        self,
        stage: PipelineStage,
        data: Any,
    ) -> Any:
        func = stage.func

        if asyncio.iscoroutinefunction(func):
            return await func(data, **stage.config)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: func(data, **stage.config))

    def _update_stats(self, results: list[PipelineResult]) -> None:
        self._stats.total_items = len(results)
        self._stats.successful = sum(1 for r in results if r.success)
        self._stats.failed = self._stats.total_items - self._stats.successful
        self._stats.total_duration_ms = sum(r.duration_ms for r in results)

        for stage in self._stages:
            stage_times = [r.stage_results.get(stage.name, {}).get("_duration_ms", 0) for r in results]
            self._stats.stage_stats[stage.name] = {
                "count": len(stage_times),
                "total_ms": sum(stage_times),
                "avg_ms": sum(stage_times) / len(stage_times) if stage_times else 0,
            }

    def get_stats(self) -> PipelineStats:
        """Get pipeline statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = PipelineStats()

    def get_stage_names(self) -> list[str]:
        """Get list of stage names in order."""
        return [s.name for s in self._stages]


class StageBuilder:
    """Helper to build common pipeline stages."""

    @staticmethod
    def fetch_stage(headers: Optional[dict[str, str]] = None) -> Callable:
        """Create a fetch stage using HTTP client."""
        from request_handler_action import HTTPClient
        client = HTTPClient()

        async def fetch(url: str) -> tuple[bool, dict[str, Any]]:
            resp = await client.get(url, headers=headers or {})
            if resp.status == 200:
                return True, {"url": url, "content": resp.content, "headers": resp.headers}
            return False, None

        return fetch

    @staticmethod
    def extract_stage(extract_func: Callable) -> Callable:
        """Create an extraction stage."""
        async def extract(data: dict[str, Any]) -> Any:
            content = data.get("content", b"").decode("utf-8", errors="replace")
            return extract_func(content, data.get("url", ""))
        return extract

    @staticmethod
    def transform_stage(transform_func: Callable) -> Callable:
        """Create a transformation stage."""
        async def transform(data: Any) -> Any:
            return transform_func(data)
        return transform

    @staticmethod
    def validate_stage(validator: Callable, rules: dict) -> Callable:
        """Create a validation stage."""
        async def validate(data: dict[str, Any]) -> tuple[bool, Any]:
            valid, errors = validator.validate_record(data, rules)
            if valid:
                return True, data
            return False, None
        return validate

    @staticmethod
    def store_stage(storage_func: Callable) -> Callable:
        """Create a storage stage."""
        async def store(data: Any) -> Any:
            await storage_func(data)
            return data
        return store


if __name__ == "__main__":
    async def test():
        pipeline = ScraperPipeline()
        pipeline.add_stage("double", lambda x: x * 2)
        pipeline.add_stage("add_ten", lambda x: x + 10)

        results = await pipeline.run([1, 2, 3, 4, 5])
        for r in results:
            print(f"Input: {r.item}, Output: {r.stage_results}")

    asyncio.run(test())
