"""Data Hook Action.

Provides a hook/extension system for data processing pipelines,
allowing custom transformations, validations, and side-effects at
any pipeline stage.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple


class HookPhase(Enum):
    """Pipeline phases where hooks can execute."""
    PRE_READ = "pre_read"
    POST_READ = "post_read"
    PRE_TRANSFORM = "pre_transform"
    POST_TRANSFORM = "post_transform"
    PRE_VALIDATE = "pre_validate"
    POST_VALIDATE = "post_validate"
    PRE_WRITE = "pre_write"
    POST_WRITE = "post_write"


@dataclass
class Hook:
    """A registered hook/callback."""
    name: str
    phase: HookPhase
    fn: Callable[[Any], Any]
    order: int = 0
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    description: str = ""
    error_policy: str = "skip"  # skip, fail, retry


@dataclass
class HookContext:
    """Context passed to each hook execution."""
    pipeline_name: str
    phase: HookPhase
    record_index: int
    total_records: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    start_time: float = field(default_factory=datetime.now().timestamp())


@dataclass
class HookResult:
    """Result of a hook execution."""
    hook_name: str
    phase: HookPhase
    success: bool
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    timestamp: float = field(default_factory=datetime.now().timestamp)


class DataHookAction:
    """Hook/extension system for data processing pipelines."""

    def __init__(self, pipeline_name: str = "default") -> None:
        self.pipeline_name = pipeline_name
        self._hooks: Dict[HookPhase, List[Hook]] = {phase: [] for phase in HookPhase}
        self._hook_patterns: Dict[HookPhase, List[Pattern]] = defaultdict(list)
        self._results: List[HookResult] = []
        self._max_results = 5000
        self._global_tags: Dict[str, str] = {}

    def register(
        self,
        name: str,
        phase: HookPhase,
        fn: Callable[[Any], Any],
        order: int = 0,
        tags: Optional[List[str]] = None,
        description: str = "",
        error_policy: str = "skip",
    ) -> None:
        """Register a new hook."""
        hook = Hook(
            name=name,
            phase=phase,
            fn=fn,
            order=order,
            tags=tags or [],
            description=description,
            error_policy=error_policy,
        )
        self._hooks[phase].append(hook)
        self._hooks[phase].sort(key=lambda h: h.order)

    def register_pattern(
        self,
        phase: HookPhase,
        pattern: str,
        fn: Callable[[Any], Any],
        tags: Optional[List[str]] = None,
    ) -> None:
        """Register a pattern-matching hook for field names."""
        compiled = re.compile(pattern)
        hook = Hook(
            name=f"pattern:{pattern}",
            phase=phase,
            fn=fn,
            tags=tags or [],
        )
        self._hooks[phase].append(hook)
        self._hook_patterns[phase].append((compiled, hook))

    def unregister(self, name: str) -> bool:
        """Unregister a hook by name. Returns True if found."""
        for phase in HookPhase:
            before = len(self._hooks[phase])
            self._hooks[phase] = [h for h in self._hooks[phase] if h.name != name]
            if len(self._hooks[phase]) < before:
                return True
        return False

    def enable(self, name: str) -> bool:
        """Enable a hook by name."""
        for phase in HookPhase:
            for hook in self._hooks[phase]:
                if hook.name == name:
                    hook.enabled = True
                    return True
        return False

    def disable(self, name: str) -> bool:
        """Disable a hook by name."""
        for phase in HookPhase:
            for hook in self._hooks[phase]:
                if hook.name == name:
                    hook.enabled = False
                    return True
        return False

    def execute(
        self,
        phase: HookPhase,
        data: Any,
        record_index: int = 0,
        total_records: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, List[HookResult]]:
        """Execute all hooks for a phase. Returns transformed data."""
        import time

        ctx = HookContext(
            pipeline_name=self.pipeline_name,
            phase=phase,
            record_index=record_index,
            total_records=total_records,
            metadata=metadata or {},
        )

        results: List[HookResult] = []
        current_data = data

        hooks_to_run = [h for h in self._hooks[phase] if h.enabled]

        for hook in hooks_to_run:
            start = time.time()
            try:
                current_data = hook.fn(current_data)
                result = HookResult(
                    hook_name=hook.name,
                    phase=phase,
                    success=True,
                    output=current_data,
                    duration_ms=(time.time() - start) * 1000,
                )
            except Exception as e:
                result = HookResult(
                    hook_name=hook.name,
                    phase=phase,
                    success=False,
                    output=current_data,
                    error=str(e),
                    duration_ms=(time.time() - start) * 1000,
                )
                if hook.error_policy == "fail":
                    raise
                ctx.errors.append(f"{hook.name}: {e}")

            results.append(result)
            self._add_result(result)

        return current_data, results

    def execute_field_hooks(
        self,
        phase: HookPhase,
        record: Dict[str, Any],
        field_patterns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Execute field-level hooks matching field names."""
        result = dict(record)
        patterns_to_check = self._hook_patterns.get(phase, [])

        for field_name, field_value in record.items():
            for compiled_pattern, hook in patterns_to_check:
                if compiled_pattern.match(field_name):
                    if hook.enabled:
                        try:
                            result[field_name] = hook.fn(field_value)
                        except Exception:
                            pass

        return result

    def _add_result(self, result: HookResult) -> None:
        """Add a hook result to history."""
        self._results.append(result)
        if len(self._results) > self._max_results:
            self._results = self._results[-self._max_results // 2:]

    def get_results(
        self,
        phase: Optional[HookPhase] = None,
        hook_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[HookResult]:
        """Get hook execution results."""
        filtered = self._results

        if phase is not None:
            filtered = [r for r in filtered if r.phase == phase]
        if hook_name is not None:
            filtered = [r for r in filtered if r.hook_name == hook_name]

        return filtered[-limit:]

    def get_hook_names(self) -> List[str]:
        """Get all registered hook names."""
        names = []
        for phase in HookPhase:
            for hook in self._hooks[phase]:
                names.append(f"{hook.phase.value}:{hook.name}")
        return names

    def set_global_tag(self, key: str, value: str) -> None:
        """Set a global tag available to all hooks."""
        self._global_tags[key] = value

    def get_stats(self) -> Dict[str, Any]:
        """Get hook execution statistics."""
        total = len(self._results)
        successful = sum(1 for r in self._results if r.success)
        failed = total - successful

        by_phase: Dict[str, Dict[str, int]] = {}
        for phase in HookPhase:
            phase_results = [r for r in self._results if r.phase == phase]
            by_phase[phase.value] = {
                "total": len(phase_results),
                "success": sum(1 for r in phase_results if r.success),
                "failed": sum(1 for r in phase_results if not r.success),
                "avg_duration_ms": (
                    sum(r.duration_ms for r in phase_results) / len(phase_results)
                    if phase_results else 0
                ),
            }

        return {
            "total_executions": total,
            "successful": successful,
            "failed": failed,
            "by_phase": by_phase,
            "registered_hooks": len(self.get_hook_names()),
        }

    def clear_results(self) -> None:
        """Clear hook execution history."""
        self._results.clear()
