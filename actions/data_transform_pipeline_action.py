"""
Data Transform Pipeline Action Module.

Builds and executes data transformation pipelines with
chained operations, conditional branching, and error handling.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class TransformStep:
    """A transformation step."""
    name: str
    transform_func: Callable
    input_field: Optional[str] = None
    output_field: Optional[str] = None
    on_error: str = "skip"  # skip, stop, log


@dataclass
class TransformPipelineResult:
    """Result of pipeline execution."""
    success: bool
    records_out: list[dict[str, Any]]
    steps_completed: int
    errors: list[str]


class DataTransformPipelineAction(BaseAction):
    """Execute chained data transformations."""

    def __init__(self) -> None:
        super().__init__("data_transform_pipeline")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute transformation pipeline.

        Args:
            context: Execution context
            params: Parameters:
                - records: Input records
                - steps: List of transform step configs
                    - name: Step name
                    - func: Transform function
                    - input_field: Field to transform
                    - output_field: Output field name
                    - on_error: skip, stop, log
                - stop_on_error: Stop pipeline on first error

        Returns:
            TransformPipelineResult
        """
        records = params.get("records", [])
        step_configs = params.get("steps", [])
        stop_on_error = params.get("stop_on_error", False)

        steps = []
        for cfg in step_configs:
            steps.append(TransformStep(
                name=cfg.get("name", "unnamed"),
                transform_func=cfg.get("func"),
                input_field=cfg.get("input_field"),
                output_field=cfg.get("output_field", cfg.get("input_field")),
                on_error=cfg.get("on_error", "skip")
            ))

        records_out = list(records)
        errors = []
        steps_completed = 0

        for step in steps:
            new_records = []
            for r in records_out:
                try:
                    if step.transform_func:
                        result = step.transform_func(r)
                        if step.output_field and result is not None:
                            r[step.output_field] = result
                        new_records.append(r)
                    else:
                        new_records.append(r)
                    steps_completed += 1
                except Exception as e:
                    errors.append(f"Step {step.name}: {str(e)}")
                    if step.on_error == "stop" or stop_on_error:
                        return TransformPipelineResult(False, records_out, steps_completed, errors).__dict__
                    elif step.on_error == "skip":
                        new_records.append(r)
            records_out = new_records

        return TransformPipelineResult(
            success=len(errors) == 0,
            records_out=records_out,
            steps_completed=steps_completed,
            errors=errors
        ).__dict__
