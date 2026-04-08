"""Pipeline Processor Action Module.

Provides pipeline processing with
stages and routing.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class PipelineStage:
    """Pipeline stage."""
    stage_id: str
    name: str
    handler: Callable
    enabled: bool = True


class Pipeline:
    """Processing pipeline."""
    def __init__(self, pipeline_id: str, name: str):
        self.pipeline_id = pipeline_id
        self.name = name
        self._stages: List[PipelineStage] = []

    def add_stage(
        self,
        name: str,
        handler: Callable
    ) -> None:
        """Add stage to pipeline."""
        stage_id = f"{self.pipeline_id}_{name}"
        self._stages.append(PipelineStage(
            stage_id=stage_id,
            name=name,
            handler=handler
        ))

    def execute(self, data: Any) -> tuple[Any, List[Dict]]:
        """Execute pipeline."""
        results = []
        current_data = data

        for stage in self._stages:
            if not stage.enabled:
                continue

            try:
                result = stage.handler(current_data)
                results.append({
                    "stage_id": stage.stage_id,
                    "name": stage.name,
                    "success": True
                })
                current_data = result

            except Exception as e:
                results.append({
                    "stage_id": stage.stage_id,
                    "name": stage.name,
                    "success": False,
                    "error": str(e)
                })
                return current_data, results

        return current_data, results


class PipelineManager:
    """Manages pipelines."""

    def __init__(self):
        self._pipelines: Dict[str, Pipeline] = {}

    def create_pipeline(self, name: str) -> str:
        """Create pipeline."""
        pipeline_id = f"pipe_{name.lower().replace(' ', '_')}"
        self._pipelines[pipeline_id] = Pipeline(pipeline_id, name)
        return pipeline_id

    def get_pipeline(self, pipeline_id: str) -> Optional[Pipeline]:
        """Get pipeline."""
        return self._pipelines.get(pipeline_id)


class PipelineProcessorAction(BaseAction):
    """Action for pipeline processing."""

    def __init__(self):
        super().__init__("pipeline_processor")
        self._manager = PipelineManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute pipeline action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_stage":
                return self._add_stage(params)
            elif operation == "execute":
                return self._execute(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create pipeline."""
        pipeline_id = self._manager.create_pipeline(params.get("name", ""))
        return ActionResult(success=True, data={"pipeline_id": pipeline_id})

    def _add_stage(self, params: Dict) -> ActionResult:
        """Add stage to pipeline."""
        pipeline = self._manager.get_pipeline(params.get("pipeline_id", ""))
        if not pipeline:
            return ActionResult(success=False, message="Pipeline not found")

        def default_handler(data):
            return data

        pipeline.add_stage(
            params.get("name", ""),
            params.get("handler") or default_handler
        )
        return ActionResult(success=True)

    def _execute(self, params: Dict) -> ActionResult:
        """Execute pipeline."""
        pipeline = self._manager.get_pipeline(params.get("pipeline_id", ""))
        if not pipeline:
            return ActionResult(success=False, message="Pipeline not found")

        result, stages = pipeline.execute(params.get("data"))
        return ActionResult(success=True, data={
            "result": result,
            "stages": stages
        })
