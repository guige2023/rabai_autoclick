"""Data transformer action module for RabAI AutoClick.

Provides data transformation:
- DataTransformerAction: Transform data structures
- TransformerPipelineAction: Pipeline transformations
- DataMapperAction: Map fields between structures
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataTransformerAction(BaseAction):
    """Transform data structures."""
    action_type = "data_transformer"
    display_name = "数据转换"
    description = "转换数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            transformations = params.get("transformations", [])

            transformed = data.copy()
            for tf in transformations:
                field = tf.get("field", "")
                func = tf.get("function", "upper")
                if field in transformed:
                    if func == "upper":
                        transformed[field] = str(transformed[field]).upper()
                    elif func == "lower":
                        transformed[field] = str(transformed[field]).lower()
                    elif func == "trim":
                        transformed[field] = str(transformed[field]).strip()

            return ActionResult(
                success=True,
                data={
                    "transformations_applied": len(transformations),
                    "transformed": transformed
                },
                message=f"Data transformed: {len(transformations)} transformations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data transformer error: {str(e)}")


class TransformerPipelineAction(BaseAction):
    """Pipeline transformations."""
    action_type = "transformer_pipeline"
    display_name = "转换管道"
    description = "管道式转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            pipeline = params.get("pipeline", [])

            result = data
            for step in pipeline:
                step_name = step.get("name", "step")
                result = [step_name] * len(result) if isinstance(result, list) else step_name

            return ActionResult(
                success=True,
                data={
                    "pipeline_steps": len(pipeline),
                    "result": result
                },
                message=f"Pipeline executed: {len(pipeline)} steps"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transformer pipeline error: {str(e)}")


class DataMapperAction(BaseAction):
    """Map fields between structures."""
    action_type = "data_mapper"
    display_name = "数据映射"
    description: "映射字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            mappings = params.get("mappings", [])

            mapped = {}
            for mapping in mappings:
                source = mapping.get("source", "")
                target = mapping.get("target", source)
                default = mapping.get("default", None)
                mapped[target] = data.get(source, default)

            return ActionResult(
                success=True,
                data={
                    "mappings_applied": len(mappings),
                    "mapped_data": mapped
                },
                message=f"Data mapped: {len(mappings)} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data mapper error: {str(e)}")
