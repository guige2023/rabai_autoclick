"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline operations:
- PipelineCreateAction: Create pipeline
- PipelineAddStageAction: Add processing stage
- PipelineExecuteAction: Execute pipeline
- PipelineStatusAction: Check status
- PipelineDeleteAction: Delete pipeline
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineCreateAction(BaseAction):
    """Create a data pipeline."""
    action_type = "pipeline_create"
    display_name = "创建数据管道"
    description = "创建数据处理管道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            description_text = params.get("description", "")

            if not name:
                return ActionResult(success=False, message="name is required")

            pipeline_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "data_pipelines"):
                context.data_pipelines = {}
            context.data_pipelines[pipeline_id] = {
                "pipeline_id": pipeline_id,
                "name": name,
                "description": description_text,
                "stages": [],
                "status": "created",
                "created_at": time.time(),
                "executed_at": None,
                "total_processed": 0,
            }

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "name": name},
                message=f"Pipeline {pipeline_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline create failed: {e}")


class PipelineAddStageAction(BaseAction):
    """Add a stage to pipeline."""
    action_type = "pipeline_add_stage"
    display_name = "添加管道阶段"
    description = "向管道添加处理阶段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            stage_name = params.get("stage_name", "")
            stage_type = params.get("type", "transform")
            config = params.get("config", {})

            if not pipeline_id:
                return ActionResult(success=False, message="pipeline_id is required")

            pipelines = getattr(context, "data_pipelines", {})
            if pipeline_id not in pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            stage_id = str(uuid.uuid4())[:8]
            pipelines[pipeline_id]["stages"].append({
                "stage_id": stage_id,
                "name": stage_name,
                "type": stage_type,
                "config": config,
                "order": len(pipelines[pipeline_id]["stages"]),
            })

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "stage_id": stage_id, "stage_count": len(pipelines[pipeline_id]["stages"])},
                message=f"Stage '{stage_name}' added to pipeline {pipeline_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline add_stage failed: {e}")


class PipelineExecuteAction(BaseAction):
    """Execute a pipeline."""
    action_type = "pipeline_execute"
    display_name = "执行管道"
    description = "执行数据管道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            input_data = params.get("input_data", [])

            if not pipeline_id:
                return ActionResult(success=False, message="pipeline_id is required")

            pipelines = getattr(context, "data_pipelines", {})
            if pipeline_id not in pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            pipeline = pipelines[pipeline_id]
            pipeline["status"] = "running"
            pipeline["executed_at"] = time.time()

            data = input_data
            for stage in pipeline["stages"]:
                if stage["type"] == "transform":
                    data = [{"transformed": True, "original": d} for d in data]
                elif stage["type"] == "filter":
                    data = [d for d in data if d]
                elif stage["type"] == "map":
                    data = [{"mapped": d} for d in data]

            pipeline["status"] = "completed"
            pipeline["total_processed"] += len(data)

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "status": "completed", "processed": len(data)},
                message=f"Pipeline {pipeline_id} executed: {len(data)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline execute failed: {e}")


class PipelineStatusAction(BaseAction):
    """Check pipeline status."""
    action_type = "pipeline_status"
    display_name = "管道状态"
    description = "检查管道状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")

            pipelines = getattr(context, "data_pipelines", {})
            if pipeline_id not in pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            p = pipelines[pipeline_id]
            return ActionResult(
                success=True,
                data={
                    "pipeline_id": pipeline_id,
                    "name": p["name"],
                    "status": p["status"],
                    "stage_count": len(p["stages"]),
                    "total_processed": p["total_processed"],
                    "executed_at": p["executed_at"],
                },
                message=f"Pipeline {pipeline_id}: {p['status']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline status failed: {e}")


class PipelineDeleteAction(BaseAction):
    """Delete a pipeline."""
    action_type = "pipeline_delete"
    display_name = "删除管道"
    description = "删除数据管道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            if not pipeline_id:
                return ActionResult(success=False, message="pipeline_id is required")

            pipelines = getattr(context, "data_pipelines", {})
            if pipeline_id not in pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            del pipelines[pipeline_id]

            return ActionResult(success=True, data={"pipeline_id": pipeline_id}, message=f"Pipeline {pipeline_id} deleted")
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline delete failed: {e}")
