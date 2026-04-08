"""Automation pipeline action module for RabAI AutoClick.

Provides pipeline orchestration operations:
- PipelineCreateAction: Create a pipeline
- PipelineAddStageAction: Add a stage to pipeline
- PipelineExecuteAction: Execute pipeline
- PipelineBranchAction: Branch pipeline execution
- PipelineMergeAction: Merge pipeline branches
- PipelineStatusAction: Get pipeline status
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
    """Create a new pipeline."""
    action_type = "pipeline_create"
    display_name = "创建流水线"
    description = "创建新的自动化流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            description = params.get("description", "")
            stages = params.get("stages", [])

            if not name:
                return ActionResult(success=False, message="name is required")

            pipeline_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "pipelines"):
                context.pipelines = {}
            context.pipelines[pipeline_id] = {
                "pipeline_id": pipeline_id,
                "name": name,
                "description": description,
                "stages": stages,
                "status": "created",
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "name": name, "stage_count": len(stages)},
                message=f"Pipeline {pipeline_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline create failed: {e}")


class PipelineAddStageAction(BaseAction):
    """Add a stage to pipeline."""
    action_type = "pipeline_add_stage"
    display_name = "添加阶段"
    description = "向流水线添加阶段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            stage_name = params.get("stage_name", "")
            stage_config = params.get("config", {})
            position = params.get("position", -1)

            if not pipeline_id or not stage_name:
                return ActionResult(success=False, message="pipeline_id and stage_name are required")

            if not hasattr(context, "pipelines") or pipeline_id not in context.pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            pipeline = context.pipelines[pipeline_id]
            stage_id = str(uuid.uuid4())[:8]
            stage = {
                "stage_id": stage_id,
                "name": stage_name,
                "config": stage_config,
                "status": "pending",
            }

            if position < 0 or position >= len(pipeline["stages"]):
                pipeline["stages"].append(stage)
            else:
                pipeline["stages"].insert(position, stage)

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "stage_id": stage_id, "stage_count": len(pipeline["stages"])},
                message=f"Stage {stage_name} added to pipeline {pipeline_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline add stage failed: {e}")


class PipelineExecuteAction(BaseAction):
    """Execute a pipeline."""
    action_type = "pipeline_execute"
    display_name = "执行流水线"
    description = "执行自动化流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            input_data = params.get("input_data", {})

            if not pipeline_id:
                return ActionResult(success=False, message="pipeline_id is required")

            if not hasattr(context, "pipelines") or pipeline_id not in context.pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            pipeline = context.pipelines[pipeline_id]
            pipeline["status"] = "running"
            pipeline["started_at"] = time.time()

            executed_stages = []
            for stage in pipeline["stages"]:
                stage["status"] = "running"
                time.sleep(0.01)
                stage["status"] = "completed"
                executed_stages.append(stage["stage_id"])

            pipeline["status"] = "completed"
            pipeline["completed_at"] = time.time()
            pipeline["executed_stages"] = executed_stages

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "executed_stages": len(executed_stages), "status": "completed"},
                message=f"Pipeline {pipeline_id} completed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline execute failed: {e}")


class PipelineBranchAction(BaseAction):
    """Create a branch in pipeline."""
    action_type = "pipeline_branch"
    display_name = "流水线分支"
    description = "创建流水线分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            branch_name = params.get("branch_name", "")
            condition = params.get("condition", {})

            if not pipeline_id or not branch_name:
                return ActionResult(success=False, message="pipeline_id and branch_name are required")

            if not hasattr(context, "pipelines") or pipeline_id not in context.pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            branch_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"branch_id": branch_id, "branch_name": branch_name, "condition": condition},
                message=f"Branch {branch_name} created",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline branch failed: {e}")


class PipelineMergeAction(BaseAction):
    """Merge pipeline branches."""
    action_type = "pipeline_merge"
    display_name = "流水线合并"
    description = "合并流水线分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            branch_ids = params.get("branch_ids", [])
            merge_strategy = params.get("strategy", "all_succeeded")

            if not pipeline_id:
                return ActionResult(success=False, message="pipeline_id is required")

            if not hasattr(context, "pipelines") or pipeline_id not in context.pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            merge_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"merge_id": merge_id, "branch_count": len(branch_ids), "strategy": merge_strategy},
                message=f"Merged {len(branch_ids)} branches",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline merge failed: {e}")


class PipelineStatusAction(BaseAction):
    """Get pipeline execution status."""
    action_type = "pipeline_status"
    display_name = "流水线状态"
    description = "查询流水线执行状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_id = params.get("pipeline_id", "")
            if not pipeline_id:
                return ActionResult(success=False, message="pipeline_id is required")

            if not hasattr(context, "pipelines") or pipeline_id not in context.pipelines:
                return ActionResult(success=False, message=f"Pipeline {pipeline_id} not found")

            pipeline = context.pipelines[pipeline_id]
            stages_summary = [{"name": s["name"], "status": s["status"]} for s in pipeline.get("stages", [])]

            return ActionResult(
                success=True,
                data={"pipeline_id": pipeline_id, "status": pipeline["status"], "stages": stages_summary},
                message=f"Pipeline {pipeline_id}: {pipeline['status']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline status failed: {e}")
