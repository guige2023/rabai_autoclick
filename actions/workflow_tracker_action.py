"""
Workflow Tracker Action Module.

Tracks workflow execution progress, estimates completion time,
and monitors resource usage with alerting thresholds.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class WorkflowProgress:
    """Progress information for a workflow."""
    workflow_id: str
    total_steps: int
    completed_steps: int
    failed_steps: int
    progress_percent: float
    elapsed_seconds: float
    estimated_remaining_seconds: float
    status: str


class WorkflowTrackerAction(BaseAction):
    """Track workflow execution progress."""

    def __init__(self) -> None:
        super().__init__("workflow_tracker")
        self._workflows: dict[str, dict[str, Any]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Track or report workflow progress.

        Args:
            context: Execution context
            params: Parameters:
                - action: track, report, or alert
                - workflow_id: Workflow identifier
                - total_steps: Total number of steps
                - completed_steps: Steps completed so far
                - failed_steps: Steps that failed
                - alert_threshold: Progress % below which to alert

        Returns:
            WorkflowProgress or alert information
        """
        import time

        action = params.get("action", "track")
        workflow_id = params.get("workflow_id", "default")

        if action == "track":
            total = params.get("total_steps", 1)
            completed = params.get("completed_steps", 0)
            failed = params.get("failed_steps", 0)

            now = time.time()
            if workflow_id not in self._workflows:
                self._workflows[workflow_id] = {
                    "start_time": now,
                    "total_steps": total,
                    "completed_steps": 0,
                    "failed_steps": 0
                }

            wf = self._workflows[workflow_id]
            wf["completed_steps"] = completed
            wf["failed_steps"] = failed
            wf["last_update"] = now

            elapsed = now - wf["start_time"]
            progress = completed / total if total > 0 else 0.0
            avg_time_per_step = elapsed / completed if completed > 0 else 0
            eta = avg_time_per_step * (total - completed - failed)

            return {
                "workflow_id": workflow_id,
                "progress_percent": progress * 100,
                "completed": completed,
                "failed": failed,
                "remaining": total - completed - failed,
                "elapsed_seconds": elapsed,
                "estimated_remaining_seconds": eta
            }

        elif action == "report":
            wf = self._workflows.get(workflow_id, {})
            if not wf:
                return {"workflow_id": workflow_id, "status": "not_found"}

            total = wf.get("total_steps", 1)
            completed = wf.get("completed_steps", 0)
            failed = wf.get("failed_steps", 0)
            elapsed = time.time() - wf.get("start_time", time.time())

            return WorkflowTrackerAction._build_progress(
                workflow_id, total, completed, failed, elapsed
            ).__dict__

        elif action == "alert":
            threshold = params.get("alert_threshold", 50)
            wf = self._workflows.get(workflow_id, {})
            if not wf:
                return {"alert": False, "reason": "workflow not found"}

            progress = (wf.get("completed_steps", 0) / wf.get("total_steps", 1)) * 100
            if progress < threshold:
                return {"alert": True, "workflow_id": workflow_id, "progress": progress, "threshold": threshold}
            return {"alert": False}

        return {"error": f"Unknown action: {action}"}

    @staticmethod
    def _build_progress(workflow_id: str, total: int, completed: int, failed: int, elapsed: float) -> WorkflowProgress:
        """Build progress object."""
        progress = completed / total if total > 0 else 0.0
        avg_time = elapsed / completed if completed > 0 else 0
        eta = avg_time * (total - completed - failed)
        status = "running" if completed < total and failed == 0 else "failed" if failed > 0 else "completed"
        return WorkflowProgress(
            workflow_id=workflow_id,
            total_steps=total,
            completed_steps=completed,
            failed_steps=failed,
            progress_percent=progress * 100,
            elapsed_seconds=elapsed,
            estimated_remaining_seconds=eta,
            status=status
        )
