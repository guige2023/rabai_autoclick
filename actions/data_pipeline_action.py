"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline orchestration:
- DataPipelineAction: Multi-stage data processing pipeline
- DataPipelineExecutorAction: Execute pipeline stages
- DataPipelineBuilderAction: Build pipelines from components
- DataPipelineMonitorAction: Monitor pipeline health
- DataPipelineSchedulerAction: Schedule pipeline runs
"""

import time
import hashlib
import json
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineState(str, Enum):
    """Pipeline states."""
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageStatus(str, Enum):
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"
    RETRYING = "retrying"


class DataPipelineAction(BaseAction):
    """Multi-stage data processing pipeline."""
    action_type = "data_pipeline"
    display_name = "数据管道"
    description = "多阶段数据处理管道"

    def __init__(self):
        super().__init__()
        self._pipelines: Dict[str, Dict] = {}
        self._pipeline_runs: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            pipeline_name = params.get("pipeline_name", "")

            if operation == "create":
                if not pipeline_name:
                    return ActionResult(success=False, message="pipeline_name required")

                stages = params.get("stages", [])
                self._pipelines[pipeline_name] = {
                    "name": pipeline_name,
                    "stages": stages,
                    "created_at": time.time(),
                    "run_count": 0,
                    "state": PipelineState.CREATED,
                    "config": params.get("config", {})
                }
                self._pipeline_runs[pipeline_name] = []

                return ActionResult(
                    success=True,
                    data={"pipeline": pipeline_name, "stages": len(stages)},
                    message=f"Pipeline '{pipeline_name}' created with {len(stages)} stages"
                )

            elif operation == "run":
                if pipeline_name not in self._pipelines:
                    return ActionResult(success=False, message=f"Pipeline '{pipeline_name}' not found")

                pipeline = self._pipelines[pipeline_name]
                pipeline["run_count"] += 1
                run_id = f"{pipeline_name}_run_{pipeline['run_count']}"
                input_data = params.get("input_data", {})
                max_retries = params.get("max_retries", 1)

                run_record = {
                    "run_id": run_id,
                    "started_at": time.time(),
                    "stage_results": [],
                    "status": PipelineState.RUNNING
                }
                self._pipeline_runs[pipeline_name].append(run_record)

                stage_outputs = {}
                overall_success = True

                for i, stage in enumerate(pipeline["stages"]):
                    stage_start = time.time()
                    stage_name = stage.get("name", f"stage_{i}")
                    stage_type = stage.get("type", "process")
                    stage_input = stage.get("input", {})
                    stage_config = stage.get("config", {})

                    retry_count = 0
                    stage_success = False
                    stage_result = None

                    while retry_count <= max_retries and not stage_success:
                        try:
                            stage_result = self._execute_stage(stage_type, stage_input, stage_config, stage_outputs, input_data)
                            stage_success = True
                        except Exception as e:
                            retry_count += 1
                            if retry_count > max_retries:
                                stage_result = {"error": str(e)}
                                overall_success = False
                                break
                            time.sleep(0.1 * retry_count)

                    stage_outputs[stage_name] = stage_result
                    run_record["stage_results"].append({
                        "stage": i,
                        "name": stage_name,
                        "type": stage_type,
                        "status": StageStatus.SUCCESS.value if stage_success else StageStatus.FAILED.value,
                        "duration": time.time() - stage_start,
                        "result": stage_result
                    })

                    if not stage_success:
                        break

                run_record["completed_at"] = time.time()
                run_record["status"] = PipelineState.COMPLETED if overall_success else PipelineState.FAILED
                run_record["total_duration"] = run_record["completed_at"] - run_record["started_at"]

                if len(self._pipeline_runs[pipeline_name]) > 100:
                    self._pipeline_runs[pipeline_name] = self._pipeline_runs[pipeline_name][-100:]

                return ActionResult(
                    success=overall_success,
                    data={
                        "run_id": run_id,
                        "pipeline": pipeline_name,
                        "status": run_record["status"].value,
                        "stages_completed": len([s for s in run_record["stage_results"] if s["status"] == StageStatus.SUCCESS.value]),
                        "total_stages": len(pipeline["stages"]),
                        "duration": round(run_record["total_duration"], 3),
                        "stage_results": run_record["stage_results"]
                    },
                    message=f"Pipeline '{pipeline_name}': {run_record['status'].value} in {run_record['total_duration']:.2f}s"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "pipelines": [
                            {"name": k, "stages": len(v["stages"]), "runs": v["run_count"], "state": v["state"].value}
                            for k, v in self._pipelines.items()
                        ]
                    }
                )

            elif operation == "history":
                if pipeline_name not in self._pipeline_runs:
                    return ActionResult(success=False, message=f"No runs for '{pipeline_name}'")

                runs = self._pipeline_runs[pipeline_name][-10:]
                return ActionResult(
                    success=True,
                    data={"pipeline": pipeline_name, "runs": runs, "total_runs": len(self._pipeline_runs[pipeline_name])}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline error: {str(e)}")

    def _execute_stage(self, stage_type: str, stage_input: Dict, stage_config: Dict, previous_outputs: Dict, input_data: Dict) -> Dict:
        if stage_type == "extract":
            return self._stage_extract(stage_input, input_data)
        elif stage_type == "transform":
            return self._stage_transform(stage_input, previous_outputs)
        elif stage_type == "load":
            return self._stage_load(stage_input, previous_outputs)
        elif stage_type == "filter":
            return self._stage_filter(stage_input, previous_outputs)
        elif stage_type == "aggregate":
            return self._stage_aggregate(stage_input, previous_outputs)
        elif stage_type == "join":
            return self._stage_join(stage_input, previous_outputs)
        elif stage_type == "process":
            return self._stage_process(stage_input, previous_outputs)
        else:
            return {"status": "executed", "type": stage_type}

    def _stage_extract(self, input_def: Dict, input_data: Dict) -> Dict:
        source = input_def.get("source", "input")
        if source == "input":
            return {"data": input_data.get("records", [])}
        return {"data": []}

    def _stage_transform(self, input_def: Dict, previous_outputs: Dict) -> Dict:
        source_stage = input_def.get("from_stage")
        transform_type = input_def.get("transform", "passthrough")

        if source_stage and source_stage in previous_outputs:
            data = previous_outputs[source_stage].get("data", [])
        else:
            data = []

        if transform_type == "passthrough":
            return {"data": data, "transformed": len(data)}
        elif transform_type == "map":
            field = input_def.get("field", "")
            func_name = input_def.get("func", "str.upper")
            if func_name == "str.upper":
                processed = [{**row, field: row.get(field, "").upper() if isinstance(row.get(field), str) else row.get(field)} for row in data]
            else:
                processed = data
            return {"data": processed, "transformed": len(processed)}
        elif transform_type == "filter":
            field = input_def.get("field", "")
            condition = input_def.get("condition", "")
            operator = input_def.get("operator", "eq")
            value = input_def.get("value")
            filtered = [row for row in data if self._evaluate_condition(row.get(field), operator, value)]
            return {"data": filtered, "filtered": len(data) - len(filtered)}
        else:
            return {"data": data}

    def _stage_filter(self, input_def: Dict, previous_outputs: Dict) -> Dict:
        source_stage = input_def.get("from_stage", "")
        if source_stage in previous_outputs:
            data = previous_outputs[source_stage].get("data", [])
        else:
            data = []
        return {"data": data, "filtered_count": 0}

    def _stage_aggregate(self, input_def: Dict, previous_outputs: Dict) -> Dict:
        source_stage = input_def.get("from_stage", "")
        if source_stage in previous_outputs:
            data = previous_outputs[source_stage].get("data", [])
        else:
            data = []

        agg_type = input_def.get("agg_type", "count")
        group_by = input_def.get("group_by", [])

        if agg_type == "count":
            if group_by:
                groups = {}
                for row in data:
                    key = tuple(row.get(g) for g in group_by)
                    groups[key] = groups.get(key, 0) + 1
                return {"data": [{**dict(zip(group_by, k)), "count": v} for k, v in groups.items()]}
            return {"data": [{"count": len(data)}]}
        elif agg_type == "sum":
            field = input_def.get("field", "value")
            return {"data": [{"sum": sum(row.get(field, 0) for row in data)}]}
        elif agg_type == "avg":
            field = input_def.get("field", "value")
            values = [row.get(field, 0) for row in data]
            return {"data": [{"avg": sum(values) / len(values) if values else 0}]}
        return {"data": data}

    def _stage_join(self, input_def: Dict, previous_outputs: Dict) -> Dict:
        left_stage = input_def.get("left_stage", "")
        right_stage = input_def.get("right_stage", "")
        join_key = input_def.get("join_key", "id")
        join_type = input_def.get("join_type", "inner")

        left_data = previous_outputs.get(left_stage, {}).get("data", []) if left_stage else []
        right_data = previous_outputs.get(right_stage, {}).get("data", []) if right_stage else []

        right_index = {row.get(join_key): row for row in right_data if row.get(join_key)}
        joined = []

        for left_row in left_data:
            key = left_row.get(join_key)
            if key in right_index:
                joined.append({**left_row, **right_index[key]})
            elif join_type == "left":
                joined.append({**left_row, **{f"{k}_right": v for k, v in right_index.get(key, {}).items()}})

        return {"data": joined, "joined": len(joined)}

    def _stage_load(self, input_def: Dict, previous_outputs: Dict) -> Dict:
        target = input_def.get("target", "output")
        return {"loaded": True, "target": target}

    def _stage_process(self, input_def: Dict, previous_outputs: Dict) -> Dict:
        return {"processed": True}

    def _evaluate_condition(self, value: Any, operator: str, compare_value: Any) -> bool:
        if operator == "eq":
            return value == compare_value
        elif operator == "ne":
            return value != compare_value
        elif operator == "gt":
            return value > compare_value
        elif operator == "lt":
            return value < compare_value
        elif operator == "gte":
            return value >= compare_value
        elif operator == "lte":
            return value <= compare_value
        elif operator == "in":
            return value in compare_value if isinstance(compare_value, list) else False
        elif operator == "not_null":
            return value is not None
        return True


class DataPipelineExecutorAction(BaseAction):
    """Execute pipeline stages with resource management."""
    action_type = "data_pipeline_executor"
    display_name = "数据管道执行器"
    description = "管道执行与资源管理"

    def __init__(self):
        super().__init__()
        self._executors: Dict[str, Dict] = {}
        self._execution_queue: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "submit")
            stage_name = params.get("stage_name", "")

            if operation == "submit":
                if not stage_name:
                    return ActionResult(success=False, message="stage_name required")

                execution_id = f"exec_{int(time.time() * 1000)}"
                self._executors[execution_id] = {
                    "execution_id": execution_id,
                    "stage_name": stage_name,
                    "config": params.get("config", {}),
                    "status": StageStatus.PENDING,
                    "submitted_at": time.time(),
                    "started_at": None,
                    "completed_at": None,
                    "result": None
                }
                self._execution_queue.append(execution_id)

                return ActionResult(
                    success=True,
                    data={"execution_id": execution_id, "queue_position": len(self._execution_queue)},
                    message=f"Stage '{stage_name}' submitted"
                )

            elif operation == "execute":
                execution_id = params.get("execution_id", "")
                if execution_id not in self._executors:
                    return ActionResult(success=False, message=f"Execution '{execution_id}' not found")

                exec_record = self._executors[execution_id]
                exec_record["status"] = StageStatus.RUNNING
                exec_record["started_at"] = time.time()

                try:
                    result = self._do_execute(exec_record["stage_name"], exec_record["config"])
                    exec_record["status"] = StageStatus.SUCCESS
                    exec_record["result"] = result
                    exec_record["completed_at"] = time.time()
                    return ActionResult(
                        success=True,
                        data={"execution_id": execution_id, "result": result},
                        message=f"Execution '{execution_id}' succeeded"
                    )
                except Exception as e:
                    exec_record["status"] = StageStatus.FAILED
                    exec_record["result"] = {"error": str(e)}
                    exec_record["completed_at"] = time.time()
                    return ActionResult(
                        success=False,
                        data={"execution_id": execution_id, "error": str(e)},
                        message=f"Execution '{execution_id}' failed"
                    )

            elif operation == "status":
                execution_id = params.get("execution_id", "")
                if execution_id:
                    if execution_id not in self._executors:
                        return ActionResult(success=False, message=f"Execution '{execution_id}' not found")
                    exec_record = self._executors[execution_id]
                    return ActionResult(
                        success=True,
                        data={
                            "execution_id": execution_id,
                            "stage_name": exec_record["stage_name"],
                            "status": exec_record["status"].value,
                            "duration": exec_record["completed_at"] - exec_record["started_at"] if exec_record["completed_at"] else None
                        }
                    )
                else:
                    active = [eid for eid, e in self._executors.items() if e["status"] == StageStatus.RUNNING.value]
                    pending = [eid for eid, e in self._executors.items() if e["status"] == StageStatus.PENDING.value]
                    return ActionResult(
                        success=True,
                        data={"active": len(active), "pending": len(pending), "total": len(self._executors)}
                    )

            elif operation == "cancel":
                execution_id = params.get("execution_id", "")
                if execution_id in self._executors:
                    self._executors[execution_id]["status"] = StageStatus.FAILED
                    self._executors[execution_id]["result"] = {"cancelled": True}
                return ActionResult(success=True, message=f"Execution '{execution_id}' cancelled")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline executor error: {str(e)}")

    def _do_execute(self, stage_name: str, config: Dict) -> Dict:
        time.sleep(0.01)
        return {"executed": stage_name, "config": config}


class DataPipelineMonitorAction(BaseAction):
    """Monitor pipeline health and performance."""
    action_type = "data_pipeline_monitor"
    display_name = "数据管道监控"
    description = "管道健康监控"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, List[Dict]] = {}
        self._alerts: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "record")
            pipeline_name = params.get("pipeline_name", "")

            if operation == "record":
                if not pipeline_name:
                    return ActionResult(success=False, message="pipeline_name required")

                if pipeline_name not in self._metrics:
                    self._metrics[pipeline_name] = []

                metric = {
                    "timestamp": time.time(),
                    "duration": params.get("duration", 0),
                    "records_in": params.get("records_in", 0),
                    "records_out": params.get("records_out", 0),
                    "error_count": params.get("error_count", 0),
                    "stage": params.get("stage", ""),
                    "status": params.get("status", "unknown")
                }

                self._metrics[pipeline_name].append(metric)
                if len(self._metrics[pipeline_name]) > 10000:
                    self._metrics[pipeline_name] = self._metrics[pipeline_name][-10000:]

                if metric["error_count"] > 0 or params.get("alert", False):
                    self._alerts.append({
                        "pipeline": pipeline_name,
                        "timestamp": time.time(),
                        "message": params.get("alert_message", "Pipeline issue detected"),
                        "severity": params.get("severity", "medium")
                    })

                return ActionResult(success=True, data={"recorded": metric})

            elif operation == "dashboard":
                if pipeline_name:
                    if pipeline_name not in self._metrics:
                        return ActionResult(success=False, message=f"No metrics for '{pipeline_name}'")

                    metrics = self._metrics[pipeline_name]
                    recent = metrics[-100:] if len(metrics) > 100 else metrics

                    if recent:
                        avg_duration = sum(m["duration"] for m in recent) / len(recent)
                        total_records_in = sum(m["records_in"] for m in recent)
                        total_records_out = sum(m["records_out"] for m in recent)
                        error_count = sum(m["error_count"] for m in recent)
                        success_rate = (len(recent) - sum(1 for m in recent if m["error_count"] > 0)) / len(recent)
                    else:
                        avg_duration = total_records_in = total_records_out = error_count = success_rate = 0

                    return ActionResult(
                        success=True,
                        data={
                            "pipeline": pipeline_name,
                            "samples": len(recent),
                            "avg_duration_ms": round(avg_duration, 2),
                            "total_records_in": total_records_in,
                            "total_records_out": total_records_out,
                            "error_count": error_count,
                            "success_rate": round(success_rate, 4)
                        }
                    )

                all_metrics = {}
                for pname, pm in self._metrics.items():
                    if pm:
                        recent = pm[-10:]
                        all_metrics[pname] = {
                            "samples": len(pm),
                            "avg_duration_ms": round(sum(m["duration"] for m in recent) / len(recent), 2) if recent else 0
                        }
                    else:
                        all_metrics[pname] = {"samples": 0, "avg_duration_ms": 0}

                return ActionResult(success=True, data={"pipelines": all_metrics})

            elif operation == "alerts":
                limit = params.get("limit", 50)
                return ActionResult(success=True, data={"alerts": self._alerts[-limit:]})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline monitor error: {str(e)}")


class DataPipelineSchedulerAction(BaseAction):
    """Schedule pipeline runs."""
    action_type = "data_pipeline_scheduler"
    display_name = "数据管道调度"
    description = "管道运行调度"

    def __init__(self):
        super().__init__()
        self._schedules: Dict[str, Dict] = {}
        self._schedule_history: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            schedule_name = params.get("schedule_name", "")

            if operation == "schedule":
                if not schedule_name:
                    return ActionResult(success=False, message="schedule_name required")

                self._schedules[schedule_name] = {
                    "name": schedule_name,
                    "pipeline_name": params.get("pipeline_name", ""),
                    "cron": params.get("cron", ""),
                    "interval_seconds": params.get("interval_seconds"),
                    "enabled": params.get("enabled", True),
                    "created_at": time.time(),
                    "last_run": None,
                    "next_run": None,
                    "run_count": 0
                }

                return ActionResult(
                    success=True,
                    data={"schedule": schedule_name},
                    message=f"Schedule '{schedule_name}' created"
                )

            elif operation == "trigger":
                if schedule_name not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_name}' not found")

                schedule = self._schedules[schedule_name]
                schedule["run_count"] += 1
                schedule["last_run"] = time.time()

                if schedule_name not in self._schedule_history:
                    self._schedule_history[schedule_name] = []
                self._schedule_history[schedule_name].append({
                    "triggered_at": time.time(),
                    "run_number": schedule["run_count"]
                })

                return ActionResult(
                    success=True,
                    data={"schedule": schedule_name, "run": schedule["run_count"]},
                    message=f"Schedule '{schedule_name}' triggered"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "schedules": [
                            {
                                "name": k,
                                "pipeline": v["pipeline_name"],
                                "enabled": v["enabled"],
                                "run_count": v["run_count"],
                                "last_run": v["last_run"]
                            }
                            for k, v in self._schedules.items()
                        ]
                    }
                )

            elif operation == "toggle":
                if schedule_name not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_name}' not found")

                self._schedules[schedule_name]["enabled"] = not self._schedules[schedule_name]["enabled"]
                return ActionResult(
                    success=True,
                    data={"schedule": schedule_name, "enabled": self._schedules[schedule_name]["enabled"]},
                    message=f"Schedule '{schedule_name}' {'enabled' if self._schedules[schedule_name]['enabled'] else 'disabled'}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline scheduler error: {str(e)}")
