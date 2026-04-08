"""ETL Pipeline action module for RabAI AutoClick.

Provides Extract-Transform-Load pipeline orchestration with stage
definition, data validation, error handling, and checkpoint support.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import csv
import io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PipelineStatus(Enum):
    """ETL pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageType(Enum):
    """ETL stage types."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    VALIDATE = "validate"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"


@dataclass
class ETLStage:
    """Represents a single ETL pipeline stage."""
    name: str
    stage_type: StageType
    config: Dict[str, Any] = field(default_factory=dict)
    transform_func: Optional[str] = None  # Name of registered function
    error_handler: Optional[str] = None
    continue_on_error: bool = False
    timeout_seconds: float = 300.0
    description: str = ""


@dataclass
class PipelineRun:
    """Tracks a pipeline execution run."""
    run_id: str
    pipeline_name: str
    status: PipelineStatus = PipelineStatus.IDLE
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    stages_completed: List[str] = field(default_factory=list)
    stages_failed: List[str] = field(default_factory=list)
    records_processed: int = 0
    records_failed: int = 0
    stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    error_log: List[Dict[str, Any]] = field(default_factory=list)
    checkpoint_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ETLRecord:
    """Represents a single record in the ETL pipeline."""
    data: Dict[str, Any]
    source: str = ""
    stage: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ValidationRule:
    """Data validation rule for ETL records."""
    
    def __init__(self, field_name: str, rule_type: str, rule_config: Dict[str, Any]):
        self.field_name = field_name
        self.rule_type = rule_type  # required, type, range, pattern, custom
        self.rule_config = rule_config
    
    def validate(self, record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate a record against this rule.
        
        Returns:
            Tuple of (is_valid, error_message).
        """
        value = record.get(self.field_name)
        
        if self.rule_type == "required":
            if value is None or value == "":
                return False, f"Field '{self.field_name}' is required"
        
        elif self.rule_type == "type":
            expected_type = self.rule_config.get("expected", "string")
            type_map = {"string": str, "int": int, "float": float, "bool": bool, "list": list, "dict": dict}
            if expected_type in type_map and not isinstance(value, type_map[expected_type]):
                return False, f"Field '{self.field_name}' must be of type {expected_type}"
        
        elif self.rule_type == "range":
            min_val = self.rule_config.get("min")
            max_val = self.rule_config.get("max")
            if value is not None:
                if min_val is not None and value < min_val:
                    return False, f"Field '{self.field_name}' value {value} below minimum {min_val}"
                if max_val is not None and value > max_val:
                    return False, f"Field '{self.field_name}' value {value} above maximum {max_val}"
        
        elif self.rule_type == "pattern":
            import re
            pattern = self.rule_config.get("pattern", "")
            if value is not None and not re.match(pattern, str(value)):
                return False, f"Field '{self.field_name}' does not match pattern {pattern}"
        
        elif self.rule_type == "enum":
            allowed = self.rule_config.get("values", [])
            if value not in allowed:
                return False, f"Field '{self.field_name}' value must be one of {allowed}"
        
        return True, None


class ETLPipeline:
    """ETL Pipeline orchestrator."""
    
    def __init__(self, name: str):
        self.name = name
        self._stages: List[ETLStage] = []
        self._transform_registry: Dict[str, Callable] = {}
        self._validation_rules: List[ValidationRule] = []
        self._checkpoint_enabled = False
        self._checkpoint_interval = 100  # records
        self._current_run: Optional[PipelineRun] = None
    
    def add_stage(self, stage: ETLStage) -> "ETLPipeline":
        """Add a stage to the pipeline. Returns self for chaining."""
        self._stages.append(stage)
        return self
    
    def register_transform(self, name: str, func: Callable) -> None:
        """Register a transform function."""
        self._transform_registry[name] = func
    
    def add_validation_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        self._validation_rules.append(rule)
    
    def enable_checkpoints(self, interval: int = 100) -> None:
        """Enable checkpointing for the pipeline."""
        self._checkpoint_enabled = True
        self._checkpoint_interval = interval
    
    def validate_record(self, record: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate a record against all rules.
        
        Returns:
            Tuple of (is_valid, list of error messages).
        """
        errors = []
        for rule in self._validation_rules:
            is_valid, error_msg = rule.validate(record)
            if not is_valid and error_msg:
                errors.append(error_msg)
        return len(errors) == 0, errors
    
    def _execute_extract(self, stage: ETLStage, 
                         input_data: Any) -> List[ETLRecord]:
        """Execute an extract stage."""
        records = []
        source_type = stage.config.get("source_type", "memory")
        
        if source_type == "memory":
            data = stage.config.get("data", [])
            if isinstance(data, list):
                for item in data:
                    records.append(ETLRecord(data=item, source=stage.name, stage=stage.name))
            else:
                records.append(ETLRecord(data=data, source=stage.name, stage=stage.name))
        
        elif source_type == "csv":
            csv_data = stage.config.get("csv_string", "")
            reader = csv.DictReader(io.StringIO(csv_data))
            for row in reader:
                records.append(ETLRecord(data=dict(row), source=stage.name, stage=stage.name))
        
        elif source_type == "json":
            json_data = stage.config.get("json_string", "[]")
            data = json.loads(json_data)
            if isinstance(data, list):
                for item in data:
                    records.append(ETLRecord(data=item, source=stage.name, stage=stage.name))
            else:
                records.append(ETLRecord(data=data, source=stage.name, stage=stage.name))
        
        elif source_type == "file":
            file_path = stage.config.get("file_path", "")
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    if file_path.endswith('.json'):
                        data = json.load(f)
                    else:
                        reader = csv.DictReader(f)
                        data = list(reader)
                    if isinstance(data, list):
                        for item in data:
                            records.append(ETLRecord(data=item, source=stage.name, stage=stage.name))
        
        return records
    
    def _execute_transform(self, stage: ETLStage,
                          records: List[ETLRecord]) -> List[ETLRecord]:
        """Execute a transform stage."""
        transform_name = stage.config.get("transform", "passthrough")
        
        if transform_name == "passthrough":
            return records
        
        if transform_name == "rename_fields":
            mapping = stage.config.get("field_mapping", {})
            for record in records:
                record.data = {mapping.get(k, k): v for k, v in record.data.items()}
            return records
        
        if transform_name == "add_field":
            field_name = stage.config.get("field_name", "")
            field_value = stage.config.get("field_value")
            for record in records:
                record.data[field_name] = field_value
            return records
        
        if transform_name == "filter_fields":
            keep_fields = stage.config.get("keep_fields", [])
            for record in records:
                record.data = {k: v for k, v in record.data.items() if k in keep_fields}
            return records
        
        if transform_name == "map_values":
            mapping = stage.config.get("value_mapping", {})
            field_name = stage.config.get("field_name", "")
            for record in records:
                if field_name in record.data:
                    record.data[field_name] = mapping.get(
                        record.data[field_name], record.data[field_name]
                    )
            return records
        
        # Custom transform function
        transform_func = self._transform_registry.get(transform_name)
        if transform_func:
            for record in records:
                record.data = transform_func(record.data)
        
        return records
    
    def _execute_filter(self, stage: ETLStage,
                       records: List[ETLRecord]) -> List[ETLRecord]:
        """Execute a filter stage."""
        filter_expr = stage.config.get("expression", "")
        
        if filter_expr == "none" or not filter_expr:
            return records
        
        # Simple key-value filter
        filter_field = stage.config.get("field", "")
        filter_value = stage.config.get("value")
        filter_operator = stage.config.get("operator", "equals")
        
        filtered = []
        for record in records:
            record_value = record.data.get(filter_field)
            if filter_operator == "equals" and record_value == filter_value:
                filtered.append(record)
            elif filter_operator == "not_equals" and record_value != filter_value:
                filtered.append(record)
            elif filter_operator == "contains" and filter_value in str(record_value):
                filtered.append(record)
            elif filter_operator == "greater_than" and record_value > filter_value:
                filtered.append(record)
            elif filter_operator == "less_than" and record_value < filter_value:
                filtered.append(record)
        
        return filtered
    
    def _execute_load(self, stage: ETLStage,
                     records: List[ETLRecord]) -> Dict[str, Any]:
        """Execute a load stage."""
        target_type = stage.config.get("target_type", "memory")
        
        loaded_count = 0
        results = []
        
        if target_type == "memory":
            # Store in memory buffer
            results = [r.data for r in records]
            loaded_count = len(records)
        
        elif target_type == "json_file":
            file_path = stage.config.get("file_path", "")
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
            with open(file_path, 'w') as f:
                json.dump([r.data for r in records], f, indent=2, default=str)
            loaded_count = len(records)
        
        elif target_type == "csv_file":
            file_path = stage.config.get("file_path", "")
            if records:
                os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
                with open(file_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=records[0].data.keys())
                    writer.writeheader()
                    writer.writerows([r.data for r in records])
            loaded_count = len(records)
        
        elif target_type == "stdout":
            for record in records:
                print(json.dumps(record.data, default=str))
            loaded_count = len(records)
        
        return {"loaded_count": loaded_count, "results": results}
    
    async def run_async(self, input_data: Any,
                        run_id: Optional[str] = None) -> PipelineRun:
        """Run the pipeline asynchronously.
        
        Args:
            input_data: Input data for the extract stage.
            run_id: Optional custom run ID.
        
        Returns:
            The completed PipelineRun.
        """
        import uuid
        run = PipelineRun(
            run_id=run_id or str(uuid.uuid4()),
            pipeline_name=self.name,
            status=PipelineStatus.RUNNING,
            start_time=time.time()
        )
        self._current_run = run
        
        records: List[ETLRecord] = []
        stage_index = 0
        
        try:
            for stage in self._stages:
                stage_start = time.time()
                stage_records = 0
                stage_error = None
                
                try:
                    if stage.stage_type == StageType.EXTRACT:
                        records = self._execute_extract(stage, input_data)
                        stage_records = len(records)
                    
                    elif stage.stage_type == StageType.TRANSFORM:
                        records = self._execute_transform(stage, records)
                        stage_records = len(records)
                    
                    elif stage.stage_type == StageType.FILTER:
                        records = self._execute_filter(stage, records)
                        stage_records = len(records)
                    
                    elif stage.stage_type == StageType.VALIDATE:
                        valid_records = []
                        for record in records:
                            is_valid, errors = self.validate_record(record.data)
                            if is_valid:
                                valid_records.append(record)
                            else:
                                run.error_log.append({
                                    "stage": stage.name,
                                    "record": record.data,
                                    "errors": errors
                                })
                        records = valid_records
                        stage_records = len(records)
                    
                    elif stage.stage_type == StageType.LOAD:
                        load_result = self._execute_load(stage, records)
                        stage_records = load_result["loaded_count"]
                    
                    run.stages_completed.append(stage.name)
                    run.stage_metrics[stage.name] = {
                        "records": stage_records,
                        "duration_ms": (time.time() - stage_start) * 1000
                    }
                    run.records_processed += stage_records
                    
                    # Checkpoint
                    if self._checkpoint_enabled and stage_index % self._checkpoint_interval == 0:
                        run.checkpoint_data[stage.name] = {
                            "processed": run.records_processed,
                            "timestamp": time.time()
                        }
                
                except Exception as e:
                    run.stages_failed.append(stage.name)
                    stage_error = str(e)
                    run.error_log.append({
                        "stage": stage.name,
                        "error": stage_error
                    })
                    
                    if not stage.continue_on_error:
                        run.status = PipelineStatus.FAILED
                        run.error_message = f"Stage '{stage.name}' failed: {stage_error}"
                        run.end_time = time.time()
                        return run
                
                stage_index += 1
            
            run.status = PipelineStatus.COMPLETED
            run.end_time = time.time()
        
        except Exception as e:
            run.status = PipelineStatus.FAILED
            run.error_message = str(e)
            run.end_time = time.time()
        
        return run


class ETLPipelineAction(BaseAction):
    """Execute ETL (Extract-Transform-Load) pipelines.
    
    Supports CSV/JSON/file extraction, field mapping, filtering,
    validation, and loading to various targets with checkpoint support.
    """
    action_type = "etl_pipeline"
    display_name = "ETL流水线"
    description = "执行ETL数据流水线，支持抽取、转换、加载和校验"
    
    def __init__(self):
        super().__init__()
        self._pipelines: Dict[str, ETLPipeline] = {}
    
    def _get_or_create_pipeline(self, params: Dict[str, Any]) -> ETLPipeline:
        """Get or create a pipeline by name."""
        name = params.get("pipeline_name", "default")
        if name not in self._pipelines:
            self._pipelines[name] = ETLPipeline(name)
        return self._pipelines[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute ETL pipeline operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "create_pipeline", "add_stage", "run", "validate",
                  "register_transform", "get_run_status"
                - For create: pipeline_name
                - For add_stage: pipeline_name, stage (dict)
                - For run: pipeline_name, input_data
                - For validate: pipeline_name, record (dict)
                - For register_transform: pipeline_name, transform_name
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "create_pipeline":
                return self._create_pipeline(params)
            elif operation == "add_stage":
                return self._add_stage(params)
            elif operation == "run":
                return self._run_pipeline(params)
            elif operation == "validate":
                return self._validate_record(params)
            elif operation == "register_transform":
                return self._register_transform(params)
            elif operation == "get_run_status":
                return self._get_run_status(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"ETL pipeline error: {str(e)}")
    
    def _create_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new ETL pipeline."""
        name = params.get("pipeline_name", "default")
        pipeline = ETLPipeline(name)
        
        if params.get("enable_checkpoints"):
            pipeline.enable_checkpoints(params.get("checkpoint_interval", 100))
        
        self._pipelines[name] = pipeline
        return ActionResult(
            success=True,
            message=f"Pipeline '{name}' created",
            data={"pipeline_name": name}
        )
    
    def _add_stage(self, params: Dict[str, Any]) -> ActionResult:
        """Add a stage to a pipeline."""
        pipeline = self._get_or_create_pipeline(params)
        stage_data = params.get("stage", {})
        
        if not stage_data or "name" not in stage_data or "stage_type" not in stage_data:
            return ActionResult(success=False, message="stage with name and stage_type is required")
        
        stage = ETLStage(
            name=stage_data["name"],
            stage_type=StageType(stage_data["stage_type"]),
            config=stage_data.get("config", {}),
            description=stage_data.get("description", ""),
            continue_on_error=stage_data.get("continue_on_error", False)
        )
        pipeline.add_stage(stage)
        return ActionResult(
            success=True,
            message=f"Stage '{stage.name}' added to pipeline '{pipeline.name}'",
            data={"stage_name": stage.name, "stage_type": stage.stage_type.value}
        )
    
    def _run_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Run an ETL pipeline."""
        pipeline = self._get_or_create_pipeline(params)
        input_data = params.get("input_data")
        
        if not pipeline._stages:
            return ActionResult(success=False, message="Pipeline has no stages")
        
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            run = loop.run_until_complete(pipeline.run_async(input_data))
            return ActionResult(
                success=run.status == PipelineStatus.COMPLETED,
                message=f"Pipeline {run.status.value}: {run.error_message or 'completed'}",
                data={
                    "run_id": run.run_id,
                    "status": run.status.value,
                    "stages_completed": run.stages_completed,
                    "stages_failed": run.stages_failed,
                    "records_processed": run.records_processed,
                    "records_failed": run.records_failed,
                    "duration_seconds": (run.end_time - run.start_time) if run.end_time else None,
                    "stage_metrics": run.stage_metrics
                }
            )
        finally:
            loop.close()
    
    def _validate_record(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a record against pipeline rules."""
        pipeline = self._get_or_create_pipeline(params)
        record = params.get("record", {})
        
        # Add validation rules if specified
        rules_data = params.get("rules", [])
        for rule_data in rules_data:
            rule = ValidationRule(
                field_name=rule_data["field_name"],
                rule_type=rule_data["rule_type"],
                rule_config=rule_data.get("rule_config", {})
            )
            pipeline.add_validation_rule(rule)
        
        is_valid, errors = pipeline.validate_record(record)
        return ActionResult(
            success=True,
            message="Record is valid" if is_valid else f"Validation failed: {errors}",
            data={"is_valid": is_valid, "errors": errors}
        )
    
    def _register_transform(self, params: Dict[str, Any]) -> ActionResult:
        """Register a custom transform function."""
        pipeline = self._get_or_create_pipeline(params)
        transform_name = params.get("transform_name", "")
        
        if not transform_name:
            return ActionResult(success=False, message="transform_name is required")
        
        # Placeholder transform - in real usage would register actual function
        def placeholder_transform(data: Dict[str, Any]) -> Dict[str, Any]:
            return data
        
        pipeline.register_transform(transform_name, placeholder_transform)
        return ActionResult(
            success=True,
            message=f"Transform '{transform_name}' registered"
        )
    
    def _get_run_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get the status of the last pipeline run."""
        pipeline_name = params.get("pipeline_name", "default")
        pipeline = self._pipelines.get(pipeline_name)
        
        if not pipeline or not pipeline._current_run:
            return ActionResult(success=False, message=f"No run found for pipeline '{pipeline_name}'")
        
        run = pipeline._current_run
        return ActionResult(
            success=True,
            message=f"Run {run.status.value}",
            data={
                "run_id": run.run_id,
                "status": run.status.value,
                "records_processed": run.records_processed,
                "stages_completed": run.stages_completed,
                "stages_failed": run.stages_failed
            }
        )
