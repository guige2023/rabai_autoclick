"""Data Pipeline Action Module.

Provides data pipeline utilities: ETL operations, transformations,
data enrichment, pipeline monitoring, and checkpoint management.

Example:
    result = execute(context, {"action": "extract", "source": {...}})
"""
from typing import Any, Optional, Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque


@dataclass
class PipelineStage:
    """A stage in the data pipeline."""
    
    name: str
    stage_type: str
    config: dict[str, Any] = field(default_factory=dict)
    transformers: list[Callable] = field(default_factory=list)
    
    def __post_init__(self) -> None:
        """Validate stage type."""
        valid_types = {"extract", "transform", "load", "filter", "aggregate", "enrich"}
        if self.stage_type not in valid_types:
            raise ValueError(f"Invalid stage_type: {self.stage_type}")


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics."""
    
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    records_in: int = 0
    records_out: int = 0
    records_failed: int = 0
    stages_completed: int = 0
    total_stages: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        """Get pipeline duration in seconds."""
        if self.end_time is None:
            end = datetime.now()
        else:
            end = self.end_time
        return (end - self.start_time).total_seconds()
    
    @property
    def throughput_records_per_second(self) -> float:
        """Get processing throughput."""
        duration = self.duration_seconds
        if duration > 0:
            return self.records_out / duration
        return 0.0


class Extractor:
    """Extracts data from various sources."""
    
    @staticmethod
    def from_dict(data: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
        """Extract from dictionary/list.
        
        Args:
            data: List of records
            
        Yields:
            Individual records
        """
        for record in data:
            yield record
    
    @staticmethod
    def from_csv_row(row: dict[str, str]) -> dict[str, Any]:
        """Extract from CSV row with type conversion.
        
        Args:
            row: CSV row as dictionary
            
        Returns:
            Converted record
        """
        converted = {}
        for key, value in row.items():
            if value is None or value == "":
                converted[key] = None
            elif value.lower() in ("true", "false"):
                converted[key] = value.lower() == "true"
            elif value.isdigit():
                converted[key] = int(value)
            else:
                try:
                    converted[key] = float(value)
                except ValueError:
                    converted[key] = value
        return converted
    
    @staticmethod
    def filter_fields(
        record: dict[str, Any],
        fields: list[str],
        exclude: bool = False,
    ) -> dict[str, Any]:
        """Filter record fields.
        
        Args:
            record: Input record
            fields: Fields to include/exclude
            exclude: If True, exclude fields; else include only
            
        Returns:
            Filtered record
        """
        if exclude:
            return {k: v for k, v in record.items() if k not in fields}
        return {k: v for k, v in record.items() if k in fields}


class Transformer:
    """Transforms data records."""
    
    @staticmethod
    def rename_fields(
        record: dict[str, Any],
        mapping: dict[str, str],
    ) -> dict[str, Any]:
        """Rename fields in record.
        
        Args:
            record: Input record
            mapping: Field name mapping
            
        Returns:
            Record with renamed fields
        """
        result = {}
        for key, value in record.items():
            new_key = mapping.get(key, key)
            result[new_key] = value
        return result
    
    @staticmethod
    def add_field(
        record: dict[str, Any],
        field_name: str,
        value: Any,
    ) -> dict[str, Any]:
        """Add computed field.
        
        Args:
            record: Input record
            field_name: New field name
            value: Field value or callable
            
        Returns:
            Record with new field
        """
        result = record.copy()
        if callable(value):
            result[field_name] = value(record)
        else:
            result[field_name] = value
        return result
    
    @staticmethod
    def remove_nulls(
        record: dict[str, Any],
        strategy: str = "drop",
    ) -> dict[str, Any]:
        """Handle null values in record.
        
        Args:
            record: Input record
            strategy: Strategy (drop, empty_string, none)
            
        Returns:
            Record with nulls handled
        """
        if strategy == "drop":
            return {k: v for k, v in record.items() if v is not None}
        elif strategy == "empty_string":
            return {k: (v if v is not None else "") for k, v in record.items()}
        return record
    
    @staticmethod
    def normalize_strings(
        record: dict[str, Any],
        fields: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Normalize string fields.
        
        Args:
            record: Input record
            fields: Fields to normalize (None = all strings)
            
        Returns:
            Record with normalized strings
        """
        result = {}
        for key, value in record.items():
            if isinstance(value, str):
                if fields is None or key in fields:
                    value = value.strip().lower()
            result[key] = value
        return result


class Loader:
    """Loads data to various destinations."""
    
    @staticmethod
    def to_dict_list(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Load records to list.
        
        Args:
            records: List of records
            
        Returns:
            Same list (for chaining)
        """
        return records
    
    @staticmethod
    def group_by(
        records: list[dict[str, Any]],
        key_field: str,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Group records by field.
        
        Args:
            records: List of records
            key_field: Field to group by
            
        Returns:
            Records grouped by key
        """
        groups: dict[Any, list[dict[str, Any]]] = {}
        for record in records:
            key = record.get(key_field)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)
        return groups


class DataEnricher:
    """Enriches data with additional information."""
    
    def __init__(self) -> None:
        """Initialize data enricher."""
        self._lookup_tables: dict[str, dict[Any, Any]] = {}
        self._enrichment_funcs: dict[str, Callable] = {}
    
    def add_lookup_table(
        self,
        name: str,
        data: dict[Any, Any],
    ) -> None:
        """Add a lookup table for enrichment.
        
        Args:
            name: Table name
            data: Lookup dictionary
        """
        self._lookup_tables[name] = data
    
    def add_enrichment_func(
        self,
        field_name: str,
        func: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Add enrichment function.
        
        Args:
            field_name: Field to enrich
            func: Enrichment function
        """
        self._enrichment_funcs[field_name] = func
    
    def enrich(
        self,
        record: dict[str, Any],
        lookups: list[str],
    ) -> dict[str, Any]:
        """Enrich record with lookup data.
        
        Args:
            record: Input record
            lookups: List of lookup table names
            
        Returns:
            Enriched record
        """
        result = record.copy()
        
        for lookup_name in lookups:
            if lookup_name not in self._lookup_tables:
                continue
            
            lookup_table = self._lookup_tables[lookup_name]
            
            for key, value in record.items():
                if key in lookup_table:
                    result[f"{lookup_name}_{key}"] = lookup_table[key].get(value)
        
        for field_name, func in self._enrichment_funcs.items():
            result[field_name] = func(record)
        
        return result


class CheckpointManager:
    """Manages pipeline checkpoints for recovery."""
    
    def __init__(self, checkpoint_dir: Optional[str] = None) -> None:
        """Initialize checkpoint manager.
        
        Args:
            checkpoint_dir: Directory for checkpoint files
        """
        self.checkpoint_dir = checkpoint_dir
        self._checkpoints: dict[str, dict[str, Any]] = {}
    
    def save_checkpoint(
        self,
        pipeline_name: str,
        stage_name: str,
        position: int,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Save pipeline checkpoint.
        
        Args:
            pipeline_name: Pipeline identifier
            stage_name: Current stage
            position: Current position
            metadata: Additional metadata
        """
        key = f"{pipeline_name}_{stage_name}"
        self._checkpoints[key] = {
            "pipeline_name": pipeline_name,
            "stage_name": stage_name,
            "position": position,
            "metadata": metadata or {},
            "timestamp": datetime.now().isoformat(),
        }
    
    def load_checkpoint(
        self,
        pipeline_name: str,
        stage_name: str,
    ) -> Optional[int]:
        """Load checkpoint position.
        
        Args:
            pipeline_name: Pipeline identifier
            stage_name: Stage name
            
        Returns:
            Checkpoint position or None
        """
        key = f"{pipeline_name}_{stage_name}"
        checkpoint = self._checkpoints.get(key)
        if checkpoint:
            return checkpoint.get("position")
        return None
    
    def clear_checkpoint(self, pipeline_name: str, stage_name: str) -> None:
        """Clear a checkpoint.
        
        Args:
            pipeline_name: Pipeline identifier
            stage_name: Stage name
        """
        key = f"{pipeline_name}_{stage_name}"
        if key in self._checkpoints:
            del self._checkpoints[key]


class Pipeline:
    """Orchestrates data pipeline execution."""
    
    def __init__(self, name: str) -> None:
        """Initialize pipeline.
        
        Args:
            name: Pipeline name
        """
        self.name = name
        self.stages: list[PipelineStage] = []
        self.metrics = PipelineMetrics(
            pipeline_name=name,
            start_time=datetime.now(),
        )
    
    def add_stage(self, stage: PipelineStage) -> None:
        """Add stage to pipeline.
        
        Args:
            stage: Stage to add
        """
        self.stages.append(stage)
        self.metrics.total_stages = len(self.stages)
    
    def execute(
        self,
        data: list[dict[str, Any]],
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Execute pipeline on data.
        
        Args:
            data: Input records
            params: Pipeline parameters
            
        Returns:
            Processed records
        """
        params = params or {}
        records = list(data)
        
        self.metrics.records_in = len(records)
        
        for stage in self.stages:
            try:
                if stage.stage_type == "extract":
                    records = list(Extractor.from_dict(records))
                
                elif stage.stage_type == "transform":
                    transformed = []
                    for record in records:
                        try:
                            transformed.append(record)
                        except Exception as e:
                            self.metrics.records_failed += 1
                            self.metrics.errors.append({
                                "stage": stage.name,
                                "error": str(e),
                            })
                    records = transformed
                
                elif stage.stage_type == "filter":
                    pass
                
                self.metrics.stages_completed += 1
            
            except Exception as e:
                self.metrics.errors.append({
                    "stage": stage.name,
                    "error": str(e),
                })
        
        self.metrics.records_out = len(records)
        self.metrics.end_time = datetime.now()
        
        return records


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute data pipeline action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "extract":
        data = params.get("data", [])
        records = list(Extractor.from_dict(data))
        result["data"] = {"extracted_count": len(records)}
    
    elif action == "transform":
        record = params.get("record", {})
        transformed = Transformer.rename_fields(
            record,
            params.get("rename_mapping", {}),
        )
        result["data"] = {"transformed": transformed}
    
    elif action == "enrich":
        enricher = DataEnricher()
        enricher.add_lookup_table(
            params.get("lookup_name", ""),
            params.get("lookup_data", {}),
        )
        enriched = enricher.enrich(
            params.get("record", {}),
            [params.get("lookup_name", "")],
        )
        result["data"] = {"enriched": enriched}
    
    elif action == "load":
        records = params.get("records", [])
        groups = Loader.group_by(records, params.get("group_by", ""))
        result["data"] = {"group_count": len(groups)}
    
    elif action == "checkpoint_save":
        manager = CheckpointManager()
        manager.save_checkpoint(
            params.get("pipeline_name", ""),
            params.get("stage_name", ""),
            params.get("position", 0),
        )
        result["data"] = {"saved": True}
    
    elif action == "checkpoint_load":
        manager = CheckpointManager()
        position = manager.load_checkpoint(
            params.get("pipeline_name", ""),
            params.get("stage_name", ""),
        )
        result["data"] = {"position": position}
    
    elif action == "execute_pipeline":
        pipeline = Pipeline(name=params.get("name", "pipeline"))
        stage = PipelineStage(
            name=params.get("stage_name", "stage"),
            stage_type=params.get("stage_type", "transform"),
        )
        pipeline.add_stage(stage)
        
        data = params.get("data", [])
        output = pipeline.execute(data)
        result["data"] = {
            "records_in": pipeline.metrics.records_in,
            "records_out": pipeline.metrics.records_out,
            "duration_seconds": pipeline.metrics.duration_seconds,
        }
    
    elif action == "metrics":
        pipeline = Pipeline(name="temp")
        result["data"] = {
            "records_in": 0,
            "records_out": 0,
            "throughput": 0.0,
        }
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
