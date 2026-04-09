"""Data Pipeline Action Module.

Provides data pipeline construction and execution for ETL workflows.
"""

import time
import json
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPipelineBuilderAction(BaseAction):
    """Build and configure data pipelines.
    
    Creates configurable data processing pipelines with sources, transforms, and sinks.
    """
    action_type = "data_pipeline_builder"
    display_name = "数据管道构建"
    description = "构建配置化数据处理管道"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build data pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, sources, transforms, sinks, config.
        
        Returns:
            ActionResult with pipeline configuration.
        """
        name = params.get('name', 'pipeline')
        sources = params.get('sources', [])
        transforms = params.get('transforms', [])
        sinks = params.get('sinks', [])
        config = params.get('config', {})
        
        if not sources:
            return ActionResult(
                success=False,
                data=None,
                error="Pipeline must have at least one source"
            )
        
        if not sinks:
            return ActionResult(
                success=False,
                data=None,
                error="Pipeline must have at least one sink"
            )
        
        pipeline_config = {
            "name": name,
            "sources": sources,
            "transforms": transforms,
            "sinks": sinks,
            "config": config,
            "created_at": time.time()
        }
        
        return ActionResult(
            success=True,
            data=pipeline_config,
            error=None
        )
    
    def _validate_source(self, source: Dict[str, Any]) -> bool:
        """Validate a pipeline source."""
        required_fields = ['type', 'connection']
        return all(field in source for field in required_fields)
    
    def _validate_transform(self, transform: Dict[str, Any]) -> bool:
        """Validate a pipeline transform."""
        return 'type' in transform
    
    def _validate_sink(self, sink: Dict[str, Any]) -> bool:
        """Validate a pipeline sink."""
        required_fields = ['type', 'destination']
        return all(field in sink for field in required_fields)


class DataPipelineExecutorAction(BaseAction):
    """Execute data pipeline stages.
    
    Runs data through configured pipeline with proper ordering.
    """
    action_type = "data_pipeline_executor"
    display_name = "数据管道执行"
    description = "按顺序执行数据处理管道"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys: pipeline_config, input_data, start_stage.
        
        Returns:
            ActionResult with pipeline execution results.
        """
        pipeline_config = params.get('pipeline_config', {})
        input_data = params.get('input_data', [])
        start_stage = params.get('start_stage', 0)
        
        if not pipeline_config:
            return ActionResult(
                success=False,
                data=None,
                error="No pipeline configuration provided"
            )
        
        current_data = input_data
        stage_results = []
        
        # Execute source stages
        sources = pipeline_config.get('sources', [])
        for i, source in enumerate(sources):
            if i < start_stage:
                continue
            try:
                result = self._execute_source(source, current_data)
                stage_results.append({
                    "stage": "source",
                    "index": i,
                    "success": True,
                    "output_count": len(result)
                })
                current_data = result
            except Exception as e:
                stage_results.append({
                    "stage": "source",
                    "index": i,
                    "success": False,
                    "error": str(e)
                })
                return ActionResult(
                    success=False,
                    data={"stages": stage_results},
                    error=f"Source stage {i} failed: {str(e)}"
                )
        
        # Execute transform stages
        transforms = pipeline_config.get('transforms', [])
        for i, transform in enumerate(transforms):
            try:
                result = self._execute_transform(transform, current_data)
                stage_results.append({
                    "stage": "transform",
                    "index": i,
                    "success": True,
                    "output_count": len(result)
                })
                current_data = result
            except Exception as e:
                stage_results.append({
                    "stage": "transform",
                    "index": i,
                    "success": False,
                    "error": str(e)
                })
                return ActionResult(
                    success=False,
                    data={"stages": stage_results},
                    error=f"Transform stage {i} failed: {str(e)}"
                )
        
        # Execute sink stages
        sinks = pipeline_config.get('sinks', [])
        for i, sink in enumerate(sinks):
            try:
                self._execute_sink(sink, current_data)
                stage_results.append({
                    "stage": "sink",
                    "index": i,
                    "success": True,
                    "records_written": len(current_data)
                })
            except Exception as e:
                stage_results.append({
                    "stage": "sink",
                    "index": i,
                    "success": False,
                    "error": str(e)
                })
                return ActionResult(
                    success=False,
                    data={"stages": stage_results},
                    error=f"Sink stage {i} failed: {str(e)}"
                )
        
        return ActionResult(
            success=True,
            data={
                "stages": stage_results,
                "total_records": len(current_data)
            },
            error=None
        )
    
    def _execute_source(self, source: Dict, data: List) -> List:
        """Execute a source stage."""
        source_type = source.get('type', 'memory')
        
        if source_type == 'memory':
            return source.get('data', [])
        elif source_type == 'file':
            return self._read_from_file(source.get('path', ''))
        elif source_type == 'api':
            return self._fetch_from_api(source.get('url', ''))
        else:
            return data
    
    def _execute_transform(self, transform: Dict, data: List) -> List:
        """Execute a transform stage."""
        transform_type = transform.get('type', 'identity')
        
        if transform_type == 'filter':
            return self._filter_data(data, transform.get('condition', {}))
        elif transform_type == 'map':
            return self._map_data(data, transform.get('mapping', {}))
        elif transform_type == 'aggregate':
            return self._aggregate_data(data, transform.get('group_by', []))
        elif transform_type == 'identity':
            return data
        else:
            return data
    
    def _execute_sink(self, sink: Dict, data: List) -> None:
        """Execute a sink stage."""
        sink_type = sink.get('type', 'memory')
        destination = sink.get('destination', '')
        
        if sink_type == 'file':
            self._write_to_file(destination, data)
        elif sink_type == 'api':
            self._send_to_api(destination, data)
        # memory sink does nothing (data is in memory)
    
    def _filter_data(self, data: List, condition: Dict) -> List:
        """Filter data based on condition."""
        field = condition.get('field', '')
        operator = condition.get('operator', 'eq')
        value = condition.get('value', None)
        
        filtered = []
        for item in data:
            if isinstance(item, dict) and field in item:
                item_value = item[field]
                if operator == 'eq' and item_value == value:
                    filtered.append(item)
                elif operator == 'gt' and item_value > value:
                    filtered.append(item)
                elif operator == 'lt' and item_value < value:
                    filtered.append(item)
                elif operator == 'contains' and value in str(item_value):
                    filtered.append(item)
            else:
                filtered.append(item)
        return filtered
    
    def _map_data(self, data: List, mapping: Dict) -> List:
        """Map data fields to new structure."""
        return [self._apply_mapping(item, mapping) for item in data]
    
    def _apply_mapping(self, item: Any, mapping: Dict) -> Dict:
        """Apply field mapping to a single item."""
        if not isinstance(item, dict):
            return item
        
        result = {}
        for target_field, source_field in mapping.items():
            if isinstance(source_field, dict):
                result[target_field] = self._apply_nested_mapping(item, source_field)
            elif source_field in item:
                result[target_field] = item[source_field]
            else:
                result[target_field] = None
        return result
    
    def _apply_nested_mapping(self, item: Dict, mapping: Dict) -> Any:
        """Apply nested field mapping."""
        value = item
        for key in mapping.get('path', []).split('.'):
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
    
    def _aggregate_data(self, data: List, group_by: List) -> List:
        """Aggregate data by grouping fields."""
        groups = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            
            key_parts = []
            for field in group_by:
                key_parts.append(str(item.get(field, '')))
            key = ':'.join(key_parts)
            
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        return [{"group_key": k, "items": v, "count": len(v)} for k, v in groups.items()]
    
    def _read_from_file(self, path: str) -> List:
        """Read data from file."""
        if path.endswith('.json'):
            with open(path, 'r') as f:
                return json.load(f)
        return []
    
    def _write_to_file(self, path: str, data: List) -> None:
        """Write data to file."""
        if path.endswith('.json'):
            with open(path, 'w') as f:
                json.dump(data, f)
    
    def _fetch_from_api(self, url: str) -> List:
        """Fetch data from API."""
        import urllib.request
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode())
        except Exception:
            return []
    
    def _send_to_api(self, url: str, data: List) -> None:
        """Send data to API."""
        import urllib.request
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            urllib.request.urlopen(req, timeout=30)
        except Exception:
            pass


class DataPipelineMonitorAction(BaseAction):
    """Monitor data pipeline execution.
    
    Tracks pipeline progress and performance metrics.
    """
    action_type = "data_pipeline_monitor"
    display_name = "数据管道监控"
    description = "追踪管道执行进度和性能指标"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pipeline monitoring.
        
        Args:
            context: Execution context.
            params: Dict with keys: pipeline_id, check_progress.
        
        Returns:
            ActionResult with pipeline status.
        """
        pipeline_id = params.get('pipeline_id', '')
        check_progress = params.get('check_progress', True)
        
        # Simulate progress tracking
        progress = 0.0
        throughput = 0.0
        error_count = 0
        
        return ActionResult(
            success=True,
            data={
                "pipeline_id": pipeline_id,
                "progress_percent": progress,
                "throughput_records_per_second": throughput,
                "error_count": error_count,
                "status": "running" if progress < 100 else "completed"
            },
            error=None
        )


def register_actions():
    """Register all Data Pipeline actions."""
    return [
        DataPipelineBuilderAction,
        DataPipelineExecutorAction,
        DataPipelineMonitorAction,
    ]
