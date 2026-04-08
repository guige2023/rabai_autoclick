"""Data pipeline action module for RabAI AutoClick.

Provides ETL-style data pipeline processing with transformation,
filtering, aggregation, and error handling capabilities.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PipelineStage(Enum):
    """Pipeline processing stages."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    LOAD = "load"
    VALIDATE = "validate"


@dataclass
class PipelineConfig:
    """Configuration for a pipeline stage."""
    stage: PipelineStage
    operation: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    items_processed: int
    items_output: int
    errors: List[Dict[str, Any]]
    stage_results: Dict[str, Any]


class DataPipelineAction(BaseAction):
    """Data pipeline action for ETL-style processing.
    
    Supports sequential stages: extract, transform, filter,
    aggregate, validate, and load with configurable operations.
    """
    action_type = "data_pipeline"
    display_name = "数据管道"
    description = "ETL数据管道处理"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data pipeline.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Input data (list of dicts)
                stages: List of pipeline stage configs
                stop_on_error: Stop on first error (default False)
                error_threshold: Max errors before stop (default -1, no limit).
        
        Returns:
            ActionResult with pipeline execution summary.
        """
        data = params.get('data', [])
        stage_configs = params.get('stages', [])
        stop_on_error = params.get('stop_on_error', False)
        error_threshold = params.get('error_threshold', -1)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="Data must be a list")
        
        stage_results = {}
        errors = []
        current_data = data
        items_processed = 0
        
        for i, stage_config in enumerate(stage_configs):
            config = self._parse_config(stage_config)
            
            result = self._execute_stage(config, current_data, errors, stop_on_error)
            
            if not result['success'] and stop_on_error:
                return ActionResult(
                    success=False,
                    message=f"Pipeline failed at stage {config.stage.value}",
                    data={
                        'stage': config.stage.value,
                        'stage_index': i,
                        'errors': errors[:10],
                        'items_processed': items_processed
                    }
                )
            
            stage_results[config.stage.value] = result
            current_data = result.get('output', [])
            items_processed += len(data)
            
            if error_threshold > 0 and len(errors) >= error_threshold:
                return ActionResult(
                    success=False,
                    message=f"Error threshold {error_threshold} exceeded",
                    data={
                        'stage': config.stage.value,
                        'errors': errors[:10],
                        'items_processed': items_processed
                    }
                )
        
        return ActionResult(
            success=len(errors) == 0,
            message=f"Pipeline completed: {len(current_data)} items output",
            data={
                'items_processed': items_processed,
                'items_output': len(current_data),
                'errors': len(errors),
                'error_count': len(errors),
                'stage_results': {k: {'success': v['success'], 'output_count': len(v.get('output', []))} 
                                  for k, v in stage_results.items()},
                'output': current_data
            }
        )
    
    def _parse_config(self, config: Union[Dict, str]) -> PipelineConfig:
        """Parse stage configuration."""
        if isinstance(config, str):
            parts = config.split(':')
            return PipelineConfig(
                stage=PipelineStage(parts[0]) if parts[0] in [s.value for s in PipelineStage] else PipelineStage.TRANSFORM,
                operation=parts[1] if len(parts) > 1 else 'pass',
                params={}
            )
        
        return PipelineConfig(
            stage=PipelineStage(config.get('stage', 'transform')),
            operation=config.get('operation', 'pass'),
            params=config.get('params', {})
        )
    
    def _execute_stage(
        self,
        config: PipelineConfig,
        data: List[Any],
        errors: List[Dict[str, Any]],
        stop_on_error: bool
    ) -> Dict[str, Any]:
        """Execute a single pipeline stage."""
        try:
            if config.stage == PipelineStage.EXTRACT:
                return self._stage_extract(config, data)
            elif config.stage == PipelineStage.TRANSFORM:
                return self._stage_transform(config, data, errors)
            elif config.stage == PipelineStage.FILTER:
                return self._stage_filter(config, data, errors)
            elif config.stage == PipelineStage.AGGREGATE:
                return self._stage_aggregate(config, data)
            elif config.stage == PipelineStage.VALIDATE:
                return self._stage_validate(config, data, errors)
            elif config.stage == PipelineStage.LOAD:
                return self._stage_load(config, data, errors)
            else:
                return {'success': True, 'output': data}
        except Exception as e:
            return {'success': False, 'error': str(e), 'output': data}
    
    def _stage_extract(self, config: PipelineConfig, data: List[Any]) -> Dict[str, Any]:
        """Extract stage - initial data processing."""
        source_type = config.params.get('source_type', 'direct')
        
        if source_type == 'flatten' and data and isinstance(data[0], dict):
            depth = config.params.get('depth', 1)
            flattened = []
            for item in data:
                if isinstance(item, dict):
                    flat = self._flatten_dict(item, depth)
                    flattened.append(flat)
                else:
                    flattened.append(item)
            return {'success': True, 'output': flattened}
        
        return {'success': True, 'output': data}
    
    def _stage_transform(self, config: PipelineConfig, data: List[Any], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transform stage - apply transformations to data items."""
        operation = config.operation
        output = []
        
        for i, item in enumerate(data):
            try:
                transformed = self._transform_item(item, operation, config.params)
                output.append(transformed)
            except Exception as e:
                errors.append({'index': i, 'error': str(e), 'item': item})
                if config.params.get('skip_on_error', False):
                    continue
                output.append(item)
        
        return {'success': True, 'output': output}
    
    def _transform_item(self, item: Any, operation: str, params: Dict[str, Any]) -> Any:
        """Apply single transformation to item."""
        if operation == 'pass':
            return item
        
        if operation == 'map_fields' and isinstance(item, dict):
            mapping = params.get('field_map', {})
            return {new_key: item.get(old_key) for new_key, old_key in mapping.items()}
        
        if operation == 'rename' and isinstance(item, dict):
            mapping = params.get('rename_map', {})
            result = dict(item)
            for old_key, new_key in mapping.items():
                if old_key in result:
                    result[new_key] = result.pop(old_key)
            return result
        
        if operation == 'add_field' and isinstance(item, dict):
            result = dict(item)
            for field_name, field_value in params.get('fields', {}).items():
                if isinstance(field_value, str) and field_value.startswith('$'):
                    result[field_name] = item.get(field_value[1:])
                else:
                    result[field_name] = field_value
            return result
        
        if operation == 'remove_fields' and isinstance(item, dict):
            fields_to_remove = params.get('fields', [])
            return {k: v for k, v in item.items() if k not in fields_to_remove}
        
        return item
    
    def _stage_filter(self, config: PipelineConfig, data: List[Any], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Filter stage - filter data items based on conditions."""
        condition = config.params.get('condition')
        output = []
        
        for i, item in enumerate(data):
            try:
                if self._evaluate_condition(item, condition, config.params):
                    output.append(item)
            except Exception as e:
                errors.append({'index': i, 'error': str(e), 'item': item})
        
        return {'success': True, 'output': output}
    
    def _evaluate_condition(self, item: Any, condition: Optional[str], params: Dict[str, Any]) -> bool:
        """Evaluate filter condition on item."""
        if not condition:
            return True
        
        if isinstance(item, dict):
            if condition == 'not_null':
                fields = params.get('fields', [])
                return all(item.get(f) is not None for f in fields)
            
            if condition == 'is_null':
                fields = params.get('fields', [])
                return any(item.get(f) is None for f in fields)
            
            if condition == 'in_values':
                field = params.get('field')
                values = params.get('values', [])
                return item.get(field) in values
            
            if '==' in condition:
                field, value = condition.split('==', 1)
                return str(item.get(field.strip(), '')).strip() == value.strip().strip('"\'')
        
        return True
    
    def _stage_aggregate(self, config: PipelineConfig, data: List[Any]) -> Dict[str, Any]:
        """Aggregate stage - aggregate data."""
        operation = config.operation
        params = config.params
        
        if not data:
            return {'success': True, 'output': []}
        
        if operation == 'group_by' and isinstance(data[0], dict):
            group_field = params.get('group_by')
            agg_fields = params.get('agg_fields', {})
            
            groups: Dict[Any, List[Any]] = {}
            for item in data:
                key = item.get(group_field)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
            
            output = []
            for key, items in groups.items():
                result = {group_field: key, 'count': len(items)}
                for agg_field, agg_op in agg_fields.items():
                    values = [item.get(agg_field) for item in items if item.get(agg_field) is not None]
                    if agg_op == 'sum':
                        result[f'{agg_field}_sum'] = sum(values)
                    elif agg_op == 'avg':
                        result[f'{agg_field}_avg'] = sum(values) / len(values) if values else 0
                    elif agg_op == 'min':
                        result[f'{agg_field}_min'] = min(values) if values else None
                    elif agg_op == 'max':
                        result[f'{agg_field}_max'] = max(values) if values else None
                    elif agg_op == 'count':
                        result[f'{agg_field}_count'] = len(values)
                output.append(result)
            
            return {'success': True, 'output': output}
        
        return {'success': True, 'output': data}
    
    def _stage_validate(self, config: PipelineConfig, data: List[Any], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate stage - validate data items."""
        rules = config.params.get('rules', [])
        output = []
        
        for i, item in enumerate(data):
            item_errors = []
            
            for rule in rules:
                rule_type = rule.get('type')
                
                if rule_type == 'required':
                    field = rule.get('field')
                    if not item.get(field):
                        item_errors.append(f"Required field '{field}' is missing")
                
                elif rule_type == 'type':
                    field = rule.get('field')
                    expected = rule.get('expected')
                    actual = type(item.get(field)).__name__
                    if actual != expected:
                        item_errors.append(f"Field '{field}' should be {expected}, got {actual}")
                
                elif rule_type == 'range':
                    field = rule.get('field')
                    min_val = rule.get('min')
                    max_val = rule.get('max')
                    value = item.get(field)
                    if value is not None:
                        if min_val is not None and value < min_val:
                            item_errors.append(f"Field '{field}' value {value} below min {min_val}")
                        if max_val is not None and value > max_val:
                            item_errors.append(f"Field '{field}' value {value} above max {max_val}")
            
            if item_errors:
                errors.append({'index': i, 'errors': item_errors, 'item': item})
                if not config.params.get('skip_on_error', False):
                    continue
            
            output.append(item)
        
        return {'success': True, 'output': output}
    
    def _stage_load(self, config: PipelineConfig, data: List[Any], errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load stage - final data output."""
        output_type = config.params.get('output_type', 'dedupe')
        
        if output_type == 'dedupe' and data and isinstance(data[0], dict):
            seen = set()
            output = []
            for item in data:
                key = tuple(sorted(item.items()))
                if key not in seen:
                    seen.add(key)
                    output.append(item)
            return {'success': True, 'output': output}
        
        if output_type == 'limit':
            limit = config.params.get('limit', 100)
            return {'success': True, 'output': data[:limit]}
        
        if output_type == 'sort' and data and isinstance(data[0], dict):
            sort_key = config.params.get('sort_by')
            reverse = config.params.get('reverse', False)
            return {'success': True, 'output': sorted(data, key=lambda x: x.get(sort_key, ''), reverse=reverse)}
        
        return {'success': True, 'output': data}
    
    def _flatten_dict(self, d: Dict, depth: int = 1, parent_key: str = '') -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            if isinstance(v, dict) and depth > 1:
                items.extend(self._flatten_dict(v, depth - 1, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)
