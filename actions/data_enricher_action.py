"""Data enricher action module for RabAI AutoClick.

Provides data enrichment by fetching additional data from external
sources, joining with reference data, and computing derived fields.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class EnrichmentSource:
    """A data enrichment source."""
    name: str
    fetch_func: Callable[[Dict], Any]
    key_field: str
    target_field: str


class DataEnricherAction(BaseAction):
    """Enrich data with additional fields from external sources.
    
    Supports lookup enrichment, computed fields, and cascading
    enrichment from multiple sources.
    """
    action_type = "data_enricher"
    display_name = "数据富化"
    description = "外部数据源富化和字段计算"
    
    def __init__(self):
        super().__init__()
        self._sources: Dict[str, EnrichmentSource] = {}
        self._lookup_cache: Dict[str, Any] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data enrichment.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'enrich', 'add_source', 'lookup', 'compute'
                - data: Data to enrich (dict or list)
                - sources: List of source configs (for enrich)
                - field: Field name (for add_source/lookup)
                - value: Lookup value (for lookup)
        
        Returns:
            ActionResult with enriched data.
        """
        operation = params.get('operation', 'enrich').lower()
        
        if operation == 'enrich':
            return self._enrich(params)
        elif operation == 'add_source':
            return self._add_source(params)
        elif operation == 'lookup':
            return self._lookup(params)
        elif operation == 'compute':
            return self._compute(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _enrich(self, params: Dict[str, Any]) -> ActionResult:
        """Enrich data with configured sources."""
        data = params.get('data')
        sources = params.get('sources', [])
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        # Add sources if provided
        for source_def in sources:
            source = EnrichmentSource(
                name=source_def['name'],
                fetch_func=source_def.get('fetch_func'),
                key_field=source_def['key_field'],
                target_field=source_def['target_field']
            )
            self._sources[source.name] = source
        
        # Enrich data
        if isinstance(data, dict):
            result = self._enrich_record(data)
        elif isinstance(data, list):
            result = [self._enrich_record(record) for record in data]
        else:
            result = data
        
        return ActionResult(
            success=True,
            message=f"Enriched data with {len(sources)} sources",
            data={'result': result, 'source_count': len(sources)}
        )
    
    def _enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single record."""
        result = dict(record)
        
        for name, source in self._sources.items():
            key_value = record.get(source.key_field)
            
            if key_value is None:
                continue
            
            # Check cache first
            cache_key = f"{name}:{key_value}"
            if cache_key in self._lookup_cache:
                result[source.target_field] = self._lookup_cache[cache_key]
                continue
            
            # Fetch from source
            try:
                enriched_value = source.fetch_func(record)
                self._lookup_cache[cache_key] = enriched_value
                result[source.target_field] = enriched_value
            except Exception:
                result[source.target_field] = None
        
        return result
    
    def _add_source(self, params: Dict[str, Any]) -> ActionResult:
        """Add an enrichment source."""
        name = params.get('name')
        key_field = params.get('key_field')
        target_field = params.get('target_field')
        fetch_func = params.get('fetch_func')
        
        if not name or not key_field or not target_field:
            return ActionResult(
                success=False,
                message="name, key_field, and target_field are required"
            )
        
        source = EnrichmentSource(
            name=name,
            fetch_func=fetch_func,
            key_field=key_field,
            target_field=target_field
        )
        self._sources[name] = source
        
        return ActionResult(
            success=True,
            message=f"Added enrichment source '{name}'",
            data={'source': name}
        )
    
    def _lookup(self, params: Dict[str, Any]) -> ActionResult:
        """Perform a lookup against a source."""
        source_name = params.get('source')
        key_value = params.get('value')
        
        if not source_name or source_name not in self._sources:
            return ActionResult(
                success=False,
                message=f"Unknown source: {source_name}"
            )
        
        source = self._sources[source_name]
        cache_key = f"{source_name}:{key_value}"
        
        if cache_key in self._lookup_cache:
            return ActionResult(
                success=True,
                message="Cache hit",
                data={'result': self._lookup_cache[cache_key]}
            )
        
        try:
            result = source.fetch_func({source.key_field: key_value})
            self._lookup_cache[cache_key] = result
            return ActionResult(
                success=True,
                message="Lookup successful",
                data={'result': result}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Lookup failed: {e}",
                data={'error': str(e)}
            )
    
    def _compute(self, params: Dict[str, Any]) -> ActionResult:
        """Compute derived fields."""
        data = params.get('data')
        computations = params.get('computations', [])
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        if isinstance(data, dict):
            result = self._compute_fields(data, computations)
        elif isinstance(data, list):
            result = [self._compute_fields(record, computations) for record in data]
        else:
            result = data
        
        return ActionResult(
            success=True,
            message=f"Computed {len(computations)} fields",
            data={'result': result}
        )
    
    def _compute_fields(
        self,
        record: Dict[str, Any],
        computations: List[Dict]
    ) -> Dict[str, Any]:
        """Compute fields for a record."""
        result = dict(record)
        
        for comp in computations:
            field_name = comp.get('field')
            expression = comp.get('expression')
            func = comp.get('func')
            
            if not field_name:
                continue
            
            value = None
            if callable(func):
                try:
                    value = func(record)
                except Exception:
                    pass
            elif expression:
                # Simple expression evaluation
                try:
                    value = eval(expression, {"record": record}, {})
                except Exception:
                    pass
            
            result[field_name] = value
        
        return result


class DataJoinerAction(BaseAction):
    """Join data from multiple sources."""
    action_type = "data_joiner"
    display_name = "数据关联"
    description = "多数据源关联join操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data join.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - left: Left dataset
                - right: Right dataset
                - left_key: Key field in left dataset
                - right_key: Key field in right dataset
                - join_type: 'inner', 'left', 'right', 'outer'
                - select: List of fields to include
        
        Returns:
            ActionResult with joined data.
        """
        left = params.get('left', [])
        right = params.get('right', [])
        left_key = params.get('left_key')
        right_key = params.get('right_key')
        join_type = params.get('join_type', 'inner')
        select = params.get('select')
        
        if not left or not right:
            return ActionResult(success=False, message="left and right datasets required")
        
        if not left_key or not right_key:
            return ActionResult(success=False, message="left_key and right_key required")
        
        # Build right lookup index
        right_index = {}
        for record in right:
            key = record.get(right_key)
            if key is not None:
                right_index[key] = record
        
        # Perform join
        results = []
        matched_right = set()
        
        for left_record in left:
            lkey = left_record.get(left_key)
            right_record = right_index.get(lkey)
            
            if right_record:
                matched_right.add(lkey)
                merged = {**left_record, **right_record}
                results.append(merged)
            elif join_type in ('left', 'outer'):
                results.append(dict(left_record))
        
        # Add unmatched right records for right/outer join
        if join_type in ('right', 'outer'):
            for record in right:
                rkey = record.get(right_key)
                if rkey not in matched_right:
                    results.append(dict(record))
        
        # Apply field selection
        if select:
            results = [
                {k: v for k, v in r.items() if k in select}
                for r in results
            ]
        
        return ActionResult(
            success=True,
            message=f"Joined {len(results)} records",
            data={'result': results, 'count': len(results)}
        )
