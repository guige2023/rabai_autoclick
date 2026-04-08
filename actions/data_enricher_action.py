"""Data enricher action module for RabAI AutoClick.

Provides data enrichment with lookup tables, computed fields,
and external data merging.
"""

import sys
import os
from typing import Any, Dict, List, Optional
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataEnricherAction(BaseAction):
    """Data enricher action for adding computed and lookup data.
    
    Supports lookup table enrichment, computed fields,
    and external data merging.
    """
    action_type = "data_enricher"
    display_name = "数据增强"
    description = "数据补充与增强"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enrichment.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to enrich
                lookups: Lookup tables for enrichment
                computed_fields: Fields to compute
                enrichments: Direct enrichment data
                merge_key: Key field for merging.
        
        Returns:
            ActionResult with enriched data.
        """
        data = params.get('data', [])
        lookups = params.get('lookups', {})
        computed_fields = params.get('computed_fields', [])
        enrichments = params.get('enrichments', {})
        merge_key = params.get('merge_key')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        result = []
        
        for item in data:
            enriched = dict(item)
            
            if lookups:
                enriched = self._apply_lookups(enriched, lookups, merge_key)
            
            for field_def in computed_fields:
                enriched = self._apply_computed_field(enriched, field_def)
            
            if enrichments:
                enriched.update(enrichments)
            
            result.append(enriched)
        
        return ActionResult(
            success=True,
            message=f"Enriched {len(result)} items",
            data={
                'items': result,
                'count': len(result)
            }
        )
    
    def _apply_lookups(
        self,
        item: Dict,
        lookups: Dict[str, Any],
        merge_key: Optional[str]
    ) -> Dict:
        """Apply lookup tables to item."""
        result = dict(item)
        
        for lookup_name, lookup_data in lookups.items():
            if isinstance(lookup_data, dict):
                if merge_key:
                    key = item.get(merge_key)
                    if key in lookup_data:
                        for k, v in lookup_data[key].items():
                            result[f"{lookup_name}_{k}"] = v
            elif isinstance(lookup_data, list):
                if merge_key:
                    key = item.get(merge_key)
                    for entry in lookup_data:
                        if isinstance(entry, dict) and entry.get(merge_key) == key:
                            for k, v in entry.items():
                                if k != merge_key:
                                    result[f"{lookup_name}_{k}"] = v
                            break
        
        return result
    
    def _apply_computed_field(
        self,
        item: Dict,
        field_def: Dict[str, Any]
    ) -> Dict:
        """Apply computed field to item."""
        result = dict(item)
        
        field_name = field_def.get('name')
        operation = field_def.get('operation')
        source_fields = field_def.get('fields', [])
        
        if not field_name or not operation:
            return result
        
        if operation == 'concat':
            separator = field_def.get('separator', ' ')
            values = [str(item.get(f, '')) for f in source_fields]
            result[field_name] = separator.join(values)
        
        elif operation == 'merge':
            values = [item.get(f) for f in source_fields if item.get(f) is not None]
            result[field_name] = values[0] if values else None
        
        elif operation == 'sum':
            values = [item.get(f, 0) for f in source_fields if isinstance(item.get(f), (int, float))]
            result[field_name] = sum(values) if values else 0
        
        elif operation == 'avg':
            values = [item.get(f, 0) for f in source_fields if isinstance(item.get(f), (int, float))]
            result[field_name] = sum(values) / len(values) if values else 0
        
        elif operation == 'count':
            result[field_name] = sum(1 for f in source_fields if item.get(f) is not None)
        
        elif operation == 'upper':
            if source_fields:
                result[field_name] = str(item.get(source_fields[0], '')).upper()
        
        elif operation == 'lower':
            if source_fields:
                result[field_name] = str(item.get(source_fields[0], '')).lower()
        
        elif operation == 'length':
            if source_fields:
                val = item.get(source_fields[0])
                result[field_name] = len(val) if val else 0
        
        elif operation == 'ternary':
            condition_field = field_def.get('condition')
            true_value = field_def.get('true')
            false_value = field_def.get('false')
            
            condition = item.get(condition_field)
            result[field_name] = true_value if condition else false_value
        
        return result
