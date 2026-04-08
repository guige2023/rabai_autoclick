"""Data enricher action module for RabAI AutoClick.

Provides data enrichment with field mapping, lookups,
external data sources, and transformation pipelines.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataEnricherAction(BaseAction):
    """Enrich data records with additional fields and lookups.
    
    Supports field mapping, lookup tables, external data sources,
    computed fields, and nested field enrichment.
    """
    action_type = "data_enricher"
    display_name = "数据填充"
    description = "数据填充和丰富化，支持字段映射和查找"
    DEFAULT_BATCH_SIZE = 100
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data enrichment.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, enrichments (list of
                   enrichment configs), batch_size.
        
        Returns:
            ActionResult with enriched records.
        """
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        if not isinstance(records, list):
            return ActionResult(success=False, message="records must be a list")
        
        enrichments = params.get('enrichments', [])
        if not enrichments:
            return ActionResult(success=False, message="No enrichments defined")
        
        batch_size = params.get('batch_size', self.DEFAULT_BATCH_SIZE)
        max_workers = params.get('max_workers', 4)
        continue_on_error = params.get('continue_on_error', True)
        
        total = len(records)
        enriched = []
        errors = []
        
        def enrich_record(record: Dict[str, Any]) -> Dict[str, Any]:
            """Enrich a single record."""
            result = record.copy()
            for enrichment in enrichments:
                try:
                    result = self._apply_enrichment(result, enrichment)
                except Exception as e:
                    if not continue_on_error:
                        raise
            return result
        
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(enrich_record, rec): idx for idx, rec in enumerate(batch)}
                    
                    for future in as_completed(futures):
                        try:
                            enriched_record = future.result()
                            enriched.append(enriched_record)
                        except Exception as e:
                            errors.append({'error': str(e)})
                            if not continue_on_error:
                                return ActionResult(
                                    success=False,
                                    message=f"Enrichment failed at record {len(enriched)}",
                                    data={'enriched': len(enriched), 'errors': errors}
                                )
            
            return ActionResult(
                success=len(errors) == 0,
                message=f"Enriched {len(enriched)}/{total} records",
                data={
                    'total': total,
                    'enriched': len(enriched),
                    'failed': len(errors),
                    'records': enriched
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Enrichment failed: {e}"
            )
    
    def _apply_enrichment(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply a single enrichment to a record."""
        enrichment_type = enrichment.get('type', 'field_map')
        target_field = enrichment.get('target_field')
        
        if not target_field:
            return record
        
        if enrichment_type == 'field_map':
            return self._enrich_field_map(record, enrichment)
        elif enrichment_type == 'lookup':
            return self._enrich_lookup(record, enrichment)
        elif enrichment_type == 'computed':
            return self._enrich_computed(record, enrichment)
        elif enrichment_type == 'constant':
            return self._enrich_constant(record, enrichment)
        elif enrichment_type == 'merge':
            return self._enrich_merge(record, enrichment)
        elif enrichment_type == 'nested':
            return self._enrich_nested(record, enrichment)
        else:
            return record
    
    def _enrich_field_map(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Map values from one field to another."""
        source_field = enrichment.get('source_field')
        target_field = enrichment.get('target_field')
        mapping = enrichment.get('mapping', {})
        default = enrichment.get('default')
        
        if not source_field or not target_field:
            return record
        
        value = record.get(source_field, default)
        
        if mapping and value in mapping:
            value = mapping[value]
        
        record[target_field] = value
        return record
    
    def _enrich_lookup(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enrich using a lookup table."""
        lookup_field = enrichment.get('lookup_field')
        target_field = enrichment.get('target_field')
        lookup_table = enrichment.get('lookup_table', {})
        default = enrichment.get('default')
        
        if not lookup_field or not target_field:
            return record
        
        key = record.get(lookup_field)
        record[target_field] = lookup_table.get(key, default)
        return record
    
    def _enrich_computed(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute field value from other fields."""
        target_field = enrichment.get('target_field')
        compute_type = enrichment.get('compute_type', 'concat')
        source_fields = enrichment.get('source_fields', [])
        separator = enrichment.get('separator', ' ')
        
        if not target_field:
            return record
        
        if compute_type == 'concat':
            values = [str(record.get(f, '')) for f in source_fields]
            record[target_field] = separator.join(values)
        elif compute_type == 'sum':
            record[target_field] = sum(
                float(record.get(f, 0)) for f in source_fields
            )
        elif compute_type == 'avg':
            values = [float(record.get(f, 0)) for f in source_fields if f in record]
            record[target_field] = sum(values) / len(values) if values else 0
        elif compute_type == 'count':
            record[target_field] = len(source_fields)
        elif compute_type == 'min':
            values = [record.get(f) for f in source_fields if f in record]
            record[target_field] = min(values) if values else None
        elif compute_type == 'max':
            values = [record.get(f) for f in source_fields if f in record]
            record[target_field] = max(values) if values else None
        
        return record
    
    def _enrich_constant(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Add a constant value."""
        target_field = enrichment.get('target_field')
        value = enrichment.get('value')
        
        if target_field:
            record[target_field] = value
        
        return record
    
    def _enrich_merge(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge multiple fields into one."""
        target_field = enrichment.get('target_field')
        source_fields = enrichment.get('source_fields', [])
        separator = enrichment.get('separator', ' ')
        include_none = enrichment.get('include_none', False)
        
        if not target_field:
            return record
        
        values = []
        for f in source_fields:
            val = record.get(f)
            if val is not None or include_none:
                values.append(str(val) if val is not None else '')
        
        record[target_field] = separator.join(values)
        return record
    
    def _enrich_nested(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enrich nested fields using dot notation."""
        target_path = enrichment.get('target_path', '')
        value = enrichment.get('value')
        merge = enrichment.get('merge', False)
        
        if not target_path:
            return record
        
        parts = target_path.split('.')
        current = record
        
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]
        
        final_key = parts[-1]
        if merge and final_key in current:
            if isinstance(current[final_key], dict) and isinstance(value, dict):
                current[final_key].update(value)
            else:
                current[final_key] = value
        else:
            current[final_key] = value
        
        return record
