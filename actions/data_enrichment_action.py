"""Data Enrichment action module for RabAI AutoClick.

Provides data enrichment operations:
- EnrichLookupAction: Lookup enrichment
- EnrichMergeAction: Merge enrichment
- EnrichComputeAction: Computed enrichment
- EnrichExternalAction: External API enrichment
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EnrichLookupAction(BaseAction):
    """Lookup enrichment."""
    action_type = "enrich_lookup"
    display_name = "查找富化"
    description = "查找数据富化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute lookup enrichment."""
        data = params.get('data', {})
        lookup_table = params.get('lookup_table', {})
        key_field = params.get('key_field', '')
        enrich_fields = params.get('enrich_fields', [])
        output_var = params.get('output_var', 'enriched_data')

        if not data or not lookup_table or not key_field:
            return ActionResult(success=False, message="data, lookup_table, and key_field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_lookup = context.resolve_value(lookup_table) if context else lookup_table

            key = resolved_data.get(key_field, '')
            lookup_row = resolved_lookup.get(key, {})

            enriched = resolved_data.copy()
            for field in enrich_fields:
                enriched[field] = lookup_row.get(field)

            result = {
                'enriched': enriched,
                'key': key,
                'found': bool(lookup_row),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Enriched with lookup: {'found' if lookup_row else 'not found'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich lookup error: {e}")


class EnrichMergeAction(BaseAction):
    """Merge enrichment."""
    action_type = "enrich_merge"
    display_name = "合并富化"
    description = "合并数据富化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute merge enrichment."""
        data = params.get('data', [])
        enrich_data = params.get('enrich_data', [])
        join_key = params.get('join_key', '')
        output_var = params.get('output_var', 'enriched_data')

        if not data or not enrich_data:
            return ActionResult(success=False, message="data and enrich_data are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_enrich = context.resolve_value(enrich_data) if context else enrich_data

            enrich_index = {r.get(join_key): r for r in resolved_enrich}

            enriched = []
            for record in resolved_data:
                key = record.get(join_key, '')
                if key in enrich_index:
                    merged = {**record, **enrich_index[key]}
                    enriched.append(merged)
                else:
                    enriched.append(record)

            result = {
                'enriched': enriched,
                'matched_count': sum(1 for r in resolved_data if r.get(join_key) in enrich_index),
                'total_count': len(resolved_data),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Merged enrichment: {result['matched_count']}/{result['total_count']} matched"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich merge error: {e}")


class EnrichComputeAction(BaseAction):
    """Computed enrichment."""
    action_type = "enrich_compute"
    display_name = "计算富化"
    description = "计算数据富化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute computed enrichment."""
        data = params.get('data', {})
        computations = params.get('computations', [])
        output_var = params.get('output_var', 'enriched_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_computations = context.resolve_value(computations) if context else computations

            enriched = resolved_data.copy()

            for comp in resolved_computations:
                field = comp.get('field', '')
                expression = comp.get('expression', '')
                func = comp.get('function', '')

                if func == 'concat':
                    separator = comp.get('separator', '')
                    fields = comp.get('fields', [])
                    values = [str(enriched.get(f, '')) for f in fields]
                    enriched[field] = separator.join(values)
                elif func == 'upper':
                    src_field = comp.get('source_field', '')
                    enriched[field] = str(enriched.get(src_field, '')).upper()
                elif func == 'lower':
                    src_field = comp.get('source_field', '')
                    enriched[field] = str(enriched.get(src_field, '')).lower()
                elif func == 'length':
                    src_field = comp.get('source_field', '')
                    enriched[field] = len(str(enriched.get(src_field, '')))
                elif func == 'round':
                    src_field = comp.get('source_field', '')
                    decimals = comp.get('decimals', 2)
                    enriched[field] = round(float(enriched.get(src_field, 0)), decimals)
                elif expression:
                    try:
                        enriched[field] = eval(expression, {'record': enriched})
                    except Exception:
                        enriched[field] = None

            result = {
                'enriched': enriched,
                'computations_applied': len(resolved_computations),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Applied {len(resolved_computations)} computations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich compute error: {e}")


class EnrichExternalAction(BaseAction):
    """External API enrichment."""
    action_type = "enrich_external"
    display_name = "外部API富化"
    description = "外部API数据富化"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute external enrichment."""
        data = params.get('data', {})
        api_url = params.get('api_url', '')
        api_key = params.get('api_key', '')
        request_field = params.get('request_field', '')
        response_path = params.get('response_path', '')
        output_var = params.get('output_var', 'enriched_data')

        if not data or not api_url:
            return ActionResult(success=False, message="data and api_url are required")

        try:
            import requests

            resolved_data = context.resolve_value(data) if context else data
            resolved_url = context.resolve_value(api_url) if context else api_url

            request_value = resolved_data.get(request_field, '')
            url = resolved_url.format(**{request_field: request_value})

            headers = {}
            if api_key:
                resolved_key = context.resolve_value(api_key) if context else api_key
                headers['Authorization'] = f'Bearer {resolved_key}'

            response = requests.get(url, headers=headers, timeout=30)

            enriched = resolved_data.copy()

            if response.ok:
                resp_data = response.json()
                if response_path:
                    parts = response_path.split('.')
                    for part in parts:
                        if part in resp_data:
                            resp_data = resp_data[part]
                        else:
                            resp_data = None
                            break
                    enriched[response_path.replace('.', '_')] = resp_data
                else:
                    enriched['api_response'] = resp_data

            result = {
                'enriched': enriched,
                'api_success': response.ok,
                'status_code': response.status_code,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"External API enrichment: {'success' if response.ok else 'failed'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Enrich external error: {e}")
