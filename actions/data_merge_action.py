"""Data Merge Action.

Merges multiple datasets with conflict resolution strategies,
ordering control, and deduplication during merge.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataMergeAction(BaseAction):
    """Merge multiple datasets with conflict resolution.
    
    Combines multiple data sources with configurable conflict
    resolution, ordering, and deduplication.
    """
    action_type = "data_merge"
    display_name = "数据合并"
    description = "合并多个数据集，支持冲突解决和去重"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Merge multiple datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - datasets: List of datasets to merge.
                - conflict_strategy: 'keep_first', 'keep_last', 'merge', 'custom'.
                - conflict_resolver: Custom function for conflict resolution.
                - key_field: Field for deduplication during merge.
                - sort_by: Field to sort final result.
                - sort_order: 'asc' or 'desc'.
                - dedupe: Deduplicate after merge (default: True).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with merged data.
        """
        try:
            datasets = params.get('datasets', [])
            conflict_strategy = params.get('conflict_strategy', 'keep_first')
            conflict_resolver = params.get('conflict_resolver')
            key_field = params.get('key_field')
            sort_by = params.get('sort_by')
            sort_order = params.get('sort_order', 'asc').lower()
            dedupe = params.get('dedupe', True)
            save_to_var = params.get('save_to_var', 'merged_data')

            if not datasets:
                return ActionResult(success=False, message="No datasets provided")

            # Flatten datasets if nested
            flat_datasets = []
            for ds in datasets:
                if isinstance(ds, list):
                    flat_datasets.extend(ds)
                elif isinstance(ds, dict):
                    flat_datasets.append(ds)

            if not flat_datasets:
                return ActionResult(success=False, message="No data to merge")

            # Remove duplicates if key_field specified
            if dedupe and key_field:
                flat_datasets = self._dedupe_by_key(flat_datasets, key_field, conflict_strategy, conflict_resolver)
            elif dedupe:
                flat_datasets = self._dedupe_duplicate_objects(flat_datasets)

            # Sort if requested
            if sort_by:
                reverse = sort_order == 'desc'
                flat_datasets.sort(key=lambda x: x.get(sort_by) if isinstance(x, dict) else x, reverse=reverse)

            summary = {
                'datasets_count': len(datasets),
                'merged_count': len(flat_datasets),
                'conflict_strategy': conflict_strategy,
                'deduped': dedupe
            }

            context.set_variable(save_to_var, flat_datasets)
            return ActionResult(success=True, data=summary,
                             message=f"Merged {len(datasets)} datasets: {len(flat_datasets)} rows")

        except Exception as e:
            return ActionResult(success=False, message=f"Merge error: {e}")

    def _dedupe_by_key(self, data: List[Dict], key_field: str, 
                      strategy: str, resolver: Optional[str]) -> List[Dict]:
        """Deduplicate by key field."""
        seen = {}
        result = []

        for item in data:
            if not isinstance(item, dict):
                continue
                
            key = item.get(key_field)
            if key is None:
                result.append(item)
                continue

            if key not in seen:
                seen[key] = len(result)
                result.append(item.copy())
            else:
                existing = result[seen[key]]
                
                if strategy == 'keep_first':
                    continue
                elif strategy == 'keep_last':
                    result[seen[key]] = item.copy()
                elif strategy == 'merge':
                    result[seen[key]] = self._merge_conflicting(existing, item, resolver)
                elif strategy == 'custom' and resolver:
                    try:
                        result[seen[key]] = eval(resolver)(existing, item)
                    except Exception:
                        result[seen[key]] = existing

        return result

    def _dedupe_duplicate_objects(self, data: List) -> List:
        """Remove exact duplicate objects."""
        seen = set()
        result = []
        
        for item in data:
            try:
                import json
                item_key = json.dumps(item, sort_keys=True)
                if item_key not in seen:
                    seen.add(item_key)
                    result.append(item)
            except Exception:
                if item not in seen:
                    seen.add(item)
                    result.append(item)
        
        return result

    def _merge_conflicting(self, existing: Dict, new: Dict, resolver: Optional[str]) -> Dict:
        """Merge two conflicting items."""
        merged = existing.copy()
        
        for key, value in new.items():
            if key not in merged:
                merged[key] = value
            elif merged[key] != value:
                if resolver:
                    try:
                        merged[key] = eval(resolver)(merged[key], value)
                    except Exception:
                        merged[key] = merged[key]
                else:
                    if isinstance(merged[key], (int, float)) and isinstance(value, (int, float)):
                        merged[key] = merged[key] + value
                    else:
                        merged[key] = value
        
        return merged
