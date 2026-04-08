"""Data Diff action module for RabAI AutoClick.

Provides data diff operations:
- DiffCompareAction: Compare two datasets
- DiffSchemaAction: Compare schemas
- DiffPatchAction: Generate/apply patches
- DiffMergeAction: Merge diffs
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional, Set

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DiffCompareAction(BaseAction):
    """Compare two datasets."""
    action_type = "diff_compare"
    display_name = "数据对比"
    description = "对比两个数据集"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data comparison."""
        data1 = params.get('data1', [])
        data2 = params.get('data2', [])
        key_field = params.get('key_field', None)
        output_var = params.get('output_var', 'diff_result')

        if not data1 or not data2:
            return ActionResult(success=False, message="data1 and data2 are required")

        try:
            resolved_data1 = context.resolve_value(data1) if context else data1
            resolved_data2 = context.resolve_value(data2) if context else data2

            if key_field:
                keys1 = {r.get(key_field) for r in resolved_data1}
                keys2 = {r.get(key_field) for r in resolved_data2}

                added = keys2 - keys1
                removed = keys1 - keys2
                common = keys1 & keys2

                added_records = [r for r in resolved_data2 if r.get(key_field) in added]
                removed_records = [r for r in resolved_data1 if r.get(key_field) in removed]

                modified = []
                for key in common:
                    r1 = next((r for r in resolved_data1 if r.get(key_field) == key), {})
                    r2 = next((r for r in resolved_data2 if r.get(key_field) == key), {})
                    if r1 != r2:
                        modified.append({'key': key, 'before': r1, 'after': r2})

                result = {
                    'added_count': len(added),
                    'removed_count': len(removed),
                    'modified_count': len(modified),
                    'unchanged_count': len(common) - len(modified),
                    'added': added_records[:10],
                    'removed': removed_records[:10],
                    'modified': modified[:10],
                }
            else:
                set1 = set(str(r) for r in resolved_data1)
                set2 = set(str(r) for r in resolved_data2)
                added = set2 - set1
                removed = set1 - set2

                result = {
                    'added_count': len(added),
                    'removed_count': len(removed),
                    'unchanged_count': len(set1 & set2),
                }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Diff: +{result['added_count']} -{result['removed_count']} ~{result.get('modified_count', 0)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Diff compare error: {e}")


class DiffSchemaAction(BaseAction):
    """Compare schemas."""
    action_type = "diff_schema"
    display_name = "Schema对比"
    description = "对比数据Schema"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute schema comparison."""
        schema1 = params.get('schema1', {})
        schema2 = params.get('schema2', {})
        output_var = params.get('output_var', 'schema_diff')

        if not schema1 or not schema2:
            return ActionResult(success=False, message="schema1 and schema2 are required")

        try:
            resolved_schema1 = context.resolve_value(schema1) if context else schema1
            resolved_schema2 = context.resolve_value(schema2) if context else schema2

            fields1 = set(resolved_schema1.keys())
            fields2 = set(resolved_schema2.keys())

            added = fields2 - fields1
            removed = fields1 - fields2
            common = fields1 & fields2

            type_changes = []
            for field in common:
                if resolved_schema1[field] != resolved_schema2[field]:
                    type_changes.append({
                        'field': field,
                        'before': resolved_schema1[field],
                        'after': resolved_schema2[field],
                    })

            result = {
                'added_fields': list(added),
                'removed_fields': list(removed),
                'type_changes': type_changes,
                'unchanged_fields': list(common - set(type_changes)),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Schema diff: +{len(added)} -{len(removed)} ~{len(type_changes)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema diff error: {e}")


class DiffPatchAction(BaseAction):
    """Generate/apply patches."""
    action_type = "diff_patch"
    display_name = "数据补丁"
    description = "生成和应用补丁"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute patch operation."""
        data = params.get('data', {})
        patch = params.get('patch', [])
        operation = params.get('operation', 'apply')
        output_var = params.get('output_var', 'patch_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_patch = context.resolve_value(patch) if context else patch

            if operation == 'generate':
                patch = []
                for key, value in resolved_data.items():
                    patch.append({'op': 'add', 'path': f'/{key}', 'value': value})

                result = {
                    'patch': patch,
                    'operation': 'generate',
                }

            elif operation == 'apply':
                patched = resolved_data.copy()
                for p in resolved_patch:
                    op = p.get('op', '')
                    path = p.get('path', '').strip('/')
                    value = p.get('value')

                    if op == 'add' or op == 'replace':
                        patched[path] = value
                    elif op == 'remove':
                        if path in patched:
                            del patched[path]

                result = {
                    'patched_data': patched,
                    'operation': 'apply',
                }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Patch {operation}: {len(resolved_patch) if operation == 'apply' else len(patch)} operations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Diff patch error: {e}")


class DiffMergeAction(BaseAction):
    """Merge diffs."""
    action_type = "diff_merge"
    display_name: "差异合并"
    description = "合并差异"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute diff merge."""
        diffs = params.get('diffs', [])
        strategy = params.get('strategy', 'last_win')
        output_var = params.get('output_var', 'merge_result')

        if not diffs:
            return ActionResult(success=False, message="diffs are required")

        try:
            resolved_diffs = context.resolve_value(diffs) if context else diffs

            merged = {}
            for diff in resolved_diffs:
                for key, value in diff.items():
                    if strategy == 'last_win':
                        merged[key] = value
                    elif strategy == 'first_win' and key not in merged:
                        merged[key] = value
                    elif strategy == 'merge_lists' and isinstance(value, list):
                        if key not in merged:
                            merged[key] = []
                        merged[key].extend(value)

            result = {
                'merged': merged,
                'diffs_merged': len(resolved_diffs),
                'strategy': strategy,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Merged {len(resolved_diffs)} diffs with {strategy} strategy"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Diff merge error: {e}")
