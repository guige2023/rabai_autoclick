"""Data Resolver action module for RabAI AutoClick.

Resolves data references, IDs, and foreign keys
across data sources.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataResolverAction(BaseAction):
    """Resolve data references and foreign keys.

    Resolves IDs to actual data, follows references,
    and handles lazy loading patterns.
    """
    action_type = "data_resolver"
    display_name = "数据解析器"
    description = "解析数据引用和外键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Resolve data references.

        Args:
            context: Execution context.
            params: Dict with keys: data, reference_map, resolve_depth,
                   lookup_sources.

        Returns:
            ActionResult with resolved data.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            reference_map = params.get('reference_map', {})
            resolve_depth = params.get('resolve_depth', 3)
            lookup_sources = params.get('lookup_sources', {})

            if data is None:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            resolved = self._resolve(data, reference_map, lookup_sources, 0, resolve_depth)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message="Resolved data references",
                data={'resolved': resolved, 'depth': resolve_depth},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Resolver error: {str(e)}",
                duration=duration,
            )

    def _resolve(self, data: Any, ref_map: Dict, sources: Dict, depth: int, max_depth: int) -> Any:
        """Recursively resolve references."""
        if depth >= max_depth:
            return data

        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in ref_map and isinstance(value, str):
                    ref = ref_map[key]
                    source_name = ref.get('source')
                    lookup_field = ref.get('lookup_field', 'id')
                    if source_name in sources and isinstance(sources[source_name], dict):
                        result[key] = sources[source_name].get(value, value)
                    else:
                        result[key] = value
                else:
                    result[key] = self._resolve(value, ref_map, sources, depth + 1, max_depth)
            return result

        elif isinstance(data, list):
            return [self._resolve(item, ref_map, sources, depth + 1, max_depth) for item in data]

        return data


class DataReferenceLookupAction(BaseAction):
    """Look up data by references and IDs.

    Performs lookup operations across multiple data
    sources using keys.
    """
    action_type = "data_reference_lookup"
    display_name = "数据引用查找"
    description = "通过引用和ID查找数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform lookups.

        Args:
            context: Execution context.
            params: Dict with keys: lookups (list), data_sources,
                   match_field, return_fields.

        Returns:
            ActionResult with lookup results.
        """
        start_time = time.time()
        try:
            lookups = params.get('lookups', [])
            data_sources = params.get('data_sources', {})
            match_field = params.get('match_field', 'id')
            return_fields = params.get('return_fields', None)

            if not lookups:
                return ActionResult(
                    success=False,
                    message="At least one lookup key is required",
                    duration=time.time() - start_time,
                )

            results = []
            for lookup in lookups:
                key = lookup.get(match_field, lookup.get('key'))
                source_name = lookup.get('source')
                found = None

                if source_name and source_name in data_sources:
                    source = data_sources[source_name]
                    if isinstance(source, dict):
                        found = source.get(key)
                    elif isinstance(source, list):
                        for item in source:
                            if isinstance(item, dict) and item.get(match_field) == key:
                                found = item
                                break

                if found and return_fields:
                    if isinstance(found, dict):
                        found = {k: found.get(k) for k in return_fields if k in found}

                results.append({
                    'key': key,
                    'source': source_name,
                    'found': found is not None,
                    'data': found,
                })

            found_count = sum(1 for r in results if r['found'])
            duration = time.time() - start_time

            return ActionResult(
                success=found_count == len(lookups),
                message=f"Lookup: {found_count}/{len(lookups)} found",
                data={
                    'results': results,
                    'found': found_count,
                    'not_found': len(lookups) - found_count,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Lookup error: {str(e)}",
                duration=duration,
            )


class DataDenormalizerAction(BaseAction):
    """Denormalize data by embedding related records.

    Flattens nested relationships by embedding
    foreign key references as full objects.
    """
    action_type = "data_denormalizer"
    display_name = "数据反规范化"
    description = "通过嵌入关联记录反规范化数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Denormalize data.

        Args:
            context: Execution context.
            params: Dict with keys: data, relations,
                   foreign_key_field, embed_as.

        Returns:
            ActionResult with denormalized data.
        """
        start_time = time.time()
        try:
            data = params.get('data', [])
            relations = params.get('relations', {})
            foreign_key_field = params.get('foreign_key_field', 'id')
            embed_as = params.get('embed_as', 'left')

            if not isinstance(data, list):
                data = [data]

            if not relations:
                return ActionResult(
                    success=False,
                    message="Relations are required",
                    duration=time.time() - start_time,
                )

            denormalized = []
            for item in data:
                if not isinstance(item, dict):
                    continue

                result = item.copy()
                for relation_name, relation_data in relations.items():
                    fk = item.get(foreign_key_field)
                    if not fk:
                        continue

                    related = None
                    if isinstance(relation_data, dict):
                        related = relation_data.get(fk)
                    elif isinstance(relation_data, list):
                        for r in relation_data:
                            if isinstance(r, dict) and r.get(foreign_key_field) == fk:
                                related = r
                                break

                    if related:
                        if embed_as == 'left':
                            result = {**related, **result}
                        elif embed_as == 'right':
                            result = {**result, **related}
                        else:
                            result[relation_name] = related

                denormalized.append(result)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Denormalized {len(denormalized)} records",
                data={'denormalized': denormalized, 'count': len(denormalized)},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Denormalizer error: {str(e)}",
                duration=duration,
            )
