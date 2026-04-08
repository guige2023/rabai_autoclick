"""Partition action module for RabAI AutoClick.

Provides data partitioning and bucketing operations
for distributing data into groups.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PartitionByValueAction(BaseAction):
    """Partition data by field values.
    
    Group records into partitions based on unique
    values of a specified field.
    """
    action_type = "partition_by_value"
    display_name = "按值分区"
    description = "按字段值将数据分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Partition data by field value.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - field: str (field to partition by)
                - include_null: bool (include null values)
                - save_to_var: str
        
        Returns:
            ActionResult with partitioned data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        include_null = params.get('include_null', True)
        save_to_var = params.get('save_to_var', 'partition_result')

        if not field:
            return ActionResult(success=False, message="field is required")

        partitions: Dict[str, List] = defaultdict(list)

        for record in data:
            if isinstance(record, dict):
                key = record.get(field)
                if key is None and not include_null:
                    continue
                partitions[str(key) if key is not None else '__null__'].append(record)
            else:
                partitions['__untitled__'].append(record)

        result = {
            'partition_count': len(partitions),
            'partitions': dict(partitions),
            'partition_keys': list(partitions.keys()),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Partitioned {len(data)} items into {len(partitions)} groups"
        )


class BucketNumbersAction(BaseAction):
    """Bucket numeric values into ranges.
    
    Create histogram-style buckets with configurable
    boundaries and labels.
    """
    action_type = "bucket_numbers"
    display_name = "数值分桶"
    description = "将数值分配到指定范围的桶中"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Bucket numeric values.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - field: str (numeric field)
                - buckets: list of {min, max, label}
                - save_to_var: str
        
        Returns:
            ActionResult with bucketed data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        buckets_def = params.get('buckets', [])
        save_to_var = params.get('save_to_var', 'bucket_result')

        if not buckets_def:
            return ActionResult(success=False, message="buckets definition required")

        # Initialize buckets
        buckets: Dict[str, List] = {b.get('label', f'bucket_{i}'): [] for i, b in enumerate(buckets_def)}
        buckets['__unmatched__'] = []

        for record in data:
            if field and isinstance(record, dict):
                value = record.get(field)
            else:
                value = record

            try:
                num_value = float(value)
            except (ValueError, TypeError):
                buckets['__unmatched__'].append(record)
                continue

            matched = False
            for b in buckets_def:
                min_val = b.get('min', float('-inf'))
                max_val = b.get('max', float('inf'))
                if min_val <= num_value < max_val:
                    label = b.get('label', 'unknown')
                    buckets[label].append(record)
                    matched = True
                    break

            if not matched:
                buckets['__unmatched__'].append(record)

        # Build summary
        summary = []
        for label, items in buckets.items():
            if label != '__unmatched__':
                summary.append({
                    'label': label,
                    'count': len(items),
                })

        unmatched_count = len(buckets['__unmatched__'])
        if unmatched_count > 0:
            summary.append({'label': '__unmatched__', 'count': unmatched_count})

        result = {
            'buckets': dict(buckets),
            'summary': summary,
            'total_buckets': len([k for k in buckets if k != '__unmatched__']),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Bucketed into {result['total_buckets']} buckets"
        )


class QuantilePartitionAction(BaseAction):
    """Partition data into quantile buckets.
    
    Divide data into n equal-sized groups based
    on sorted values.
    """
    action_type = "quantile_partition"
    display_name = "分位数分区"
    description = "按分位数将数据分成相等的组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Partition into quantiles.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of items
                - field: str (numeric field)
                - num_quantiles: int (number of groups, default 4)
                - labels: list of str (optional labels)
                - save_to_var: str
        
        Returns:
            ActionResult with quantile partitions.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        num_quantiles = params.get('num_quantiles', 4)
        labels = params.get('labels', [])
        save_to_var = params.get('save_to_var', 'quantile_result')

        # Extract and sort values
        pairs = []
        for record in data:
            if field and isinstance(record, dict):
                val = record.get(field)
            else:
                val = record
            try:
                pairs.append((float(val), record))
            except (ValueError, TypeError):
                pass

        pairs.sort(key=lambda x: x[0])

        if not pairs:
            return ActionResult(success=False, message="No valid numeric values")

        n = len(pairs)
        bucket_size = n / num_quantiles

        partitions: Dict[str, List] = {}
        for i in range(num_quantiles):
            start_idx = int(i * bucket_size)
            end_idx = int((i + 1) * bucket_size)
            label = labels[i] if i < len(labels) else f'Q{i + 1}'
            partitions[label] = [p[1] for p in pairs[start_idx:end_idx]]

        summary = {k: len(v) for k, v in partitions.items()}

        result = {
            'quantiles': partitions,
            'summary': summary,
            'num_quantiles': num_quantiles,
            'bucket_size': bucket_size,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Partitioned into {num_quantiles} quantiles"
        )


class StratifiedSampleAction(BaseAction):
    """Create stratified sample from data.
    
    Sample proportionally from subgroups to maintain
    distribution across categories.
    """
    action_type = "stratified_sample"
    display_name: "分层抽样"
    description = "分层抽样：按比例从各层抽取样本保持分布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create stratified sample.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - stratify_by: str (field to stratify by)
                - sample_size: int (total sample size)
                - proportional: bool (proportional sampling)
                - min_per_stratum: int (minimum per group)
                - save_to_var: str
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        stratify_by = params.get('stratify_by', '')
        sample_size = params.get('sample_size', 10)
        proportional = params.get('proportional', True)
        min_per_stratum = params.get('min_per_stratum', 1)
        save_to_var = params.get('save_to_var', 'sample_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Group by stratify field
        strata: Dict[str, List] = defaultdict(list)
        for record in data:
            if isinstance(record, dict) and stratify_by:
                key = str(record.get(stratify_by, '__unknown__'))
            else:
                key = '__all__'
            strata[key].append(record)

        total = len(data)
        sampled = []

        for stratum_name, stratum_data in strata.items():
            stratum_size = len(stratum_data)

            if proportional:
                # Proportional allocation
                ratio = stratum_size / total
                n = max(min_per_stratum, int(sample_size * ratio))
            else:
                # Equal allocation
                n = max(min_per_stratum, sample_size // len(strata))

            n = min(n, stratum_size)
            sampled.extend(stratum_data[:n])

        result = {
            'sample_size': len(sampled),
            'original_size': total,
            'strata_sampled': len(strata),
            'sample': sampled,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Stratified sample: {len(sampled)}/{total} items from {len(strata)} strata"
        )
