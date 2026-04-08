"""Data Sample Action.

Samples data using various strategies (random, stratified, cluster, reservoir)
with configurable sample sizes and reproducibility.
"""

import sys
import os
import random
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSampleAction(BaseAction):
    """Sample data using various strategies.
    
    Supports random, stratified, cluster, and reservoir sampling
    with configurable sizes and random seed for reproducibility.
    """
    action_type = "data_sample"
    display_name = "数据采样"
    description = "数据采样，支持随机/分层/聚类/水库等多种采样策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sample data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to sample.
                - method: 'random', 'stratified', 'cluster', 'reservoir', 'systematic'.
                - size: Sample size (int or float 0-1 for percentage).
                - seed: Random seed for reproducibility.
                - stratify_by: Field for stratified sampling.
                - cluster_by: Field for cluster sampling.
                - replace: Allow replacement (default: False).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with sampled data.
        """
        try:
            data = params.get('data')
            method = params.get('method', 'random').lower()
            size = params.get('size')
            seed = params.get('seed')
            stratify_by = params.get('stratify_by')
            cluster_by = params.get('cluster_by')
            replace = params.get('replace', False)
            save_to_var = params.get('save_to_var', 'sampled_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if seed is not None:
                random.seed(seed)

            if size is None:
                return ActionResult(success=False, message="size is required")

            # Convert percentage to count
            if isinstance(size, float) and 0 < size <= 1:
                size = int(len(data) * size)

            if size > len(data) and not replace:
                size = len(data)

            if method == 'random':
                result = self._random_sample(data, size, replace)
            elif method == 'stratified':
                result = self._stratified_sample(data, size, stratify_by, replace)
            elif method == 'cluster':
                result = self._cluster_sample(data, size, cluster_by, replace)
            elif method == 'reservoir':
                result = self._reservoir_sample(data, size)
            elif method == 'systematic':
                result = self._systematic_sample(data, size)
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            summary = {
                'original_size': len(data),
                'sample_size': len(result),
                'method': method,
                'seed': seed,
                'replace': replace
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=summary,
                             message=f"Sampled {len(result)}/{len(data)} using {method}")

        except Exception as e:
            return ActionResult(success=False, message=f"Sample error: {e}")

    def _random_sample(self, data: List, size: int, replace: bool) -> List:
        """Random sampling with/without replacement."""
        if replace:
            return random.choices(data, k=size)
        else:
            return random.sample(data, k=min(size, len(data)))

    def _stratified_sample(self, data: List, size: int, stratify_by: str, replace: bool) -> List:
        """Stratified sampling - proportional from each stratum."""
        if not stratify_by:
            return self._random_sample(data, size, replace)

        # Group by stratum
        strata = {}
        for item in data:
            if isinstance(item, dict):
                key = item.get(stratify_by)
            else:
                key = item
            if key not in strata:
                strata[key] = []
            strata[key].append(item)

        # Calculate proportional sizes
        total = len(data)
        samples = []
        remaining_size = size

        for key, items in strata.items():
            proportion = len(items) / total
            stratum_size = max(1, int(size * proportion))
            stratum_size = min(stratum_size, remaining_size)
            remaining_size -= stratum_size

            stratum_sample = self._random_sample(items, stratum_size, replace)
            samples.extend(stratum_sample)

        # If we still have remaining, add randomly
        while remaining_size > 0 and samples:
            samples.append(random.choice(samples))
            remaining_size -= 1

        return samples

    def _cluster_sample(self, data: List, size: int, cluster_by: str, replace: bool) -> List:
        """Cluster sampling - select clusters, return all items in cluster."""
        if not cluster_by:
            return self._random_sample(data, size, replace)

        # Group by cluster
        clusters = {}
        for item in data:
            if isinstance(item, dict):
                key = item.get(cluster_by)
            else:
                continue
            if key not in clusters:
                clusters[key] = []
            clusters[key].append(item)

        cluster_keys = list(clusters.keys())
        num_clusters = max(1, size // 10)  # Approximate

        selected_clusters = self._random_sample(cluster_keys, min(num_clusters, len(cluster_keys)), replace=False)

        result = []
        for cluster_key in selected_clusters:
            result.extend(clusters[cluster_key])

        return result

    def _reservoir_sample(self, data: List, size: int) -> List:
        """Reservoir sampling - maintains uniform sample for streaming data."""
        if size >= len(data):
            return list(data)

        reservoir = list(data[:size])
        for i in range(size, len(data)):
            j = random.randint(0, i)
            if j < size:
                reservoir[j] = data[i]

        return reservoir

    def _systematic_sample(self, data: List, size: int) -> List:
        """Systematic sampling - every k-th item."""
        if size >= len(data):
            return list(data)

        step = len(data) // size
        result = []
        for i in range(0, len(data), step):
            result.append(data[i])
            if len(result) >= size:
                break

        return result
