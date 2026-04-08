"""Sampler action module for RabAI AutoClick.

Provides data sampling strategies for large datasets.
"""

import random
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SamplerAction(BaseAction):
    """Data sampling operations for datasets.
    
    Supports random sampling, stratified sampling, systematic sampling,
    cluster sampling, and reservoir sampling for streaming data.
    """
    action_type = "sampler"
    display_name = "数据采样"
    description = "多种采样策略：随机、分层、系统、储层"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sampling operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'random', 'stratified', 'systematic', 'cluster', 'reservoir'
                - data: List of items to sample
                - n: Number of samples (for random/systematic/reservoir)
                - fraction: Fraction to sample (alternative to n)
                - strata: Stratification key function or field name
                - seed: Random seed for reproducibility
                - clusters: Number of clusters (for cluster sampling)
        
        Returns:
            ActionResult with sampled data.
        """
        command = params.get('command', 'random')
        data = params.get('data', [])
        n = params.get('n')
        fraction = params.get('fraction')
        strata = params.get('strata')
        seed = params.get('seed')
        clusters = params.get('clusters', 5)
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if seed is not None:
            random.seed(seed)
        
        total = len(data)
        if n is None and fraction is not None:
            n = max(1, int(total * fraction))
        elif n is None:
            n = max(1, total // 10)
        n = min(n, total)
        
        if command == 'random':
            return self._random_sample(data, n)
        if command == 'stratified':
            return self._stratified_sample(data, n, strata)
        if command == 'systematic':
            return self._systematic_sample(data, n)
        if command == 'cluster':
            return self._cluster_sample(data, n, clusters)
        if command == 'reservoir':
            return self._reservoir_sample(data, n)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _random_sample(self, data: List[Any], n: int) -> ActionResult:
        """Simple random sampling."""
        sampled = random.sample(data, n)
        return ActionResult(
            success=True,
            message=f"Random sample: {n}/{len(data)}",
            data={'sampled': sampled, 'count': n, 'total': len(data)}
        )
    
    def _stratified_sample(self, data: List[Any], n: int, strata: Optional[str]) -> ActionResult:
        """Stratified sampling by field."""
        if strata is None:
            return self._random_sample(data, n)
        
        groups: Dict[Any, List[Any]] = {}
        for item in data:
            key = item.get(strata) if isinstance(item, dict) else getattr(item, strata, None)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        total = len(data)
        sampled: List[Any] = []
        for group_key, group_items in groups.items():
            group_n = max(1, int(n * len(group_items) / total))
            group_n = min(group_n, len(group_items))
            sampled.extend(random.sample(group_items, group_n))
        
        return ActionResult(
            success=True,
            message=f"Stratified sample: {len(sampled)}/{len(data)} across {len(groups)} strata",
            data={'sampled': sampled, 'count': len(sampled), 'total': len(data), 'strata': len(groups)}
        )
    
    def _systematic_sample(self, data: List[Any], n: int) -> ActionResult:
        """Systematic sampling with interval."""
        interval = len(data) // n
        start = random.randint(0, interval - 1) if interval > 1 else 0
        sampled = [data[i] for i in range(start, len(data), interval)][:n]
        return ActionResult(
            success=True,
            message=f"Systematic sample: {len(sampled)}/{len(data)}",
            data={'sampled': sampled, 'count': len(sampled), 'total': len(data), 'interval': interval}
        )
    
    def _cluster_sample(self, data: List[Any], n: int, num_clusters: int) -> ActionResult:
        """Cluster sampling - select clusters randomly."""
        cluster_size = max(1, len(data) // num_clusters)
        cluster_indices = list(range(0, len(data), cluster_size))
        selected_cluster_starts = random.sample(cluster_indices, min(num_clusters, len(cluster_indices)))
        sampled = []
        for start in selected_cluster_starts:
            end = min(start + cluster_size, len(data))
            sampled.extend(data[start:end])
        sampled = sampled[:n]
        return ActionResult(
            success=True,
            message=f"Cluster sample: {len(sampled)}/{len(data)} from {len(selected_cluster_starts)} clusters",
            data={'sampled': sampled, 'count': len(sampled), 'total': len(data), 'clusters': len(selected_cluster_starts)}
        )
    
    def _reservoir_sample(self, data: List[Any], n: int) -> ActionResult:
        """Reservoir sampling - for streaming/unbounded data."""
        if len(data) <= n:
            return ActionResult(
                success=True,
                message=f"Data smaller than k, returning all: {len(data)}",
                data={'sampled': data, 'count': len(data), 'total': len(data), 'reservoir': True}
            )
        reservoir = data[:n]
        for i in range(n, len(data)):
            j = random.randint(0, i)
            if j < n:
                reservoir[j] = data[i]
        return ActionResult(
            success=True,
            message=f"Reservoir sample: {n}/{len(data)}",
            data={'sampled': reservoir, 'count': n, 'total': len(data), 'reservoir': True}
        )
