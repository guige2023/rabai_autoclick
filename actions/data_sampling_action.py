"""Data sampling action module for RabAI AutoClick.

Provides various data sampling strategies including
random, stratified, systematic, and reservoir sampling.
"""

import sys
import os
import random
import math
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SamplingStrategy(Enum):
    """Sampling strategy types."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"
    WEIGHTED = "weighted"


class DataSamplingAction(BaseAction):
    """Sample data using various strategies.
    
    Supports random, stratified, systematic, reservoir,
    cluster, and weighted sampling methods.
    """
    action_type = "data_sampling"
    display_name = "数据采样"
    description = "多种数据采样策略：随机/分层/系统/水库采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sample data from a dataset.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list, data to sample from
                - strategy: str (random/stratified/systematic/reservoir/cluster/weighted)
                - sample_size: int or float (if < 1, fraction)
                - seed: int, random seed for reproducibility
                - stratify_by: str, field name for stratified sampling
                - weights: list of floats for weighted sampling
                - step: int, step size for systematic sampling
                - save_to_var: str
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        strategy = params.get('strategy', 'random')
        sample_size = params.get('sample_size', 10)
        seed = params.get('seed', None)
        stratify_by = params.get('stratify_by', None)
        weights = params.get('weights', None)
        step = params.get('step', None)
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Set random seed
        if seed is not None:
            random.seed(seed)

        # Convert sample_size to absolute if fraction
        if 0 < sample_size < 1:
            sample_size = max(1, int(len(data) * sample_size))

        sample_size = min(sample_size, len(data))

        if strategy == 'random':
            result = self._random_sample(data, sample_size)
        elif strategy == 'stratified':
            result = self._stratified_sample(data, sample_size, stratify_by)
        elif strategy == 'systematic':
            result = self._systematic_sample(data, sample_size, step)
        elif strategy == 'reservoir':
            result = self._reservoir_sample(data, sample_size)
        elif strategy == 'cluster':
            result = self._cluster_sample(data, sample_size)
        elif strategy == 'weighted':
            result = self._weighted_sample(data, sample_size, weights)
        else:
            return ActionResult(success=False, message=f"Unknown strategy: {strategy}")

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"Sampled {len(result)} items using {strategy} strategy",
            data=result
        )

    def _random_sample(self, data: List, sample_size: int) -> List:
        """Simple random sampling without replacement."""
        return random.sample(data, sample_size)

    def _stratified_sample(
        self, data: List, sample_size: int, stratify_by: Optional[str]
    ) -> List:
        """Stratified sampling - equal representation from each stratum."""
        if not stratify_by:
            return self._random_sample(data, sample_size)

        # Group by stratum
        strata = defaultdict(list)
        for item in data:
            if isinstance(item, dict) and stratify_by in item:
                key = str(item[stratify_by])
            else:
                key = "unknown"
            strata[key].append(item)

        # Calculate samples per stratum
        n_strata = len(strata)
        per_stratum = max(1, sample_size // n_strata)

        sampled = []
        for stratum_key, stratum_items in strata.items():
            k = min(per_stratum, len(stratum_items))
            sampled.extend(random.sample(stratum_items, k))

        # If we need more to reach sample_size
        remaining = sample_size - len(sampled)
        if remaining > 0:
            already_sampled = set(id(x) for x in sampled)
            remaining_items = [x for x in data if id(x) not in already_sampled]
            sampled.extend(random.sample(remaining_items, min(remaining, len(remaining_items))))

        return sampled[:sample_size]

    def _systematic_sample(
        self, data: List, sample_size: int, step: Optional[int]
    ) -> List:
        """Systematic sampling - every k-th item."""
        if len(data) <= sample_size:
            return data[:]

        k = step if step else len(data) // sample_size
        start = random.randint(0, min(k - 1, len(data) - 1))
        sampled = []
        idx = start
        while idx < len(data):
            sampled.append(data[idx])
            idx += k
        return sampled

    def _reservoir_sample(self, data: List, sample_size: int) -> List:
        """Reservoir sampling - streaming-compatible uniform sampling."""
        if len(data) <= sample_size:
            return data[:]

        reservoir = data[:sample_size]
        for i in range(sample_size, len(data)):
            j = random.randint(0, i)
            if j < sample_size:
                reservoir[j] = data[i]
        return reservoir

    def _cluster_sample(self, data: List, sample_size: int) -> List:
        """Cluster sampling - random clusters of adjacent items."""
        if len(data) <= sample_size:
            return data[:]

        cluster_size = max(1, sample_size // 3)
        n_clusters = max(1, sample_size // cluster_size)
        start_idx = random.randint(0, max(0, len(data) - n_clusters * cluster_size))

        sampled = []
        for _ in range(n_clusters):
            end_idx = min(start_idx + cluster_size, len(data))
            sampled.extend(data[start_idx:end_idx])
            start_idx = end_idx
            if len(sampled) >= sample_size:
                break

        return sampled[:sample_size]

    def _weighted_sample(
        self, data: List, sample_size: int, weights: Optional[List[float]]
    ) -> List:
        """Weighted sampling - higher weight = higher probability."""
        if len(data) == 0:
            return []
        if not weights:
            return self._random_sample(data, sample_size)

        if len(weights) != len(data):
            return self._random_sample(data, sample_size)

        total_weight = sum(w for w in weights if w > 0)
        if total_weight <= 0:
            return self._random_sample(data, sample_size)

        # Normalize weights
        norm_weights = [w / total_weight for w in weights]

        sampled = []
        indices = list(range(len(data)))
        for _ in range(sample_size):
            idx = random.choices(indices, weights=norm_weights, k=1)[0]
            sampled.append(data[idx])

        return sampled

    def get_required_params(self) -> List[str]:
        return ['data', 'strategy']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'sample_size': 10,
            'seed': None,
            'stratify_by': None,
            'weights': None,
            'step': None,
            'save_to_var': None,
        }
