"""Data Sampler action module for RabAI AutoClick.

Sampling strategies for data: random, stratified,
reservoir, and weighted selection.
"""

import random
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSamplerAction(BaseAction):
    """Sample data with various strategies.

    Random, stratified, reservoir, and weighted sampling
    for data reduction and testing.
    """
    action_type = "data_sampler"
    display_name = "数据采样器"
    description = "随机、分层、水库加权采样"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sample data.

        Args:
            context: Execution context.
            params: Dict with keys: items, strategy (random/stratified/reservoir/weighted),
                   sample_size, strata_field, weights_field, seed.

        Returns:
            ActionResult with sampled data.
        """
        start_time = time.time()
        try:
            items = params.get('items', [])
            strategy = params.get('strategy', 'random')
            sample_size = params.get('sample_size', 10)
            strata_field = params.get('strata_field')
            weights_field = params.get('weights_field')
            seed = params.get('seed')

            if seed is not None:
                random.seed(seed)

            if not items:
                return ActionResult(success=False, message="items is required", duration=time.time() - start_time)

            if strategy == 'random':
                sample = random.sample(items, min(sample_size, len(items)))

            elif strategy == 'reservoir':
                sample = self._reservoir_sample(items, sample_size)

            elif strategy == 'stratified':
                if not strata_field:
                    return ActionResult(success=False, message="strata_field required for stratified sampling", duration=time.time() - start_time)
                strata = {}
                for item in items:
                    if isinstance(item, dict):
                        key = item.get(strata_field, 'unknown')
                    else:
                        key = str(item)
                    strata.setdefault(key, []).append(item)
                sample = []
                per_strata = max(1, sample_size // max(1, len(strata)))
                for items_in_strata in strata.values():
                    sample.extend(random.sample(items_in_strata, min(per_strata, len(items_in_strata))))
                if len(sample) > sample_size:
                    sample = sample[:sample_size]

            elif strategy == 'weighted':
                if not weights_field:
                    return ActionResult(success=False, message="weights_field required for weighted sampling", duration=time.time() - start_time)
                weights = [item.get(weights_field, 1) if isinstance(item, dict) else 1 for item in items]
                total = sum(weights)
                if total == 0:
                    sample = items[:sample_size]
                else:
                    cumsum = 0
                    cumulative = []
                    for w in weights:
                        cumsum += w / total
                        cumulative.append(cumsum)
                    sample = []
                    for _ in range(min(sample_size, len(items))):
                        r = random.random()
                        for i, c in enumerate(cumulative):
                            if r <= c:
                                if items[i] not in sample:
                                    sample.append(items[i])
                                break

            else:
                sample = items[:sample_size]

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Sampled {len(sample)} from {len(items)} ({strategy})",
                data={'sample': sample, 'count': len(sample), 'total': len(items), 'strategy': strategy},
                duration=duration,
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sampler error: {str(e)}", duration=time.time() - start_time)

    def _reservoir_sample(self, items: List, k: int) -> List:
        """Reservoir sampling algorithm R."""
        if k >= len(items):
            return items[:]
        reservoir = items[:k]
        for i in range(k, len(items)):
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = items[i]
        return reservoir
