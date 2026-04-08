"""Sampling action module for RabAI AutoClick.

Provides data sampling actions for selecting subsets of data
using various sampling strategies.
"""

import random
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RandomSampleAction(BaseAction):
    """Random sampling from data.
    
    Selects random subset of items.
    """
    action_type = "random_sample"
    display_name = "随机抽样"
    description = "随机选择数据子集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Random sample.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, size, percentage,
                   seed, replace.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        size = params.get('size', 0)
        percentage = params.get('percentage', 0)
        seed = params.get('seed', None)
        replace = params.get('replace', False)

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            if seed is not None:
                random.seed(seed)

            total = len(data)

            if percentage > 0:
                sample_size = int(total * percentage / 100)
            elif size > 0:
                sample_size = min(size, total)
            else:
                return ActionResult(success=False, message="size or percentage required")

            if replace:
                sampled = random.choices(data, k=sample_size)
            else:
                sampled = random.sample(data, k=sample_size)

            return ActionResult(
                success=True,
                message=f"Sampled {len(sampled)} items",
                data={
                    'sample': sampled,
                    'sample_size': len(sampled),
                    'original_size': total,
                    'method': 'random',
                    'seed': seed
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Random sampling failed: {str(e)}")


class StratifiedSampleAction(BaseAction):
    """Stratified sampling from data.
    
    Ensures proportional representation from groups.
    """
    action_type = "stratified_sample"
    display_name = "分层抽样"
    description = "分层比例抽样"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Stratified sample.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, strata_field, sample_size,
                   percentage, seed.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        strata_field = params.get('strata_field', '')
        sample_size = params.get('sample_size', 0)
        percentage = params.get('percentage', 0)
        seed = params.get('seed', None)

        if not data:
            return ActionResult(success=False, message="data is required")
        if not strata_field:
            return ActionResult(success=False, message="strata_field required")

        try:
            if seed is not None:
                random.seed(seed)

            from collections import defaultdict
            strata = defaultdict(list)

            for item in data:
                if isinstance(item, dict):
                    key = item.get(strata_field)
                    if key is not None:
                        strata[key].append(item)

            sampled = []
            strata_info = {}

            for key, items in strata.items():
                if percentage > 0:
                    n = max(1, int(len(items) * percentage / 100))
                elif sample_size > 0:
                    total = sum(len(s) for s in strata.values())
                    n = max(1, int(sample_size * len(items) / total))
                else:
                    n = 1

                n = min(n, len(items))
                stratum_sample = random.sample(items, k=n)
                sampled.extend(stratum_sample)
                strata_info[key] = {'original': len(items), 'sampled': n}

            return ActionResult(
                success=True,
                message=f"Stratified sample: {len(sampled)} items",
                data={
                    'sample': sampled,
                    'sample_size': len(sampled),
                    'original_size': len(data),
                    'strata_info': strata_info,
                    'strata_field': strata_field
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stratified sampling failed: {str(e)}")


class SystematicSampleAction(BaseAction):
    """Systematic sampling at regular intervals.
    
    Selects every Nth item from data.
    """
    action_type = "systematic_sample"
    display_name = "系统抽样"
    description = "等距系统抽样"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Systematic sample.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, interval, start,
                   shuffle_before.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        interval = params.get('interval', 10)
        start = params.get('start', 0)
        shuffle_before = params.get('shuffle_before', False)

        if not data:
            return ActionResult(success=False, message="data is required")
        if interval < 1:
            return ActionResult(success=False, message="interval must be >= 1")

        try:
            items = list(data)
            
            if shuffle_before:
                random.shuffle(items)

            start_idx = start % len(items) if items else 0
            sampled = items[start_idx::interval]

            return ActionResult(
                success=True,
                message=f"Systematic sample: {len(sampled)} items (interval={interval})",
                data={
                    'sample': sampled,
                    'sample_size': len(sampled),
                    'original_size': len(data),
                    'interval': interval,
                    'start': start
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Systematic sampling failed: {str(e)}")


class ClusterSampleAction(BaseAction):
    """Cluster sampling - select entire clusters.
    
    Randomly selects clusters of items.
    """
    action_type = "cluster_sample"
    display_name = "整群抽样"
    description = "整群抽样"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Cluster sample.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, cluster_field, num_clusters,
                   seed.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        cluster_field = params.get('cluster_field', '')
        num_clusters = params.get('num_clusters', 1)
        seed = params.get('seed', None)

        if not data:
            return ActionResult(success=False, message="data is required")
        if not cluster_field:
            return ActionResult(success=False, message="cluster_field required")

        try:
            if seed is not None:
                random.seed(seed)

            from collections import defaultdict
            clusters = defaultdict(list)

            for item in data:
                if isinstance(item, dict):
                    key = item.get(cluster_field)
                    if key is not None:
                        clusters[key].append(item)

            cluster_keys = list(clusters.keys())
            num_to_select = min(num_clusters, len(cluster_keys))
            
            selected_keys = random.sample(cluster_keys, k=num_to_select)
            sampled = []
            
            for key in selected_keys:
                sampled.extend(clusters[key])

            return ActionResult(
                success=True,
                message=f"Cluster sample: {len(sampled)} items from {num_to_select} clusters",
                data={
                    'sample': sampled,
                    'sample_size': len(sampled),
                    'original_size': len(data),
                    'clusters_selected': num_to_select,
                    'total_clusters': len(cluster_keys),
                    'selected_clusters': selected_keys
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cluster sampling failed: {str(e)}")


class ReservoirSampleAction(BaseAction):
    """Reservoir sampling for large datasets.
    
    Provides statistically representative sample without knowing total size.
    """
    action_type = "reservoir_sample"
    display_name = "水库抽样"
    description = "大数据集水库抽样"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reservoir sample.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, k, seed.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        k = params.get('k', 10)
        seed = params.get('seed', None)

        if not data:
            return ActionResult(success=False, message="data is required")
        if k < 1:
            return ActionResult(success=False, message="k must be >= 1")

        try:
            if seed is not None:
                random.seed(seed)

            n = len(data)
            k = min(k, n)

            reservoir = list(data[:k])

            for i in range(k, n):
                j = random.randint(0, i)
                if j < k:
                    reservoir[j] = data[i]

            return ActionResult(
                success=True,
                message=f"Reservoir sample: {k} items",
                data={
                    'sample': reservoir,
                    'sample_size': k,
                    'original_size': n,
                    'method': 'reservoir'
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Reservoir sampling failed: {str(e)}")
