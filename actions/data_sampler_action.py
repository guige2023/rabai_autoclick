"""Data sampler action module for RabAI AutoClick.

Provides data sampling capabilities including random sampling,
stratified sampling, and systematic sampling.
"""

import random
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SamplingMethod(Enum):
    """Sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    RESERVOIR = "reservoir"


from enum import Enum


class DataSamplerAction(BaseAction):
    """Data sampler action for statistical sampling.
    
    Supports random, stratified, systematic, cluster, and reservoir
    sampling methods.
    """
    action_type = "data_sampler"
    display_name = "数据采样"
    description = "统计采样与数据抽取"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sampling operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to sample
                method: Sampling method
                sample_size: Number of samples
                sample_rate: Sample rate (alternative to sample_size)
                random_state: Random seed for reproducibility
                strata_field: Field for stratified sampling
                cluster_field: Field for cluster sampling.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        method = params.get('method', 'random')
        sample_size = params.get('sample_size')
        sample_rate = params.get('sample_rate')
        random_state = params.get('random_state', 42)
        strata_field = params.get('strata_field')
        cluster_field = params.get('cluster_field')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if random_state is not None:
            random.seed(random_state)
        
        total = len(data)
        
        if sample_size is None and sample_rate is None:
            sample_size = max(1, total // 10)
        elif sample_rate is not None:
            sample_size = max(1, int(total * sample_rate))
        
        sample_size = min(sample_size, total)
        
        if method == 'random':
            return self._random_sample(data, sample_size)
        elif method == 'stratified':
            return self._stratified_sample(data, sample_size, strata_field)
        elif method == 'systematic':
            return self._systematic_sample(data, sample_size)
        elif method == 'cluster':
            return self._cluster_sample(data, sample_size, cluster_field)
        elif method == 'reservoir':
            return self._reservoir_sample(data, sample_size)
        else:
            return ActionResult(success=False, message=f"Unknown sampling method: {method}")
    
    def _random_sample(
        self,
        data: List[Any],
        sample_size: int
    ) -> ActionResult:
        """Random sampling without replacement."""
        sampled = random.sample(data, sample_size)
        
        return ActionResult(
            success=True,
            message=f"Random sampled {sample_size} items",
            data={
                'samples': sampled,
                'sample_size': sample_size,
                'original_size': len(data),
                'method': 'random'
            }
        )
    
    def _stratified_sample(
        self,
        data: List[Any],
        sample_size: int,
        strata_field: Optional[str]
    ) -> ActionResult:
        """Stratified sampling by field."""
        if not strata_field:
            return ActionResult(success=False, message="strata_field required for stratified sampling")
        
        if not isinstance(data[0], dict) if data else False:
            return ActionResult(success=False, message="Stratified sampling requires list of dicts")
        
        strata: Dict[Any, List[Any]] = {}
        for item in data:
            if isinstance(item, dict):
                key = item.get(strata_field)
                if key not in strata:
                    strata[key] = []
                strata[key].append(item)
        
        total = len(data)
        sampled = []
        
        for stratum_key, stratum_data in strata.items():
            stratum_size = max(1, int(sample_size * len(stratum_data) / total))
            stratum_size = min(stratum_size, len(stratum_data))
            
            stratum_sample = random.sample(stratum_data, stratum_size)
            sampled.extend(stratum_sample)
        
        return ActionResult(
            success=True,
            message=f"Stratified sampled {len(sampled)} items across {len(strata)} strata",
            data={
                'samples': sampled,
                'sample_size': len(sampled),
                'original_size': len(data),
                'strata_count': len(strata),
                'method': 'stratified'
            }
        )
    
    def _systematic_sample(
        self,
        data: List[Any],
        sample_size: int
    ) -> ActionResult:
        """Systematic sampling with fixed interval."""
        total = len(data)
        interval = total // sample_size
        
        if interval <= 0:
            interval = 1
        
        sampled = []
        for i in range(0, total, interval):
            sampled.append(data[i])
            if len(sampled) >= sample_size:
                break
        
        return ActionResult(
            success=True,
            message=f"Systematic sampled {len(sampled)} items",
            data={
                'samples': sampled,
                'sample_size': len(sampled),
                'original_size': len(data),
                'interval': interval,
                'method': 'systematic'
            }
        )
    
    def _cluster_sample(
        self,
        data: List[Any],
        sample_size: int,
        cluster_field: Optional[str]
    ) -> ActionResult:
        """Cluster sampling by field."""
        if not cluster_field:
            return ActionResult(success=False, message="cluster_field required for cluster sampling")
        
        if not isinstance(data[0], dict) if data else False:
            return ActionResult(success=False, message="Cluster sampling requires list of dicts")
        
        clusters: Dict[Any, List[Any]] = {}
        for item in data:
            if isinstance(item, dict):
                key = item.get(cluster_field)
                if key not in clusters:
                    clusters[key] = []
                clusters[key].append(item)
        
        num_clusters = max(1, sample_size)
        cluster_keys = random.sample(list(clusters.keys()), min(num_clusters, len(clusters)))
        
        sampled = []
        for key in cluster_keys:
            sampled.extend(clusters[key])
        
        return ActionResult(
            success=True,
            message=f"Cluster sampled {len(sampled)} items from {len(cluster_keys)} clusters",
            data={
                'samples': sampled,
                'sample_size': len(sampled),
                'original_size': len(data),
                'clusters_selected': len(cluster_keys),
                'total_clusters': len(clusters),
                'method': 'cluster'
            }
        )
    
    def _reservoir_sample(
        self,
        data: List[Any],
        sample_size: int
    ) -> ActionResult:
        """Reservoir sampling for streaming data."""
        if len(data) <= sample_size:
            return ActionResult(
                success=True,
                message=f"Reservoir sampled all {len(data)} items",
                data={
                    'samples': list(data),
                    'sample_size': len(data),
                    'original_size': len(data),
                    'method': 'reservoir'
                }
            )
        
        reservoir = data[:sample_size]
        
        for i in range(sample_size, len(data)):
            j = random.randint(0, i)
            if j < sample_size:
                reservoir[j] = data[i]
        
        return ActionResult(
            success=True,
            message=f"Reservoir sampled {sample_size} items",
            data={
                'samples': reservoir,
                'sample_size': sample_size,
                'original_size': len(data),
                'method': 'reservoir'
            }
        )
