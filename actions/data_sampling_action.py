"""Data sampling action module for RabAI AutoClick.

Provides data sampling operations:
- RandomSamplingAction: Random sampling
- StratifiedSamplingAction: Stratified sampling
- SystematicSamplingAction: Systematic sampling
- ClusterSamplingAction: Cluster sampling
- ReservoirSamplingAction: Reservoir sampling for large datasets
"""

from typing import Any, Dict, List, Optional
import random
import math

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomSamplingAction(BaseAction):
    """Random sampling from data."""
    action_type = "random_sampling"
    display_name = "随机抽样"
    description = "从数据中进行随机抽样"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            n = params.get("n", 10)
            replace = params.get("replace", False)
            seed = params.get("seed")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if seed is not None:
                random.seed(seed)
            
            if replace:
                sampled = random.choices(data, k=n)
            else:
                n = min(n, len(data))
                sampled = random.sample(data, k=n)
            
            return ActionResult(
                success=True,
                message=f"Random sampling complete",
                data={
                    "original_count": len(data),
                    "sample_count": len(sampled),
                    "sample": sampled,
                    "replace": replace,
                    "seed": seed
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class StratifiedSamplingAction(BaseAction):
    """Stratified sampling by groups."""
    action_type = "stratified_sampling"
    display_name = "分层抽样"
    description = "按组进行分层抽样"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by")
            n = params.get("n", 10)
            proportional = params.get("proportional", True)
            seed = params.get("seed")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not group_by:
                return ActionResult(success=False, message="group_by is required")
            
            if seed is not None:
                random.seed(seed)
            
            groups: Dict[str, List] = {}
            for item in data:
                if isinstance(item, dict):
                    key = str(item.get(group_by, "unknown"))
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(item)
            
            sampled = []
            total = len(data)
            
            for group_key, group_items in groups.items():
                if proportional:
                    group_n = max(1, int(n * len(group_items) / total))
                else:
                    group_n = max(1, n // len(groups))
                
                group_n = min(group_n, len(group_items))
                sampled.extend(random.sample(group_items, k=group_n))
            
            return ActionResult(
                success=True,
                message="Stratified sampling complete",
                data={
                    "original_count": len(data),
                    "sample_count": len(sampled),
                    "group_by": group_by,
                    "groups": {k: len(v) for k, v in groups.items()},
                    "proportional": proportional,
                    "sample": sampled[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class SystematicSamplingAction(BaseAction):
    """Systematic sampling at regular intervals."""
    action_type = "systematic_sampling"
    display_name = "系统抽样"
    description = "按固定间隔进行系统抽样"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            n = params.get("n", 10)
            start = params.get("start", 0)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if n <= 0:
                return ActionResult(success=False, message="n must be positive")
            
            total = len(data)
            interval = max(1, total // n)
            
            indices = list(range(start, total, interval))[:n]
            sampled = [data[i] for i in indices]
            
            return ActionResult(
                success=True,
                message="Systematic sampling complete",
                data={
                    "original_count": total,
                    "sample_count": len(sampled),
                    "interval": interval,
                    "start": start,
                    "indices": indices,
                    "sample": sampled
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ClusterSamplingAction(BaseAction):
    """Cluster sampling by grouping."""
    action_type = "cluster_sampling"
    display_name = "整群抽样"
    description = "按分组进行整群抽样"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            cluster_by = params.get("cluster_by")
            n_clusters = params.get("n_clusters", 2)
            seed = params.get("seed")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not cluster_by:
                return ActionResult(success=False, message="cluster_by is required")
            
            if seed is not None:
                random.seed(seed)
            
            clusters: Dict[str, List] = {}
            for item in data:
                if isinstance(item, dict):
                    key = str(item.get(cluster_by, "unknown"))
                    if key not in clusters:
                        clusters[key] = []
                    clusters[key].append(item)
            
            cluster_keys = list(clusters.keys())
            n_clusters = min(n_clusters, len(cluster_keys))
            
            selected_clusters = random.sample(cluster_keys, k=n_clusters)
            
            sampled = []
            for cluster_key in selected_clusters:
                sampled.extend(clusters[cluster_key])
            
            return ActionResult(
                success=True,
                message="Cluster sampling complete",
                data={
                    "original_count": len(data),
                    "sample_count": len(sampled),
                    "total_clusters": len(clusters),
                    "selected_clusters": selected_clusters,
                    "cluster_by": cluster_by,
                    "sample": sampled[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ReservoirSamplingAction(BaseAction):
    """Reservoir sampling for large datasets."""
    action_type = "reservoir_sampling"
    display_name = "水塘抽样"
    description = "对大数据集进行水塘抽样"
    
    def __init__(self):
        super().__init__()
        self._reservoir: List[Any] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream = params.get("stream", [])
            k = params.get("k", 10)
            seed = params.get("seed")
            
            if seed is not None:
                random.seed(seed)
            
            if not stream:
                return ActionResult(
                    success=True,
                    message="Reservoir sampling complete (empty stream)",
                    data={
                        "sample_count": 0,
                        "k": k,
                        "sample": []
                    }
                )
            
            reservoir = self._reservoir_sampling(stream, k)
            
            return ActionResult(
                success=True,
                message="Reservoir sampling complete",
                data={
                    "stream_size": len(stream),
                    "sample_count": len(reservoir),
                    "k": k,
                    "sample": reservoir
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _reservoir_sampling(self, stream: List, k: int) -> List:
        reservoir = []
        
        for i, item in enumerate(stream):
            if i < k:
                reservoir.append(item)
            else:
                j = random.randint(0, i)
                if j < k:
                    reservoir[j] = item
        
        return reservoir


class BootstrapSamplingAction(BaseAction):
    """Bootstrap sampling with replacement."""
    action_type = "bootstrap_sampling"
    display_name = "Bootstrap抽样"
    description = "带放回的Bootstrap抽样"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            n = params.get("n", None)
            seed = params.get("seed")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if seed is not None:
                random.seed(seed)
            
            n = n if n is not None else len(data)
            
            sampled = random.choices(data, k=n)
            
            return ActionResult(
                success=True,
                message="Bootstrap sampling complete",
                data={
                    "original_count": len(data),
                    "sample_count": len(sampled),
                    "with_replacement": True,
                    "sample": sampled[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
