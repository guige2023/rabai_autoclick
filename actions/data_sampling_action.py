"""Data sampling action module for RabAI AutoClick.

Provides data sampling operations:
- RandomSamplingAction: Random sampling from datasets
- StratifiedSamplingAction: Stratified sampling
- ClusterSamplingAction: Cluster-based sampling
- ReservoirSamplingAction: Reservoir sampling for streaming data
"""

import random
import hashlib
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomSamplingAction(BaseAction):
    """Random sampling from datasets."""
    action_type = "random_sampling"
    display_name = "随机抽样"
    description = "从数据集中随机抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sample_size = params.get("sample_size", 10)
            sample_fraction = params.get("sample_fraction", None)
            seed = params.get("seed", None)
            replacement = params.get("replacement", False)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if seed is not None:
                random.seed(seed)

            n = len(data)
            if n == 0:
                return ActionResult(success=False, message="Empty dataset")

            if sample_fraction is not None:
                sample_size = int(n * sample_fraction)

            if sample_size > n and not replacement:
                sample_size = n

            if replacement:
                sampled = [random.choice(data) for _ in range(sample_size)]
            else:
                sampled = random.sample(data, min(sample_size, n))

            return ActionResult(
                success=True,
                message=f"Sampled {len(sampled)} items from {n}",
                data={"sample": sampled, "sample_size": len(sampled), "total_size": n},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RandomSampling error: {e}")


class StratifiedSamplingAction(BaseAction):
    """Stratified sampling maintaining group proportions."""
    action_type = "stratified_sampling"
    display_name = "分层抽样"
    description = "分层抽样保持各层比例"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            strata_field = params.get("strata_field", "category")
            sample_fraction = params.get("sample_fraction", 0.1)
            min_sample = params.get("min_sample", 1)
            seed = params.get("seed", None)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if seed is not None:
                random.seed(seed)

            strata: Dict[str, List] = {}
            for item in data:
                if isinstance(item, dict):
                    key = str(item.get(strata_field, "unknown"))
                else:
                    key = "default"
                if key not in strata:
                    strata[key] = []
                strata[key].append(item)

            sampled = []
            strata_info = {}

            for stratum_name, stratum_data in strata.items():
                stratum_size = len(stratum_data)
                n_sample = max(min_sample, int(stratum_size * sample_fraction))
                n_sample = min(n_sample, stratum_size)
                stratum_sample = random.sample(stratum_data, n_sample)
                sampled.extend(stratum_sample)
                strata_info[stratum_name] = {
                    "original_size": stratum_size,
                    "sample_size": n_sample,
                    "fraction": round(n_sample / stratum_size, 4) if stratum_size > 0 else 0,
                }

            return ActionResult(
                success=True,
                message=f"Stratified sample: {len(sampled)} items across {len(strata)} strata",
                data={"sample": sampled, "strata_info": strata_info, "total": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StratifiedSampling error: {e}")


class ClusterSamplingAction(BaseAction):
    """Cluster-based sampling."""
    action_type = "cluster_sampling"
    display_name = "整群抽样"
    description = "基于簇的整群抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            cluster_field = params.get("cluster_field", "cluster")
            num_clusters = params.get("num_clusters", 5)
            sample_within_clusters = params.get("sample_within_clusters", True)
            sample_per_cluster = params.get("sample_per_cluster", 2)
            seed = params.get("seed", None)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if seed is not None:
                random.seed(seed)

            clusters: Dict[str, List] = {}
            for item in data:
                if isinstance(item, dict):
                    key = str(item.get(cluster_field, "unknown"))
                else:
                    key = "default"
                if key not in clusters:
                    clusters[key] = []
                clusters[key].append(item)

            cluster_ids = list(clusters.keys())
            selected_clusters = random.sample(cluster_ids, min(num_clusters, len(cluster_ids)))

            sampled = []
            cluster_info = {}

            for cid in selected_clusters:
                cluster_items = clusters[cid]
                if sample_within_clusters:
                    n = min(sample_per_cluster, len(cluster_items))
                    cluster_sample = random.sample(cluster_items, n)
                    sampled.extend(cluster_sample)
                    cluster_info[cid] = {"total": len(cluster_items), "sampled": n}
                else:
                    sampled.extend(cluster_items)
                    cluster_info[cid] = {"total": len(cluster_items), "sampled": len(cluster_items)}

            return ActionResult(
                success=True,
                message=f"Cluster sample: {len(sampled)} items from {len(selected_clusters)} clusters",
                data={"sample": sampled, "clusters": cluster_info, "selected_clusters": selected_clusters},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ClusterSampling error: {e}")


class ReservoirSamplingAction(BaseAction):
    """Reservoir sampling for streaming/unbounded data."""
    action_type = "reservoir_sampling"
    display_name = "水库抽样"
    description = "对流式数据的水库抽样算法"

    def __init__(self):
        super().__init__()
        self._reservoir: List = []
        self._count = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            reservoir_size = params.get("reservoir_size", 100)
            reset = params.get("reset", False)

            if reset:
                self._reservoir = []
                self._count = 0

            if not isinstance(items, list):
                items = [items]

            for item in items:
                self._count += 1
                if len(self._reservoir) < reservoir_size:
                    self._reservoir.append(item)
                else:
                    j = random.randint(0, self._count - 1)
                    if j < reservoir_size:
                        self._reservoir[j] = item

            return ActionResult(
                success=True,
                message=f"Reservoir: {len(self._reservoir)}/{self._count} items",
                data={"reservoir": self._reservoir, "reservoir_size": len(self._reservoir), "total_seen": self._count},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ReservoirSampling error: {e}")
