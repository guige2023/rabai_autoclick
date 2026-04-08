"""Data sampler action module for RabAI AutoClick.

Provides data sampling operations:
- DataSamplerAction: Sample data from datasets
- StratifiedSamplerAction: Stratified sampling
- ClusterSamplerAction: Cluster-based sampling
- SequentialSamplerAction: Sequential sampling
"""

import random
import math
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSamplerAction(BaseAction):
    """Sample data from datasets."""
    action_type = "data_sampler"
    display_name = "数据采样器"
    description = "从数据集中采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sample_size = params.get("sample_size", 10)
            sampling_method = params.get("sampling_method", "random")
            replace = params.get("replace", False)
            seed = params.get("seed")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to sample")

            if seed is not None:
                random.seed(seed)

            if sample_size > len(data) and not replace:
                sample_size = len(data)

            if sampling_method == "random":
                sampled = random.sample(data, sample_size) if not replace else [random.choice(data) for _ in range(sample_size)]

            elif sampling_method == "first":
                sampled = data[:sample_size]

            elif sampling_method == "last":
                sampled = data[-sample_size:]

            elif sampling_method == "systematic":
                interval = max(1, len(data) // sample_size)
                sampled = [data[i] for i in range(0, len(data), interval)][:sample_size]

            elif sampling_method == "reservoir":
                sampled = self._reservoir_sampling(data, sample_size)

            else:
                sampled = random.sample(data, min(sample_size, len(data)))

            return ActionResult(
                success=True,
                data={
                    "sampled": sampled,
                    "sample_size": len(sampled),
                    "original_size": len(data),
                    "sampling_method": sampling_method,
                    "replace": replace
                },
                message=f"Sampled {len(sampled)} items from {len(data)} using {sampling_method}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data sampler error: {str(e)}")

    def _reservoir_sampling(self, data: List, k: int) -> List:
        reservoir = data[:k]
        for i in range(k, len(data)):
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = data[i]
        return reservoir

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"sample_size": 10, "sampling_method": "random", "replace": False, "seed": None}


class StratifiedSamplerAction(BaseAction):
    """Stratified sampling."""
    action_type = "data_stratified_sampler"
    display_name = "分层采样器"
    description = "分层采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            stratum_field = params.get("stratum_field", "")
            sample_per_stratum = params.get("sample_per_stratum", 5)
            sampling_method = params.get("sampling_method", "proportional")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to sample")

            if not stratum_field:
                return ActionResult(success=False, message="stratum_field is required")

            strata = defaultdict(list)
            for item in data:
                if isinstance(item, dict):
                    stratum_value = item.get(stratum_field, "unknown")
                else:
                    stratum_value = "unknown"
                strata[stratum_value].append(item)

            sampled = []
            stratum_info = {}

            total_records = len(data)

            for stratum_value, stratum_data in strata.items():
                if sampling_method == "proportional":
                    proportion = len(stratum_data) / total_records
                    n = max(1, int(proportion * sample_per_stratum * len(strata)))
                    n = min(n, len(stratum_data))
                else:
                    n = min(sample_per_stratum, len(stratum_data))

                stratum_sample = random.sample(stratum_data, n)
                sampled.extend(stratum_sample)
                stratum_info[stratum_value] = {
                    "original_size": len(stratum_data),
                    "sampled_size": len(stratum_sample)
                }

            return ActionResult(
                success=True,
                data={
                    "sampled": sampled,
                    "sample_size": len(sampled),
                    "original_size": len(data),
                    "stratum_info": stratum_info,
                    "strata_count": len(strata),
                    "sampling_method": sampling_method
                },
                message=f"Stratified sample: {len(sampled)} from {len(data)} across {len(strata)} strata"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stratified sampler error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"stratum_field": "", "sample_per_stratum": 5, "sampling_method": "proportional"}


class ClusterSamplerAction(BaseAction):
    """Cluster-based sampling."""
    action_type = "data_cluster_sampler"
    display_name = "聚类采样器"
    description = "基于聚类的采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            num_clusters = params.get("num_clusters", 5)
            samples_per_cluster = params.get("samples_per_cluster", 2)
            cluster_by = params.get("cluster_by")
            method = params.get("method", "kmeans_simulation")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to sample")

            if len(data) < num_clusters:
                num_clusters = len(data)

            if method == "kmeans_simulation":
                clusters = self._simulate_kmeans_clusters(data, num_clusters, cluster_by)
            elif method == "sequential":
                clusters = self._sequential_clusters(data, num_clusters)
            else:
                clusters = self._simulate_kmeans_clusters(data, num_clusters, cluster_by)

            sampled = []
            cluster_info = {}

            for cluster_id, cluster_data in clusters.items():
                n = min(samples_per_cluster, len(cluster_data))
                cluster_sample = random.sample(cluster_data, n)
                sampled.extend(cluster_sample)
                cluster_info[cluster_id] = {
                    "original_size": len(cluster_data),
                    "sampled_size": n
                }

            return ActionResult(
                success=True,
                data={
                    "sampled": sampled,
                    "sample_size": len(sampled),
                    "original_size": len(data),
                    "num_clusters": num_clusters,
                    "cluster_info": cluster_info
                },
                message=f"Cluster sample: {len(sampled)} from {len(data)} in {num_clusters} clusters"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cluster sampler error: {str(e)}")

    def _simulate_kmeans_clusters(self, data: List, num_clusters: int, cluster_by: str) -> Dict:
        clusters = defaultdict(list)
        for i, item in enumerate(data):
            if cluster_by and isinstance(item, dict):
                cluster_id = hash(item.get(cluster_by, i)) % num_clusters
            else:
                cluster_id = i % num_clusters
            clusters[cluster_id].append(item)
        return clusters

    def _sequential_clusters(self, data: List, num_clusters: int) -> Dict:
        cluster_size = math.ceil(len(data) / num_clusters)
        clusters = defaultdict(list)
        for i, item in enumerate(data):
            cluster_id = i // cluster_size
            if cluster_id >= num_clusters:
                cluster_id = num_clusters - 1
            clusters[cluster_id].append(item)
        return clusters

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"num_clusters": 5, "samples_per_cluster": 2, "cluster_by": None, "method": "kmeans_simulation"}


class SequentialSamplerAction(BaseAction):
    """Sequential sampling."""
    action_type = "data_sequential_sampler"
    display_name = "顺序采样器"
    description = "顺序采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            start_index = params.get("start_index", 0)
            end_index = params.get("end_index")
            step = params.get("step", 1)
            every_nth = params.get("every_nth")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to sample")

            if end_index is None:
                end_index = len(data)

            if every_nth:
                sampled = data[start_index:min(end_index, len(data)):every_nth]
            else:
                sampled = data[start_index:min(end_index, len(data)):step]

            return ActionResult(
                success=True,
                data={
                    "sampled": sampled,
                    "sample_size": len(sampled),
                    "original_size": len(data),
                    "start_index": start_index,
                    "end_index": min(end_index, len(data)),
                    "step": step,
                    "every_nth": every_nth
                },
                message=f"Sequential sample: indices {start_index}-{min(end_index, len(data))}, step={step}, got {len(sampled)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sequential sampler error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"start_index": 0, "end_index": None, "step": 1, "every_nth": None}
