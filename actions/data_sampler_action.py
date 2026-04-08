"""Data sampler action module for RabAI AutoClick.

Provides data sampling operations:
- SampleRandomAction: Random sampling
- SampleStratifiedAction: Stratified sampling
- SampleSystematicAction: Systematic sampling
- SampleClusterAction: Cluster sampling
- SampleReserveAction: Reserve sample for testing
"""

import random
from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SampleRandomAction(BaseAction):
    """Random sampling."""
    action_type = "sample_random"
    display_name = "随机采样"
    description = "随机抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sample_size = params.get("sample_size", 10)
            replace = params.get("replace", False)
            seed = params.get("seed", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if seed is not None:
                random.seed(seed)

            if replace:
                sample = [random.choice(data) for _ in range(sample_size)]
            else:
                sample_size = min(sample_size, len(data))
                sample = random.sample(data, sample_size)

            return ActionResult(
                success=True,
                data={"sample": sample, "sample_size": len(sample), "original_size": len(data)},
                message=f"Random sample: {len(sample)}/{len(data)}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sample random failed: {e}")


class SampleStratifiedAction(BaseAction):
    """Stratified sampling."""
    action_type = "sample_stratified"
    display_name = "分层采样"
    description = "分层抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            strata_field = params.get("strata_field", "category")
            samples_per_stratum = params.get("samples_per_stratum", 5)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not strata_field:
                return ActionResult(success=False, message="strata_field is required")

            strata: Dict = {}
            for item in data:
                stratum = str(item.get(strata_field, "unknown"))
                if stratum not in strata:
                    strata[stratum] = []
                strata[stratum].append(item)

            sample = []
            for stratum_name, stratum_data in strata.items():
                k = min(samples_per_stratum, len(stratum_data))
                sample.extend(random.sample(stratum_data, k))

            return ActionResult(
                success=True,
                data={"sample": sample, "strata_count": len(strata), "total_sampled": len(sample)},
                message=f"Stratified sample: {len(sample)} across {len(strata)} strata",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sample stratified failed: {e}")


class SampleSystematicAction(BaseAction):
    """Systematic sampling."""
    action_type = "sample_systematic"
    display_name = "系统采样"
    description = "系统抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            interval = params.get("interval", 5)
            start_offset = params.get("start_offset", 0)

            if not data:
                return ActionResult(success=False, message="data is required")

            sample = data[start_offset :: interval]

            return ActionResult(
                success=True,
                data={"sample": sample, "sample_size": len(sample), "interval": interval, "original_size": len(data)},
                message=f"Systematic sample: every {interval}th item, {len(sample)} selected",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sample systematic failed: {e}")


class SampleClusterAction(BaseAction):
    """Cluster sampling."""
    action_type = "sample_cluster"
    display_name = "整群采样"
    description = "整群抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            cluster_field = params.get("cluster_field", "cluster")
            clusters_to_select = params.get("clusters_to_select", 2)

            if not data:
                return ActionResult(success=False, message="data is required")

            clusters: Dict = {}
            for item in data:
                cluster_id = str(item.get(cluster_field, "unknown"))
                if cluster_id not in clusters:
                    clusters[cluster_id] = []
                clusters[cluster_id].append(item)

            all_cluster_ids = list(clusters.keys())
            selected = random.sample(all_cluster_ids, min(clusters_to_select, len(all_cluster_ids)))
            sample = []
            for c in selected:
                sample.extend(clusters[c])

            return ActionResult(
                success=True,
                data={"sample": sample, "clusters_selected": len(selected), "total_sampled": len(sample)},
                message=f"Cluster sample: {len(selected)} clusters, {len(sample)} total items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sample cluster failed: {e}")


class SampleReserveAction(BaseAction):
    """Reserve sample for testing."""
    action_type = "sample_reserve"
    display_name = "预留样本"
    description = "预留测试样本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            reserve_size = params.get("reserve_size", 10)
            seed = params.get("seed", 42)

            if not data:
                return ActionResult(success=False, message="data is required")

            random.seed(seed)
            reserve_size = min(reserve_size, len(data))
            indices = random.sample(range(len(data)), reserve_size)
            reserve = [data[i] for i in indices]
            train = [data[i] for i in range(len(data)) if i not in indices]

            return ActionResult(
                success=True,
                data={"reserve": reserve, "train": train, "reserve_size": len(reserve), "train_size": len(train)},
                message=f"Reserved {len(reserve)} for testing, {len(train)} for training",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sample reserve failed: {e}")
