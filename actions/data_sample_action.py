"""Data sample action module for RabAI AutoClick.

Provides data sampling:
- DataSampleAction: Sample data
- RandomSamplerAction: Random sampling
- StratifiedSamplerAction: Stratified sampling
"""

import random
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSampleAction(BaseAction):
    """Sample data."""
    action_type = "data_sample"
    display_name = "数据采样"
    description = "从数据中采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sample_size = params.get("sample_size", 10)
            sampling_method = params.get("method", "random")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if len(data) <= sample_size:
                sampled = data
            elif sampling_method == "random":
                sampled = random.sample(data, sample_size)
            elif sampling_method == "first":
                sampled = data[:sample_size]
            elif sampling_method == "last":
                sampled = data[-sample_size:]
            else:
                sampled = data[:sample_size]

            return ActionResult(
                success=True,
                data={
                    "original_size": len(data),
                    "sample_size": len(sampled),
                    "sampled": sampled,
                    "method": sampling_method
                },
                message=f"Sampled {len(sampled)} items using {sampling_method} method"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data sample error: {str(e)}")


class RandomSamplerAction(BaseAction):
    """Random sampling."""
    action_type = "random_sampler"
    display_name = "随机采样"
    description = "随机抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sample_size = params.get("sample_size", 10)
            seed = params.get("seed", None)

            if seed is not None:
                random.seed(seed)

            sampled = random.sample(data, min(sample_size, len(data)))

            return ActionResult(
                success=True,
                data={
                    "sample_size": len(sampled),
                    "sampled": sampled,
                    "seed": seed
                },
                message=f"Random sample: {len(sampled)} items (seed={seed})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Random sampler error: {str(e)}")


class StratifiedSamplerAction(BaseAction):
    """Stratified sampling."""
    action_type = "stratified_sampler"
    display_name = "分层采样"
    description = "分层抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            strata_field = params.get("strata_field", "category")
            samples_per_stratum = params.get("samples_per_stratum", 5)

            strata = {}
            for item in data:
                if isinstance(item, dict) and strata_field in item:
                    key = item[strata_field]
                else:
                    key = "default"
                if key not in strata:
                    strata[key] = []
                strata[key].append(item)

            sampled = []
            for stratum_key, stratum_items in strata.items():
                stratum_sample = random.sample(stratum_items, min(samples_per_stratum, len(stratum_items)))
                sampled.extend(stratum_sample)

            return ActionResult(
                success=True,
                data={
                    "strata_count": len(strata),
                    "total_samples": len(sampled),
                    "sampled": sampled
                },
                message=f"Stratified sample: {len(sampled)} items from {len(strata)} strata"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stratified sampler error: {str(e)}")
