"""Data Resample action module for RabAI AutoClick.

Provides data resampling operations:
- ResampleUpsampleAction: Upsample minority class
- ResampleDownsampleAction: Downsample majority class
- ResampleSMOTEAction: SMOTE-style resampling
- ResampleBootstrapAction: Bootstrap resampling
"""

from __future__ import annotations

import sys
import os
import random
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ResampleUpsampleAction(BaseAction):
    """Upsample minority class."""
    action_type = "resample_upsample"
    display_name = "上采样"
    description = "上采样少数类"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute upsampling."""
        data = params.get('data', [])
        class_field = params.get('class_field', 'label')
        target_ratio = params.get('target_ratio', 1.0)
        output_var = params.get('output_var', 'resampled_data')

        if not data or not class_field:
            return ActionResult(success=False, message="data and class_field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            class_counts = {}
            for record in resolved_data:
                cls = record.get(class_field, 'unknown')
                class_counts[cls] = class_counts.get(cls, 0) + 1

            max_count = max(class_counts.values())
            min_count = min(class_counts.values())

            if target_ratio > 0:
                target_min = int(max_count * target_ratio)
            else:
                target_min = max_count

            resampled = []
            class_samples = {cls: [] for cls in class_counts}

            for record in resolved_data:
                cls = record.get(class_field, 'unknown')
                class_samples[cls].append(record)

            for cls, samples in class_samples.items():
                resampled.extend(samples)
                if len(samples) < target_min:
                    needed = target_min - len(samples)
                    resampled.extend(random.choices(samples, k=needed))

            result = {
                'data': resampled,
                'original_count': len(resolved_data),
                'resampled_count': len(resampled),
                'class_distribution': class_counts,
                'method': 'upsample',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Upsampled: {len(resolved_data)} -> {len(resampled)} (ratio: {target_ratio})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Upsample error: {e}")


class ResampleDownsampleAction(BaseAction):
    """Downsample majority class."""
    action_type: "resample_downsample"
    display_name = "下采样"
    description = "下采样多数类"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute downsampling."""
        data = params.get('data', [])
        class_field = params.get('class_field', 'label')
        target_ratio = params.get('target_ratio', 1.0)
        output_var = params.get('output_var', 'resampled_data')

        if not data or not class_field:
            return ActionResult(success=False, message="data and class_field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data

            class_counts = {}
            for record in resolved_data:
                cls = record.get(class_field, 'unknown')
                class_counts[cls] = class_counts.get(cls, 0) + 1

            min_count = min(class_counts.values())

            if target_ratio > 0:
                target_max = int(min_count * target_ratio)
            else:
                target_max = min_count

            class_samples = {cls: [] for cls in class_counts}
            for record in resolved_data:
                cls = record.get(class_field, 'unknown')
                class_samples[cls].append(record)

            resampled = []
            for cls, samples in class_samples.items():
                resampled.extend(samples)
                if len(samples) > target_max:
                    resampled = resampled[:-len(samples)] + random.sample(samples, target_max)

            result = {
                'data': resampled,
                'original_count': len(resolved_data),
                'resampled_count': len(resampled),
                'class_distribution': {cls: len([r for r in resampled if r.get(class_field) == cls]) for cls in class_counts},
                'method': 'downsample',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Downsampled: {len(resolved_data)} -> {len(resampled)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Downsample error: {e}")


class ResampleBootstrapAction(BaseAction):
    """Bootstrap resampling."""
    action_type = "resample_bootstrap"
    display_name = "Bootstrap采样"
    description = "Bootstrap重采样"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute bootstrap resampling."""
        data = params.get('data', [])
        sample_size = params.get('sample_size', None)
        n_iterations = params.get('n_iterations', 1)
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'bootstrap_samples')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            if seed is not None:
                random.seed(seed)

            size = sample_size or len(resolved_data)
            samples = []

            for _ in range(n_iterations):
                sample = random.choices(resolved_data, k=size)
                samples.append(sample)

            result = {
                'samples': samples,
                'sample_count': n_iterations,
                'sample_size': size,
                'original_size': len(resolved_data),
                'method': 'bootstrap',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Bootstrap: {n_iterations} samples of size {size}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Bootstrap error: {e}")
