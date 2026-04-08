"""Data Sample action module for RabAI AutoClick.

Provides data sampling operations:
- SampleRandomAction: Random sampling
- SampleStratifiedAction: Stratified sampling
- SampleSystematicAction: Systematic sampling
- SampleClusterAction: Cluster sampling
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


class SampleRandomAction(BaseAction):
    """Random sampling."""
    action_type = "sample_random"
    display_name = "随机抽样"
    description = "随机抽样"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random sampling."""
        data = params.get('data', [])
        sample_size = params.get('sample_size', 10)
        replace = params.get('replace', False)
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'sample_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_size = context.resolve_value(sample_size) if context else sample_size

            if seed is not None:
                random.seed(seed)

            if replace:
                sample = [random.choice(resolved_data) for _ in range(resolved_size)]
            else:
                sample = random.sample(resolved_data, min(resolved_size, len(resolved_data)))

            result = {
                'sample': sample,
                'sample_size': len(sample),
                'original_size': len(resolved_data),
                'method': 'random',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Random sample: {len(sample)}/{len(resolved_data)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Random sample error: {e}")


class SampleStratifiedAction(BaseAction):
    """Stratified sampling."""
    action_type = "sample_stratified"
    display_name = "分层抽样"
    description = "分层抽样"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute stratified sampling."""
        data = params.get('data', [])
        strata_field = params.get('strata_field', '')
        samples_per_stratum = params.get('samples_per_stratum', 1)
        output_var = params.get('output_var', 'sample_result')

        if not data or not strata_field:
            return ActionResult(success=False, message="data and strata_field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_samples = context.resolve_value(samples_per_stratum) if context else samples_per_stratum

            strata = {}
            for record in resolved_data:
                key = record.get(strata_field, 'unknown')
                if key not in strata:
                    strata[key] = []
                strata[key].append(record)

            sample = []
            for stratum_key, stratum_data in strata.items():
                stratum_sample = random.sample(stratum_data, min(resolved_samples, len(stratum_data)))
                sample.extend(stratum_sample)

            result = {
                'sample': sample,
                'sample_size': len(sample),
                'original_size': len(resolved_data),
                'strata_count': len(strata),
                'method': 'stratified',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Stratified sample: {len(sample)} from {len(strata)} strata"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stratified sample error: {e}")


class SampleSystematicAction(BaseAction):
    """Systematic sampling."""
    action_type = "sample_systematic"
    display_name = "系统抽样"
    description = "系统抽样"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute systematic sampling."""
        data = params.get('data', [])
        interval = params.get('interval', 10)
        start = params.get('start', 0)
        output_var = params.get('output_var', 'sample_result')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_interval = context.resolve_value(interval) if context else interval
            resolved_start = context.resolve_value(start) if context else start

            sample = resolved_data[resolved_start::resolved_interval]

            result = {
                'sample': sample,
                'sample_size': len(sample),
                'original_size': len(resolved_data),
                'interval': resolved_interval,
                'start': resolved_start,
                'method': 'systematic',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Systematic sample: {len(sample)} from {len(resolved_data)} (interval={resolved_interval})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Systematic sample error: {e}")


class SampleClusterAction(BaseAction):
    """Cluster sampling."""
    action_type = "sample_cluster"
    display_name = "整群抽样"
    description = "整群抽样"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cluster sampling."""
        data = params.get('data', [])
        cluster_field = params.get('cluster_field', '')
        num_clusters = params.get('num_clusters', 2)
        output_var = params.get('output_var', 'sample_result')

        if not data or not cluster_field:
            return ActionResult(success=False, message="data and cluster_field are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_clusters = context.resolve_value(num_clusters) if context else num_clusters

            clusters = {}
            for record in resolved_data:
                key = record.get(cluster_field, 'unknown')
                if key not in clusters:
                    clusters[key] = []
                clusters[key].append(record)

            cluster_keys = list(clusters.keys())
            selected_clusters = random.sample(cluster_keys, min(resolved_clusters, len(cluster_keys)))

            sample = []
            for ck in selected_clusters:
                sample.extend(clusters[ck])

            result = {
                'sample': sample,
                'sample_size': len(sample),
                'original_size': len(resolved_data),
                'clusters_selected': len(selected_clusters),
                'total_clusters': len(clusters),
                'method': 'cluster',
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Cluster sample: {len(sample)} from {len(selected_clusters)}/{len(clusters)} clusters"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cluster sample error: {e}")
