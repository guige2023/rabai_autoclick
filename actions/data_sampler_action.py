"""Data Sampler Action Module.

Provides data sampling strategies for large datasets.
"""

import random
import math
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSamplerAction(BaseAction):
    """Sample data using various strategies.
    
    Supports random, stratified, systematic, and reservoir sampling.
    """
    action_type = "data_sampler"
    display_name = "数据采样"
    description = "多种采样策略支持"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sampling operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, strategy, sample_size, options.
        
        Returns:
            ActionResult with sampled data.
        """
        data = params.get('data', [])
        strategy = params.get('strategy', 'random')
        sample_size = params.get('sample_size', 100)
        options = params.get('options', {})
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to sample"
            )
        
        try:
            if strategy == 'random':
                sampled = self._random_sample(data, sample_size, options)
            elif strategy == 'stratified':
                sampled = self._stratified_sample(data, sample_size, options)
            elif strategy == 'systematic':
                sampled = self._systematic_sample(data, sample_size, options)
            elif strategy == 'reservoir':
                sampled = self._reservoir_sample(data, sample_size, options)
            elif strategy == 'cluster':
                sampled = self._cluster_sample(data, sample_size, options)
            elif strategy == 'weight':
                sampled = self._weighted_sample(data, sample_size, options)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown strategy: {strategy}"
                )
            
            return ActionResult(
                success=True,
                data={
                    'sampled_data': sampled,
                    'original_size': len(data),
                    'sample_size': len(sampled),
                    'strategy': strategy
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Sampling failed: {str(e)}"
            )
    
    def _random_sample(
        self,
        data: List,
        sample_size: int,
        options: Dict
    ) -> List:
        """Perform random sampling."""
        replace = options.get('replace', False)
        seed = options.get('seed', None)
        
        if seed is not None:
            random.seed(seed)
        
        if replace:
            return [random.choice(data) for _ in range(sample_size)]
        else:
            sample_size = min(sample_size, len(data))
            return random.sample(data, sample_size)
    
    def _stratified_sample(
        self,
        data: List,
        sample_size: int,
        options: Dict
    ) -> List:
        """Perform stratified sampling."""
        stratify_field = options.get('stratify_field', 'category')
        proportions = options.get('proportions', {})
        
        if not isinstance(data[0], dict):
            return self._random_sample(data, sample_size, options)
        
        # Group by stratify field
        groups = {}
        for item in data:
            key = item.get(stratify_field, 'unknown')
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        
        # Calculate sample sizes per group
        total = len(data)
        sampled = []
        
        for group_key, group_data in groups.items():
            group_proportion = proportions.get(group_key, len(group_data) / total)
            group_sample_size = max(1, int(sample_size * group_proportion))
            group_sample_size = min(group_sample_size, len(group_data))
            
            sampled.extend(self._random_sample(group_data, group_sample_size, options))
        
        return sampled
    
    def _systematic_sample(
        self,
        data: List,
        sample_size: int,
        options: Dict
    ) -> List:
        """Perform systematic sampling."""
        start_offset = options.get('start_offset', 0)
        
        if len(data) <= sample_size:
            return data
        
        interval = len(data) / sample_size
        sampled = []
        
        for i in range(sample_size):
            idx = int((start_offset + i * interval) % len(data))
            sampled.append(data[idx])
        
        return sampled
    
    def _reservoir_sample(
        self,
        data: List,
        sample_size: int,
        options: Dict
    ) -> List:
        """Perform reservoir sampling (for streaming data)."""
        seed = options.get('seed', None)
        
        if seed is not None:
            random.seed(seed)
        
        # Algorithm R for reservoir sampling
        reservoir = data[:sample_size] if len(data) >= sample_size else data
        
        for i in range(sample_size, len(data)):
            j = random.randint(0, i)
            if j < sample_size:
                reservoir[j] = data[i]
        
        return reservoir
    
    def _cluster_sample(
        self,
        data: List,
        sample_size: int,
        options: Dict
    ) -> List:
        """Perform cluster sampling."""
        cluster_field = options.get('cluster_field', 'cluster')
        
        if not isinstance(data[0], dict):
            return self._random_sample(data, sample_size, options)
        
        # Group into clusters
        clusters = {}
        for item in data:
            key = item.get(cluster_field, id(item))
            if key not in clusters:
                clusters[key] = []
            clusters[key].append(item)
        
        # Sample clusters
        cluster_keys = list(clusters.keys())
        num_clusters = min(sample_size, len(cluster_keys))
        sampled_clusters = random.sample(cluster_keys, num_clusters)
        
        sampled = []
        for key in sampled_clusters:
            sampled.extend(clusters[key])
        
        return sampled
    
    def _weighted_sample(
        self,
        data: List,
        sample_size: int,
        options: Dict
    ) -> List:
        """Perform weighted sampling."""
        weight_field = options.get('weight_field', 'weight')
        
        if not isinstance(data[0], dict):
            return self._random_sample(data, sample_size, options)
        
        weights = []
        for item in data:
            weight = item.get(weight_field, 1.0)
            weights.append(max(0, weight))
        
        total_weight = sum(weights)
        if total_weight <= 0:
            return self._random_sample(data, sample_size, options)
        
        # Normalize weights
        normalized_weights = [w / total_weight for w in weights]
        
        # Build cumulative distribution
        cum_weights = []
        cumsum = 0
        for w in normalized_weights:
            cumsum += w
            cum_weights.append(cumsum)
        
        # Sample
        sampled = []
        for _ in range(sample_size):
            r = random.random()
            for i, cw in enumerate(cum_weights):
                if r <= cw:
                    sampled.append(data[i])
                    break
        
        return sampled


class DataSampleSizeCalculatorAction(BaseAction):
    """Calculate required sample size for statistical significance.
    
    Computes appropriate sample size based on confidence level and margin of error.
    """
    action_type = "data_sample_size_calculator"
    display_name = "样本大小计算"
    description = "计算统计显著性所需样本量"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate sample size.
        
        Args:
            context: Execution context.
            params: Dict with keys: confidence_level, margin_of_error,
                   population_size, proportion.
        
        Returns:
            ActionResult with calculated sample size.
        """
        confidence_level = params.get('confidence_level', 0.95)
        margin_of_error = params.get('margin_of_error', 0.05)
        population_size = params.get('population_size', float('inf'))
        proportion = params.get('proportion', 0.5)
        
        if margin_of_error <= 0 or margin_of_error >= 1:
            return ActionResult(
                success=False,
                data=None,
                error="Margin of error must be between 0 and 1"
            )
        
        # Z-score for confidence level
        z_scores = {
            0.90: 1.645,
            0.95: 1.96,
            0.99: 2.576
        }
        z = z_scores.get(confidence_level, 1.96)
        
        # Calculate sample size for infinite population
        p = proportion
        q = 1 - p
        e = margin_of_error
        
        n_infinite = (z * z * p * q) / (e * e)
        
        # Adjust for finite population
        if math.isinf(population_size) or population_size <= 0:
            n_final = math.ceil(n_infinite)
        else:
            n_adj = n_infinite / (1 + ((n_infinite - 1) / population_size))
            n_final = math.ceil(n_adj)
        
        return ActionResult(
            success=True,
            data={
                'sample_size': n_final,
                'confidence_level': confidence_level,
                'margin_of_error': margin_of_error,
                'population_size': population_size,
                'proportion': proportion,
                'z_score': z
            },
            error=None
        )


class DataStratificationAction(BaseAction):
    """Stratify data into homogeneous groups.
    
    Partitions data based on specified stratification variables.
    """
    action_type = "data_stratification"
    display_name = "数据分层"
    description = "基于分层变量划分数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stratification.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, stratify_by, compute_stats.
        
        Returns:
            ActionResult with stratified data.
        """
        data = params.get('data', [])
        stratify_by = params.get('stratify_by', [])
        compute_stats = params.get('compute_stats', True)
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to stratify"
            )
        
        if not stratify_by:
            return ActionResult(
                success=False,
                data=None,
                error="No stratification fields specified"
            )
        
        # Build stratification key
        strata = {}
        
        for item in data:
            if isinstance(item, dict):
                key_parts = [str(item.get(field, '')) for field in stratify_by]
            else:
                key_parts = [str(item)]
            
            key = '|'.join(key_parts)
            
            if key not in strata:
                strata[key] = {
                    'key': key,
                    'items': [],
                    'count': 0
                }
            
            strata[key]['items'].append(item)
            strata[key]['count'] += 1
        
        # Compute statistics per stratum
        if compute_stats:
            for stratum in strata.values():
                items = stratum['items']
                if items and isinstance(items[0], dict):
                    stratum['stats'] = self._compute_stats(items)
        
        return ActionResult(
            success=True,
            data={
                'strata': strata,
                'num_strata': len(strata),
                'total_count': len(data)
            },
            error=None
        )
    
    def _compute_stats(self, items: List[Dict]) -> Dict:
        """Compute basic statistics for numeric fields."""
        stats = {}
        
        if not items:
            return stats
        
        # Get all numeric fields
        numeric_fields = set()
        for item in items:
            for key, value in item.items():
                if isinstance(value, (int, float)):
                    numeric_fields.add(key)
        
        for field in numeric_fields:
            values = [item.get(field, 0) for item in items if isinstance(item.get(field), (int, float))]
            if values:
                stats[field] = {
                    'min': min(values),
                    'max': max(values),
                    'mean': sum(values) / len(values),
                    'count': len(values)
                }
        
        return stats


def register_actions():
    """Register all Data Sampler actions."""
    return [
        DataSamplerAction,
        DataSampleSizeCalculatorAction,
        DataStratificationAction,
    ]
