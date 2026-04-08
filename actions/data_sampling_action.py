"""
Data Sampling Action - Statistical sampling of datasets.

This module provides data sampling capabilities including random,
stratified, systematic, and reservoir sampling methods.
"""

from __future__ import annotations

import random
import math
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class SamplingMethod(Enum):
    """Sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"
    WEIGHTED = "weighted"


@dataclass
class SamplingConfig:
    """Configuration for sampling."""
    method: SamplingMethod = SamplingMethod.RANDOM
    sample_size: int | None = None
    sample_rate: float | None = None
    seed: int | None = None
    replace: bool = False


@dataclass
class SamplingResult:
    """Result of sampling operation."""
    original_size: int
    sample_size: int
    sample: list[dict[str, Any]]
    method: SamplingMethod
    metadata: dict[str, Any] = field(default_factory=dict)


class RandomSampler:
    """Random sampling implementation."""
    
    def __init__(self, seed: int | None = None) -> None:
        if seed is not None:
            random.seed(seed)
    
    def sample(
        self,
        data: list[dict[str, Any]],
        size: int | None = None,
        rate: float | None = None,
        replace: bool = False,
    ) -> list[dict[str, Any]]:
        """Perform random sampling."""
        if size is None and rate is not None:
            size = int(len(data) * rate)
        elif size is None:
            size = len(data)
        
        if replace:
            return [random.choice(data) for _ in range(size)]
        else:
            size = min(size, len(data))
            return random.sample(data, size)


class StratifiedSampler:
    """Stratified sampling implementation."""
    
    def __init__(self, seed: int | None = None) -> None:
        if seed is not None:
            random.seed(seed)
    
    def sample(
        self,
        data: list[dict[str, Any]],
        stratum_field: str,
        sample_size: int | None = None,
        sample_rate: float | None = None,
        min_per_stratum: int = 1,
    ) -> list[dict[str, Any]]:
        """Perform stratified sampling."""
        strata: dict[Any, list[dict[str, Any]]] = {}
        
        for record in data:
            stratum_value = record.get(stratum_field)
            if stratum_value not in strata:
                strata[stratum_value] = []
            strata[stratum_value].append(record)
        
        total_size = sum(len(s) for s in strata.values())
        
        if sample_rate is not None:
            samples = []
            for stratum_records in strata.values():
                stratum_size = max(min_per_stratum, int(len(stratum_records) * sample_rate))
                stratum_size = min(stratum_size, len(stratum_records))
                samples.extend(random.sample(stratum_records, stratum_size))
            return samples
        
        if sample_size is not None:
            samples = []
            for stratum_records in strata.values():
                proportion = len(stratum_records) / total_size
                stratum_size = max(min_per_stratum, int(sample_size * proportion))
                stratum_size = min(stratum_size, len(stratum_records))
                samples.extend(random.sample(stratum_records, stratum_size))
            return samples
        
        return data


class SystematicSampler:
    """Systematic sampling implementation."""
    
    def __init__(self, seed: int | None = None) -> None:
        if seed is not None:
            random.seed(seed)
    
    def sample(
        self,
        data: list[dict[str, Any]],
        interval: int | None = None,
        rate: float | None = None,
        start_offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """Perform systematic sampling."""
        if interval is None:
            if rate is not None:
                interval = max(1, int(1 / rate))
            else:
                interval = 1
        
        if start_offset is None:
            start_offset = random.randint(0, interval - 1)
        
        return data[start_offset::interval]


class ReservoirSampler:
    """Reservoir sampling for large datasets."""
    
    def __init__(self, seed: int | None = None) -> None:
        self._seed = seed
        if seed is not None:
            random.seed(seed)
    
    def sample(
        self,
        data: list[dict[str, Any]],
        k: int,
    ) -> list[dict[str, Any]]:
        """Perform reservoir sampling (Algorithm R)."""
        k = min(k, len(data))
        reservoir = data[:k]
        
        for i in range(k, len(data)):
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = data[i]
        
        return reservoir


class DataSamplingAction:
    """
    Data sampling action for automation workflows.
    
    Example:
        action = DataSamplingAction()
        result = await action.sample(
            records,
            method=SamplingMethod.RANDOM,
            sample_size=100
        )
    """
    
    def __init__(self, seed: int | None = None) -> None:
        self._random_sampler = RandomSampler(seed)
        self._stratified_sampler = StratifiedSampler(seed)
        self._systematic_sampler = SystematicSampler(seed)
        self._reservoir_sampler = ReservoirSampler(seed)
    
    async def sample(
        self,
        data: list[dict[str, Any]],
        method: SamplingMethod = SamplingMethod.RANDOM,
        size: int | None = None,
        rate: float | None = None,
        replace: bool = False,
        **kwargs,
    ) -> SamplingResult:
        """Sample data using specified method."""
        if method == SamplingMethod.RANDOM:
            sample_data = self._random_sampler.sample(data, size, rate, replace)
        
        elif method == SamplingMethod.STRATIFIED:
            stratum_field = kwargs.get("stratum_field", "category")
            sample_data = self._stratified_sampler.sample(
                data, stratum_field, size, rate
            )
        
        elif method == SamplingMethod.SYSTEMATIC:
            interval = kwargs.get("interval")
            sample_data = self._systematic_sampler.sample(data, interval, rate)
        
        elif method == SamplingMethod.RESERVOIR:
            k = size or kwargs.get("k", 100)
            sample_data = self._reservoir_sampler.sample(data, k)
        
        elif method == SamplingMethod.WEIGHTED:
            weight_field = kwargs.get("weight_field", "weight")
            sample_data = self._weighted_sample(data, weight_field, size)
        
        else:
            sample_data = data
        
        return SamplingResult(
            original_size=len(data),
            sample_size=len(sample_data),
            sample=sample_data,
            method=method,
            metadata={"size_specified": size, "rate_specified": rate},
        )
    
    def _weighted_sample(
        self,
        data: list[dict[str, Any]],
        weight_field: str,
        size: int | None = None,
    ) -> list[dict[str, Any]]:
        """Perform weighted sampling."""
        weights = [record.get(weight_field, 1.0) for record in data]
        total_weight = sum(weights)
        
        if size is None:
            size = len(data)
        
        normalized = [w / total_weight for w in weights]
        
        indices = list(range(len(data)))
        return [data[i] for i in random.choices(indices, weights=normalized, k=size)]


# Export public API
__all__ = [
    "SamplingMethod",
    "SamplingConfig",
    "SamplingResult",
    "RandomSampler",
    "StratifiedSampler",
    "SystematicSampler",
    "ReservoirSampler",
    "DataSamplingAction",
]
