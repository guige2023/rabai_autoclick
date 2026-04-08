"""
Data Resample Action - Resamples data for up/downsampling.

This module provides resampling capabilities for
handling imbalanced datasets.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any
from collections import defaultdict


@dataclass
class ResampleConfig:
    """Configuration for resampling."""
    strategy: str = "undersample"
    target_ratio: float = 1.0
    random_seed: int | None = None


class DataResampler:
    """Resamples data."""
    
    def __init__(self, config: ResampleConfig | None = None) -> None:
        self.config = config or ResampleConfig()
        if self.config.random_seed:
            random.seed(self.config.random_seed)
    
    def resample(
        self,
        data: list[dict[str, Any]],
        label_field: str,
    ) -> list[dict[str, Any]]:
        """Resample data to balance classes."""
        by_label = defaultdict(list)
        for record in data:
            label = record.get(label_field)
            by_label[label].append(record)
        
        if not by_label:
            return data
        
        max_count = max(len(records) for records in by_label.values())
        
        if self.config.strategy == "undersample":
            target_count = int(max_count / self.config.target_ratio)
            sampled = []
            for label, records in by_label.items():
                if len(records) > target_count:
                    sampled.extend(random.sample(records, target_count))
                else:
                    sampled.extend(records)
            return sampled
        
        elif self.config.strategy == "oversample":
            min_count = min(len(records) for records in by_label.values())
            target_count = int(min_count * self.config.target_ratio)
            sampled = []
            for label, records in by_label.items():
                sampled.extend(records)
                while len([r for r in sampled if r.get(label_field) == label]) < target_count:
                    sampled.append(random.choice(records))
            return sampled
        
        return data


class DataResampleAction:
    """Data resample action for automation workflows."""
    
    def __init__(self, strategy: str = "undersample") -> None:
        self.config = ResampleConfig(strategy=strategy)
        self.resampler = DataResampler(self.config)
    
    async def resample(self, data: list[dict[str, Any]], label_field: str) -> list[dict[str, Any]]:
        """Resample data."""
        return self.resampler.resample(data, label_field)


__all__ = ["ResampleConfig", "DataResampler", "DataResampleAction"]
