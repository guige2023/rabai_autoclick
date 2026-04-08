"""Data Normalization v2 Action.

Advanced data normalization with scaling, encoding, and transforms.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass
import math


T = TypeVar("T")


@dataclass
class NormalizationConfig:
    method: str
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None


class DataNormalizationV2Action:
    """Advanced data normalization."""

    def __init__(self) -> None:
        self.configs: Dict[str, NormalizationConfig] = {}

    def fit(
        self,
        items: List[float],
        method: str = "minmax",
        field_name: str = "default",
    ) -> NormalizationConfig:
        if not items:
            raise ValueError("Cannot fit on empty data")
        if method == "minmax":
            min_val, max_val = min(items), max(items)
            config = NormalizationConfig(method=method, min_val=min_val, max_val=max_val)
        elif method == "zscore":
            mean = sum(items) / len(items)
            variance = sum((x - mean) ** 2 for x in items) / len(items)
            std = math.sqrt(variance)
            config = NormalizationConfig(method=method, mean=mean, std=std)
        else:
            raise ValueError(f"Unknown method: {method}")
        self.configs[field_name] = config
        return config

    def transform(
        self,
        items: List[float],
        field_name: str = "default",
    ) -> List[float]:
        config = self.configs.get(field_name)
        if not config:
            raise ValueError(f"No config found for field: {field_name}")
        if config.method == "minmax":
            if config.min_val == config.max_val:
                return [0.5] * len(items)
            return [(x - config.min_val) / (config.max_val - config.min_val) for x in items]
        elif config.method == "zscore":
            if config.std == 0:
                return [0.0] * len(items)
            return [(x - config.mean) / config.std for x in items]
        return list(items)

    def fit_transform(
        self,
        items: List[float],
        method: str = "minmax",
        field_name: str = "default",
    ) -> List[float]:
        self.fit(items, method, field_name)
        return self.transform(items, field_name)

    def encode_categorical(
        self,
        items: List[str],
        encoding: str = "ordinal",
    ) -> List[int]:
        if encoding == "ordinal":
            unique = list(dict.fromkeys(items))
            return [unique.index(x) for x in items]
        elif encoding == "onehot":
            unique = list(dict.fromkeys(items))
            return [unique.index(x) for x in items]
        return [0] * len(items)
