"""Data bloom filter action module for RabAI AutoClick.

Provides Bloom filter for probabilistic data filtering:
- BloomFilter: Memory-efficient membership testing
- ScalableBloomFilter: Automatically scales Bloom filter
- BloomFilterManager: Manage multiple Bloom filters
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import math
import mmh3
from dataclasses import dataclass, field

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class BloomFilterConfig:
    """Configuration for Bloom filter."""
    size: int = 10000
    false_positive_rate: float = 0.01
    num_hashes: int = 7
    bit_array_size: int = 95858


class BloomFilter:
    """Bloom filter implementation."""
    
    def __init__(self, config: Optional[BloomFilterConfig] = None):
        self.config = config or BloomFilterConfig()
        self._bits: List[bool] = [False] * self.config.bit_array_size
        self._lock = threading.RLock()
        self._count = 0
        self._stats = {"adds": 0, "lookups": 0, "positives": 0, "probable_negatives": 0}
    
    def _get_bit_positions(self, item: str) -> List[int]:
        """Get bit positions for item."""
        positions = []
        for i in range(self.config.num_hashes):
            pos = mmh3.hash(item, i) % self.config.bit_array_size
            positions.append(pos)
        return positions
    
    def add(self, item: str) -> bool:
        """Add item to Bloom filter."""
        with self._lock:
            positions = self._get_bit_positions(str(item))
            for pos in positions:
                self._bits[pos] = True
            self._count += 1
            self._stats["adds"] += 1
            return True
    
    def contains(self, item: str) -> bool:
        """Check if item might be in set."""
        with self._lock:
            self._stats["lookups"] += 1
            positions = self._get_bit_positions(str(item))
            for pos in positions:
                if not self._bits[pos]:
                    self._stats["probable_negatives"] += 1
                    return False
            self._stats["positives"] += 1
            return True
    
    def clear(self):
        """Clear all items."""
        with self._lock:
            self._bits = [False] * self.config.bit_array_size
            self._count = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Bloom filter statistics."""
        with self._lock:
            fill_rate = sum(self._bits) / len(self._bits) if self._bits else 0
            return {
                "count": self._count,
                "fill_rate": fill_rate,
                "size": self.config.bit_array_size,
                **{k: v for k, v in self._stats.items()},
            }


class DataBloomFilterAction(BaseAction):
    """Data bloom filter action."""
    action_type = "data_bloom_filter"
    display_name = "数据布隆过滤器"
    description = "概率数据去重与成员检测"
    
    def __init__(self):
        super().__init__()
        self._filters: Dict[str, BloomFilter] = {}
        self._lock = threading.Lock()
    
    def _get_filter(self, name: str, config: Optional[BloomFilterConfig] = None) -> BloomFilter:
        """Get or create Bloom filter."""
        with self._lock:
            if name not in self._filters:
                self._filters[name] = BloomFilter(config)
            return self._filters[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Bloom filter operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "add")
            
            config = BloomFilterConfig(
                size=params.get("size", 10000),
                false_positive_rate=params.get("false_positive_rate", 0.01),
            )
            
            bfilter = self._get_filter(name, config)
            
            if command == "add":
                item = params.get("item")
                if item:
                    bfilter.add(str(item))
                return ActionResult(success=True)
            
            elif command == "contains":
                item = params.get("item")
                if item:
                    result = bfilter.contains(str(item))
                    return ActionResult(success=True, data={"contains": result})
                return ActionResult(success=False, message="item required")
            
            elif command == "stats":
                stats = bfilter.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            elif command == "clear":
                bfilter.clear()
                return ActionResult(success=True)
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataBloomFilterAction error: {str(e)}")
