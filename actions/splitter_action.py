"""Splitter action module for RabAI AutoClick.

Provides data splitting and partitioning operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SplitterAction(BaseAction):
    """Data splitting and partitioning operations.
    
    Supports train/test split, stratified split, chunking,
    bucket partitioning, and conditional splitting.
    """
    action_type = "splitter"
    display_name = "数据分割"
    description = "数据分割：训练测试集、分层、桶、分块"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute split operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'train_test', 'stratified', 'chunk', 'bucket', 'conditional'
                - data: List of items to split
                - test_size: Fraction for test set (default 0.2)
                - train_size: Fraction for train set (alternative to test_size)
                - stratify_by: Field name for stratified split
                - num_chunks: Number of chunks (for chunk command)
                - num_buckets: Number of buckets (for bucket command)
                - bucket_by: Field to hash for bucketing
                - conditions: Dict of field -> values for conditional split
        
        Returns:
            ActionResult with split data.
        """
        command = params.get('command', 'train_test')
        data = params.get('data', [])
        test_size = params.get('test_size', 0.2)
        train_size = params.get('train_size')
        stratify_by = params.get('stratify_by')
        num_chunks = params.get('num_chunks', 3)
        num_buckets = params.get('num_buckets', 5)
        bucket_by = params.get('bucket_by')
        conditions = params.get('conditions')
        seed = params.get('seed')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if seed is not None:
            import random
            random.seed(seed)
            random.shuffle(data)
        
        if command == 'train_test':
            return self._train_test_split(data, test_size, train_size)
        if command == 'stratified':
            return self._stratified_split(data, test_size, stratify_by)
        if command == 'chunk':
            return self._chunk_split(data, num_chunks)
        if command == 'bucket':
            return self._bucket_split(data, num_buckets, bucket_by)
        if command == 'conditional':
            return self._conditional_split(data, conditions)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _train_test_split(self, data: List[Any], test_size: float, train_size: Optional[float]) -> ActionResult:
        """Split into train and test sets."""
        if train_size is not None:
            test_size = 1 - train_size
        n_test = max(1, int(len(data) * test_size))
        n_train = len(data) - n_test
        train = data[:n_train]
        test = data[n_train:]
        return ActionResult(
            success=True,
            message=f"Train/test split: {len(train)}/{len(test)}",
            data={'train': train, 'test': test, 'train_size': len(train), 'test_size': len(test)}
        )
    
    def _stratified_split(self, data: List[Dict], test_size: float, stratify_by: str) -> ActionResult:
        """Stratified split preserving class distribution."""
        if stratify_by is None:
            return self._train_test_split(data, test_size, None)
        
        groups: Dict[Any, List[Dict]] = {}
        for row in data:
            key = str(row.get(stratify_by, 'unknown'))
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        
        train = []
        test = []
        for key, group in groups.items():
            n_test = max(1, int(len(group) * test_size))
            n_train = len(group) - n_test
            train.extend(group[:n_train])
            test.extend(group[n_train:])
        
        return ActionResult(
            success=True,
            message=f"Stratified split: {len(train)}/{len(test)} across {len(groups)} strata",
            data={'train': train, 'test': test, 'train_size': len(train), 'test_size': len(test), 'strata': len(groups)}
        )
    
    def _chunk_split(self, data: List[Any], num_chunks: int) -> ActionResult:
        """Split data into N chunks."""
        num_chunks = min(num_chunks, len(data))
        chunk_size = len(data) // num_chunks
        chunks = []
        for i in range(num_chunks):
            start = i * chunk_size
            if i == num_chunks - 1:
                end = len(data)
            else:
                end = start + chunk_size
            chunks.append(data[start:end])
        return ActionResult(
            success=True,
            message=f"Split into {len(chunks)} chunks",
            data={'chunks': chunks, 'num_chunks': len(chunks), 'chunk_sizes': [len(c) for c in chunks]}
        )
    
    def _bucket_split(self, data: List[Dict], num_buckets: int, bucket_by: Optional[str]) -> ActionResult:
        """Partition into N buckets by hash."""
        import hashlib
        buckets: List[List[Dict]] = [[] for _ in range(num_buckets)]
        
        for row in data:
            if bucket_by:
                val = str(row.get(bucket_by, ''))
                hash_val = int(hashlib.md5(val.encode()).hexdigest(), 16)
                bucket_idx = hash_val % num_buckets
            else:
                import random
                bucket_idx = random.randint(0, num_buckets - 1)
            buckets[bucket_idx].append(row)
        
        return ActionResult(
            success=True,
            message=f"Partitioned into {num_buckets} buckets",
            data={'buckets': buckets, 'num_buckets': num_buckets, 'bucket_sizes': [len(b) for b in buckets]}
        )
    
    def _conditional_split(self, data: List[Dict], conditions: Optional[Dict]) -> ActionResult:
        """Split by conditions (each condition gets its own bucket)."""
        if not conditions:
            return ActionResult(success=False, message="conditions required for conditional split")
        
        results: Dict[str, List[Dict]] = {name: [] for name in conditions.keys()}
        results['_unmatched'] = []
        
        for row in data:
            matched = False
            for name, condition in conditions.items():
                if self._matches_condition(row, condition):
                    results[name].append(row)
                    matched = True
                    break
            if not matched:
                results['_unmatched'].append(row)
        
        return ActionResult(
            success=True,
            message=f"Conditional split: {sum(len(v) for v in results.values())} total",
            data={'splits': results, 'split_counts': {k: len(v) for k, v in results.items()}}
        )
    
    def _matches_condition(self, row: Dict, condition: Dict) -> bool:
        """Check if a row matches a condition."""
        for field, expected in condition.items():
            actual = row.get(field)
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            else:
                if actual != expected:
                    return False
        return True
