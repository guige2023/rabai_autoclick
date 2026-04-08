"""Data splitter action module for RabAI AutoClick.

Provides data splitting capabilities for dividing datasets
into training/testing sets, chunks, and conditional splits.
"""

import sys
import os
import random
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSplitterAction(BaseAction):
    """Data splitter action for splitting datasets.
    
    Supports train/test splits, stratified splits,
    chunking, and conditional splitting.
    """
    action_type = "data_splitter"
    display_name = "数据分割器"
    description = "数据集分割与分层采样"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to split
                operation: split|chunk|stratify|conditional
                split_ratio: Ratio for train/test split (e.g., 0.8)
                ratios: Multiple ratios for multiple splits
                stratify_by: Field for stratified sampling
                chunk_size: Size of each chunk
                condition_field: Field for conditional split
                condition_value: Value to match for first split.
        
        Returns:
            ActionResult with split data.
        """
        data = params.get('data', [])
        operation = params.get('operation', 'split')
        random_state = params.get('random_state', 42)
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if random_state is not None:
            random.seed(random_state)
        
        if operation == 'split':
            split_ratio = params.get('split_ratio', 0.8)
            return self._train_test_split(data, split_ratio)
        elif operation == 'ratios':
            ratios = params.get('ratios', [0.7, 0.2, 0.1])
            return self._ratio_split(data, ratios)
        elif operation == 'chunk':
            chunk_size = params.get('chunk_size', 100)
            return self._chunk_split(data, chunk_size)
        elif operation == 'stratify':
            stratify_by = params.get('stratify_by')
            split_ratio = params.get('split_ratio', 0.8)
            return self._stratified_split(data, stratify_by, split_ratio)
        elif operation == 'conditional':
            condition_field = params.get('condition_field')
            condition_value = params.get('condition_value')
            return self._conditional_split(data, condition_field, condition_value)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _train_test_split(
        self,
        data: List[Any],
        split_ratio: float
    ) -> ActionResult:
        """Split into train and test sets."""
        split_point = int(len(data) * split_ratio)
        shuffled = list(data)
        random.shuffle(shuffled)
        
        train = shuffled[:split_point]
        test = shuffled[split_point:]
        
        return ActionResult(
            success=True,
            message=f"Split into train ({len(train)}) and test ({len(test)})",
            data={
                'train': train,
                'test': test,
                'train_count': len(train),
                'test_count': len(test),
                'split_ratio': split_ratio
            }
        )
    
    def _ratio_split(
        self,
        data: List[Any],
        ratios: List[float]
    ) -> ActionResult:
        """Split into multiple sets by ratios."""
        if abs(sum(ratios) - 1.0) > 0.001:
            return ActionResult(success=False, message=f"Ratios must sum to 1.0, got {sum(ratios)}")
        
        shuffled = list(data)
        random.shuffle(shuffled)
        
        splits = []
        cumulative = 0
        
        for ratio in ratios:
            split_point = int(len(data) * ratio)
            split = shuffled[cumulative:cumulative + split_point]
            splits.append(split)
            cumulative += split_point
        
        if cumulative < len(data):
            splits[-1].extend(shuffled[cumulative:])
        
        return ActionResult(
            success=True,
            message=f"Split into {len(splits)} sets",
            data={
                'splits': splits,
                'counts': [len(s) for s in splits],
                'ratios': ratios
            }
        )
    
    def _chunk_split(
        self,
        data: List[Any],
        chunk_size: int
    ) -> ActionResult:
        """Split into chunks of specified size."""
        chunks = []
        
        for i in range(0, len(data), chunk_size):
            chunks.append(data[i:i + chunk_size])
        
        return ActionResult(
            success=True,
            message=f"Split into {len(chunks)} chunks",
            data={
                'chunks': chunks,
                'chunk_count': len(chunks),
                'chunk_size': chunk_size
            }
        )
    
    def _stratified_split(
        self,
        data: List[Any],
        stratify_by: Optional[str],
        split_ratio: float
    ) -> ActionResult:
        """Stratified split maintaining class distribution."""
        if not stratify_by:
            return ActionResult(success=False, message="stratify_by field required")
        
        if not isinstance(data[0], dict):
            return ActionResult(success=False, message="Stratify requires list of dicts")
        
        strata: Dict[Any, List[Any]] = defaultdict(list)
        
        for item in data:
            key = item.get(stratify_by)
            strata[key].append(item)
        
        train = []
        test = []
        
        for key, stratum in strata.items():
            split_point = int(len(stratum) * split_ratio)
            random.shuffle(stratum)
            train.extend(stratum[:split_point])
            test.extend(stratum[split_point:])
        
        return ActionResult(
            success=True,
            message=f"Stratified split: train {len(train)}, test {len(test)}",
            data={
                'train': train,
                'test': test,
                'train_count': len(train),
                'test_count': len(test),
                'strata_count': len(strata),
                'split_ratio': split_ratio
            }
        )
    
    def _conditional_split(
        self,
        data: List[Any],
        condition_field: Optional[str],
        condition_value: Any
    ) -> ActionResult:
        """Split based on condition."""
        if not condition_field:
            return ActionResult(success=False, message="condition_field required")
        
        matching = []
        non_matching = []
        
        for item in data:
            if isinstance(item, dict):
                if item.get(condition_field) == condition_value:
                    matching.append(item)
                else:
                    non_matching.append(item)
            else:
                non_matching.append(item)
        
        return ActionResult(
            success=True,
            message=f"Conditional split: {len(matching)} matching, {len(non_matching)} non-matching",
            data={
                'matching': matching,
                'non_matching': non_matching,
                'matching_count': len(matching),
                'non_matching_count': len(non_matching),
                'condition_field': condition_field,
                'condition_value': condition_value
            }
        )
