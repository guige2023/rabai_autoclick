"""Data split action module for RabAI AutoClick.

Provides data splitting operations:
- SplitChunkAction: Split into chunks
- SplitTrainTestAction: Train/test split
- SplitStratifyAction: Stratified split
- SplitGroupAction: Split by group
- SplitConditionalAction: Conditional split
"""

import random
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SplitChunkAction(BaseAction):
    """Split data into chunks."""
    action_type = "split_chunk"
    display_name = "分块"
    description = "将数据分块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            chunk_size = params.get("chunk_size", 10)
            if not data:
                return ActionResult(success=False, message="data is required")
            if chunk_size <= 0:
                return ActionResult(success=False, message="chunk_size must be positive")

            chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

            return ActionResult(
                success=True,
                data={"chunk_count": len(chunks), "chunk_size": chunk_size, "total_items": len(data)},
                message=f"Split into {len(chunks)} chunks of ~{chunk_size}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split chunk failed: {e}")


class SplitTrainTestAction(BaseAction):
    """Train/test split."""
    action_type = "split_train_test"
    display_name = "训练测试分割"
    description = "训练测试集分割"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            test_size = params.get("test_size", 0.2)
            shuffle = params.get("shuffle", True)
            seed = params.get("seed", 42)

            if not data:
                return ActionResult(success=False, message="data is required")

            if shuffle:
                random.seed(seed)
                data_copy = data[:]
                random.shuffle(data_copy)
            else:
                data_copy = data

            split_idx = int(len(data_copy) * (1 - test_size))
            train = data_copy[:split_idx]
            test = data_copy[split_idx:]

            return ActionResult(
                success=True,
                data={"train_count": len(train), "test_count": len(test), "test_ratio": test_size, "shuffled": shuffle},
                message=f"Train/test split: {len(train)} train, {len(test)} test",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split train/test failed: {e}")


class SplitStratifyAction(BaseAction):
    """Stratified split."""
    action_type = "split_stratify"
    display_name = "分层分割"
    description = "分层抽样分割"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            stratify_by = params.get("stratify_by", "")
            test_size = params.get("test_size", 0.2)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not stratify_by:
                return ActionResult(success=False, message="stratify_by is required")

            groups: Dict[str, List] = {}
            for item in data:
                group_key = str(item.get(stratify_by, "unknown"))
                if group_key not in groups:
                    groups[group_key] = []
                groups[group_key].append(item)

            train_groups = {}
            test_groups = {}
            for group_key, group_data in groups.items():
                split_idx = max(1, int(len(group_data) * (1 - test_size)))
                train_groups[group_key] = group_data[:split_idx]
                test_groups[group_key] = group_data[split_idx:]

            train = [item for g in train_groups.values() for item in g]
            test = [item for g in test_groups.values() for item in g]

            return ActionResult(
                success=True,
                data={"train_count": len(train), "test_count": len(test), "group_count": len(groups)},
                message=f"Stratified split: {len(train)} train, {len(test)} test across {len(groups)} groups",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split stratify failed: {e}")


class SplitGroupAction(BaseAction):
    """Split by group."""
    action_type = "split_group"
    display_name = "分组分割"
    description = "按分组分割数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", "")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not group_by:
                return ActionResult(success=False, message="group_by is required")

            groups: Dict[str, List] = {}
            for item in data:
                key = str(item.get(group_by, "unknown"))
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)

            return ActionResult(
                success=True,
                data={"group_count": len(groups), "groups": {k: len(v) for k, v in groups.items()}},
                message=f"Split into {len(groups)} groups",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split group failed: {e}")


class SplitConditionalAction(BaseAction):
    """Conditional split."""
    action_type = "split_conditional"
    display_name = "条件分割"
    description = "按条件分割数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            condition = params.get("condition", {})

            if not data:
                return ActionResult(success=False, message="data is required")

            field = condition.get("field", "")
            operator = condition.get("operator", "eq")
            value = condition.get("value", "")

            if not field:
                return ActionResult(success=False, message="condition field is required")

            matched = []
            not_matched = []
            for item in data:
                item_val = str(item.get(field, ""))
                val_str = str(value)

                if operator == "eq" and item_val == val_str:
                    matched.append(item)
                elif operator == "ne" and item_val != val_str:
                    matched.append(item)
                elif operator == "gt" and item_val > val_str:
                    matched.append(item)
                elif operator == "lt" and item_val < val_str:
                    matched.append(item)
                elif operator == "contains" and val_str in item_val:
                    matched.append(item)
                else:
                    not_matched.append(item)

            return ActionResult(
                success=True,
                data={"matched_count": len(matched), "not_matched_count": len(not_matched)},
                message=f"Conditional split: {len(matched)} matched, {len(not_matched)} not matched",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Split conditional failed: {e}")
