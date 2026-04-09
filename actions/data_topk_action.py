"""Data Top-K Action Module.

Provides Top-K selection algorithms for finding the most significant
items from large datasets with various scoring strategies.
"""

import heapq
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class SelectionStrategy(Enum):
    """Top-K selection strategies."""
    LARGEST = "largest"
    SMALLEST = "smallest"
    MOST_FREQUENT = "most_frequent"
    HIGHEST_SCORE = "highest_score"
    RECENT = "recent"
    CUSTOM = "custom"


@dataclass
class ScoredItem:
    """An item with a computed score."""
    item: Any
    score: float
    key: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataTopKAction(BaseAction):
    """Top-K selection action.

    Finds the top K items from a dataset using various
    scoring and selection strategies.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (select, update, clear, status)
            - items: List of items to process
            - k: Number of top items to select
            - key_field: Field name to use as item key
            - score_field: Field name to use for scoring
            - strategy: Selection strategy
            - min_score: Minimum score threshold
            - dataset_id: Identifier for cached dataset
    """
    action_type = "data_topk"
    display_name = "数据Top-K"
    description = "Top-K项选择与排名算法"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "items": [],
            "k": 10,
            "key_field": None,
            "score_field": "score",
            "strategy": "largest",
            "min_score": None,
            "dataset_id": "default",
            "custom_score_fn": None,
        }

    def __init__(self) -> None:
        super().__init__()
        self._datasets: Dict[str, List[ScoredItem]] = {}
        self._topk_cache: Dict[str, List[ScoredItem]] = {}
        self._custom_scorers: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Top-K operation."""
        start_time = time.time()

        operation = params.get("operation", "select")
        items = params.get("items", [])
        k = params.get("k", 10)
        key_field = params.get("key_field")
        score_field = params.get("score_field", "score")
        strategy = params.get("strategy", "largest")
        min_score = params.get("min_score")
        dataset_id = params.get("dataset_id", "default")

        if operation == "select":
            return self._select_topk(
                items, k, key_field, score_field, strategy,
                min_score, dataset_id, start_time
            )
        elif operation == "update":
            return self._update_dataset(items, dataset_id, start_time)
        elif operation == "merge":
            return self._merge_datasets(
                params.get("dataset_ids", []), k, dataset_id, start_time
            )
        elif operation == "clear":
            return self._clear_dataset(dataset_id, start_time)
        elif operation == "status":
            return self._get_status(dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _select_topk(
        self,
        items: List[Any],
        k: int,
        key_field: Optional[str],
        score_field: str,
        strategy: str,
        min_score: Optional[float],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Select top K items from the dataset."""
        if not items:
            items = self._datasets.get(dataset_id, [])
            if not items:
                return ActionResult(
                    success=True,
                    message=f"Dataset '{dataset_id}' is empty",
                    data={"k": k, "selected": [], "total_available": 0},
                    duration=time.time() - start_time
                )

        # Score items
        scored_items = self._score_items(items, key_field, score_field, strategy)

        # Store in dataset
        if dataset_id not in self._datasets:
            self._datasets[dataset_id] = []
        self._datasets[dataset_id].extend(scored_items)

        # Filter by min_score
        if min_score is not None:
            scored_items = [item for item in scored_items if item.score >= min_score]

        # Select top K based on strategy
        if strategy == "smallest":
            topk = heapq.nsmallest(k, scored_items, key=lambda x: x.score)
        else:
            topk = heapq.nlargest(k, scored_items, key=lambda x: x.score)

        # Sort by score
        reverse = strategy != "smallest"
        topk.sort(key=lambda x: x.score, reverse=reverse)

        # Update cache
        self._topk_cache[dataset_id] = topk

        return ActionResult(
            success=True,
            message=f"Selected top {len(topk)} from {len(scored_items)} items",
            data={
                "k": k,
                "selected_count": len(topk),
                "total_available": len(scored_items),
                "strategy": strategy,
                "top_items": [
                    {
                        "rank": i + 1,
                        "key": item.key,
                        "score": item.score,
                        "item": item.item,
                        "metadata": item.metadata,
                    }
                    for i, item in enumerate(topk)
                ]
            },
            duration=time.time() - start_time
        )

    def _score_items(
        self,
        items: List[Any],
        key_field: Optional[str],
        score_field: str,
        strategy: str
    ) -> List[ScoredItem]:
        """Score items based on strategy."""
        scored = []
        for item in items:
            if isinstance(item, dict):
                key = str(item.get(key_field, item.get("id", ""))) if key_field else str(item)
                score = float(item.get(score_field, 0))
                metadata = {k: v for k, v in item.items() if k not in (key_field, score_field)}
            elif hasattr(item, score_field):
                key = str(getattr(item, key_field, "") if key_field else item)
                score = float(getattr(item, score_field, 0))
                metadata = {}
            else:
                key = str(item)
                score = float(item) if self._is_numeric(item) else 0.0
                metadata = {}

            if strategy == "most_frequent":
                # Frequency counted separately
                score = 1.0

            scored.append(ScoredItem(item=item, score=score, key=key, metadata=metadata))

        return scored

    def _is_numeric(self, value: Any) -> bool:
        """Check if a value is numeric."""
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False

    def _update_dataset(
        self,
        items: List[Any],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Update items in a dataset."""
        if dataset_id not in self._datasets:
            self._datasets[dataset_id] = []
        self._datasets[dataset_id].extend(items)
        # Clear stale topk cache
        if dataset_id in self._topk_cache:
            del self._topk_cache[dataset_id]
        return ActionResult(
            success=True,
            message=f"Updated dataset '{dataset_id}' with {len(items)} items",
            data={"dataset_id": dataset_id, "total_items": len(self._datasets[dataset_id])},
            duration=time.time() - start_time
        )

    def _merge_datasets(
        self,
        dataset_ids: List[str],
        k: int,
        output_id: str,
        start_time: float
    ) -> ActionResult:
        """Merge multiple datasets and select top K."""
        merged = []
        for did in dataset_ids:
            merged.extend(self._datasets.get(did, []))
        self._datasets[output_id] = merged
        # Clear cache for output
        if output_id in self._topk_cache:
            del self._topk_cache[output_id]
        return ActionResult(
            success=True,
            message=f"Merged {len(dataset_ids)} datasets into '{output_id}'",
            data={"output_id": output_id, "total_items": len(merged)},
            duration=time.time() - start_time
        )

    def _clear_dataset(self, dataset_id: str, start_time: float) -> ActionResult:
        """Clear a dataset."""
        if dataset_id in self._datasets:
            count = len(self._datasets[dataset_id])
            del self._datasets[dataset_id]
        if dataset_id in self._topk_cache:
            del self._topk_cache[dataset_id]
        return ActionResult(
            success=True,
            message=f"Cleared dataset '{dataset_id}'",
            data={"dataset_id": dataset_id, "cleared_count": count if 'count' in dir() else 0},
            duration=time.time() - start_time
        )

    def _get_status(self, dataset_id: str, start_time: float) -> ActionResult:
        """Get dataset status."""
        items = self._datasets.get(dataset_id, [])
        topk = self._topk_cache.get(dataset_id, [])
        return ActionResult(
            success=True,
            message=f"Dataset '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "total_items": len(items),
                "cached_topk_count": len(topk),
                "has_cache": dataset_id in self._topk_cache,
            },
            duration=time.time() - start_time
        )


from enum import Enum
