"""API Federation Action Module.

Provides API federation capabilities for combining results from
multiple API sources with deduplication, ranking, and fusion strategies.
"""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class FusionStrategy(Enum):
    """Fusion strategies for combining API results."""
    UNION = "union"
    INTERSECTION = "intersection"
    RANK_AVERAGED = "rank_averaged"
    SCORE_WEIGHTED = "score_weighted"
    LATEST = "latest"
    MOST_RELEVANT = "most_relevant"


@dataclass
class APISource:
    """An API source in the federation."""
    name: str
    url: str
    priority: int = 1
    weight: float = 1.0
    enabled: bool = True
    timeout: float = 10.0


@dataclass
class FederatedResult:
    """A result item from federation."""
    content: Any
    source: str
    score: float = 1.0
    rank: int = 0
    content_hash: str = ""
    timestamp: float = field(default_factory=time.time)


class APIFederationAction(BaseAction):
    """Federated API aggregation action.

    Combines and deduplicates results from multiple API sources
    using various fusion strategies.

    Args:
        context: Execution context.
        params: Dict with keys:
            - sources: List[Dict] with source configs
            - results: Dict mapping source_name -> list of results
            - strategy: Fusion strategy (union, intersection, rank_averaged, etc.)
            - operation: Operation type (fuse, add_source, get_status)
    """
    action_type = "api_federation"
    display_name = "API联邦"
    description = "多API源结果聚合与去重"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "sources": [],
            "results": {},
            "strategy": "union",
            "dedup_enabled": True,
            "min_score_threshold": 0.0,
            "max_results": 100,
        }

    def __init__(self) -> None:
        super().__init__()
        self._sources: Dict[str, APISource] = {}
        self._result_cache: Dict[str, List[FederatedResult]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute federation operation."""
        start_time = time.time()

        operation = params.get("operation", "fuse")
        sources = params.get("sources", [])
        strategy = params.get("strategy", "union")
        dedup_enabled = params.get("dedup_enabled", True)
        min_score = params.get("min_score_threshold", 0.0)
        max_results = params.get("max_results", 100)

        # Initialize sources
        for src in sources:
            name = src.get("name")
            if name:
                self._sources[name] = APISource(**src)

        if operation == "add_source":
            return self._add_sources(sources, start_time)
        elif operation == "fuse":
            results = params.get("results", {})
            return self._fuse_results(
                results, strategy, dedup_enabled, min_score, max_results, start_time
            )
        elif operation == "get_status":
            return self._get_federation_status(start_time)
        elif operation == "clear_cache":
            return self._clear_cache(start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _add_sources(self, sources: List[Dict], start_time: float) -> ActionResult:
        """Add API sources to the federation."""
        added = 0
        for src in sources:
            name = src.get("name")
            if name and name not in self._sources:
                self._sources[name] = APISource(**src)
                added += 1
        return ActionResult(
            success=True,
            message=f"Added {added} sources to federation",
            data={"sources_count": len(self._sources), "added": added},
            duration=time.time() - start_time
        )

    def _fuse_results(
        self,
        results: Dict[str, List],
        strategy: str,
        dedup_enabled: bool,
        min_score: float,
        max_results: int,
        start_time: float
    ) -> ActionResult:
        """Fuse results from multiple sources."""
        all_results: List[FederatedResult] = []

        for source_name, items in results.items():
            if source_name not in self._sources:
                self._sources[source_name] = APISource(name=source_name, url="", priority=1)
            source = self._sources[source_name]

            if not source.enabled:
                continue

            for idx, item in enumerate(items):
                content = item if isinstance(item, dict) else {"value": item}
                content_hash = self._compute_hash(content)
                score = content.get("_score", 1.0) * source.weight
                result = FederatedResult(
                    content=content,
                    source=source_name,
                    score=score,
                    rank=idx,
                    content_hash=content_hash,
                    timestamp=content.get("_timestamp", time.time())
                )
                all_results.append(result)

        # Deduplication
        if dedup_enabled:
            seen_hashes: Set[str] = set()
            unique_results: List[FederatedResult] = []
            for r in all_results:
                if r.content_hash not in seen_hashes:
                    seen_hashes.add(r.content_hash)
                    unique_results.append(r)
            all_results = unique_results

        # Filter by score
        all_results = [r for r in all_results if r.score >= min_score]

        # Apply fusion strategy
        fused = self._apply_fusion_strategy(all_results, strategy)

        # Sort by score descending and limit
        fused.sort(key=lambda x: x.score, reverse=True)
        fused = fused[:max_results]

        # Update ranks
        for i, r in enumerate(fused):
            r.rank = i + 1

        return ActionResult(
            success=True,
            message=f"Federated {len(all_results)} results using '{strategy}' strategy, returned {len(fused)}",
            data={
                "strategy": strategy,
                "total_results": len(all_results),
                "returned_results": len(fused),
                "deduplicated": dedup_enabled,
                "results": [
                    {
                        "rank": r.rank,
                        "source": r.source,
                        "score": r.score,
                        "content": r.content,
                        "timestamp": r.timestamp,
                    }
                    for r in fused
                ]
            },
            duration=time.time() - start_time
        )

    def _apply_fusion_strategy(
        self,
        results: List[FederatedResult],
        strategy: str
    ) -> List[FederatedResult]:
        """Apply fusion strategy to results."""
        if strategy == "union":
            return results
        elif strategy == "most_relevant":
            return sorted(results, key=lambda x: x.score, reverse=True)
        elif strategy == "latest":
            return sorted(results, key=lambda x: x.timestamp, reverse=True)
        elif strategy == "score_weighted":
            # Weight scores by source priority
            for r in results:
                source = self._sources.get(r.source)
                if source:
                    r.score *= source.priority
            return results
        elif strategy == "rank_averaged":
            # Normalize and average ranks across sources
            by_hash: Dict[str, List[FederatedResult]] = {}
            for r in results:
                if r.content_hash not in by_hash:
                    by_hash[r.content_hash] = []
                by_hash[r.content_hash].append(r)

            fused: List[FederatedResult] = []
            for hash_val, items in by_hash.items():
                avg_rank = sum(r.rank for r in items) / len(items)
                avg_score = sum(r.score for r in items) / len(items)
                fused_result = FederatedResult(
                    content=items[0].content,
                    source=",".join(set(r.source for r in items)),
                    score=avg_score,
                    rank=int(avg_rank),
                    content_hash=hash_val,
                    timestamp=max(r.timestamp for r in items),
                )
                fused.append(fused_result)
            return fused
        else:
            return results

    def _compute_hash(self, content: Any) -> str:
        """Compute a hash for deduplication."""
        try:
            import json
            serialized = json.dumps(content, sort_keys=True, default=str)
            return hashlib.md5(serialized.encode()).hexdigest()
        except Exception:
            return hashlib.md5(str(content).encode()).hexdigest()

    def _get_federation_status(self, start_time: float) -> ActionResult:
        """Get federation status."""
        source_status = {
            name: {
                "enabled": src.enabled,
                "priority": src.priority,
                "weight": src.weight,
                "url": src.url,
                "timeout": src.timeout,
            }
            for name, src in self._sources.items()
        }
        return ActionResult(
            success=True,
            message="Federation status retrieved",
            data={
                "sources_count": len(self._sources),
                "enabled_count": sum(1 for s in self._sources.values() if s.enabled),
                "sources": source_status,
            },
            duration=time.time() - start_time
        )

    def _clear_cache(self, start_time: float) -> ActionResult:
        """Clear the result cache."""
        self._result_cache.clear()
        return ActionResult(
            success=True,
            message="Federation cache cleared",
            duration=time.time() - start_time
        )


from enum import Enum
