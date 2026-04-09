"""Data Union-Find Action Module.

Provides Union-Find (Disjoint Set Union) data structure for
grouping related data items and tracking connectivity.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class UnionFindState:
    """Union-Find internal state."""
    parent: Dict[str, str]
    rank: Dict[str, int]
    sets: Dict[str, Set[str]]
    num_sets: int = 0


class DataUnionFindAction(BaseAction):
    """Union-Find (Disjoint Set Union) action.

    Groups related data items and tracks connectivity using
    Union-Find with path compression and union by rank.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (make_set, union, find, connected, get_groups, clear)
            - elements: List of elements for make_set
            - element: Single element for find/union
            - a: First element for union/connected
            - b: Second element for union/connected
            - group_a: First group identifier for group union
            - group_b: Second group identifier for group union
            - dataset_id: Identifier for the union-find instance
    """
    action_type = "data_union_find"
    display_name = "数据并查集"
    description = "并查集数据结构用于元素分组与连通性检测"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "elements": [],
            "element": None,
            "a": None,
            "b": None,
            "dataset_id": "default",
        }

    def __init__(self) -> None:
        super().__init__()
        self._datasets: Dict[str, UnionFindState] = {}

    def _get_dataset(self, dataset_id: str) -> UnionFindState:
        """Get or create a UnionFindState for dataset_id."""
        if dataset_id not in self._datasets:
            self._datasets[dataset_id] = UnionFindState(
                parent={},
                rank={},
                sets={},
                num_sets=0
            )
        return self._datasets[dataset_id]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute union-find operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        elements = params.get("elements", [])
        element = params.get("element")
        a = params.get("a")
        b = params.get("b")
        dataset_id = params.get("dataset_id", "default")

        uf = self._get_dataset(dataset_id)

        if operation == "make_set":
            return self._make_set(uf, elements, element, dataset_id, start_time)
        elif operation == "union":
            return self._union(uf, a, b, dataset_id, start_time)
        elif operation == "find":
            return self._find(uf, element, dataset_id, start_time)
        elif operation == "connected":
            return self._connected(uf, a, b, dataset_id, start_time)
        elif operation == "get_groups":
            return self._get_groups(uf, dataset_id, start_time)
        elif operation == "clear":
            return self._clear(uf, dataset_id, start_time)
        elif operation == "status":
            return self._status(uf, dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _make_set(
        self,
        uf: UnionFindState,
        elements: List[str],
        element: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Create new sets for elements."""
        created = 0
        to_create = list(elements)
        if element:
            to_create.append(element)

        for elem in to_create:
            elem_str = str(elem)
            if elem_str not in uf.parent:
                uf.parent[elem_str] = elem_str
                uf.rank[elem_str] = 0
                uf.sets[elem_str] = {elem_str}
                uf.num_sets += 1
                created += 1

        return ActionResult(
            success=True,
            message=f"Created {created} new sets in '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "created": created,
                "total_sets": uf.num_sets,
                "total_elements": len(uf.parent),
            },
            duration=time.time() - start_time
        )

    def _find(self, uf: UnionFindState, element: Optional[str], dataset_id: str, start_time: float) -> ActionResult:
        """Find the root/representative of element's set with path compression."""
        if not element:
            return ActionResult(success=False, message="element required for find", duration=time.time() - start_time)

        elem = str(element)
        if elem not in uf.parent:
            return ActionResult(success=False, message=f"Element '{elem}' not found", duration=time.time() - start_time)

        # Find with path compression
        root = self._find_root(uf, elem)

        return ActionResult(
            success=True,
            message=f"Find root of '{elem}': '{root}'",
            data={
                "element": elem,
                "root": root,
                "dataset_id": dataset_id,
                "set_size": len(uf.sets.get(root, set())),
            },
            duration=time.time() - start_time
        )

    def _find_root(self, uf: UnionFindState, elem: str) -> str:
        """Internal find with path compression."""
        if uf.parent[elem] != elem:
            uf.parent[elem] = self._find_root(uf, uf.parent[elem])  # Path compression
        return uf.parent[elem]

    def _union(
        self,
        uf: UnionFindState,
        a: Optional[str],
        b: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Union two sets by rank."""
        if not a or not b:
            return ActionResult(success=False, message="Both 'a' and 'b' required for union", duration=time.time() - start_time)

        a_str = str(a)
        b_str = str(b)

        if a_str not in uf.parent or b_str not in uf.parent:
            return ActionResult(success=False, message=f"Elements not found", duration=time.time() - start_time)

        root_a = self._find_root(uf, a_str)
        root_b = self._find_root(uf, b_str)

        if root_a == root_b:
            return ActionResult(
                success=True,
                message=f"'{a_str}' and '{b_str}' already in the same set",
                data={"a": a_str, "b": b_str, "root": root_a, "merged": False, "dataset_id": dataset_id},
                duration=time.time() - start_time
            )

        # Union by rank
        if uf.rank[root_a] < uf.rank[root_b]:
            uf.parent[root_a] = root_b
            uf.sets[root_b].update(uf.sets[root_a])
            del uf.sets[root_a]
        elif uf.rank[root_a] > uf.rank[root_b]:
            uf.parent[root_b] = root_a
            uf.sets[root_a].update(uf.sets[root_b])
            del uf.sets[root_b]
        else:
            uf.parent[root_b] = root_a
            uf.rank[root_a] += 1
            uf.sets[root_a].update(uf.sets[root_b])
            del uf.sets[root_b]

        uf.num_sets -= 1

        return ActionResult(
            success=True,
            message=f"Merged '{a_str}' and '{b_str}' into same set",
            data={
                "a": a_str,
                "b": b_str,
                "merged": True,
                "remaining_sets": uf.num_sets,
                "dataset_id": dataset_id,
            },
            duration=time.time() - start_time
        )

    def _connected(
        self,
        uf: UnionFindState,
        a: Optional[str],
        b: Optional[str],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Check if two elements are in the same set."""
        if not a or not b:
            return ActionResult(success=False, message="Both 'a' and 'b' required for connected", duration=time.time() - start_time)

        a_str = str(a)
        b_str = str(b)

        if a_str not in uf.parent or b_str not in uf.parent:
            return ActionResult(success=False, message=f"Elements not found", duration=time.time() - start_time)

        root_a = self._find_root(uf, a_str)
        root_b = self._find_root(uf, b_str)
        is_connected = root_a == root_b

        return ActionResult(
            success=True,
            message=f"'{a_str}' and '{b_str}' are {'connected' if is_connected else 'not connected'}",
            data={
                "a": a_str,
                "b": b_str,
                "connected": is_connected,
                "root_a": root_a,
                "root_b": root_b,
                "dataset_id": dataset_id,
            },
            duration=time.time() - start_time
        )

    def _get_groups(self, uf: UnionFindState, dataset_id: str, start_time: float) -> ActionResult:
        """Get all current groups/sets."""
        groups = []
        for root, members in uf.sets.items():
            groups.append({
                "representative": root,
                "size": len(members),
                "members": sorted(list(members)),
            })

        groups.sort(key=lambda g: -g["size"])  # Largest first

        return ActionResult(
            success=True,
            message=f"Retrieved {len(groups)} groups from '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "num_groups": len(groups),
                "num_elements": len(uf.parent),
                "groups": groups,
            },
            duration=time.time() - start_time
        )

    def _clear(self, uf: UnionFindState, dataset_id: str, start_time: float) -> ActionResult:
        """Clear the union-find dataset."""
        count = len(uf.parent)
        uf.parent.clear()
        uf.rank.clear()
        uf.sets.clear()
        uf.num_sets = 0
        return ActionResult(
            success=True,
            message=f"Cleared {count} elements from '{dataset_id}'",
            data={"dataset_id": dataset_id, "cleared": count},
            duration=time.time() - start_time
        )

    def _status(self, uf: UnionFindState, dataset_id: str, start_time: float) -> ActionResult:
        """Get status of the union-find dataset."""
        return ActionResult(
            success=True,
            message=f"Union-Find status for '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "num_elements": len(uf.parent),
                "num_groups": uf.num_sets,
                "largest_group_size": max((len(m) for m in uf.sets.values()), default=0),
            },
            duration=time.time() - start_time
        )
