"""Data Comparer Action Module. Compares datasets and generates diff reports."""
import sys, os
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class DiffEntry:
    record_id: Any; field_name: str; left_value: Any; right_value: Any; diff_type: str

@dataclass
class ComparisonResult:
    total_left: int; total_right: int; matching: int; only_left: int
    only_right: int; modified: int; diffs: list; match_percentage: float

class DataComparerAction(BaseAction):
    action_type = "data_comparer"; display_name = "数据比对"
    description = "比对数据集"
    def __init__(self) -> None: super().__init__()
    def execute(self, context: Any, params: dict) -> ActionResult:
        left = params.get("left",[]); right = params.get("right",[])
        id_field = params.get("id_field"); ignore_fields = set(params.get("ignore_fields",[]))
        if not isinstance(left, list) or not isinstance(right, list): return ActionResult(success=False, message="Data must be lists")
        if not left and not right: return ActionResult(success=False, message="Both datasets empty")
        diffs = []
        if id_field and left and isinstance(left[0], dict):
            left_by_id = {str(r.get(id_field, i)): r for i, r in enumerate(left)}
            right_by_id = {str(r.get(id_field, i)): r for i, r in enumerate(right)}
            left_keys = set(left_by_id.keys()); right_keys = set(right_by_id.keys())
            only_left_ids = left_keys - right_keys; only_right_ids = right_keys - left_keys
            common_ids = left_keys & right_keys
            for id_val in only_left_ids:
                for k, v in left_by_id[id_val].items():
                    if k != id_field and k not in ignore_fields:
                        diffs.append(DiffEntry(id_val, k, v, None, "removed"))
            for id_val in only_right_ids:
                for k, v in right_by_id[id_val].items():
                    if k != id_field and k not in ignore_fields:
                        diffs.append(DiffEntry(id_val, k, None, v, "added"))
            for id_val in common_ids:
                lrec = left_by_id[id_val]; rrec = right_by_id[id_val]
                fields = set(lrec.keys()) | set(rrec.keys())
                for fld in fields:
                    if fld == id_field or fld in ignore_fields: continue
                    lv = lrec.get(fld); rv = rrec.get(fld)
                    if lv != rv: diffs.append(DiffEntry(id_val, fld, lv, rv, "modified"))
            matching = len(common_ids); only_left = len(only_left_ids); only_right = len(only_right_ids)
        else:
            only_left = sum(1 for x in left if x not in right)
            only_right = sum(1 for x in right if x not in left)
            matching = len(left) - only_left; modified = 0
        total = len(left) + len(right)
        match_pct = matching / max(len(left), len(right), 1) * 100
        result = ComparisonResult(total_left=len(left), total_right=len(right), matching=matching,
                                  only_left=only_left, only_right=only_right, modified=len(diffs),
                                  diffs=diffs[:100], match_percentage=match_pct)
        return ActionResult(success=True, message=f"Compare: {matching} match, {only_left} left-only, {only_right} right-only",
                          data=vars(result))
