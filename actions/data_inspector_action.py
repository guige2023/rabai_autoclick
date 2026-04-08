"""Data Inspector Action Module. Inspects data structures and calculates statistics."""
import sys, os, json
from typing import Any
from dataclasses import dataclass, field
from collections import Counter
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class FieldStats:
    name: str; type: str; null_count: int; unique_count: int
    sample_values: list = field(default_factory=list)
    min_value: Any = None; max_value: Any = None

@dataclass
class DataProfile:
    total_rows: int; total_columns: int; memory_bytes: int; field_stats: list
    type_distribution: dict; null_percentage: float; duplicate_rows: int

class DataInspectorAction(BaseAction):
    action_type = "data_inspector"; display_name = "数据检查"
    description = "检查数据结构和统计"
    def __init__(self) -> None: super().__init__()
    def _infer_type(self, value: Any) -> str:
        if value is None: return "null"
        t = type(value).__name__
        return {"bool": "boolean", "int": "integer", "float": "float", "str": "string", "list": "array", "dict": "object"}.get(t, t)
    def _profile_list(self, data: list) -> DataProfile:
        if not data: return DataProfile(0, 0, 0, [], {}, 0.0, 0)
        if isinstance(data[0], dict):
            cols = list(data[0].keys()); field_stats = []
            for col in cols:
                values = [r.get(col) for r in data]
                types = [self._infer_type(v) for v in values]
                null_count = sum(1 for v in values if v is None or v == "")
                unique_vals = set(str(v) for v in values if v is not None)
                field_stats.append(FieldStats(name=col, type=Counter(types).most_common(1)[0][0],
                                             null_count=null_count, unique_count=len(unique_vals),
                                             sample_values=list(unique_vals)[:5]))
            all_vals = [str(r) for r in data]
            dup_rows = len(all_vals) - len(set(all_vals))
            null_pct = sum(1 for r in data for v in r.values() if v is None) / (len(data)*len(cols)) if data else 0
            type_dist = dict(Counter(s.type for s in field_stats))
        else:
            values = data
            type_dist = Counter(self._infer_type(v) for v in values)
            field_stats = [FieldStats(name="value", type=type_dist.most_common(1)[0][0],
                                     null_count=sum(1 for v in values if v is None),
                                     unique_count=len(set(str(v) for v in values)),
                                     sample_values=list(set(values))[:5])]
            dup_rows = len(values) - len(set(str(v) for v in values))
            null_pct = sum(1 for v in values if v is None) / len(values) if values else 0
        return DataProfile(total_rows=len(data), total_columns=len(cols) if isinstance(data[0], dict) else 1,
                          memory_bytes=len(str(data).encode()), field_stats=field_stats,
                          type_distribution=type_dist, null_percentage=null_pct*100,
                          duplicate_rows=dup_rows)
    def execute(self, context: Any, params: dict) -> ActionResult:
        data = params.get("data"); source = params.get("source"); sample_size = params.get("sample_size")
        if source and not data:
            try:
                with open(source, "r", encoding="utf-8", errors="replace") as f:
                    raw = f.read()
                data = json.loads(raw) if raw.strip().startswith(("[","{")) else raw.splitlines()
            except Exception as e: return ActionResult(success=False, message=f"Load failed: {e}")
        if not data: return ActionResult(success=False, message="No data")
        if isinstance(data, str):
            try: data = json.loads(data)
            except: pass
        if isinstance(data, list):
            if sample_size and len(data) > sample_size:
                import random; data = random.sample(data, sample_size)
            profile = self._profile_list(data)
        elif isinstance(data, dict): profile = self._profile_list([data])
        else: return ActionResult(success=False, message=f"Unsupported: {type(data)}")
        return ActionResult(success=True, message=f"Profile: {profile.total_rows} rows, {profile.total_columns} cols, {profile.null_percentage:.1f}% nulls",
                          data=vars(profile))
