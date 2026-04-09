"""
Data Comparison and Diff Module.

Compares data structures, records, and files to identify
differences with detailed diff reporting and change summaries.

Author: AutoGen
"""
from __future__ import annotations

import difflib
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class DiffType(Enum):
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    UNCHANGED = auto()


@dataclass
class DiffEntry:
    path: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DiffReport:
    left_name: str
    right_name: str
    diffs: List[DiffEntry] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.utcnow)


class DataComparator:
    """
    Compares data structures and generates detailed diff reports.
    """

    def __init__(self, ignore_fields: Optional[List[str]] = None):
        self.ignore_fields = set(ignore_fields or [])

    def compare_dicts(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        path: str = "",
    ) -> List[DiffEntry]:
        diffs: List[DiffEntry] = []
        all_keys = set(left.keys()) | set(right.keys())

        for key in sorted(all_keys):
            if key in self.ignore_fields:
                continue

            current_path = f"{path}.{key}" if path else key
            left_val = left.get(key)
            right_val = right.get(key)

            if key not in left:
                diffs.append(DiffEntry(
                    path=current_path,
                    diff_type=DiffType.ADDED,
                    new_value=right_val,
                ))
            elif key not in right:
                diffs.append(DiffEntry(
                    path=current_path,
                    diff_type=DiffType.REMOVED,
                    old_value=left_val,
                ))
            elif left_val != right_val:
                if isinstance(left_val, dict) and isinstance(right_val, dict):
                    diffs.extend(self.compare_dicts(left_val, right_val, current_path))
                elif isinstance(left_val, list) and isinstance(right_val, list):
                    diffs.extend(self.compare_lists(left_val, right_val, current_path))
                else:
                    diffs.append(DiffEntry(
                        path=current_path,
                        diff_type=DiffType.MODIFIED,
                        old_value=left_val,
                        new_value=right_val,
                    ))

        return diffs

    def compare_lists(
        self, left: List[Any], right: List[Any], path: str = ""
    ) -> List[DiffEntry]:
        diffs: List[DiffEntry] = []

        left_str = [json.dumps(v, sort_keys=True, default=str) for v in left]
        right_str = [json.dumps(v, sort_keys=True, default=str) for v in right]

        matcher = difflib.SequenceMatcher(None, left_str, right_str)
        opcodes = matcher.get_opcodes()

        for tag, i1, i2, j1, j2 in opcodes:
            if tag == "replace":
                for i in range(i1, i2):
                    diffs.append(DiffEntry(
                        path=f"{path}[{i}]",
                        diff_type=DiffType.REMOVED,
                        old_value=left[i],
                    ))
                for j in range(j1, j2):
                    diffs.append(DiffEntry(
                        path=f"{path}[{j}]",
                        diff_type=DiffType.ADDED,
                        new_value=right[j],
                    ))
            elif tag == "delete":
                for i in range(i1, i2):
                    diffs.append(DiffEntry(
                        path=f"{path}[{i}]",
                        diff_type=DiffType.REMOVED,
                        old_value=left[i],
                    ))
            elif tag == "insert":
                for j in range(j1, j2):
                    diffs.append(DiffEntry(
                        path=f"{path}[{j}]",
                        diff_type=DiffType.ADDED,
                        new_value=right[j],
                    ))

        return diffs

    def compare_records(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key_field: str = "id",
    ) -> Tuple[List[str], List[str], List[Tuple[Any, Any]]]:
        """Compares two lists of records by key field."""
        left_keys = {r.get(key_field) for r in left}
        right_keys = {r.get(key_field) for r in right}

        added = sorted(left_keys - right_keys)
        removed = sorted(right_keys - left_keys)

        left_dict = {r.get(key_field): r for r in left}
        right_dict = {r.get(key_field): r for r in right}

        modified = []
        common_keys = left_keys & right_keys
        for key in sorted(common_keys):
            l_record = left_dict[key]
            r_record = right_dict[key]
            if l_record != r_record:
                diffs = self.compare_dicts(l_record, r_record)
                if diffs:
                    modified.append((key, diffs))

        return added, removed, modified

    def generate_report(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        left_name: str = "left",
        right_name: str = "right",
    ) -> DiffReport:
        diffs = self.compare_dicts(left, right)

        summary = {
            "total": len(diffs),
            "added": sum(1 for d in diffs if d.diff_type == DiffType.ADDED),
            "removed": sum(1 for d in diffs if d.diff_type == DiffType.REMOVED),
            "modified": sum(1 for d in diffs if d.diff_type == DiffType.MODIFIED),
        }

        return DiffReport(
            left_name=left_name,
            right_name=right_name,
            diffs=diffs,
            summary=summary,
        )

    def to_json(self, report: DiffReport) -> str:
        return json.dumps({
            "left_name": report.left_name,
            "right_name": report.right_name,
            "summary": report.summary,
            "diffs": [
                {
                    "path": d.path,
                    "type": d.diff_type.name,
                    "old_value": str(d.old_value) if d.old_value is not None else None,
                    "new_value": str(d.new_value) if d.new_value is not None else None,
                }
                for d in report.diffs
            ],
            "generated_at": report.generated_at.isoformat(),
        }, indent=2)


class TextDiff:
    """Generates line-by-line text diffs."""

    @staticmethod
    def diff_text(
        left_text: str,
        right_text: str,
        left_name: str = "left",
        right_name: str = "right",
    ) -> str:
        left_lines = left_text.splitlines(keepends=True)
        right_lines = right_text.splitlines(keepends=True)
        diff = difflib.unified_diff(
            left_lines, right_lines,
            fromfile=left_name, tofile=right_name,
            lineterm="",
        )
        return "".join(diff)

    @staticmethod
    def diff_html(left_text: str, right_text: str) -> str:
        left_lines = left_text.splitlines()
        right_lines = right_text.splitlines()
        matcher = difflib.SequenceMatcher(None, left_lines, right_lines)
        html = ["<div class='diff'>"]
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                for line in left_lines[i1:i2]:
                    html.append(f"<span class='unchanged'>{line}</span>")
            elif tag == "replace":
                for line in left_lines[i1:i2]:
                    html.append(f"<span class='removed'>{line}</span>")
                for line in right_lines[j1:j2]:
                    html.append(f"<span class='added'>{line}</span>")
            elif tag == "delete":
                for line in left_lines[i1:i2]:
                    html.append(f"<span class='removed'>{line}</span>")
            elif tag == "insert":
                for line in right_lines[j1:j2]:
                    html.append(f"<span class='added'>{line}</span>")
        html.append("</div>")
        return "\n".join(html)
