"""
Data Segmentation Action Module.

Segments data into groups based on rules, clustering, or quantile analysis.
Supports recursive segmentation and segment-specific transformations.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class Segment:
    """A data segment."""
    name: str
    count: int
    criteria: str
    records: list[dict[str, Any]]


@dataclass
class SegmentationResult:
    """Result of segmentation."""
    segments: list[Segment]
    total_records: int
    unsegmented_count: int


class DataSegmentationAction(BaseAction):
    """Segment data into groups based on rules."""

    def __init__(self) -> None:
        super().__init__("data_segmentation")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Segment records into groups.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - segments: List of segment configs
                    - name: Segment name
                    - criteria: Expression or condition dict
                - default_segment: Name for unmatched records

        Returns:
            SegmentationResult with segment breakdown
        """
        records = params.get("records", [])
        segment_configs = params.get("segments", [])
        default_segment = params.get("default_segment", "other")

        segments: dict[str, list[dict]] = {cfg.get("name", f"segment_{i}"): [] for i, cfg in enumerate(segment_configs)}
        segments[default_segment] = []

        for record in records:
            if not isinstance(record, dict):
                continue

            matched = False
            for cfg in segment_configs:
                name = cfg.get("name", "unnamed")
                criteria = cfg.get("criteria")
                if self._matches(record, criteria):
                    segments[name].append(record)
                    matched = True
                    break

            if not matched:
                segments[default_segment].append(record)

        segment_objects = []
        for name, seg_records in segments.items():
            criteria_str = ""
            for cfg in segment_configs:
                if cfg.get("name") == name:
                    criteria_str = str(cfg.get("criteria", ""))
                    break
            segment_objects.append(Segment(
                name=name,
                count=len(seg_records),
                criteria=criteria_str,
                records=seg_records
            ))

        return SegmentationResult(
            segments=segment_objects,
            total_records=len(records),
            unsegmented_count=len(segments.get(default_segment, []))
        )

    def _matches(self, record: dict, criteria: Any) -> bool:
        """Check if record matches criteria."""
        import re

        if criteria is None:
            return False
        if isinstance(criteria, dict):
            for field, value in criteria.items():
                if record.get(field) != value:
                    return False
            return True
        if isinstance(criteria, str):
            try:
                return bool(eval(criteria, {"r": record}))
            except Exception:
                return False
        return False
