"""Data Funnel Analysis Action Module.

Provides funnel analysis capabilities for tracking user conversions,
drop-off rates, step-by-step analysis, and multi-branch funnel comparison.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class FunnelType(Enum):
    """Types of funnel analysis."""
    LINEAR = "linear"
    BRANCHING = "branching"
    PARALLEL = "parallel"
    LOOPING = "looping"


@dataclass
class FunnelStep:
    """A single step in a funnel."""
    step_id: str
    name: str
    event_name: str
    description: str = ""
    expected_duration_ms: Optional[int] = None
    optional: bool = False


@dataclass
class FunnelStage:
    """A stage in the funnel with aggregated metrics."""
    step: FunnelStep
    entered_count: int = 0
    exited_count: int = 0
    dropped_count: int = 0
    conversion_rate: float = 0.0
    avg_time_in_stage_ms: float = 0.0
    segment_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class FunnelResult:
    """Complete funnel analysis result."""
    funnel_id: str
    funnel_type: FunnelType
    stages: List[FunnelStage]
    total_entrances: int = 0
    final_conversions: int = 0
    overall_conversion_rate: float = 0.0
    total_drop_offs: int = 0
    drop_off_rate: float = 0.0
    avg_completion_time_ms: float = 0.0
    step_conversions: Dict[str, float] = field(default_factory=dict)
    drop_off_points: List[Tuple[str, float]] = field(default_factory=list)


@dataclass
class FunnelConfig:
    """Configuration for funnel analysis."""
    funnel_type: FunnelType = FunnelType.LINEAR
    min_sample_size: int = 100
    confidence_level: float = 0.95
    track_segments: bool = True
    track_timing: bool = True
    detect_anomalies: bool = True
    anomaly_threshold: float = 0.2


class FunnelCalculator:
    """Calculate funnel metrics."""

    @staticmethod
    def calculate_conversion_rate(entered: int, exited: int) -> float:
        """Calculate conversion rate between two funnel stages."""
        if entered == 0:
            return 0.0
        return exited / entered

    @staticmethod
    def calculate_drop_off_rate(entered: int, dropped: int) -> float:
        """Calculate drop-off rate."""
        if entered == 0:
            return 0.0
        return dropped / entered

    @staticmethod
    def calculate_aggregated_conversion(stages: List[FunnelStage]) -> float:
        """Calculate overall funnel conversion rate."""
        if not stages:
            return 0.0
        first_count = stages[0].entered_count
        last_count = stages[-1].exited_count
        if first_count == 0:
            return 0.0
        return last_count / first_count

    @staticmethod
    def identify_drop_off_points(
        stages: List[FunnelStage],
        threshold: float = 0.1
    ) -> List[Tuple[str, float]]:
        """Identify funnel steps with significant drop-off."""
        drop_off_points = []
        for i, stage in enumerate(stages):
            if stage.entered_count > 0:
                drop_rate = FunnelCalculator.calculate_drop_off_rate(
                    stage.entered_count, stage.dropped_count
                )
                if drop_rate >= threshold:
                    drop_off_points.append((stage.step.step_id, drop_rate))
        return drop_off_points


class SegmentedFunnelAnalyzer:
    """Analyze funnels across different segments."""

    def __init__(self):
        self._segment_data: Dict[str, Dict[str, FunnelStage]] = defaultdict(dict)

    def record_segment_step(
        self,
        segment_id: str,
        step_id: str,
        count: int,
        time_ms: Optional[float] = None
    ):
        """Record a step event for a specific segment."""
        if segment_id not in self._segment_data:
            self._segment_data[segment_id] = {}

    def get_segment_comparison(self, segment_ids: List[str]) -> Dict[str, Any]:
        """Compare funnel performance across segments."""
        comparison = {}
        for seg_id in segment_ids:
            if seg_id in self._segment_data:
                stages = list(self._segment_data[seg_id].values())
                if stages:
                    comparison[seg_id] = {
                        "total_entrances": stages[0].entered_count if stages else 0,
                        "final_exits": stages[-1].exited_count if stages else 0,
                        "conversion_rate": FunnelCalculator.calculate_aggregated_conversion(stages)
                    }
        return comparison


class DataFunnelAnalysisAction(BaseAction):
    """Action for funnel analysis."""

    def __init__(self):
        super().__init__(name="data_funnel_analysis")
        self._config = FunnelConfig()
        self._funnels: Dict[str, List[FunnelStep]] = {}
        self._event_buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._funnel_results: List[FunnelResult] = []
        self._segment_analyzer = SegmentedFunnelAnalyzer()

    def configure(self, config: FunnelConfig):
        """Configure funnel analysis settings."""
        self._config = config

    def define_funnel(
        self,
        funnel_id: str,
        steps: List[FunnelStep],
        funnel_type: FunnelType = FunnelType.LINEAR
    ) -> ActionResult:
        """Define a new funnel structure."""
        try:
            if len(steps) < 2:
                return ActionResult(success=False, error="Funnel must have at least 2 steps")

            if funnel_id in self._funnels:
                return ActionResult(
                    success=False,
                    error=f"Funnel {funnel_id} already exists"
                )

            self._funnels[funnel_id] = steps
            return ActionResult(success=True, data={"funnel_id": funnel_id, "steps": len(steps)})
        except Exception as e:
            logger.exception(f"Failed to define funnel {funnel_id}")
            return ActionResult(success=False, error=str(e))

    def record_event(
        self,
        funnel_id: str,
        step_id: str,
        user_id: str,
        timestamp: Optional[datetime] = None,
        segment: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Record a funnel step event."""
        try:
            if funnel_id not in self._funnels:
                return ActionResult(success=False, error=f"Funnel {funnel_id} not found")

            event = {
                "funnel_id": funnel_id,
                "step_id": step_id,
                "user_id": user_id,
                "timestamp": timestamp or datetime.now(),
                "segment": segment,
                "metadata": metadata or {}
            }
            self._event_buffer[funnel_id].append(event)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def analyze(self, funnel_id: str) -> ActionResult:
        """Analyze funnel performance."""
        try:
            if funnel_id not in self._funnels:
                return ActionResult(success=False, error=f"Funnel {funnel_id} not found")

            steps = self._funnels[funnel_id]
            events = self._event_buffer[funnel_id]

            if len(events) < self._config.min_sample_size:
                logger.warning(
                    f"Funnel {funnel_id} has {len(events)} events, "
                    f"minimum is {self._config.min_sample_size}"
                )

            user_journeys = self._build_user_journeys(events, steps)
            stages = self._calculate_stages(user_journeys, steps)
            drop_off_points = FunnelCalculator.identify_drop_off_points(
                stages, self._config.anomaly_threshold
            )

            total_entrances = stages[0].entered_count if stages else 0
            final_exits = stages[-1].exited_count if stages else 0
            total_drop_offs = sum(s.dropped_count for s in stages)

            result = FunnelResult(
                funnel_id=funnel_id,
                funnel_type=self._config.funnel_type,
                stages=stages,
                total_entrances=total_entrances,
                final_conversions=final_exits,
                overall_conversion_rate=FunnelCalculator.calculate_aggregated_conversion(stages),
                total_drop_offs=total_drop_offs,
                drop_off_rate=FunnelCalculator.calculate_drop_off_rate(total_entrances, total_drop_offs),
                step_conversions={
                    s.step.step_id: s.conversion_rate for s in stages
                },
                drop_off_points=drop_off_points
            )

            self._funnel_results.append(result)
            return ActionResult(
                success=True,
                data={
                    "funnel_id": result.funnel_id,
                    "total_entrances": result.total_entrances,
                    "final_conversions": result.final_conversions,
                    "overall_conversion_rate": result.overall_conversion_rate,
                    "drop_off_rate": result.drop_off_rate,
                    "drop_off_points": [
                        {"step_id": step_id, "rate": rate}
                        for step_id, rate in result.drop_off_points
                    ]
                }
            )
        except Exception as e:
            logger.exception(f"Funnel analysis failed for {funnel_id}")
            return ActionResult(success=False, error=str(e))

    def _build_user_journeys(
        self,
        events: List[Dict[str, Any]],
        steps: List[FunnelStep]
    ) -> Dict[str, List[Tuple[str, datetime]]]:
        """Build user journey paths from events."""
        journeys: Dict[str, List[Tuple[str, datetime]]] = defaultdict(list)

        for event in events:
            user_id = event["user_id"]
            journeys[user_id].append((event["step_id"], event["timestamp"]))

        for user_id in journeys:
            journeys[user_id].sort(key=lambda x: x[1])

        return journeys

    def _calculate_stages(
        self,
        journeys: Dict[str, List[Tuple[str, datetime]]],
        steps: List[FunnelStep]
    ) -> List[FunnelStage]:
        """Calculate funnel stage metrics."""
        stage_map = {step.step_id: FunnelStage(step=step) for step in steps}

        for user_id, journey in journeys.items():
            entered_steps = set()
            for step_id, timestamp in journey:
                if step_id in stage_map:
                    entered_steps.add(step_id)
                    stage_map[step_id].entered_count += 1

            for i, (step_id, timestamp) in enumerate(journey):
                if step_id in stage_map:
                    if i == len(journey) - 1:
                        stage_map[step_id].exited_count += 1
                    else:
                        next_step_id = journey[i + 1][0]
                        if next_step_id in stage_map:
                            stage_map[step_id].exited_count += 1
                        else:
                            stage_map[step_id].dropped_count += 1

        for stage in stage_map.values():
            if stage.entered_count > 0:
                stage.conversion_rate = FunnelCalculator.calculate_conversion_rate(
                    stage.entered_count, stage.exited_count
                )

        return list(stage_map.values())

    def get_results(self) -> List[FunnelResult]:
        """Get all funnel analysis results."""
        return self._funnel_results.copy()
