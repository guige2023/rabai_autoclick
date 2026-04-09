"""
Funnel analysis module for conversion tracking.

Provides funnel construction, conversion rate computation,
and drop-off analysis for business analytics workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class FunnelStage:
    """Represents a single stage in a funnel."""
    name: str
    entry_count: int
    exit_count: int
    conversion_rate: float
    drop_off_count: int
    drop_off_rate: float


@dataclass
class FunnelResult:
    """Complete funnel analysis result."""
    stages: list[FunnelStage]
    total_entries: int
    final_completions: int
    overall_conversion_rate: float
    total_drop_offs: int
    largest_drop_off_stage: str
    average_conversion_time: Optional[float]


class FunnelAnalyzer:
    """
    Analyzes conversion funnels with stage-by-stage metrics.
    
    Example:
        analyzer = FunnelAnalyzer()
        stages = ['view', 'click', 'add_cart', 'purchase']
        counts = [1000, 400, 150, 80]
        result = analyzer.analyze(stages, counts)
    """

    def __init__(self) -> None:
        """Initialize funnel analyzer."""
        self._last_result: Optional[FunnelResult] = None

    def analyze(
        self,
        stage_names: list[str],
        stage_counts: list[int],
        stage_times: Optional[list[float]] = None
    ) -> FunnelResult:
        """
        Analyze a conversion funnel.
        
        Args:
            stage_names: Names of funnel stages in order.
            stage_counts: Number of users/units at each stage.
            stage_times: Optional time spent at each stage (for avg conversion time).
            
        Returns:
            FunnelResult with detailed funnel metrics.
            
        Raises:
            ValueError: If stage_names and stage_counts have different lengths.
        """
        if len(stage_names) != len(stage_counts):
            raise ValueError("stage_names and stage_counts must have same length")
        if not stage_names:
            raise ValueError("Funnel must have at least one stage")

        stages = []
        total_entries = stage_counts[0]
        final_completions = stage_counts[-1]
        largest_drop_off = 0
        largest_drop_stage = stage_names[0]

        for i, (name, count) in enumerate(zip(stage_names, stage_counts)):
            if i == 0:
                entry_count = count
                exit_count = stage_counts[i + 1] if i + 1 < len(stage_counts) else count
            else:
                entry_count = stage_counts[i - 1]
                exit_count = count

            drop_off = entry_count - exit_count
            conversion_rate = (exit_count / entry_count * 100) if entry_count > 0 else 0.0
            drop_off_rate = (drop_off / entry_count * 100) if entry_count > 0 else 0.0

            if drop_off > largest_drop_off:
                largest_drop_off = drop_off
                largest_drop_stage = name

            stages.append(FunnelStage(
                name=name,
                entry_count=entry_count,
                exit_count=exit_count,
                conversion_rate=round(conversion_rate, 2),
                drop_off_count=drop_off,
                drop_off_rate=round(drop_off_rate, 2)
            ))

        overall_rate = (final_completions / total_entries * 100) if total_entries > 0 else 0.0
        total_drops = total_entries - final_completions

        avg_time = None
        if stage_times:
            avg_time = sum(stage_times) / len(stage_times) if stage_times else None

        result = FunnelResult(
            stages=stages,
            total_entries=total_entries,
            final_completions=final_completions,
            overall_conversion_rate=round(overall_rate, 2),
            total_drop_offs=total_drops,
            largest_drop_off_stage=largest_drop_stage,
            average_conversion_time=round(avg_time, 2) if avg_time else None
        )

        self._last_result = result
        return result

    def compare_funnels(
        self,
        funnel_a: FunnelResult,
        funnel_b: FunnelResult
    ) -> dict[str, any]:
        """
        Compare two funnels side by side.
        
        Args:
            funnel_a: First funnel result.
            funnel_b: Second funnel result.
            
        Returns:
            Dictionary with comparison metrics.
        """
        if len(funnel_a.stages) != len(funnel_b.stages):
            return {'error': 'Funnels have different number of stages'}

        comparisons = {
            'overall_conversion_diff': round(
                funnel_a.overall_conversion_rate - funnel_b.overall_conversion_rate, 2
            ),
            'stage_comparisons': []
        }

        for s_a, s_b in zip(funnel_a.stages, funnel_b.stages):
            comparisons['stage_comparisons'].append({
                'stage': s_a.name,
                'conversion_diff': round(s_a.conversion_rate - s_b.conversion_rate, 2),
                'drop_off_diff': round(s_a.drop_off_rate - s_b.drop_off_rate, 2),
            })

        return comparisons

    def to_ascii_chart(self, result: FunnelResult, width: int = 60) -> str:
        """
        Render funnel as ASCII art.
        
        Args:
            result: Funnel analysis result.
            width: Maximum width of the chart.
            
        Returns:
            ASCII representation of the funnel.
        """
        if not result.stages:
            return "Empty funnel"

        max_count = max(s.entry_count for s in result.stages)
        lines = [f"Funnel Analysis: {result.total_entries} entries → {result.final_completions} completions",
                 f"Overall Conversion: {result.overall_conversion_rate}%",
                 f"Largest Drop-off: {result.largest_drop_off_stage}",
                 "=" * width]

        for stage in result.stages:
            bar_width = int((stage.entry_count / max_count) * (width - 30)) if max_count > 0 else 0
            bar = '█' * bar_width
            lines.append(
                f"{stage.name:<15} {stage.entry_count:>8,} "
                f"| {bar} {stage.conversion_rate:>5.1f}%"
            )

        return "\n".join(lines)
