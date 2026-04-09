"""Data Cohort Analysis Action Module.

Provides cohort analysis for user behavior segmentation including
retention curves, engagement patterns, lifetime value analysis,
and comparative cohort performance metrics.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class CohortPeriodType(Enum):
    """Time period types for cohort grouping."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class MetricType(Enum):
    """Types of cohort metrics."""
    RETENTION = "retention"
    REVENUE = "revenue"
    ENGAGEMENT = "engagement"
    CONVERSION = "conversion"


@dataclass
class CohortDefinition:
    """Definition of a user cohort."""
    cohort_id: str
    name: str
    start_date: datetime
    end_date: datetime
    criteria: Dict[str, Any] = field(default_factory=dict)
    user_ids: Set[str] = field(default_factory=set)


@dataclass
class CohortPeriodMetrics:
    """Metrics for a single cohort period."""
    period_index: int
    period_start: datetime
    active_users: int = 0
    retained_users: int = 0
    revenue: float = 0.0
    events_count: int = 0
    engagement_score: float = 0.0
    retention_rate: float = 0.0
    churn_rate: float = 0.0


@dataclass
class CohortAnalysisResult:
    """Complete cohort analysis result."""
    cohort_id: str
    cohort_name: str
    cohort_size: int = 0
    periods: List[CohortPeriodMetrics] = field(default_factory=list)
    avg_retention_rate: float = 0.0
    max_retention_period: int = 0
    lifetime_value: float = 0.0
    total_revenue: float = 0.0
    period_over_period_retention: List[float] = field(default_factory=list)


@dataclass
class CohortConfig:
    """Configuration for cohort analysis."""
    period_type: CohortPeriodType = CohortPeriodType.WEEKLY
    max_periods: int = 12
    min_cohort_size: int = 30
    retention_thresholds: Tuple[float, float, float] = (0.5, 0.25, 0.1)
    compute_lifetime_value: bool = True
    compute_engagement_scores: bool = True
    compare_cohorts: bool = True


class RetentionCalculator:
    """Calculate retention metrics for cohorts."""

    @staticmethod
    def calculate_retention_rate(retained: int, initial: int) -> float:
        """Calculate retention rate as percentage."""
        if initial == 0:
            return 0.0
        return (retained / initial) * 100.0

    @staticmethod
    def calculate_churn_rate(retained: int, previous: int) -> float:
        """Calculate churn rate between periods."""
        if previous == 0:
            return 0.0
        return ((previous - retained) / previous) * 100.0

    @staticmethod
    def calculate_retention_curve(
        retention_rates: List[float]
    ) -> Dict[str, float]:
        """Analyze retention curve shape."""
        if not retention_rates:
            return {}

        n = len(retention_rates)
        avg_retention = sum(retention_rates) / n

        first_half_avg = sum(retention_rates[:n//2]) / (n // 2 or 1)
        second_half_avg = sum(retention_rates[n//2:]) / ((n - n//2) or 1)

        curve_slope = (second_half_avg - first_half_avg) / (n / 2)

        return {
            "avg_retention": avg_retention,
            "initial_retention": retention_rates[0],
            "final_retention": retention_rates[-1],
            "max_drop": max(math.fabs(r - retention_rates[i-1])
                          for i, r in enumerate(retention_rates[1:], 1)),
            "curve_slope": curve_slope,
            "is_sticky": avg_retention > 40.0,
        }


class LifetimeValueCalculator:
    """Calculate customer lifetime value."""

    @staticmethod
    def calculate_ltv(
        cohort_data: List[CohortPeriodMetrics],
        avg_customer_acquisition_cost: float = 0.0,
        discount_rate: float = 0.1
    ) -> float:
        """Calculate discounted lifetime value."""
        if not cohort_data:
            return 0.0

        total_revenue = sum(p.revenue for p in cohort_data)

        if discount_rate <= 0:
            return total_revenue

        discounted_revenue = 0.0
        for i, period in enumerate(cohort_data):
            discount_factor = 1.0 / ((1 + discount_rate) ** i)
            discounted_revenue += period.revenue * discount_factor

        ltv = discounted_revenue - avg_customer_acquisition_cost
        return max(0.0, ltv)

    @staticmethod
    def calculate_ltv_cohort_comparison(
        cohort_results: List[CohortAnalysisResult]
    ) -> Dict[str, Any]:
        """Compare LTV across cohorts."""
        if not cohort_results:
            return {}

        ltvs = [(c.cohort_id, c.lifetime_value, c.cohort_size) for c in cohort_results]

        best_cohort_id = max(ltvs, key=lambda x: x[1])[0]
        avg_ltv = sum(c.lifetime_value for c in cohort_results) / len(cohort_results)

        return {
            "avg_ltv": avg_ltv,
            "best_cohort_id": best_cohort_id,
            "ltv_variance": sum((c.lifetime_value - avg_ltv) ** 2 for c in cohort_results) / len(cohort_results),
        }


class DataCohortAnalysisAction(BaseAction):
    """Action for cohort-based user analysis."""

    def __init__(self):
        super().__init__(name="data_cohort_analysis")
        self._config = CohortConfig()
        self._cohorts: Dict[str, CohortDefinition] = {}
        self._event_data: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._results: List[CohortAnalysisResult] = []

    def configure(self, config: CohortConfig):
        """Configure cohort analysis settings."""
        self._config = config

    def define_cohort(
        self,
        cohort_id: str,
        name: str,
        start_date: datetime,
        end_date: datetime,
        user_ids: Set[str],
        criteria: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Define a new user cohort."""
        try:
            if cohort_id in self._cohorts:
                return ActionResult(success=False, error=f"Cohort {cohort_id} already exists")

            if len(user_ids) < self._config.min_cohort_size:
                logger.warning(
                    f"Cohort {cohort_id} has {len(user_ids)} users, "
                    f"minimum is {self._config.min_cohort_size}"
                )

            cohort = CohortDefinition(
                cohort_id=cohort_id,
                name=name,
                start_date=start_date,
                end_date=end_date,
                criteria=criteria or {},
                user_ids=user_ids
            )
            self._cohorts[cohort_id] = cohort
            return ActionResult(success=True, data={"cohort_id": cohort_id, "size": len(user_ids)})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def record_event(
        self,
        user_id: str,
        event_name: str,
        event_data: Dict[str, Any],
        timestamp: Optional[datetime] = None
    ) -> ActionResult:
        """Record a user event for cohort analysis."""
        try:
            event = {
                "user_id": user_id,
                "event_name": event_name,
                "event_data": event_data,
                "timestamp": timestamp or datetime.now()
            }
            self._event_data[user_id].append(event)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def analyze_cohort(self, cohort_id: str) -> ActionResult:
        """Analyze a specific cohort."""
        try:
            if cohort_id not in self._cohorts:
                return ActionResult(success=False, error=f"Cohort {cohort_id} not found")

            cohort = self._cohorts[cohort_id]
            periods = self._generate_periods(cohort)

            period_metrics = []
            for period in periods:
                metrics = self._calculate_period_metrics(cohort, period)
                period_metrics.append(metrics)

            retention_rates = [p.retention_rate for p in period_metrics]
            retention_analysis = RetentionCalculator.calculate_retention_curve(retention_rates)

            total_revenue = sum(p.revenue for p in period_metrics)
            avg_retention = sum(retention_rates) / len(retention_rates) if retention_rates else 0.0

            ltv = 0.0
            if self._config.compute_lifetime_value:
                ltv = LifetimeValueCalculator.calculate_ltv(period_metrics)

            result = CohortAnalysisResult(
                cohort_id=cohort_id,
                cohort_name=cohort.name,
                cohort_size=len(cohort.user_ids),
                periods=period_metrics,
                avg_retention_rate=avg_retention,
                max_retention_period=len([r for r in retention_rates if r > 0]),
                lifetime_value=ltv,
                total_revenue=total_revenue,
                period_over_period_retention=retention_rates
            )

            self._results.append(result)
            return ActionResult(
                success=True,
                data={
                    "cohort_id": cohort_id,
                    "cohort_size": result.cohort_size,
                    "avg_retention_rate": result.avg_retention_rate,
                    "max_retention_period": result.max_retention_period,
                    "lifetime_value": result.lifetime_value,
                    "total_revenue": result.total_revenue,
                    "retention_curve": retention_analysis
                }
            )
        except Exception as e:
            logger.exception(f"Cohort analysis failed for {cohort_id}")
            return ActionResult(success=False, error=str(e))

    def _generate_periods(self, cohort: CohortDefinition) -> List[Tuple[datetime, datetime]]:
        """Generate time periods for analysis."""
        periods = []
        current = cohort.start_date

        delta_map = {
            CohortPeriodType.DAILY: timedelta(days=1),
            CohortPeriodType.WEEKLY: timedelta(weeks=1),
            CohortPeriodType.MONTHLY: timedelta(days=30),
        }

        delta = delta_map[self._config.period_type]

        for _ in range(self._config.max_periods):
            period_end = current + delta
            if period_end > datetime.now():
                period_end = datetime.now()
            periods.append((current, period_end))
            current = period_end

            if current >= datetime.now():
                break

        return periods

    def _calculate_period_metrics(
        self,
        cohort: CohortDefinition,
        period: Tuple[datetime, datetime]
    ) -> CohortPeriodMetrics:
        """Calculate metrics for a specific period."""
        period_start, period_end = period
        period_index = self._get_period_index(cohort.start_date, period_start)

        active_user_ids = set()
        revenue = 0.0
        events_count = 0
        engagement_sum = 0.0

        for user_id in cohort.user_ids:
            user_events = self._event_data.get(user_id, [])
            period_events = [
                e for e in user_events
                if period_start <= e["timestamp"] <= period_end
            ]

            if period_events:
                active_user_ids.add(user_id)
                events_count += len(period_events)

                for event in period_events:
                    revenue += event["event_data"].get("revenue", 0.0)
                    engagement_sum += event["event_data"].get("engagement", 1.0)

        retained = len(active_user_ids)
        initial = len(cohort.user_ids)
        retention_rate = RetentionCalculator.calculate_retention_rate(retained, initial)

        return CohortPeriodMetrics(
            period_index=period_index,
            period_start=period_start,
            active_users=len(active_user_ids),
            retained_users=retained,
            revenue=revenue,
            events_count=events_count,
            engagement_score=engagement_sum / events_count if events_count > 0 else 0.0,
            retention_rate=retention_rate,
            churn_rate=RetentionCalculator.calculate_churn_rate(retained, initial)
        )

    def _get_period_index(self, cohort_start: datetime, period_start: datetime) -> int:
        """Calculate period index from cohort start."""
        if self._config.period_type == CohortPeriodType.DAILY:
            delta = (period_start - cohort_start).days
        elif self._config.period_type == CohortPeriodType.WEEKLY:
            delta = (period_start - cohort_start).days // 7
        else:
            delta = (period_start - cohort_start).days // 30
        return max(0, delta)

    def compare_cohorts(self, cohort_ids: List[str]) -> ActionResult:
        """Compare multiple cohorts side by side."""
        try:
            cohort_results = []
            for cid in cohort_ids:
                result = next((r for r in self._results if r.cohort_id == cid), None)
                if not result:
                    result_data = self.analyze_cohort(cid)
                    if not result_data.success:
                        continue

            results = [r for r in self._results if r.cohort_id in cohort_ids]
            if not results:
                return ActionResult(success=False, error="No cohort results to compare")

            ltv_comparison = LifetimeValueCalculator.calculate_ltv_cohort_comparison(results)

            comparison = {
                "cohorts": [
                    {
                        "cohort_id": r.cohort_id,
                        "cohort_size": r.cohort_size,
                        "avg_retention": r.avg_retention_rate,
                        "ltv": r.lifetime_value,
                        "total_revenue": r.total_revenue
                    }
                    for r in results
                ],
                "ltv_comparison": ltv_comparison
            }

            return ActionResult(success=True, data=comparison)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_results(self) -> List[CohortAnalysisResult]:
        """Get all cohort analysis results."""
        return self._results.copy()
