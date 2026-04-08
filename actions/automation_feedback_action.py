"""
Automation Feedback Action Module.

Provides feedback collection, processing, and analysis
for continuous improvement of automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import logging
import uuid
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)


class FeedbackType(Enum):
    """Types of feedback."""
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    IMPROVEMENT = "improvement"
    METRIC = "metric"
    CUSTOM = "custom"


class FeedbackSeverity(Enum):
    """Severity of feedback."""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class FeedbackItem:
    """Single feedback item."""
    feedback_id: str
    feedback_type: FeedbackType
    source: str
    content: str
    severity: FeedbackSeverity = FeedbackSeverity.INFO
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    parent_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeedbackSummary:
    """Summary of feedback analysis."""
    total_feedback: int
    by_type: Dict[str, int]
    by_severity: Dict[str, int]
    by_source: Dict[str, int]
    trend: str
    top_issues: List[Tuple[str, int]]
    recommendations: List[str]


@dataclass
class ImprovementAction:
    """Action to improve based on feedback."""
    action_id: str
    description: str
    priority: int
    target_component: str
    expected_impact: str
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None


class FeedbackCollector:
    """Collects feedback from various sources."""

    def __init__(self):
        self.feedback_items: Dict[str, FeedbackItem] = {}
        self._handlers: Dict[FeedbackType, List[Callable]] = defaultdict(list)
        self._filters: List[Callable] = []

    def add_feedback(
        self,
        feedback_type: FeedbackType,
        source: str,
        content: str,
        severity: FeedbackSeverity = FeedbackSeverity.INFO,
        context: Optional[Dict[str, Any]] = None,
        tags: Optional[Set[str]] = None,
        parent_id: Optional[str] = None
    ) -> FeedbackItem:
        """Add new feedback item."""
        feedback_id = str(uuid.uuid4())

        feedback = FeedbackItem(
            feedback_id=feedback_id,
            feedback_type=feedback_type,
            source=source,
            content=content,
            severity=severity,
            context=context or {},
            tags=tags or set(),
            parent_id=parent_id
        )

        for feedback_filter in self._filters:
            if not feedback_filter(feedback):
                logger.debug(f"Feedback filtered out: {feedback_id}")
                return feedback

        self.feedback_items[feedback_id] = feedback

        for handler in self._handlers.get(feedback_type, []):
            try:
                handler(feedback)
            except Exception as e:
                logger.error(f"Feedback handler error: {e}")

        return feedback

    def add_handler(self, feedback_type: FeedbackType, handler: Callable):
        """Add handler for feedback type."""
        self._handlers[feedback_type].append(handler)

    def add_filter(self, feedback_filter: Callable):
        """Add filter for feedback items."""
        self._filters.append(feedback_filter)

    def get_feedback(
        self,
        feedback_id: str
    ) -> Optional[FeedbackItem]:
        """Get feedback by ID."""
        return self.feedback_items.get(feedback_id)

    def get_feedback_list(
        self,
        feedback_type: Optional[FeedbackType] = None,
        source: Optional[str] = None,
        severity: Optional[FeedbackSeverity] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[FeedbackItem]:
        """Get filtered list of feedback."""
        items = self.feedback_items.values()

        if feedback_type:
            items = [i for i in items if i.feedback_type == feedback_type]
        if source:
            items = [i for i in items if i.source == source]
        if severity:
            items = [i for i in items if i.severity == severity]
        if since:
            items = [i for i in items if i.timestamp >= since]

        items = sorted(items, key=lambda i: i.timestamp, reverse=True)
        return items[:limit]

    def get_related_feedback(
        self,
        feedback_id: str
    ) -> List[FeedbackItem]:
        """Get feedback related to a parent."""
        feedback = self.feedback_items.get(feedback_id)
        if not feedback:
            return []

        return [
            item for item in self.feedback_items.values()
            if item.parent_id == feedback_id
        ]

    def delete_feedback(self, feedback_id: str) -> bool:
        """Delete feedback item."""
        if feedback_id in self.feedback_items:
            del self.feedback_items[feedback_id]
            return True
        return False


class FeedbackAnalyzer:
    """Analyzes feedback for patterns and insights."""

    def __init__(self, collector: FeedbackCollector):
        self.collector = collector
        self._anomaly_threshold: float = 2.0

    def analyze(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> FeedbackSummary:
        """Analyze feedback and generate summary."""
        items = self.collector.get_feedback_list(limit=10000)

        if since:
            items = [i for i in items if i.timestamp >= since]
        if until:
            items = [i for i in items if i.timestamp <= until]

        by_type = Counter(i.feedback_type.value for i in items)
        by_severity = Counter(i.severity.value for i in items)
        by_source = Counter(i.source for i in items)

        top_issues = self._identify_top_issues(items)

        trend = self._calculate_trend(items)

        recommendations = self._generate_recommendations(items)

        return FeedbackSummary(
            total_feedback=len(items),
            by_type=dict(by_type),
            by_severity=dict(by_severity),
            by_source=dict(by_source),
            trend=trend,
            top_issues=top_issues,
            recommendations=recommendations
        )

    def _identify_top_issues(
        self,
        items: List[FeedbackItem]
    ) -> List[Tuple[str, int]]:
        """Identify most common issues."""
        issues = [
            item.content for item in items
            if item.feedback_type == FeedbackType.FAILURE
        ]

        counter = Counter(issues)
        return counter.most_common(5)

    def _calculate_trend(
        self,
        items: List[FeedbackItem]
    ) -> str:
        """Calculate feedback trend."""
        if not items:
            return "no_data"

        now = datetime.now()
        recent = [i for i in items if i.timestamp >= now - timedelta(hours=1)]
        older = [i for i in items if now - timedelta(hours=2) <= i.timestamp < now - timedelta(hours=1)]

        if not older:
            return "insufficient_data"

        recent_failure_rate = sum(
            1 for i in recent if i.feedback_type == FeedbackType.FAILURE
        ) / max(len(recent), 1)

        older_failure_rate = sum(
            1 for i in older if i.feedback_type == FeedbackType.FAILURE
        ) / max(len(older), 1)

        if recent_failure_rate > older_failure_rate * 1.2:
            return "deteriorating"
        elif recent_failure_rate < older_failure_rate * 0.8:
            return "improving"
        return "stable"

    def _generate_recommendations(
        self,
        items: List[FeedbackItem]
    ) -> List[str]:
        """Generate improvement recommendations."""
        recommendations = []

        failures = [i for i in items if i.feedback_type == FeedbackType.FAILURE]
        if len(failures) > 10:
            recommendations.append("High failure rate detected - consider adding more robust error handling")

        warnings = [i for i in items if i.feedback_type == FeedbackType.WARNING]
        if len(warnings) > 5:
            recommendations.append("Multiple warnings observed - review warning thresholds")

        by_source = Counter(i.source for i in items)
        high_source = max(by_source.items(), key=lambda x: x[1], default=(None, 0))
        if high_source[1] > len(items) * 0.5:
            recommendations.append(f"Source '{high_source[0]}' generates most feedback - investigate")

        return recommendations

    def detect_anomalies(
        self,
        metric_name: str
    ) -> List[FeedbackItem]:
        """Detect anomalous feedback patterns."""
        metric_items = [
            item for item in self.collector.get_feedback_list()
            if item.feedback_type == FeedbackType.METRIC
            and item.context.get("metric_name") == metric_name
        ]

        if len(metric_items) < 3:
            return []

        values = [item.context.get("value", 0) for item in metric_items]
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = variance ** 0.5

        anomalies = []
        for item in metric_items:
            value = item.context.get("value", 0)
            if abs(value - mean) > self._anomaly_threshold * std:
                anomalies.append(item)

        return anomalies


class ImprovementTracker:
    """Tracks improvement actions based on feedback."""

    def __init__(self):
        self.actions: Dict[str, ImprovementAction] = {}

    def create_action(
        self,
        description: str,
        priority: int,
        target_component: str,
        expected_impact: str
    ) -> ImprovementAction:
        """Create new improvement action."""
        action_id = str(uuid.uuid4())

        action = ImprovementAction(
            action_id=action_id,
            description=description,
            priority=priority,
            target_component=target_component,
            expected_impact=expected_impact
        )

        self.actions[action_id] = action
        return action

    def complete_action(self, action_id: str):
        """Mark action as completed."""
        if action_id in self.actions:
            self.actions[action_id].status = "completed"
            self.actions[action_id].completed_at = datetime.now()

    def get_pending_actions(
        self,
        target_component: Optional[str] = None
    ) -> List[ImprovementAction]:
        """Get pending improvement actions."""
        actions = [
            a for a in self.actions.values()
            if a.status == "pending"
        ]

        if target_component:
            actions = [
                a for a in actions
                if a.target_component == target_component
            ]

        return sorted(actions, key=lambda a: a.priority, reverse=True)

    def get_action_stats(self) -> Dict[str, int]:
        """Get action statistics."""
        return {
            "total": len(self.actions),
            "pending": sum(1 for a in self.actions.values() if a.status == "pending"),
            "completed": sum(1 for a in self.actions.values() if a.status == "completed")
        }


class FeedbackLoop:
    """Complete feedback loop orchestrator."""

    def __init__(self):
        self.collector = FeedbackCollector()
        self.analyzer = FeedbackAnalyzer(self.collector)
        self.improvement_tracker = ImprovementTracker()
        self._automation_handlers: Dict[str, Callable] = {}

    def register_automation_handler(
        self,
        component: str,
        handler: Callable
    ):
        """Register handler for automated improvements."""
        self._automation_handlers[component] = handler

    def collect_success(
        self,
        source: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Collect success feedback."""
        self.collector.add_feedback(
            feedback_type=FeedbackType.SUCCESS,
            source=source,
            content="Operation completed successfully",
            severity=FeedbackSeverity.INFO,
            context=context
        )

    def collect_failure(
        self,
        source: str,
        error: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Collect failure feedback."""
        self.collector.add_feedback(
            feedback_type=FeedbackType.FAILURE,
            source=source,
            content=error,
            severity=FeedbackSeverity.HIGH,
            context=context
        )

    def analyze_and_improve(self) -> List[ImprovementAction]:
        """Analyze feedback and create improvement actions."""
        summary = self.analyzer.analyze()

        new_actions = []

        for issue, count in summary.top_issues[:3]:
            if count >= 3:
                action = self.improvement_tracker.create_action(
                    description=f"Address recurring issue: {issue[:50]}",
                    priority=count,
                    target_component="system",
                    expected_impact="Reduce failure rate"
                )
                new_actions.append(action)

        for recommendation in summary.recommendations:
            action = self.improvement_tracker.create_action(
                description=recommendation,
                priority=5,
                target_component="general",
                expected_impact="Improve system health"
            )
            new_actions.append(action)

        return new_actions

    def get_dashboard(self) -> Dict[str, Any]:
        """Get feedback dashboard data."""
        summary = self.analyzer.analyze()
        pending = self.improvement_tracker.get_pending_actions()

        return {
            "summary": {
                "total_feedback": summary.total_feedback,
                "trend": summary.trend,
                "top_issues": summary.top_issues
            },
            "improvements": {
                "pending_count": len(pending),
                "stats": self.improvement_tracker.get_action_stats()
            },
            "recent_feedback": [
                {
                    "id": f.feedback_id,
                    "type": f.feedback_type.value,
                    "content": f.content[:100],
                    "timestamp": f.timestamp.isoformat()
                }
                for f in self.collector.get_feedback_list(limit=10)
            ]
        }


async def main():
    """Demonstrate feedback system."""
    feedback_loop = FeedbackLoop()

    feedback_loop.collect_success("api_gateway", {"endpoint": "/users"})
    feedback_loop.collect_failure("api_gateway", "Connection timeout", {"endpoint": "/orders"})

    summary = feedback_loop.analyzer.analyze()
    print(f"Total feedback: {summary.total_feedback}")
    print(f"Trend: {summary.trend}")

    actions = feedback_loop.analyze_and_improve()
    print(f"Improvement actions: {len(actions)}")

    dashboard = feedback_loop.get_dashboard()
    print(f"Dashboard: {dashboard}")


if __name__ == "__main__":
    asyncio.run(main())
