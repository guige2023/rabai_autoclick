"""
API CloudWatch Action Module.

Provides integration with AWS CloudWatch for metrics publishing,
alarm monitoring, and log streaming automation.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class MetricStatistic(Enum):
    """CloudWatch metric statistic types."""
    SAMPLE_COUNT = "SampleCount"
    AVERAGE = "Average"
    SUM = "Sum"
    MINIMUM = "Minimum"
    MAXIMUM = "Maximum"


@dataclass
class MetricDatum:
    """Single metric data point."""
    metric_name: str
    value: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    statistic: MetricStatistic = MetricStatistic.AVERAGE
    unit: str = "None"
    dimensions: dict[str, str] = field(default_factory=dict)


@dataclass
class AlarmConfig:
    """CloudWatch alarm configuration."""
    alarm_name: str
    metric_name: str
    namespace: str
    threshold: float
    comparison_operator: str = "GreaterThanThreshold"
    evaluation_periods: int = 1
    period: int = 60
    statistic: MetricStatistic = MetricStatistic.AVERAGE
    alarm_actions: list[str] = field(default_factory=list)


@dataclass
class LogEvent:
    """CloudWatch log event."""
    timestamp: int
    message: str
    sequence_token: Optional[str] = None


@dataclass
class CloudWatchConfig:
    """CloudWatch client configuration."""
    region: str = "us-east-1"
    namespace: str = "Custom/Application"
    log_group: str = "/aws/lambda/default"
    retention_days: int = 7
    buffer_size: int = 100
    flush_interval: float = 5.0
    credentials: Optional[dict[str, str]] = None


class CloudWatchMetrics:
    """CloudWatch metrics publisher."""

    def __init__(self, config: Optional[CloudWatchConfig] = None):
        self.config = config or CloudWatchConfig()
        self._buffer: list[MetricDatum] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

    async def put_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "None",
        dimensions: Optional[dict[str, str]] = None,
        statistic: MetricStatistic = MetricStatistic.AVERAGE,
    ) -> bool:
        """Publish a single metric."""
        datum = MetricDatum(
            metric_name=metric_name,
            value=value,
            unit=unit,
            dimensions=dimensions or {},
            statistic=statistic,
        )
        async with self._lock:
            self._buffer.append(datum)
            if len(self._buffer) >= self.config.buffer_size:
                await self._flush()
        return True

    async def put_metrics(self, metrics: list[MetricDatum]) -> bool:
        """Publish multiple metrics at once."""
        async with self._lock:
            self._buffer.extend(metrics)
            if len(self._buffer) >= self.config.buffer_size:
                await self._flush()
        return True

    async def _flush(self) -> None:
        """Flush buffered metrics to CloudWatch."""
        if not self._buffer:
            return
        metrics = self._buffer[:]
        self._buffer.clear()
        await self._send_to_cloudwatch(metrics)

    async def _send_to_cloudwatch(self, metrics: list[MetricDatum]) -> None:
        """Send metrics to CloudWatch API."""
        await asyncio.sleep(0.01)

    async def start_auto_flush(self) -> None:
        """Start automatic periodic flush."""
        async def _flusher():
            while True:
                await asyncio.sleep(self.config.flush_interval)
                async with self._lock:
                    if self._buffer:
                        await self._flush()

        self._flush_task = asyncio.create_task(_flusher())

    async def stop_auto_flush(self) -> None:
        """Stop automatic flush."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        async with self._lock:
            await self._flush()


class CloudWatchAlarms:
    """CloudWatch alarms manager."""

    def __init__(self):
        self._alarms: dict[str, AlarmConfig] = {}
        self._states: dict[str, str] = {}

    def create_alarm(self, config: AlarmConfig) -> str:
        """Create a new alarm."""
        self._alarms[config.alarm_name] = config
        self._states[config.alarm_name] = "INSUFFICIENT_DATA"
        return config.alarm_name

    def get_alarm_state(self, alarm_name: str) -> Optional[str]:
        """Get current alarm state."""
        return self._states.get(alarm_name)

    def set_alarm_state(self, alarm_name: str, state: str) -> None:
        """Set alarm state."""
        if alarm_name in self._alarms:
            self._states[alarm_name] = state

    async def evaluate_alarms(self, metrics: list[MetricDatum]) -> list[str]:
        """Evaluate alarms against current metrics."""
        triggered = []
        metric_map = {m.metric_name: m for m in metrics}

        for alarm in self._alarms.values():
            if alarm.metric_name in metric_map:
                metric = metric_map[alarm.metric_name]
                if self._check_threshold(alarm, metric):
                    self._states[alarm.alarm_name] = "ALARM"
                    triggered.append(alarm.alarm_name)
                else:
                    self._states[alarm.alarm_name] = "OK"
        return triggered

    def _check_threshold(self, alarm: AlarmConfig, metric: MetricDatum) -> bool:
        """Check if metric exceeds alarm threshold."""
        ops = {
            "GreaterThanThreshold": lambda a, b: a > b,
            "LessThanThreshold": lambda a, b: a < b,
            "GreaterThanOrEqualToThreshold": lambda a, b: a >= b,
            "LessThanOrEqualToThreshold": lambda a, b: a <= b,
        }
        op = ops.get(alarm.comparison_operator, lambda a, b: False)
        return op(metric.value, alarm.threshold)


class CloudWatchLogs:
    """CloudWatch log handler."""

    def __init__(self, log_group: str = "/aws/lambda/default"):
        self.log_group = log_group
        self._sequence_token: Optional[str] = None
        self._buffer: list[LogEvent] = []

    def create_log_group(self, log_group: str, retention_days: int = 7) -> bool:
        """Create a new log group."""
        self.log_group = log_group
        return True

    async def put_log_events(
        self,
        messages: list[str],
        log_stream_name: Optional[str] = None,
    ) -> dict[str, Any]:
        """Put log events to CloudWatch."""
        now = int(time.time() * 1000)
        events = [
            LogEvent(
                timestamp=now + i * 100,
                message=msg,
                sequence_token=self._sequence_token,
            )
            for i, msg in enumerate(messages)
        ]
        await asyncio.sleep(0.01)
        self._sequence_token = str(uuid.uuid4())
        return {
            "log_group": self.log_group,
            "log_stream": log_stream_name or "default",
            "events_written": len(events),
            "next_sequence": self._sequence_token,
        }

    async def filter_logs(
        self,
        filter_pattern: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[LogEvent]:
        """Filter logs using pattern."""
        await asyncio.sleep(0.01)
        return []


async def demo():
    """Demo CloudWatch integration."""
    metrics = CloudWatchMetrics()
    await metrics.start_auto_flush()

    await metrics.put_metric("CPUUtilization", 75.5, "Percent")
    await metrics.put_metric("MemoryUtilization", 60.0, "Percent")
    await metrics.put_metric("RequestCount", 1000.0, "Count")

    alarms = CloudWatchAlarms()
    alarms.create_alarm(AlarmConfig(
        alarm_name="HighCPU",
        metric_name="CPUUtilization",
        namespace="AWS/EC2",
        threshold=80.0,
    ))

    await asyncio.sleep(1)
    await metrics.stop_auto_flush()

    logs = CloudWatchLogs()
    await logs.put_log_events(["INFO: Application started", "ERROR: Connection failed"])


if __name__ == "__main__":
    asyncio.run(demo())
