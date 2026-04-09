"""
Reporter Action Module.

Provides reporting and aggregation of action results
with multiple output formats.
"""

import time
import json
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import deque


class ReportFormat(Enum):
    """Output format for reports."""
    TEXT = "text"
    JSON = "json"
    CSV = "csv"
    HTML = "html"
    MARKDOWN = "markdown"


@dataclass
class ReportEntry:
    """Single entry in a report."""
    timestamp: float
    name: str
    status: str
    duration: float
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReportSummary:
    """Summary statistics for a report."""
    total_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    min_duration: float = float("inf")
    max_duration: float = 0.0
    success_rate: float = 0.0


class ReportBuffer:
    """Buffer for collecting report entries."""

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._entries: deque = deque(maxlen=max_size)
        self._lock = threading.RLock()

    def add(self, entry: ReportEntry) -> None:
        with self._lock:
            self._entries.append(entry)

    def get_all(self) -> List[ReportEntry]:
        with self._lock:
            return list(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._entries)


class ReporterAction:
    """
    Action for generating reports on executed operations.

    Example:
        reporter = ReporterAction("task_reporter")
        reporter.record("task1", "success", 1.5)
        reporter.record("task2", "failure", 0.5, message="Error occurred")
        print(reporter.generate_text())
    """

    def __init__(
        self,
        name: str,
        max_entries: int = 10000,
    ):
        self.name = name
        self._buffer = ReportBuffer(max_size=max_entries)
        self._lock = threading.RLock()
        self._filters: List[Callable] = []

    def add_filter(self, filter_fn: Callable[[ReportEntry], bool]) -> None:
        """Add a filter function for entries."""
        self._filters.append(filter_fn)

    def record(
        self,
        name: str,
        status: str,
        duration: float,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ReportEntry:
        """Record a report entry."""
        entry = ReportEntry(
            timestamp=time.time(),
            name=name,
            status=status,
            duration=duration,
            message=message,
            metadata=metadata or {},
        )

        for filter_fn in self._filters:
            if not filter_fn(entry):
                return entry

        self._buffer.add(entry)
        return entry

    def record_success(
        self,
        name: str,
        duration: float,
        message: str = "",
        **metadata,
    ) -> ReportEntry:
        """Record a successful operation."""
        return self.record(name, "success", duration, message, metadata)

    def record_failure(
        self,
        name: str,
        duration: float,
        message: str = "",
        **metadata,
    ) -> ReportEntry:
        """Record a failed operation."""
        return self.record(name, "failure", duration, message, metadata)

    def _compute_summary(self, entries: List[ReportEntry]) -> ReportSummary:
        """Compute summary statistics from entries."""
        summary = ReportSummary()

        for entry in entries:
            summary.total_count += 1
            summary.total_duration += entry.duration

            if entry.status == "success":
                summary.success_count += 1
            elif entry.status == "failure":
                summary.failure_count += 1

            summary.min_duration = min(summary.min_duration, entry.duration)
            summary.max_duration = max(summary.max_duration, entry.duration)

        if summary.total_count > 0:
            summary.avg_duration = summary.total_duration / summary.total_count
            summary.success_rate = (
                summary.success_count / summary.total_count * 100
            )

        if summary.min_duration == float("inf"):
            summary.min_duration = 0.0

        return summary

    def generate_summary(self) -> ReportSummary:
        """Generate summary statistics."""
        entries = self._buffer.get_all()
        return self._compute_summary(entries)

    def generate_json(
        self,
        include_entries: bool = True,
        indent: int = 2,
    ) -> str:
        """Generate report in JSON format."""
        entries = self._buffer.get_all()
        summary = self._compute_summary(entries)

        data = {
            "name": self.name,
            "generated_at": time.time(),
            "summary": asdict(summary),
        }

        if include_entries:
            data["entries"] = [
                {
                    "timestamp": e.timestamp,
                    "name": e.name,
                    "status": e.status,
                    "duration": e.duration,
                    "message": e.message,
                    "metadata": e.metadata,
                }
                for e in entries
            ]

        return json.dumps(data, indent=indent, default=str)

    def generate_text(self, max_entries: Optional[int] = None) -> str:
        """Generate report in plain text format."""
        entries = self._buffer.get_all()
        summary = self._compute_summary(entries)

        lines = [
            f"=== Report: {self.name} ===",
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "--- Summary ---",
            f"Total:    {summary.total_count}",
            f"Success:  {summary.success_count}",
            f"Failed:   {summary.failure_count}",
            f"Duration: {summary.total_duration:.3f}s",
            f"Avg:      {summary.avg_duration:.3f}s",
            f"Rate:     {summary.success_rate:.1f}%",
            "",
            "--- Entries ---",
        ]

        display_entries = (
            entries[-max_entries:] if max_entries else entries
        )

        for entry in display_entries:
            status_icon = "✓" if entry.status == "success" else "✗"
            time_str = time.strftime(
                "%H:%M:%S", time.localtime(entry.timestamp)
            )
            lines.append(
                f"{status_icon} [{time_str}] {entry.name}: "
                f"{entry.duration:.3f}s - {entry.message}"
            )

        return "\n".join(lines)

    def generate_csv(self) -> str:
        """Generate report in CSV format."""
        entries = self._buffer.get_all()

        lines = [
            "timestamp,name,status,duration,message,metadata",
        ]

        for entry in entries:
            metadata = json.dumps(entry.metadata, default=str)
            lines.append(
                f"{entry.timestamp},{entry.name},{entry.status},"
                f"{entry.duration},{entry.message},{metadata}"
            )

        return "\n".join(lines)

    def generate_html(self) -> str:
        """Generate report in HTML format."""
        entries = self._buffer.get_all()
        summary = self._compute_summary(entries)

        rows = []
        for entry in entries:
            status_class = (
                "success" if entry.status == "success" else "failure"
            )
            rows.append(
                f"<tr><td>{entry.name}</td>"
                f"<td class='{status_class}'>{entry.status}</td>"
                f"<td>{entry.duration:.3f}s</td>"
                f"<td>{entry.message}</td></tr>"
            )
        rows_str = "\n".join(rows)

        return f"""<!DOCTYPE html>
<html>
<head><title>{self.name} Report</title>
<style>
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; }}
th {{ background-color: #4CAF50; color: white; }}
.success {{ color: green; }}
.failure {{ color: red; }}
</style></head>
<body>
<h1>{self.name} Report</h1>
<p>Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
<h2>Summary</h2>
<ul>
<li>Total: {summary.total_count}</li>
<li>Success: {summary.success_count}</li>
<li>Failed: {summary.failure_count}</li>
<li>Success Rate: {summary.success_rate:.1f}%</li>
<li>Avg Duration: {summary.avg_duration:.3f}s</li>
</ul>
<h2>Entries</h2>
<table>
<tr><th>Name</th><th>Status</th><th>Duration</th><th>Message</th></tr>
{rows_str}
</table>
</body></html>"""

    def generate_markdown(self) -> str:
        """Generate report in Markdown format."""
        entries = self._buffer.get_all()
        summary = self._compute_summary(entries)

        lines = [
            f"# Report: {self.name}",
            "",
            f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Summary",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Total | {summary.total_count} |",
            f"| Success | {summary.success_count} |",
            f"| Failed | {summary.failure_count} |",
            f"| Success Rate | {summary.success_rate:.1f}% |",
            f"| Avg Duration | {summary.avg_duration:.3f}s |",
            "",
            "## Entries",
            "",
            "| Name | Status | Duration | Message |",
            "|------|--------|----------|---------|",
        ]

        for entry in entries:
            status = "✓" if entry.status == "success" else "✗"
            lines.append(
                f"| {entry.name} | {status} | "
                f"{entry.duration:.3f}s | {entry.message} |"
            )

        return "\n".join(lines)

    def generate(
        self,
        format: ReportFormat = ReportFormat.TEXT,
        **kwargs,
    ) -> str:
        """Generate report in specified format."""
        generators = {
            ReportFormat.TEXT: self.generate_text,
            ReportFormat.JSON: self.generate_json,
            ReportFormat.CSV: self.generate_csv,
            ReportFormat.HTML: self.generate_html,
            ReportFormat.MARKDOWN: self.generate_markdown,
        }

        generator = generators.get(format, self.generate_text)
        return generator(**kwargs)

    def clear(self) -> None:
        """Clear all entries."""
        self._buffer.clear()

    def get_entries(
        self,
        status_filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ReportEntry]:
        """Get entries with optional filtering."""
        entries = self._buffer.get_all()

        if status_filter:
            entries = [e for e in entries if e.status == status_filter]

        if limit:
            entries = entries[-limit:]

        return entries

    def export(self, filepath: str, format: ReportFormat = ReportFormat.JSON):
        """Export report to file."""
        content = self.generate(format)
        with open(filepath, "w") as f:
            f.write(content)
