"""
Reporting Utilities for UI Automation.

This module provides utilities for generating reports and summaries
of automation execution results.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Optional
from pathlib import Path


class ReportFormat(Enum):
    """Report output formats."""
    HTML = auto()
    JSON = auto()
    MARKDOWN = auto()
    TEXT = auto()


@dataclass
class TestResult:
    """
    Result of a test or automation run.
    
    Attributes:
        name: Test/result name
        status: Pass/fail status
        duration_ms: Execution duration
        message: Optional message
        error: Optional error details
        timestamp: When result was recorded
    """
    name: str
    passed: bool
    duration_ms: float = 0.0
    message: Optional[str] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReportSummary:
    """
    Summary statistics for a report.
    
    Attributes:
        total: Total number of tests/results
        passed: Number passed
        failed: Number failed
        skipped: Number skipped
        duration_ms: Total duration
    """
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    duration_ms: float = 0.0
    
    @property
    def pass_rate(self) -> float:
        """Calculate pass rate percentage."""
        if self.total == 0:
            return 0.0
        return (self.passed / self.total) * 100
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate percentage."""
        if self.total == 0:
            return 0.0
        return (self.failed / self.total) * 100


class Report:
    """
    A test automation report.
    
    Example:
        report = Report(title="Login Tests")
        report.add_result(TestResult(name="test_login", passed=True))
        report.save("report.html")
    """
    
    def __init__(
        self,
        title: str = "Automation Report",
        description: Optional[str] = None
    ):
        self.title = title
        self.description = description
        self.results: list[TestResult] = []
        self.created_at = time.time()
        self.metadata: dict[str, Any] = {}
    
    def add_result(self, result: TestResult) -> None:
        """Add a result to the report."""
        self.results.append(result)
    
    def get_summary(self) -> ReportSummary:
        """Get summary statistics."""
        summary = ReportSummary(
            total=len(self.results),
            passed=sum(1 for r in self.results if r.passed),
            failed=sum(1 for r in self.results if not r.passed and r.error is None),
            skipped=sum(1 for r in self.results if r.error is None and not r.passed),
            duration_ms=sum(r.duration_ms for r in self.results)
        )
        return summary
    
    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "title": self.title,
            "description": self.description,
            "created_at": self.created_at,
            "created_at_formatted": datetime.fromtimestamp(self.created_at).isoformat(),
            "summary": {
                "total": self.get_summary().total,
                "passed": self.get_summary().passed,
                "failed": self.get_summary().failed,
                "skipped": self.get_summary().skipped,
                "pass_rate": self.get_summary().pass_rate,
                "duration_ms": self.get_summary().duration_ms
            },
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "duration_ms": r.duration_ms,
                    "message": r.message,
                    "error": r.error,
                    "timestamp": r.timestamp,
                    **r.metadata
                }
                for r in self.results
            ],
            "metadata": self.metadata
        }
    
    def save(
        self,
        path: str,
        format: ReportFormat = ReportFormat.HTML
    ) -> None:
        """
        Save report to a file.
        
        Args:
            path: Output file path
            format: Output format
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == ReportFormat.JSON:
            content = json.dumps(self.to_dict(), indent=2)
            path.write_text(content)
        elif format == ReportFormat.HTML:
            content = self._generate_html()
            path.write_text(content)
        elif format == ReportFormat.MARKDOWN:
            content = self._generate_markdown()
            path.write_text(content)
        else:
            content = self._generate_text()
            path.write_text(content)
    
    def _generate_html(self) -> str:
        """Generate HTML report."""
        summary = self.get_summary()
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{self.title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .summary {{ background: #f5f5f5; padding: 15px; border-radius: 5px; }}
        .passed {{ color: green; }}
        .failed {{ color: red; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
    </style>
</head>
<body>
    <h1>{self.title}</h1>
    <div class="summary">
        <h2>Summary</h2>
        <p>Total: {summary.total}</p>
        <p class="passed">Passed: {summary.passed}</p>
        <p class="failed">Failed: {summary.failed}</p>
        <p>Pass Rate: {summary.pass_rate:.1f}%</p>
        <p>Duration: {summary.duration_ms:.2f}ms</p>
    </div>
    
    <h2>Results</h2>
    <table>
        <tr>
            <th>Test</th>
            <th>Status</th>
            <th>Duration (ms)</th>
            <th>Message</th>
        </tr>
"""
        
        for result in self.results:
            status = '<span class="passed">PASSED</span>' if result.passed else '<span class="failed">FAILED</span>'
            html += f"""        <tr>
            <td>{result.name}</td>
            <td>{status}</td>
            <td>{result.duration_ms:.2f}</td>
            <td>{result.message or ''}</td>
        </tr>
"""
        
        html += """    </table>
</body>
</html>"""
        return html
    
    def _generate_markdown(self) -> str:
        """Generate Markdown report."""
        summary = self.get_summary()
        
        md = f"""# {self.title}

## Summary

| Metric | Value |
|--------|-------|
| Total | {summary.total} |
| Passed | {summary.passed} |
| Failed | {summary.failed} |
| Pass Rate | {summary.pass_rate:.1f}% |
| Duration | {summary.duration_ms:.2f}ms |

## Results

| Test | Status | Duration (ms) | Message |
|------|--------|---------------|---------|
"""
        
        for result in self.results:
            status = "✅ PASSED" if result.passed else "❌ FAILED"
            msg = result.message or ""
            md += f"| {result.name} | {status} | {result.duration_ms:.2f} | {msg} |\n"
        
        return md
    
    def _generate_text(self) -> str:
        """Generate plain text report."""
        summary = self.get_summary()
        
        text = f"""{'=' * 60}
{self.title}
{'=' * 60}

SUMMARY
-------
Total: {summary.total}
Passed: {summary.passed}
Failed: {summary.failed}
Pass Rate: {summary.pass_rate:.1f}%
Duration: {summary.duration_ms:.2f}ms

RESULTS
-------
"""
        
        for result in self.results:
            status = "PASSED" if result.passed else "FAILED"
            text += f"{status}: {result.name} ({result.duration_ms:.2f}ms)\n"
            if result.message:
                text += f"  Message: {result.message}\n"
        
        return text


class ReportBuilder:
    """
    Builder for constructing reports.
    
    Example:
        builder = ReportBuilder()
        builder.title("Test Report")
        builder.add_metadata("environment", "production")
        
        with builder.section("Login Tests"):
            builder.add_result(TestResult(name="test_1", passed=True))
        
        report = builder.build()
    """
    
    def __init__(self):
        self._report = Report()
        self._sections: list[str] = []
    
    def title(self, title: str) -> 'ReportBuilder':
        """Set report title."""
        self._report.title = title
        return self
    
    def description(self, desc: str) -> 'ReportBuilder':
        """Set report description."""
        self._report.description = desc
        return self
    
    def add_metadata(self, key: str, value: Any) -> 'ReportBuilder':
        """Add metadata."""
        self._report.metadata[key] = value
        return self
    
    def add_result(self, result: TestResult) -> 'ReportBuilder':
        """Add a result."""
        self._report.add_result(result)
        return self
    
    def section(self, name: str) -> 'ReportBuilder':
        """Start a new section."""
        self._sections.append(name)
        return self
    
    def build(self) -> Report:
        """Build and return the report."""
        return self._report
