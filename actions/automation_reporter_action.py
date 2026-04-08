# Copyright (c) 2024. coded by claude
"""Automation Reporter Action Module.

Generates reports for automation workflow execution including
step summaries, timing breakdowns, and error analysis.
"""
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ReportFormat(Enum):
    TEXT = "text"
    HTML = "html"
    JSON = "json"
    MARKDOWN = "markdown"


@dataclass
class StepReport:
    step_name: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_ms: Optional[float]
    error: Optional[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowReport:
    workflow_name: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    steps: List[StepReport] = field(default_factory=list)
    total_duration_ms: Optional[float] = None
    success_rate: float = 0.0


class AutomationReporter:
    def __init__(self):
        self._reports: Dict[str, WorkflowReport] = {}

    def create_report(self, workflow_name: str, run_id: str) -> WorkflowReport:
        report = WorkflowReport(
            workflow_name=workflow_name,
            run_id=run_id,
            start_time=datetime.now(),
            end_time=None,
            status="running",
        )
        self._reports[run_id] = report
        return report

    def add_step(
        self,
        run_id: str,
        step_name: str,
        status: str,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if run_id not in self._reports:
            return
        report = self._reports[run_id]
        duration_ms = None
        if end_time:
            duration_ms = (end_time - start_time).total_seconds() * 1000
        step = StepReport(
            step_name=step_name,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration_ms=duration_ms,
            error=error,
            metadata=metadata or {},
        )
        report.steps.append(step)

    def finalize_report(self, run_id: str, status: str) -> Optional[WorkflowReport]:
        if run_id not in self._reports:
            return None
        report = self._reports[run_id]
        report.end_time = datetime.now()
        report.status = status
        if report.start_time and report.end_time:
            report.total_duration_ms = (report.end_time - report.start_time).total_seconds() * 1000
        total_steps = len(report.steps)
        if total_steps > 0:
            successful = sum(1 for s in report.steps if s.status == "success")
            report.success_rate = successful / total_steps * 100
        return report

    def format_report(self, report: WorkflowReport, format: ReportFormat = ReportFormat.TEXT) -> str:
        if format == ReportFormat.TEXT:
            return self._format_text(report)
        elif format == ReportFormat.JSON:
            return self._format_json(report)
        elif format == ReportFormat.HTML:
            return self._format_html(report)
        elif format == ReportFormat.MARKDOWN:
            return self._format_markdown(report)
        return str(report)

    def _format_text(self, report: WorkflowReport) -> str:
        lines = [
            f"Workflow: {report.workflow_name}",
            f"Run ID: {report.run_id}",
            f"Status: {report.status}",
            f"Duration: {report.total_duration_ms:.2f}ms" if report.total_duration_ms else "Duration: N/A",
            f"Success Rate: {report.success_rate:.1f}%",
            "",
            "Steps:",
        ]
        for step in report.steps:
            lines.append(f"  - {step.step_name}: {step.status} ({step.duration_ms:.2f}ms)" if step.duration_ms else f"  - {step.step_name}: {step.status}")
        return "\n".join(lines)

    def _format_json(self, report: WorkflowReport) -> str:
        import json
        return json.dumps({
            "workflow_name": report.workflow_name,
            "run_id": report.run_id,
            "status": report.status,
            "total_duration_ms": report.total_duration_ms,
            "success_rate": report.success_rate,
            "steps": [
                {
                    "name": s.step_name,
                    "status": s.status,
                    "duration_ms": s.duration_ms,
                    "error": s.error,
                }
                for s in report.steps
            ],
        }, indent=2)

    def _format_html(self, report: WorkflowReport) -> str:
        return f"<html><body><h1>{report.workflow_name}</h1><p>Status: {report.status}</p></body></html>"

    def _format_markdown(self, report: WorkflowReport) -> str:
        lines = [f"# {report.workflow_name}", f"**Status**: {report.status}", ""]
        for step in report.steps:
            lines.append(f"- {step.step_name}: {step.status}")
        return "\n".join(lines)
