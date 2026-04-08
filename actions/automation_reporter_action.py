"""Automation Reporter Action.

Generates execution reports in various formats.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time


@dataclass
class ReportSection:
    title: str
    content: Any
    format: str = "table"


class AutomationReporterAction:
    """Generates automation execution reports."""

    def __init__(
        self,
        title: str,
        include_timestamps: bool = True,
        include_stats: bool = True,
    ) -> None:
        self.title = title
        self.include_timestamps = include_timestamps
        self.include_stats = include_stats
        self.sections: List[ReportSection] = []
        self.metadata: Dict[str, Any] = {}

    def add_section(self, title: str, content: Any, format: str = "table") -> "AutomationReporterAction":
        self.sections.append(ReportSection(title=title, content=content, format=format))
        return self

    def add_table(self, title: str, headers: List[str], rows: List[List[Any]]) -> "AutomationReporterAction":
        content = {"headers": headers, "rows": rows}
        return self.add_section(title, content, format="table")

    def add_metric(self, title: str, value: Any) -> "AutomationReporterAction":
        return self.add_section(title, value, format="metric")

    def add_text(self, title: str, text: str) -> "AutomationReporterAction":
        return self.add_section(title, text, format="text")

    def generate_text(self) -> str:
        lines = []
        lines.append(f"=== {self.title} ===")
        if self.include_timestamps:
            lines.append(f"Generated: {datetime.now().isoformat()}")
        lines.append("")
        for section in self.sections:
            lines.append(f"[{section.title}]")
            if section.format == "table":
                content = section.content
                if isinstance(content, dict) and "headers" in content:
                    lines.append("  " + " | ".join(str(h) for h in content["headers"]))
                    lines.append("  " + "-" * (sum(len(str(h)) for h in content["headers"]) + 3 * (len(content["headers"]) - 1)))
                    for row in content["rows"]:
                        lines.append("  " + " | ".join(str(c) for c in row))
                else:
                    lines.append(f"  {content}")
            elif section.format == "metric":
                lines.append(f"  {section.content}")
            elif section.format == "text":
                lines.append(f"  {section.content}")
            lines.append("")
        return "\n".join(lines)

    def generate_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "generated_at": datetime.now().isoformat(),
            "sections": [
                {"title": s.title, "content": s.content, "format": s.format}
                for s in self.sections
            ],
            "metadata": self.metadata,
        }
