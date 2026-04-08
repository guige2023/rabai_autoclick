"""
Automation reporting utilities for test results and logging.

Provides structured reporting, HTML generation,
and test result aggregation for automation workflows.
"""

from __future__ import annotations

import json
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class ReportLevel(Enum):
    """Report severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ReportEntry:
    """Single report entry."""
    timestamp: float
    level: ReportLevel
    category: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    screenshot_path: Optional[str] = None


@dataclass
class TestReport:
    """Automation test report."""
    report_id: str
    name: str
    start_time: float
    end_time: Optional[float] = None
    entries: List[ReportEntry] = field(default_factory=list)
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def total(self) -> int:
        return self.passed + self.failed + self.skipped
    
    @property
    def pass_rate(self) -> float:
        total = self.total
        return (self.passed / total * 100) if total > 0 else 0


class ReportGenerator:
    """Generates automation reports."""
    
    def __init__(self, name: str = "automation_report"):
        """
        Initialize report generator.
        
        Args:
            name: Report name.
        """
        self.report = TestReport(
            report_id=str(time.time()),
            name=name,
            start_time=time.time()
        )
    
    def add_entry(self, level: ReportLevel, category: str,
                 message: str, details: Optional[Dict] = None,
                 screenshot_path: Optional[str] = None) -> None:
        """
        Add report entry.
        
        Args:
            level: Entry level.
            category: Category.
            message: Message.
            details: Optional details dict.
            screenshot_path: Optional screenshot.
        """
        entry = ReportEntry(
            timestamp=time.time(),
            level=level,
            category=category,
            message=message,
            details=details or {},
            screenshot_path=screenshot_path
        )
        self.report.entries.append(entry)
    
    def info(self, category: str, message: str, **kwargs) -> None:
        """Add info entry."""
        self.add_entry(ReportLevel.INFO, category, message, **kwargs)
    
    def warning(self, category: str, message: str, **kwargs) -> None:
        """Add warning entry."""
        self.add_entry(ReportLevel.WARNING, category, message, **kwargs)
    
    def error(self, category: str, message: str, **kwargs) -> None:
        """Add error entry."""
        self.add_entry(ReportLevel.ERROR, category, message, **kwargs)
    
    def log_action(self, action: str, result: str, **kwargs) -> None:
        """Log action result."""
        self.add_entry(ReportLevel.INFO, "action", f"{action}: {result}", **kwargs)
    
    def log_screenshot(self, label: str, path: str) -> None:
        """Log screenshot capture."""
        self.add_entry(
            ReportLevel.INFO,
            "screenshot",
            f"Screenshot: {label}",
            screenshot_path=path
        )
    
    def increment_passed(self) -> None:
        """Increment passed count."""
        self.report.passed += 1
    
    def increment_failed(self) -> None:
        """Increment failed count."""
        self.report.failed += 1
    
    def increment_skipped(self) -> None:
        """Increment skipped count."""
        self.report.skipped += 1
    
    def finish(self) -> TestReport:
        """
        Finish and finalize report.
        
        Returns:
            Final TestReport.
        """
        self.report.end_time = time.time()
        return self.report
    
    def export_json(self, path: str) -> bool:
        """
        Export report as JSON.
        
        Args:
            path: Output file path.
            
        Returns:
            True if successful.
        """
        try:
            report = self.finish()
            data = {
                'report_id': report.report_id,
                'name': report.name,
                'start_time': report.start_time,
                'end_time': report.end_time,
                'passed': report.passed,
                'failed': report.failed,
                'skipped': report.skipped,
                'pass_rate': report.pass_rate,
                'metadata': report.metadata,
                'entries': [
                    {
                        'timestamp': e.timestamp,
                        'level': e.level.value,
                        'category': e.category,
                        'message': e.message,
                        'details': e.details,
                        'screenshot_path': e.screenshot_path,
                    }
                    for e in report.entries
                ]
            }
            
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False
    
    def export_html(self, path: str) -> bool:
        """
        Export report as HTML.
        
        Args:
            path: Output file path.
            
        Returns:
            True if successful.
        """
        try:
            report = self.finish()
            
            rows = []
            for entry in report.entries:
                level_class = entry.level.value.lower()
                dt = datetime.fromtimestamp(entry.timestamp).strftime("%H:%M:%S")
                
                row = f'''
                <tr class="{level_class}">
                    <td>{dt}</td>
                    <td>{entry.level.value}</td>
                    <td>{entry.category}</td>
                    <td>{entry.message}</td>
                    <td>{entry.screenshot_path or ''}</td>
                </tr>
                '''
                rows.append(row)
            
            html = f'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Automation Report - {report.name}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .summary {{ display: flex; gap: 20px; margin-bottom: 20px; }}
                    .stat {{ background: #f0f0f0; padding: 15px; border-radius: 5px; }}
                    .stat.passed {{ background: #d4edda; }}
                    .stat.failed {{ background: #f8d7da; }}
                    .stat.skipped {{ background: #fff3cd; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #333; color: white; }}
                    tr.debug {{ background-color: #f8f8f8; }}
                    tr.info {{ background-color: #e8f4f8; }}
                    tr.warning {{ background-color: #fff3cd; }}
                    tr.error {{ background-color: #f8d7da; }}
                    tr.critical {{ background-color: #f5c2c7; }}
                </style>
            </head>
            <body>
                <h1>Automation Report: {report.name}</h1>
                <div class="summary">
                    <div class="stat passed">
                        <strong>Passed:</strong> {report.passed}
                    </div>
                    <div class="stat failed">
                        <strong>Failed:</strong> {report.failed}
                    </div>
                    <div class="stat skipped">
                        <strong>Skipped:</strong> {report.skipped}
                    </div>
                    <div class="stat">
                        <strong>Pass Rate:</strong> {report.pass_rate:.1f}%
                    </div>
                </div>
                <table>
                    <tr>
                        <th>Time</th>
                        <th>Level</th>
                        <th>Category</th>
                        <th>Message</th>
                        <th>Screenshot</th>
                    </tr>
                    {''.join(rows)}
                </table>
            </body>
            </html>
            '''
            
            with open(path, 'w') as f:
                f.write(html)
            return True
        except Exception:
            return False
