"""Report action module for RabAI AutoClick.

Provides report generation actions: table formatting,
summary statistics, and structured output generation.
"""

import sys
import os
import json
import csv
import io
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TableFormatAction(BaseAction):
    """Format data as ASCII table.
    
    Generate formatted text tables for console output.
    """
    action_type = "table_format"
    display_name = "表格格式化"
    description = "将数据格式化为ASCII表格"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Format data as a table.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - columns: list of str (column names to include)
                - headers: dict (column header overrides)
                - max_col_width: int
                - align: dict (column: left/center/right)
                - format: str (simple/plain/grid/rst)
                - save_to_var: str
        
        Returns:
            ActionResult with formatted table.
        """
        data = params.get('data', [])
        columns = params.get('columns', [])
        headers = params.get('headers', {})
        max_col_width = params.get('max_col_width', 30)
        align = params.get('align', {})
        fmt = params.get('format', 'grid')
        save_to_var = params.get('save_to_var', 'table')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Determine columns
        if not columns:
            columns = list(data[0].keys()) if data else []

        # Truncate columns
        columns = [str(c)[:max_col_width] for c in columns]

        # Get header overrides
        header_map = {c: headers.get(c, c) for c in columns}

        # Measure column widths
        col_widths = {}
        for col in columns:
            header = header_map[col]
            col_widths[col] = max(len(str(header)), max_col_width)
            for row in data:
                val = row.get(col, '')
                col_widths[col] = min(max_col_width, max(col_widths[col], len(str(val))))

        # Build table
        lines = []

        if fmt == 'grid':
            # Top border
            border = '┌' + '┬'.join('─' * (col_widths[c] + 2) for c in columns) + '┐'
            lines.append(border)

            # Header
            header_cells = []
            for col in columns:
                h = header_map[col]
                width = col_widths[col]
                alignment = align.get(col, 'center')
                if alignment == 'left':
                    header_cells.append(f' {h:<{width}} ')
                elif alignment == 'right':
                    header_cells.append(f' {h:>{width}} ')
                else:
                    header_cells.append(f' {h:^{width}} ')
            lines.append('│' + '│'.join(header_cells) + '│')

            # Header separator
            sep = '├' + '┼'.join('─' * (col_widths[c] + 2) for c in columns) + '┤'
            lines.append(sep)

            # Rows
            for row in data:
                cells = []
                for col in columns:
                    val = str(row.get(col, ''))[:max_col_width]
                    width = col_widths[col]
                    alignment = align.get(col, 'left')
                    if alignment == 'right':
                        cells.append(f' {val:>{width}} ')
                    elif alignment == 'center':
                        cells.append(f' {val:^{width}} ')
                    else:
                        cells.append(f' {val:<{width}} ')
                lines.append('│' + '│'.join(cells) + '│')

            # Bottom border
            lines.append('└' + '┴'.join('─' * (col_widths[c] + 2) for c in columns) + '┘')

        elif fmt == 'simple':
            # Header
            header_cells = [header_map[c].center(col_widths[c]) for c in columns]
            lines.append(' '.join(header_cells))
            lines.append(' '.join('-' * col_widths[c] for c in columns))
            for row in data:
                cells = [str(row.get(c, ''))[:col_widths[c]].ljust(col_widths[c]) for c in columns]
                lines.append(' '.join(cells))

        elif fmt == 'plain':
            # Just comma-separated
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
            table_text = output.getvalue()
            if context and save_to_var:
                context.variables[save_to_var] = table_text
            return ActionResult(success=True, data={'table': table_text}, message=f"Formatted {len(data)} rows as CSV")

        elif fmt == 'rst':
            # RST-style table
            sep = '  '.join('=' * col_widths[c] for c in columns)
            lines.append(sep)
            header_cells = [header_map[c].ljust(col_widths[c]) for c in columns]
            lines.append('  '.join(header_cells))
            lines.append(sep)
            for row in data:
                cells = [str(row.get(c, ''))[:col_widths[c]].ljust(col_widths[c]) for c in columns]
                lines.append('  '.join(cells))
            lines.append(sep)

        table_text = '\n'.join(lines)

        if context and save_to_var:
            context.variables[save_to_var] = table_text

        return ActionResult(
            success=True,
            data={'table': table_text, 'rows': len(data), 'columns': len(columns)},
            message=f"Formatted {len(data)} rows, {len(columns)} columns"
        )


class SummaryAction(BaseAction):
    """Generate statistical summary of data.
    
    Compute count, sum, mean, median, std, min, max,
    and distribution statistics.
    """
    action_type = "summary"
    display_name = "数据摘要"
    description = "生成数据的统计摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate data summary.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of dicts
                - fields: list of str (fields to summarize)
                - include_distribution: bool
                - save_to_var: str
        
        Returns:
            ActionResult with summary statistics.
        """
        import math
        data = params.get('data', [])
        fields = params.get('fields', [])
        include_distribution = params.get('include_distribution', False)
        save_to_var = params.get('save_to_var', 'summary')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Determine fields
        if not fields:
            # Auto-detect numeric fields
            for row in data:
                if isinstance(row, dict):
                    fields = [k for k, v in row.items()
                             if isinstance(v, (int, float))]
                    break

        summaries = {}
        for field in fields:
            values = []
            for row in data:
                val = row.get(field) if isinstance(row, dict) else row
                if val is not None:
                    try:
                        values.append(float(val))
                    except (ValueError, TypeError):
                        pass

            if not values:
                continue

            values.sort()
            n = len(values)
            s = sum(values)
            mean = s / n

            variance = sum((x - mean) ** 2 for x in values) / n
            std = math.sqrt(variance)

            if n % 2 == 0:
                median = (values[n // 2 - 1] + values[n // 2]) / 2
            else:
                median = values[n // 2]

            summary = {
                'count': n,
                'sum': round(s, 6),
                'mean': round(mean, 6),
                'median': round(median, 6),
                'std': round(std, 6),
                'min': values[0],
                'max': values[-1],
                'range': values[-1] - values[0],
            }

            if include_distribution:
                # Compute quartiles
                q1_idx = n // 4
                q3_idx = 3 * n // 4
                summary['q1'] = values[q1_idx]
                summary['q3'] = values[q3_idx]
                summary['iqr'] = summary['q3'] - summary['q1']

            summaries[field] = summary

        result = {
            'row_count': len(data),
            'field_count': len(fields),
            'summaries': summaries,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Summary of {len(data)} rows across {len(fields)} fields"
        )


class MarkdownReportAction(BaseAction):
    """Generate a Markdown report from data.
    
    Create structured Markdown reports with sections,
    tables, and formatted content.
    """
    action_type = "markdown_report"
    display_name = "Markdown报告"
    description = "从数据生成Markdown格式的报告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate Markdown report.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - title: str
                - sections: list of {heading, content, table, list}
                - data: dict (template data for content interpolation)
                - include_toc: bool
                - save_to_var: str
        
        Returns:
            ActionResult with Markdown report.
        """
        title = params.get('title', 'Report')
        sections = params.get('sections', [])
        template_data = params.get('data', {})
        include_toc = params.get('include_toc', False)
        save_to_var = params.get('save_to_var', 'report')

        lines = []
        toc_entries = []

        # Title
        lines.append(f"# {title}")
        lines.append("")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # Table of contents
        if include_toc and sections:
            for sec in sections:
                heading = sec.get('heading', '')
                if heading:
                    anchor = heading.lower().replace(' ', '-')
                    toc_entries.append(f"- [{heading}](#{anchor})")

            lines.append("## Table of Contents")
            lines.append("")
            lines.extend(toc_entries)
            lines.append("")
            lines.append("---")
            lines.append("")

        # Sections
        for sec in sections:
            heading = sec.get('heading', '')
            if heading:
                lines.append(f"## {heading}")
                lines.append("")

            content = sec.get('content', '')
            if content:
                # Interpolate template data
                for k, v in template_data.items():
                    content = content.replace(f'{{{{{k}}}}}', str(v))
                lines.append(content)
                lines.append("")

            table = sec.get('table')
            if table:
                if isinstance(table, list) and table:
                    headers = list(table[0].keys()) if isinstance(table[0], dict) else []
                    if headers:
                        lines.append('| ' + ' | '.join(str(h) for h in headers) + ' |')
                        lines.append('| ' + ' | '.join('---' for _ in headers) + ' |')
                        for row in table:
                            lines.append('| ' + ' | '.join(str(row.get(h, '')) for h in headers) + ' |')
                        lines.append("")
                elif isinstance(table, str):
                    lines.append(table)
                    lines.append("")

            list_items = sec.get('list', [])
            if list_items:
                for item in list_items:
                    lines.append(f"- {item}")
                lines.append("")

        report_text = '\n'.join(lines)

        if context and save_to_var:
            context.variables[save_to_var] = report_text

        return ActionResult(
            success=True,
            data={'report': report_text, 'sections': len(sections)},
            message=f"Generated Markdown report with {len(sections)} sections"
        )
