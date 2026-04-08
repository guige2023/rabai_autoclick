"""Chart action module for RabAI AutoClick.

Provides chart/visualization generation actions:
bar, line, pie, scatter, and data-driven SVG/ASCII charts.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AsciiChartAction(BaseAction):
    """Generate ASCII bar/line charts.
    
    Create text-based charts for terminal output.
    """
    action_type = "ascii_chart"
    display_name = "ASCII图表"
    description = "生成ASCII文本图表用于终端显示"

    CHART_TYPES = ['bar', 'line', 'scatter', 'area']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate an ASCII chart.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - chart_type: str (bar/line/scatter/area)
                - data: list of numbers or list of {label, value} dicts
                - label_field: str (label field name)
                - value_field: str (value field name)
                - width: int (chart width in chars)
                - height: int (chart height in chars)
                - show_values: bool
                - show_labels: bool
                - title: str
                - save_to_var: str
        
        Returns:
            ActionResult with chart text.
        """
        chart_type = params.get('chart_type', 'bar')
        data = params.get('data', [])
        label_field = params.get('label_field', 'label')
        value_field = params.get('value_field', 'value')
        width = params.get('width', 60)
        height = params.get('height', 15)
        show_values = params.get('show_values', True)
        show_labels = params.get('show_labels', True)
        title = params.get('title', '')
        save_to_var = params.get('save_to_var', 'chart')

        if not data:
            return ActionResult(success=False, message="No data provided")

        # Extract values and labels
        values = []
        labels = []
        for item in data:
            if isinstance(item, dict):
                val = item.get(value_field, 0)
                lbl = str(item.get(label_field, ''))
            else:
                val = item
                lbl = ''
            try:
                values.append(float(val))
            except (ValueError, TypeError):
                values.append(0)
            labels.append(lbl)

        if not values:
            return ActionResult(success=False, message="No valid numeric values")

        max_val = max(values)
        min_val = min(values) if chart_type == 'line' else 0

        chart_lines = []
        if title:
            chart_lines.append(title)
            chart_lines.append('=' * min(len(title), width))

        if chart_type == 'bar':
            chart_lines.append(self._ascii_bar(values, labels, max_val, width, show_values, show_labels))
        elif chart_type == 'line':
            chart_lines.append(self._ascii_line(values, labels, max_val, min_val, width, height, show_labels))
        elif chart_type == 'scatter':
            chart_lines.append(self._ascii_scatter(values, labels, max_val, width, height, show_labels))
        elif chart_type == 'area':
            chart_lines.append(self._ascii_area(values, labels, max_val, width, height, show_labels))

        chart_text = '\n'.join(chart_lines)

        if context and save_to_var:
            context.variables[save_to_var] = chart_text

        return ActionResult(
            success=True,
            data={'chart': chart_text, 'chart_type': chart_type},
            message=f"Generated {chart_type} chart"
        )

    def _ascii_bar(self, values: List[float], labels: List[str],
                   max_val: float, width: int, show_values: bool,
                   show_labels: bool) -> str:
        """Generate ASCII bar chart."""
        lines = []
        bar_area_width = width - 10  # Reserve space for values

        for i, (val, label) in enumerate(zip(values, labels)):
            bar_len = int((val / max_val) * bar_area_width) if max_val > 0 else 0
            bar = '█' * bar_len
            if show_values:
                line = f"{val:>8.2f} │{bar}"
            else:
                line = f"{'':>8} │{bar}"
            
            if show_labels and label:
                line += f" {label[:20]}"
            lines.append(line)

        lines.append('─' * (bar_area_width + 10))
        return '\n'.join(lines)

    def _ascii_line(self, values: List[float], labels: List[str],
                    max_val: float, min_val: float,
                    width: int, height: int,
                    show_labels: bool) -> str:
        """Generate ASCII line chart."""
        if max_val == min_val:
            return '│' + '─' * width + '│'

        grid = [[' ' for _ in range(width)] for _ in range(height)]
        range_val = max_val - min_val

        # Plot points
        for i, val in enumerate(values):
            x = int((i / (len(values) - 1)) * (width - 1)) if len(values) > 1 else width // 2
            y = int(((val - min_val) / range_val) * (height - 1))
            y = height - 1 - y  # Flip Y axis
            y = max(0, min(height - 1, y))
            grid[y][x] = '●'

        # Draw horizontal guide lines
        for row_idx in [0, height // 2, height - 1]:
            grid[row_idx] = ['─' if c == ' ' else c for c in grid[row_idx]]

        # Convert to lines
        lines = []
        for row in grid:
            lines.append('│' + ''.join(row) + '│')

        lines.append('└' + '─' * width + '┘')
        
        if show_labels and labels:
            label_line = ' ' + labels[0][:width//2] + ' ' * (width - len(labels[0][:width//2]) - len(labels[-1][:width//2]) - 1) + labels[-1][:width//2]
            lines.append(label_line)

        return '\n'.join(lines)

    def _ascii_scatter(self, values: List[float], labels: List[str],
                       max_val: float, width: int, height: int,
                       show_labels: bool) -> str:
        """Generate ASCII scatter plot."""
        grid = [[' ' for _ in range(width)] for _ in range(height)]

        for i, val in enumerate(values):
            x = int((i / (len(values) - 1)) * (width - 1)) if len(values) > 1 else width // 2
            y = int((val / max_val) * (height - 1)) if max_val > 0 else height // 2
            y = height - 1 - y
            y = max(0, min(height - 1, y))
            grid[y][x] = '●'

        lines = []
        for row in grid:
            lines.append('│' + ''.join(row) + '│')
        lines.append('└' + '─' * width + '┘')

        return '\n'.join(lines)

    def _ascii_area(self, values: List[float], labels: List[str],
                    max_val: float, width: int, height: int,
                    show_labels: bool) -> str:
        """Generate ASCII area chart."""
        if max_val == 0:
            return ' ' * width

        grid = [[' ' for _ in range(width)] for _ in range(height)]

        for i, val in enumerate(values):
            x = int((i / (len(values) - 1)) * (width - 1)) if len(values) > 1 else width // 2
            bar_height = int((val / max_val) * height)
            for y in range(height):
                row_from_bottom = height - y
                if row_from_bottom <= bar_height:
                    grid[y][x] = '█'

        lines = []
        for row in grid:
            lines.append('│' + ''.join(row) + '│')
        lines.append('└' + '─' * width + '┘')

        return '\n'.join(lines)


class JsonChartAction(BaseAction):
    """Generate chart data in Vega-Lite / Chart.js JSON format.
    
    Output chart specifications for web visualization.
    """
    action_type = "json_chart"
    display_name = "JSON图表"
    description = "生成Vega-Lite/Chart.js格式的图表规范"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate chart specification JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - chart_type: str (bar/line/pie/scatter)
                - data: list of dicts
                - label_field: str
                - value_fields: list of str (or single str)
                - title: str
                - format: str (vegalite/chartjs)
                - save_to_var: str
        
        Returns:
            ActionResult with chart specification.
        """
        chart_type = params.get('chart_type', 'bar')
        data = params.get('data', [])
        label_field = params.get('label_field', 'label')
        value_fields = params.get('value_fields', 'value')
        title = params.get('title', '')
        fmt = params.get('format', 'vegalite')
        save_to_var = params.get('save_to_var', 'chart_json')

        if isinstance(value_fields, str):
            value_fields = [value_fields]

        if fmt == 'vegalite':
            spec = self._vegalite_spec(chart_type, data, label_field, value_fields, title)
        else:
            spec = self._chartjs_spec(chart_type, data, label_field, value_fields, title)

        if context and save_to_var:
            context.variables[save_to_var] = spec

        return ActionResult(
            success=True,
            data=spec,
            message=f"Generated {fmt} chart specification"
        )

    def _vegalite_spec(self, chart_type: str, data: List, label_field: str,
                       value_fields: List[str], title: str) -> Dict:
        """Generate Vega-Lite specification."""
        spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "title": title,
            "width": "container",
            "height": 300,
            "data": {"values": data},
        }

        encodings = {}

        if label_field:
            encodings["x"] = {
                "field": label_field,
                "type": "nominal",
                "title": label_field,
            }

        # Determine mark type
        if chart_type == 'bar':
            encodings["y"] = {
                "field": value_fields[0],
                "type": "quantitative",
                "title": value_fields[0],
            }
            spec["mark"] = "bar"
            spec["encoding"] = encodings
        elif chart_type == 'line':
            encodings["y"] = {
                "field": value_fields[0],
                "type": "quantitative",
                "title": value_fields[0],
            }
            spec["mark"] = {"type": "line", "point": True}
            spec["encoding"] = encodings
        elif chart_type == 'pie':
            spec["mark"] = "arc"
            spec["encoding"] = {
                "theta": {"field": value_fields[0], "type": "quantitative"},
                "color": {"field": label_field, "type": "nominal"} if label_field else {"value": "steelblue"},
            }
        elif chart_type == 'scatter':
            if len(value_fields) >= 2:
                encodings["x"] = {"field": value_fields[0], "type": "quantitative"}
                encodings["y"] = {"field": value_fields[1], "type": "quantitative"}
            spec["mark"] = "point"
            spec["encoding"] = encodings

        return spec

    def _chartjs_spec(self, chart_type: str, data: List, label_field: str,
                       value_fields: List[str], title: str) -> Dict:
        """Generate Chart.js specification."""
        labels = [str(d.get(label_field, '')) for d in data]

        datasets = []
        colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF']
        for i, vf in enumerate(value_fields):
            values = []
            for d in data:
                val = d.get(vf, 0)
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    values.append(0)

            datasets.append({
                "label": vf,
                "data": values,
                "backgroundColor": colors[i % len(colors)],
                "borderColor": colors[i % len(colors)],
                "fill": False,
            })

        return {
            "type": chart_type if chart_type != 'area' else 'line',
            "data": {
                "labels": labels,
                "datasets": datasets,
            },
            "options": {
                "responsive": True,
                "title": {"display": bool(title), "text": title},
            }
        }
