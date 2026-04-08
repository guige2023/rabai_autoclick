"""
Data Visualizer Action Module.

Generates data visualizations including charts, graphs, and plots
with configurable styles and export options.

Author: RabAi Team
"""

from __future__ import annotations

import base64
import io
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Use Agg backend for non-interactive rendering


class ChartType(Enum):
    """Supported chart types."""
    LINE = "line"
    BAR = "bar"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    PIE = "pie"
    BOX = "box"
    HEATMAP = "heatmap"
    AREA = "area"


class ExportFormat(Enum):
    """Export formats."""
    PNG = "png"
    SVG = "svg"
    PDF = "pdf"
    JSON = "json"
    HTML = "html"


@dataclass
class ChartConfig:
    """Configuration for chart styling."""
    title: str = ""
    xlabel: str = ""
    ylabel: str = ""
    figsize: Tuple[int, int] = (10, 6)
    style: str = "seaborn-v0_8"
    color_palette: str = "tab10"
    grid: bool = True
    legend: bool = True
    fontsize: int = 12
    title_fontsize: int = 14
    rot_x: int = 0
    rot_y: int = 0


@dataclass
class VisualizationResult:
    """Result of a visualization operation."""
    chart_type: ChartType
    image_data: Optional[bytes] = None
    html_snippet: Optional[str] = None
    json_data: Optional[Dict] = None
    width: int = 0
    height: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataVisualizer:
    """
    Data visualization engine.

    Creates charts, graphs, and plots from DataFrames with
    configurable styling and multiple export formats.

    Example:
        >>> viz = DataVisualizer()
        >>> result = viz.create_chart(df, ChartType.LINE, x="date", y="sales")
        >>> result.image_data  # PNG bytes
    """

    def __init__(self):
        self._style = "seaborn-v0_8"
        plt.style.use(self._style)

    def create_chart(
        self,
        df: pd.DataFrame,
        chart_type: ChartType,
        x: Optional[str] = None,
        y: Optional[Union[str, List[str]]] = None,
        config: Optional[ChartConfig] = None,
        **kwargs,
    ) -> VisualizationResult:
        """Create a chart from DataFrame data."""
        config = config or ChartConfig()

        fig, ax = plt.subplots(figsize=config.figsize)

        if chart_type == ChartType.LINE:
            self._create_line_chart(ax, df, x, y, **kwargs)
        elif chart_type == ChartType.BAR:
            self._create_bar_chart(ax, df, x, y, **kwargs)
        elif chart_type == ChartType.SCATTER:
            self._create_scatter_chart(ax, df, x, y, **kwargs)
        elif chart_type == ChartType.HISTOGRAM:
            self._create_histogram(ax, df, x, **kwargs)
        elif chart_type == ChartType.PIE:
            return self._create_pie_chart(df, y, config, **kwargs)
        elif chart_type == ChartType.BOX:
            self._create_box_plot(ax, df, x, y, **kwargs)
        elif chart_type == ChartType.HEATMAP:
            return self._create_heatmap(df, x, config, **kwargs)
        elif chart_type == ChartType.AREA:
            self._create_area_chart(ax, df, x, y, **kwargs)

        if config.title:
            ax.set_title(config.title, fontsize=config.title_fontsize)
        if config.xlabel:
            ax.set_xlabel(config.xlabel, fontsize=config.fontsize)
        if config.ylabel:
            ax.set_ylabel(config.ylabel, fontsize=config.fontsize)
        if config.grid:
            ax.grid(True)
        if config.legend:
            ax.legend()

        plt.xticks(rotation=config.rot_x)
        plt.yticks(rotation=config.rot_y)
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100)
        plt.close(fig)

        buf.seek(0)
        image_data = buf.read()

        return VisualizationResult(
            chart_type=chart_type,
            image_data=image_data,
            width=config.figsize[0] * 100,
            height=config.figsize[1] * 100,
        )

    def _create_line_chart(
        self,
        ax,
        df: pd.DataFrame,
        x: Optional[str],
        y: Optional[Union[str, List[str]]],
        **kwargs,
    ) -> None:
        if x and y:
            if isinstance(y, list):
                for col in y:
                    ax.plot(df[x], df[col], label=col, **kwargs)
            else:
                ax.plot(df[x], df[y], **kwargs)
        else:
            df.plot(ax=ax, **kwargs)

    def _create_bar_chart(
        self,
        ax,
        df: pd.DataFrame,
        x: Optional[str],
        y: Optional[Union[str, List[str]]],
        **kwargs,
    ) -> None:
        if x and y:
            if isinstance(y, list):
                df.plot(kind="bar", x=x, y=y, ax=ax, **kwargs)
            else:
                ax.bar(df[x], df[y], **kwargs)
        else:
            df.plot(kind="bar", ax=ax, **kwargs)

    def _create_scatter_chart(
        self,
        ax,
        df: pd.DataFrame,
        x: Optional[str],
        y: Optional[Union[str, List[str]]],
        **kwargs,
    ) -> None:
        if x and y:
            ax.scatter(df[x], df[y], **kwargs)

    def _create_histogram(
        self,
        ax,
        df: pd.DataFrame,
        x: Optional[str],
        **kwargs,
    ) -> None:
        if x:
            ax.hist(df[x].dropna(), **kwargs)

    def _create_pie_chart(
        self,
        df: pd.DataFrame,
        y: Optional[Union[str, List[str]]],
        config: ChartConfig,
        **kwargs,
    ) -> VisualizationResult:
        if not y:
            return VisualizationResult(chart_type=ChartType.PIE)

        if isinstance(y, list):
            data = df[y[0]]
        else:
            data = df[y]

        fig, ax = plt.subplots(figsize=config.figsize)
        ax.pie(data, labels=df.index if df.index.name else None, **kwargs)
        if config.title:
            ax.set_title(config.title)

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100)
        plt.close(fig)
        buf.seek(0)

        return VisualizationResult(
            chart_type=ChartType.PIE,
            image_data=buf.read(),
            width=config.figsize[0] * 100,
            height=config.figsize[1] * 100,
        )

    def _create_box_plot(
        self,
        ax,
        df: pd.DataFrame,
        x: Optional[str],
        y: Optional[Union[str, List[str]]],
        **kwargs,
    ) -> None:
        if x and y:
            df.boxplot(column=y, by=x, ax=ax, **kwargs)
        else:
            df.boxplot(ax=ax, **kwargs)

    def _create_heatmap(
        self,
        df: pd.DataFrame,
        x: Optional[str],
        config: ChartConfig,
        **kwargs,
    ) -> VisualizationResult:
        numeric_df = df.select_dtypes(include=[np.number])
        corr = numeric_df.corr()

        fig, ax = plt.subplots(figsize=config.figsize)
        im = ax.imshow(corr, cmap="coolwarm", aspect="auto", vmin=-1, vmax=1)

        ax.set_xticks(range(len(corr.columns)))
        ax.set_yticks(range(len(corr.index)))
        ax.set_xticklabels(corr.columns, rotation=45)
        ax.set_yticklabels(corr.index)

        plt.colorbar(im, ax=ax)
        if config.title:
            ax.set_title(config.title)

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100)
        plt.close(fig)
        buf.seek(0)

        return VisualizationResult(
            chart_type=ChartType.HEATMAP,
            image_data=buf.read(),
            width=config.figsize[0] * 100,
            height=config.figsize[1] * 100,
        )

    def _create_area_chart(
        self,
        ax,
        df: pd.DataFrame,
        x: Optional[str],
        y: Optional[Union[str, List[str]]],
        **kwargs,
    ) -> None:
        if x and y:
            if isinstance(y, list):
                df.plot(kind="area", x=x, y=y, ax=ax, **kwargs)
            else:
                ax.fill_between(df[x], df[y], **kwargs)
        else:
            df.plot(kind="area", ax=ax, **kwargs)


def create_visualizer() -> DataVisualizer:
    """Factory to create a data visualizer."""
    return DataVisualizer()
