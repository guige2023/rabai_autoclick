"""
Data export utilities: CSV, Excel, JSON, PDF.

Provides a unified ExportManager that routes data to the appropriate
formatter based on file extension or explicit format flag.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Iterable

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ExportOptions:
    """Common export options across all formats."""
    include_header: bool = True
    datetime_format: str = "%Y-%m-%d %H:%M:%S"
    encoding: str = "utf-8-sig"
    delimiter: str = ","
    sheet_name: str = "Sheet1"
    pdf_orientation: str = "portrait"  # portrait or landscape
    pdf_page_size: str = "A4"
    title: str = ""
    columns: list[str] | None = None  # column order/filter


# ─────────────────────────────────────────────────────────────────────────────
# Formatter Interface
# ─────────────────────────────────────────────────────────────────────────────

class ExportFormatter(ABC):
    """Abstract base for export formatters."""

    extension: str = ""

    @abstractmethod
    def export(self, data: list[dict[str, Any]], options: ExportOptions, output: BinaryIO) -> None:
        """Write data to the output stream."""
        ...

    def _flatten_row(self, row: dict[str, Any], datetime_format: str) -> dict[str, Any]:
        """Flatten nested values and convert dates to strings."""
        out: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.strftime(datetime_format)
            elif isinstance(v, (list, dict)):
                out[k] = json.dumps(v, ensure_ascii=False)
            else:
                out[k] = v
        return out


# ─────────────────────────────────────────────────────────────────────────────
# CSV Formatter
# ─────────────────────────────────────────────────────────────────────────────

class CSVFormatter(ExportFormatter):
    """Comma-separated values (with BOM for Excel compatibility)."""

    extension = ".csv"

    def export(self, data: list[dict[str, Any]], options: ExportOptions, output: BinaryIO) -> None:
        if not data:
            output.write(b"")
            return

        cols = options.columns or list(data[0].keys())
        filtered = [{k: row.get(k, "") for k in cols} for row in data]
        flattened = [self._flatten_row(r, options.datetime_format) for r in filtered]

        # Write BOM for Excel UTF-8 compatibility
        if options.encoding == "utf-8-sig":
            output.write("\ufeff".encode("utf-8"))

        writer = csv.DictWriter(
            output,
            fieldnames=cols,
            delimiter=options.delimiter,
            lineterminator="\n",
            extrasaction="ignore",
        )
        if options.include_header:
            writer.writeheader()
        writer.writerows(flattened)


# ─────────────────────────────────────────────────────────────────────────────
# JSON Formatter
# ─────────────────────────────────────────────────────────────────────────────

class JSONFormatter(ExportFormatter):
    """Pretty-printed JSON."""

    extension = ".json"

    def export(self, data: list[dict[str, Any]], options: ExportOptions, output: BinaryIO) -> None:
        if options.columns:
            data = [{k: row.get(k) for k in options.columns} for row in data]
        flattened = [self._flatten_row(r, options.datetime_format) for r in data]
        text = json.dumps(flattened, ensure_ascii=False, indent=2)
        output.write(text.encode(options.encoding or "utf-8"))


# ─────────────────────────────────────────────────────────────────────────────
# Excel Formatter (.xlsx via openpyxl if available)
# ─────────────────────────────────────────────────────────────────────────────

class ExcelFormatter(ExportFormatter):
    """Excel 2007+ (.xlsx) via openpyxl."""

    extension = ".xlsx"

    def __init__(self) -> None:
        self._openpyxl = self._import_openpyxl()

    @staticmethod
    def _import_openpyxl():
        try:
            import openpyxl
            return openpyxl
        except ImportError:
            raise ImportError("openpyxl is required for Excel export: pip install openpyxl")

    def export(self, data: list[dict[str, Any]], options: ExportOptions, output: BinaryIO) -> None:
        if not data:
            output.write(b"")
            return

        openpyxl = self._openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = options.sheet_name

        cols = options.columns or list(data[0].keys())
        filtered = [{k: row.get(k, "") for k in cols} for row in data]
        flattened = [self._flatten_row(r, options.datetime_format) for r in filtered]

        if options.include_header:
            ws.append(cols)
            for cell in ws[1]:
                cell.font = openpyxl.styles.Font(bold=True)

        for row in flattened:
            ws.append([row.get(c, "") for c in cols])

        wb.save(output)


# ─────────────────────────────────────────────────────────────────────────────
# PDF Formatter (basic table layout via reportlab if available)
# ─────────────────────────────────────────────────────────────────────────────

class PDFFormatter(ExportFormatter):
    """PDF export using reportlab."""

    extension = ".pdf"

    def __init__(self) -> None:
        self._reportlab = self._import_reportlab()

    @staticmethod
    def _import_reportlab():
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib import colors
            return (A4, SimpleDocTemplate, Table, TableStyle, Paragraph, getSampleStyleSheet, colors)
        except ImportError:
            raise ImportError("reportlab is required for PDF export: pip install reportlab")

    def export(self, data: list[dict[str, Any]], options: ExportOptions, output: BinaryIO) -> None:
        if not data:
            output.write(b"")
            return

        A4, SimpleDocTemplate, Table, TableStyle, Paragraph, getSampleStyleSheet, colors = self._reportlab

        page_size = A4
        doc = SimpleDocTemplate(
            output,
            pagesize=page_size,
            leftMargin=40,
            rightMargin=40,
            topMargin=60,
            bottomMargin=40,
        )

        cols = options.columns or list(data[0].keys())
        flattened = [self._flatten_row(r, options.datetime_format) for r in data]

        table_data = [cols] + [[row.get(c, "") for c in cols] for row in flattened]
        tbl = Table(table_data, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4472C4")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#D9E2F3"), colors.white]),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("PADDING", (0, 0), (-1, -1), 4),
        ]))

        elements: list[Any] = []
        if options.title:
            styles = getSampleStyleSheet()
            elements.append(Paragraph(options.title, styles["Title"]))
        elements.append(tbl)

        doc.build(elements)


# ─────────────────────────────────────────────────────────────────────────────
# Export Manager
# ─────────────────────────────────────────────────────────────────────────────

class ExportManager:
    """
    Unified export dispatcher. Routes data to the correct formatter
    based on file extension or explicit format name.

    Usage:
        manager = ExportManager()
        manager.export(data, "report.xlsx", options)
        manager.export(data, "/path/report.csv", ExportOptions(delimiter=";"))
    """

    def __init__(self) -> None:
        self._formatters: dict[str, ExportFormatter] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register built-in formatters."""
        for fmt in [CSVFormatter(), JSONFormatter()]:
            self.register(fmt)

        # Excel and PDF are optional (soft import)
        for name, cls in [("excel", ExcelFormatter), ("pdf", PDFFormatter)]:
            try:
                self.register(cls())
            except ImportError as e:
                logger.debug("Formatter '%s' not available: %s", name, e)

    def register(self, formatter: ExportFormatter) -> None:
        """Register a custom formatter."""
        self._formatters[formatter.extension] = formatter

    def export(
        self,
        data: list[dict[str, Any]],
        output_path: str | Path,
        options: ExportOptions | None = None,
        format_hint: str | None = None,
    ) -> Path:
        """
        Export data to file. Returns the output Path.

        Args:
            data: List of row dictionaries.
            output_path: File path. Extension determines format.
            options: Export options (optional).
            format_hint: Override format (extension or name like "excel").
        """
        opts = options or ExportOptions()
        path = Path(output_path)

        if format_hint:
            ext = format_hint if format_hint.startswith(".") else f".{format_hint}"
        else:
            ext = path.suffix.lower()

        formatter = self._formatters.get(ext)
        if not formatter:
            available = ", ".join(self._formatters.keys())
            raise ValueError(f"No formatter for '{ext}'. Available: {available}")

        with path.open("wb" if ext != ".json" else "w", encoding=opts.encoding) as f:
            formatter.export(data, opts, f)

        logger.info("Exported %d rows to %s", len(data), path)
        return path

    def export_to_bytes(
        self,
        data: list[dict[str, Any]],
        format_hint: str,
        options: ExportOptions | None = None,
    ) -> bytes:
        """Export to bytes (useful for HTTP responses)."""
        opts = options or ExportOptions()
        ext = format_hint if format_hint.startswith(".") else f".{format_hint}"
        formatter = self._formatters.get(ext)
        if not formatter:
            raise ValueError(f"No formatter for '{ext}'")

        buf = io.BytesIO() if ext != ".json" else io.StringIO()
        formatter.export(data, opts, buf)
        return buf.getvalue()
