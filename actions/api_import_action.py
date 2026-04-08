"""API Import Action.

Imports data from various formats into API-ready structures.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import csv
import io
import json
import xml.etree.ElementTree as ET


@dataclass
class ImportResult:
    records: List[Dict[str, Any]]
    errors: List[str]
    total_count: int
    success_count: int

    @property
    def success_rate(self) -> float:
        return self.success_count / self.total_count if self.total_count > 0 else 0.0


class APIImportAction:
    """Imports data from various formats."""

    def __init__(self, strict_mode: bool = False) -> None:
        self.strict_mode = strict_mode
        self.transformers: Dict[str, Callable[[Any], Any]] = {}

    def register_transformer(
        self,
        field_name: str,
        transformer: Callable[[Any], Any],
    ) -> None:
        self.transformers[field_name] = transformer

    def import_json(
        self,
        content: str,
    ) -> ImportResult:
        try:
            data = json.loads(content)
            records = [data] if isinstance(data, dict) else data
            return self._normalize_records(records)
        except json.JSONDecodeError as e:
            return ImportResult(records=[], errors=[str(e)], total_count=0, success_count=0)

    def import_csv(
        self,
        content: str,
        delimiter: str = ",",
    ) -> ImportResult:
        try:
            reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
            records = list(reader)
            return self._normalize_records(records)
        except Exception as e:
            return ImportResult(records=[], errors=[str(e)], total_count=0, success_count=0)

    def import_xml(
        self,
        content: str,
    ) -> ImportResult:
        try:
            root = ET.fromstring(content)
            records = []
            for child in root:
                record = {elem.tag: elem.text for elem in child}
                records.append(record)
            return self._normalize_records(records)
        except ET.ParseError as e:
            return ImportResult(records=[], errors=[str(e)], total_count=0, success_count=0)

    def _normalize_records(
        self,
        records: List[Any],
    ) -> ImportResult:
        errors = []
        normalized = []
        for i, record in enumerate(records):
            try:
                if isinstance(record, str):
                    record = json.loads(record)
                if isinstance(record, dict):
                    for field, transformer in self.transformers.items():
                        if field in record:
                            record[field] = transformer(record[field])
                    normalized.append(record)
                else:
                    errors.append(f"Record {i}: not a dict or JSON string")
            except Exception as e:
                errors.append(f"Record {i}: {str(e)}")
        return ImportResult(
            records=normalized,
            errors=errors,
            total_count=len(records),
            success_count=len(normalized),
        )

    def import_from_file(
        self,
        path: str,
        format: Optional[str] = None,
    ) -> ImportResult:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        fmt = format or self._detect_format(path)
        if fmt == "json":
            return self.import_json(content)
        elif fmt == "csv":
            return self.import_csv(content)
        elif fmt == "xml":
            return self.import_xml(content)
        else:
            return ImportResult(records=[], errors=[f"Unknown format: {format}"], total_count=0, success_count=0)

    def _detect_format(self, path: str) -> str:
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        return {"json": "json", "csv": "csv", "xml": "xml"}.get(ext, "json")
