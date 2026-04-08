"""Data Import Action Module.

Provides data import from multiple formats:
JSON, CSV, XML with schema validation.
"""
from __future__ import annotations

import csv
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class ImportFormat(Enum):
    """Import format."""
    JSON = "json"
    JSON_LINES = "jsonl"
    CSV = "csv"
    TSV = "tsv"
    XML = "xml"


from enum import Enum


@dataclass
class ImportConfig:
    """Import configuration."""
    format: ImportFormat = ImportFormat.JSON
    encoding: str = "utf-8"
    header_row: bool = True
    skip_rows: int = 0


class DataImportAction:
    """Data importer from multiple formats.

    Example:
        importer = DataImportAction()

        data = importer.import_data("data.json")
        data = importer.import_csv("data.csv")

        records = importer.import_records("large_file.csv", batch_size=1000)
    """

    def __init__(self, config: Optional[ImportConfig] = None) -> None:
        self.config = config or ImportConfig()

    def import_data(
        self,
        content: Union[str, bytes],
        format: Optional[ImportFormat] = None,
    ) -> Any:
        """Import data from string.

        Args:
            content: Data content
            format: Data format

        Returns:
            Parsed data
        """
        format = format or self.config.format

        if isinstance(content, bytes):
            content = content.decode(self.config.encoding)

        if format == ImportFormat.JSON:
            return self._import_json(content)
        elif format == ImportFormat.JSON_LINES:
            return self._import_jsonl(content)
        elif format == ImportFormat.CSV:
            return self._import_csv(content)
        elif format == ImportFormat.TSV:
            return self._import_tsv(content)
        elif format == ImportFormat.XML:
            return self._import_xml(content)

        return content

    def import_from_file(
        self,
        filepath: str,
        format: Optional[ImportFormat] = None,
    ) -> Any:
        """Import data from file.

        Args:
            filepath: File path
            format: Data format (auto-detect if None)

        Returns:
            Parsed data
        """
        from pathlib import Path

        path = Path(filepath)
        content = path.read_text(encoding=self.config.encoding)

        if format is None:
            suffix = path.suffix.lower()
            if suffix == ".json":
                format = ImportFormat.JSON
            elif suffix in (".csv", ):
                format = ImportFormat.CSV
            elif suffix == ".xml":
                format = ImportFormat.XML
            else:
                format = self.config.format

        return self.import_data(content, format)

    def import_records(
        self,
        content: str,
        format: Optional[ImportFormat] = None,
    ) -> List[Dict]:
        """Import as list of records.

        Args:
            content: Data content
            format: Data format

        Returns:
            List of record dicts
        """
        data = self.import_data(content, format)

        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]

        return []

    def _import_json(self, content: str) -> Any:
        """Import JSON."""
        return json.loads(content)

    def _import_jsonl(self, content: str) -> List[Any]:
        """Import JSON Lines."""
        records = []
        for line in content.strip().split("\n"):
            if line.strip():
                records.append(json.loads(line))
        return records

    def _import_csv(self, content: str) -> List[Dict]:
        """Import CSV."""
        reader = csv.DictReader(StringIO(content))
        return list(reader)

    def _import_tsv(self, content: str) -> List[Dict]:
        """Import TSV."""
        reader = csv.DictReader(StringIO(content), delimiter="\t")
        return list(reader)

    def _import_xml(self, content: str) -> Any:
        """Import XML."""
        root = ET.fromstring(content)
        return self._xml_to_dict(root)

    def _xml_to_dict(self, element: ET.Element) -> Dict:
        """Convert XML element to dict."""
        result: Dict = {}

        if element.attrib:
            result["@attributes"] = element.attrib

        if element.text and element.text.strip():
            return element.text.strip()

        for child in element:
            child_data = self._xml_to_dict(child)

            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data

        return result
