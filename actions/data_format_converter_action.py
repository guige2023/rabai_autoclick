"""Data format converter action for transforming between data formats.

Converts data between JSON, XML, CSV, YAML, and other formats
with schema validation and error handling.
"""

import csv
import io
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


class FormatType(Enum := __import__("enum").Enum):
    """Supported data format types."""
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    YAML = "yaml"
    DICT = "dict"


@dataclass
class ConversionResult:
    """Result of a format conversion."""
    success: bool
    data: Any
    source_format: str
    target_format: str
    error: Optional[str] = None


class DataFormatConverterAction:
    """Convert data between various format representations.

    Example:
        >>> converter = DataFormatConverterAction()
        >>> result = converter.convert(data, from_format="json", to_format="csv")
    """

    def __init__(self) -> None:
        self._xml_parser: Optional[Any] = None

    def convert(
        self,
        data: Any,
        from_format: str,
        to_format: str,
    ) -> ConversionResult:
        """Convert data from one format to another.

        Args:
            data: Input data to convert.
            from_format: Source format identifier.
            to_format: Target format identifier.

        Returns:
            Conversion result with converted data.
        """
        try:
            parsed = self._parse(data, from_format)
            converted = self._serialize(parsed, to_format)
            return ConversionResult(
                success=True,
                data=converted,
                source_format=from_format,
                target_format=to_format,
            )
        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            return ConversionResult(
                success=False,
                data=None,
                source_format=from_format,
                target_format=to_format,
                error=str(e),
            )

    def _parse(self, data: Any, format_type: str) -> Any:
        """Parse input data based on format.

        Args:
            data: Input data.
            format_type: Format to parse as.

        Returns:
            Parsed data structure.
        """
        fmt = format_type.lower()

        if fmt in ("json", "dict"):
            if isinstance(data, str):
                return json.loads(data)
            return data

        if fmt == "csv":
            return self._parse_csv(data)

        if fmt == "xml":
            return self._parse_xml(data)

        if fmt == "yaml":
            return self._parse_yaml(data)

        raise ValueError(f"Unsupported format: {format_type}")

    def _serialize(self, data: Any, format_type: str) -> Any:
        """Serialize data to target format.

        Args:
            data: Data to serialize.
            format_type: Target format.

        Returns:
            Serialized data.
        """
        fmt = format_type.lower()

        if fmt == "json":
            return json.dumps(data, ensure_ascii=False, indent=2)

        if fmt == "dict":
            if isinstance(data, dict):
                return data
            return json.loads(json.dumps(data))

        if fmt == "csv":
            return self._to_csv(data)

        if fmt == "xml":
            return self._to_xml(data)

        if fmt == "yaml":
            return self._to_yaml(data)

        raise ValueError(f"Unsupported format: {format_type}")

    def _parse_csv(self, data: str) -> list[dict]:
        """Parse CSV data to list of dicts.

        Args:
            data: CSV string.

        Returns:
            List of row dictionaries.
        """
        reader = csv.DictReader(io.StringIO(data))
        return list(reader)

    def _parse_xml(self, data: str) -> dict:
        """Parse XML data to dictionary.

        Args:
            data: XML string.

        Returns:
            Dictionary representation.
        """
        try:
            import xmltodict
            return xmltodict.parse(data)
        except ImportError:
            return self._simple_xml_parse(data)

    def _simple_xml_parse(self, data: str) -> dict:
        """Simple XML parsing without external dependencies.

        Args:
            data: XML string.

        Returns:
            Dictionary representation.
        """
        result: dict[str, Any] = {}
        import re

        for match in re.finditer(r"<(\w+)>(.*?)</\1>", data, re.DOTALL):
            key = match.group(1)
            value = match.group(2).strip()
            result[key] = value

        return result

    def _parse_yaml(self, data: str) -> Any:
        """Parse YAML data.

        Args:
            data: YAML string.

        Returns:
            Parsed YAML data.
        """
        try:
            import yaml
            return yaml.safe_load(data)
        except ImportError:
            logger.warning("PyYAML not available, treating as plain text")
            return data

    def _to_csv(self, data: list[dict]) -> str:
        """Convert list of dicts to CSV.

        Args:
            data: List of row dictionaries.

        Returns:
            CSV string.
        """
        if not data:
            return ""

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    def _to_xml(self, data: dict, root: str = "root") -> str:
        """Convert dictionary to XML.

        Args:
            data: Dictionary to convert.
            root: Root element name.

        Returns:
            XML string.
        """
        def dict_to_xml(d: dict, parent: str) -> str:
            result = []
            for key, value in d.items():
                if isinstance(value, dict):
                    result.append(f"<{key}>{dict_to_xml(value, key)}</{key}>")
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            result.append(f"<{key}>{dict_to_xml(item, key)}</{key}>")
                        else:
                            result.append(f"<{key}>{item}</{key}>")
                else:
                    result.append(f"<{key}>{value}</{key}>")
            return "".join(result)

        return f"<{root}>{dict_to_xml(data, root)}</{root}>"

    def _to_yaml(self, data: Any) -> str:
        """Convert data to YAML.

        Args:
            data: Data to convert.

        Returns:
            YAML string.
        """
        try:
            import yaml
            return yaml.dump(data, allow_unicode=True, default_flow_style=False)
        except ImportError:
            logger.warning("PyYAML not available, returning JSON")
            return json.dumps(data, ensure_ascii=False, indent=2)

    def validate_format(self, data: str, format_type: str) -> bool:
        """Validate if data matches expected format.

        Args:
            data: Data to validate.
            format_type: Format to validate against.

        Returns:
            True if data is valid for format.
        """
        try:
            result = self.convert(data, format_type, "dict")
            return result.success
        except Exception:
            return False
