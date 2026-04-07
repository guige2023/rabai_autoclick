"""Data converter action for format conversion.

This module provides data format conversion including
JSON, CSV, XML, YAML, and binary formats.

Example:
    >>> action = DataConverterAction()
    >>> result = action.execute(operation="json_to_csv", data='{"a":1}')
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ConversionResult:
    """Result from data conversion."""
    success: bool
    data: Any = None
    format: str = ""
    error: Optional[str] = None


class DataConverterAction:
    """Data format conversion action.

    Converts between common data formats including
    JSON, CSV, XML, YAML, and encoded formats.

    Example:
        >>> action = DataConverterAction()
        >>> result = action.execute(
        ...     operation="csv_to_json",
        ...     data="a,b\\nc,d"
        ... )
    """

    def __init__(self) -> None:
        """Initialize data converter."""
        self._last_conversion: Optional[ConversionResult] = None

    def execute(
        self,
        operation: str,
        data: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute conversion operation.

        Args:
            operation: Conversion operation (json_to_csv, etc.).
            data: Input data to convert.
            **kwargs: Additional parameters.

        Returns:
            Conversion result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        # JSON conversions
        if op == "json_to_csv":
            result.update(self._json_to_csv(data, kwargs.get("delimiter", ",")))
            result["format"] = "csv"
        elif op == "json_to_xml":
            result.update(self._json_to_xml(data))
            result["format"] = "xml"
        elif op == "json_to_yaml":
            result.update(self._json_to_yaml(data))
            result["format"] = "yaml"
        elif op == "csv_to_json":
            result.update(self._csv_to_json(data, kwargs.get("delimiter", ",")))
            result["format"] = "json"
        elif op == "xml_to_json":
            result.update(self._xml_to_json(data))
            result["format"] = "json"
        elif op == "yaml_to_json":
            result.update(self._yaml_to_json(data))
            result["format"] = "json"
        elif op == "dict_to_json":
            result["data"] = json.dumps(data, indent=kwargs.get("indent", 2), ensure_ascii=False)
            result["format"] = "json"
        elif op == "json_to_dict":
            result["data"] = json.loads(data) if isinstance(data, str) else data
            result["format"] = "dict"
        elif op == "json_prettify":
            result["data"] = json.dumps(json.loads(data) if isinstance(data, str) else data, indent=2)
            result["format"] = "json"
        elif op == "json_minify":
            result["data"] = json.dumps(json.loads(data) if isinstance(data, str) else data, separators=(",", ":"))
            result["format"] = "json"

        # Encoding conversions
        elif op == "to_base64":
            result.update(self._to_base64(data))
            result["format"] = "base64"
        elif op == "from_base64":
            result.update(self._from_base64(data))
            result["format"] = "string"
        elif op == "to_hex":
            result.update(self._to_hex(data))
            result["format"] = "hex"
        elif op == "from_hex":
            result.update(self._from_hex(data))
            result["format"] = "string"
        elif op == "to_urlencoding":
            result.update(self._to_urlencoding(data))
            result["format"] = "urlencoded"
        elif op == "from_urlencoding":
            result.update(self._from_urlencoding(data))
            result["format"] = "string"

        # Type conversions
        elif op == "to_string":
            result["data"] = str(data)
            result["format"] = "string"
        elif op == "to_int":
            try:
                result["data"] = int(data)
                result["format"] = "int"
            except (ValueError, TypeError) as e:
                result["success"] = False
                result["error"] = str(e)
        elif op == "to_float":
            try:
                result["data"] = float(data)
                result["format"] = "float"
            except (ValueError, TypeError) as e:
                result["success"] = False
                result["error"] = str(e)
        elif op == "to_bool":
            result["data"] = bool(data)
            result["format"] = "bool"
        elif op == "to_list":
            result["data"] = list(data) if not isinstance(data, list) else data
            result["format"] = "list"
        elif op == "to_dict":
            result["data"] = dict(data) if not isinstance(data, dict) else data
            result["format"] = "dict"

        # Array conversions
        elif op == "list_to_dict":
            key = kwargs.get("key")
            if not key:
                return {"success": False, "error": "key required for list_to_dict"}
            result["data"] = {item.get(key) if isinstance(item, dict) else item: item for item in data}
            result["format"] = "dict"
        elif op == "dict_to_list":
            result["data"] = list(data.values()) if isinstance(data, dict) else data
            result["format"] = "list"

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

        return result

    def _json_to_csv(self, data: Any, delimiter: str) -> dict[str, Any]:
        """Convert JSON to CSV.

        Args:
            data: JSON data (list or dict).
            delimiter: CSV delimiter.

        Returns:
            Result dictionary.
        """
        import csv
        import io

        if isinstance(data, str):
            data = json.loads(data)

        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list):
            return {"success": False, "error": "Data must be list or dict"}

        if not data:
            return {"data": "", "success": True}

        output = io.StringIO()
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(data)

        return {"data": output.getvalue()}

    def _csv_to_json(self, data: str, delimiter: str) -> dict[str, Any]:
        """Convert CSV to JSON.

        Args:
            data: CSV string.
            delimiter: CSV delimiter.

        Returns:
            Result dictionary.
        """
        import csv
        import io

        reader = csv.DictReader(io.StringIO(data), delimiter=delimiter)
        rows = list(reader)

        return {"data": rows}

    def _json_to_xml(self, data: Any, root: str = "root") -> dict[str, Any]:
        """Convert JSON to XML.

        Args:
            data: JSON data.
            root: Root element name.

        Returns:
            Result dictionary.
        """
        def json_to_xml_element(key: str, value: Any) -> str:
            if isinstance(value, dict):
                children = "".join(json_to_xml_element(k, v) for k, v in value.items())
                return f"<{key}>{children}</{key}>"
            elif isinstance(value, list):
                items = "".join(json_to_xml_element("item", v) for v in value)
                return f"<{key}>{items}</{key}>"
            else:
                return f"<{key}>{value}</{key}>"

        try:
            if isinstance(data, str):
                data = json.loads(data)

            xml = f"<{root}>" + "".join(json_to_xml_element(k, v) for k, v in data.items()) + f"</{root}>"
            return {"data": xml}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _xml_to_json(self, data: str) -> dict[str, Any]:
        """Convert XML to JSON.

        Args:
            data: XML string.

        Returns:
            Result dictionary.
        """
        try:
            import xmltodict
            result = xmltodict.parse(data)
            return {"data": json.loads(json.dumps(result))}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _json_to_yaml(self, data: Any) -> dict[str, Any]:
        """Convert JSON to YAML.

        Args:
            data: JSON data.

        Returns:
            Result dictionary.
        """
        try:
            import yaml
            if isinstance(data, str):
                data = json.loads(data)
            return {"data": yaml.dump(data, allow_unicode=True)}
        except ImportError:
            return {"success": False, "error": "PyYAML not installed"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _yaml_to_json(self, data: str) -> dict[str, Any]:
        """Convert YAML to JSON.

        Args:
            data: YAML string.

        Returns:
            Result dictionary.
        """
        try:
            import yaml
            parsed = yaml.safe_load(data)
            return {"data": parsed}
        except ImportError:
            return {"success": False, "error": "PyYAML not installed"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _to_base64(self, data: Any) -> dict[str, Any]:
        """Convert to base64.

        Args:
            data: Input data.

        Returns:
            Result dictionary.
        """
        try:
            if isinstance(data, dict):
                data = json.dumps(data)
            if isinstance(data, str):
                data = data.encode()
            return {"data": base64.b64encode(data).decode()}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _from_base64(self, data: str) -> dict[str, Any]:
        """Decode base64.

        Args:
            data: Base64 string.

        Returns:
            Result dictionary.
        """
        try:
            decoded = base64.b64decode(data.encode()).decode()
            # Try to parse as JSON
            try:
                return {"data": json.loads(decoded)}
            except (json.JSONDecodeError, TypeError):
                return {"data": decoded}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _to_hex(self, data: Any) -> dict[str, Any]:
        """Convert to hex.

        Args:
            data: Input data.

        Returns:
            Result dictionary.
        """
        try:
            if isinstance(data, str):
                data = data.encode()
            return {"data": data.hex()}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _from_hex(self, data: str) -> dict[str, Any]:
        """Decode hex.

        Args:
            data: Hex string.

        Returns:
            Result dictionary.
        """
        try:
            decoded = bytes.fromhex(data)
            return {"data": decoded.decode()}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _to_urlencoding(self, data: Any) -> dict[str, Any]:
        """Convert to URL encoding.

        Args:
            data: Input data.

        Returns:
            Result dictionary.
        """
        try:
            from urllib.parse import urlencode
            if isinstance(data, dict):
                return {"data": urlencode(data)}
            return {"data": urlencode({"data": data})}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _from_urlencoding(self, data: str) -> dict[str, Any]:
        """Decode URL encoding.

        Args:
            data: URL encoded string.

        Returns:
            Result dictionary.
        """
        try:
            from urllib.parse import parse_qs
            parsed = parse_qs(data)
            return {"data": {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}}
        except Exception as e:
            return {"success": False, "error": str(e)}
