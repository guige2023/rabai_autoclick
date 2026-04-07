"""
JSON Data Handler Action Module.

Provides JSON parsing, validation, schema checking, path querying,
patching, and streaming parsing for large JSON files.

Example:
    >>> from json_handler_action import JSONHandler, JSONSchema
    >>> handler = JSONHandler()
    >>> data = handler.parse_file("/tmp/data.json")
    >>> value = handler.query(data, "$.store.book[*].title")
    >>> handler.validate(data, schema)
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Iterator


@dataclass
class JSONSchema:
    """Minimal JSON schema validator definition."""
    type: Optional[str] = None
    properties: dict[str, "JSONSchema"] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    items: Optional["JSONSchema"] = None
    enum: list[Any] = field(default_factory=list)
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None


@dataclass
class JSONPathResult:
    """Result of a JSON path query."""
    path: str
    value: Any
    parent: Any
    parent_path: str


class JSONHandler:
    """Handle JSON parsing, querying, patching, and validation."""

    def __init__(self):
        self._last_error: Optional[str] = None

    def parse_string(self, json_str: str) -> Any:
        """Parse JSON string to Python object."""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            self._last_error = str(e)
            return None

    def parse_file(self, path: str) -> Any:
        """Parse JSON from file."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            self._last_error = f"File not found: {path}"
            return None
        except json.JSONDecodeError as e:
            self._last_error = str(e)
            return None

    def to_string(self, obj: Any, indent: int = 2, sort_keys: bool = False) -> str:
        """Serialize Python object to JSON string."""
        return json.dumps(obj, indent=indent, sort_keys=sort_keys, ensure_ascii=False, default=str)

    def to_file(self, obj: Any, path: str, indent: int = 2, sort_keys: bool = False) -> bool:
        """Write Python object to JSON file."""
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, indent=indent, sort_keys=sort_keys, ensure_ascii=False, default=str)
            return True
        except Exception as e:
            self._last_error = str(e)
            return False

    def query(self, data: Any, path: str) -> list[Any]:
        """
        Query JSON using JSONPath-like syntax.

        Supports:
            $.key - root key
            $.key.nested - nested key
            $.key[0] - array index
            $.key[*] - all array elements
            $.key[?@.field=="value"] - filter

        Args:
            data: Parsed JSON data
            path: JSONPath expression

        Returns:
            List of matched values
        """
        if path.startswith("$."):
            path = path[2:]
        parts = self._parse_path(path)
        return self._query_recursive(data, parts)

    def _parse_path(self, path: str) -> list[str]:
        tokens: list[str] = []
        current = ""
        i = 0
        while i < len(path):
            c = path[i]
            if c == ".":
                if current:
                    tokens.append(current)
                    current = ""
            elif c == "[":
                if current:
                    tokens.append(current)
                    current = ""
                j = path.index("]", i)
                token = path[i + 1:j]
                tokens.append(f"[{token}]")
                i = j
            else:
                current += c
            i += 1
        if current:
            tokens.append(current)
        return tokens

    def _query_recursive(self, data: Any, parts: list[str]) -> list[Any]:
        if not parts:
            return [data]

        part = parts[0]
        rest = parts[1:]

        if part == "*":
            if isinstance(data, dict):
                results = list(data.values())
            elif isinstance(data, list):
                results = list(data)
            else:
                results = [data]
        elif part.startswith("[?@"):
            filter_expr = part[3:-1]
            if isinstance(data, list):
                filtered = [item for item in data if self._filter_matches(item, filter_expr)]
                results = filtered
            else:
                results = [data] if self._filter_matches(data, filter_expr) else []
        elif part.startswith("[") and part.endswith("]"):
            idx_str = part[1:-1]
            if isinstance(data, list):
                if idx_str == "*":
                    results = list(data)
                elif "," in idx_str:
                    indices = [int(x) for x in idx_str.split(",")]
                    results = [data[i] for i in indices if 0 <= i < len(data)]
                elif idx_str.startswith("-") or idx_str.isdigit():
                    idx = int(idx_str)
                    results = [data[idx]] if -len(data) <= idx < len(data) else []
                else:
                    results = list(data)
            else:
                results = []
        elif isinstance(data, dict):
            results = [data.get(part)] if part in data else []
        else:
            results = []

        if not rest:
            return [r for r in results if r is not None]

        combined: list[Any] = []
        for item in results:
            combined.extend(self._query_recursive(item, rest))
        return combined

    def _filter_matches(self, item: Any, expr: str) -> bool:
        m = re.match(r"@\.(\w+)([<>=!]+)(.+)", expr)
        if not m:
            return False
        field_name, op, value = m.group(1), m.group(2), m.group(3).strip('"\'')
        val = item.get(field_name) if isinstance(item, dict) else None
        value = self._coerce_value(value, val)
        if op == "==":
            return val == value
        elif op == "!=":
            return val != value
        elif op == ">":
            return val is not None and val > value
        elif op == "<":
            return val is not None and val < value
        elif op == ">=":
            return val is not None and val >= value
        elif op == "<=":
            return val is not None and val <= value
        return False

    def _coerce_value(self, s: str, ref: Any) -> Any:
        if s == "null":
            return None
        if s == "true":
            return True
        if s == "false":
            return False
        if isinstance(ref, bool):
            return s.lower() == "true"
        try:
            if "." in s:
                return float(s)
            return int(s)
        except ValueError:
            return s.strip('"\'')

    def patch(self, data: Any, path: str, value: Any) -> Any:
        """Patch JSON data at path with new value (returns new object)."""
        import copy
        data = copy.deepcopy(data)
        if path.startswith("$."):
            path = path[2:]
        parts = self._parse_path(path)
        if not parts:
            return data
        self._patch_recursive(data, parts, value)
        return data

    def _patch_recursive(self, data: Any, parts: list[str], value: Any) -> bool:
        if len(parts) == 1:
            part = parts[0]
            if part.startswith("[") and part.endswith("]"):
                idx = int(part[1:-1])
                if isinstance(data, list):
                    if idx < 0:
                        idx = len(data) + idx
                    if 0 <= idx < len(data):
                        data[idx] = value
                        return True
            elif isinstance(data, dict):
                data[part] = value
                return True
            return False
        part = parts[0]
        rest = parts[1:]
        if part.startswith("[") and part.endswith("]"):
            idx = int(part[1:-1])
            if isinstance(data, list):
                if idx < 0:
                    idx = len(data) + idx
                if 0 <= idx < len(data):
                    return self._patch_recursive(data[idx], rest, value)
        elif isinstance(data, dict) and part in data:
            return self._patch_recursive(data[part], rest, value)
        return False

    def diff(self, left: Any, right: Any) -> list[dict[str, Any]]:
        """Compute differences between two JSON structures."""
        diffs: list[dict[str, Any]] = []
        self._diff_recursive("", left, right, diffs)
        return diffs

    def _diff_recursive(self, path: str, left: Any, right: Any, diffs: list) -> None:
        if type(left) != type(right):
            diffs.append({"path": path, "type": "type_changed", "left": left, "right": right})
            return
        if isinstance(left, dict):
            all_keys = set(left.keys()) | set(right.keys())
            for k in all_keys:
                p = f"{path}.{k}" if path else k
                if k not in left:
                    diffs.append({"path": p, "type": "added", "value": right[k]})
                elif k not in right:
                    diffs.append({"path": p, "type": "removed", "value": left[k]})
                else:
                    self._diff_recursive(p, left[k], right[k], diffs)
        elif isinstance(left, list):
            max_len = max(len(left), len(right))
            for i in range(max_len):
                p = f"{path}[{i}]"
                if i >= len(left):
                    diffs.append({"path": p, "type": "added", "value": right[i]})
                elif i >= len(right):
                    diffs.append({"path": p, "type": "removed", "value": left[i]})
                else:
                    self._diff_recursive(p, left[i], right[i], diffs)
        else:
            if left != right:
                diffs.append({"path": path, "type": "value_changed", "left": left, "right": right})

    def validate(self, data: Any, schema: JSONSchema) -> tuple[bool, list[str]]:
        """Validate data against JSONSchema definition."""
        errors: list[str] = []
        self._validate_recursive(data, schema, "", errors)
        return len(errors) == 0, errors

    def _validate_recursive(self, data: Any, schema: JSONSchema, path: str, errors: list[str]) -> None:
        if schema.type:
            type_map = {"string": str, "number": (int, float), "integer": int, "boolean": bool, "array": list, "object": dict, "null": type(None)}
            expected = type_map.get(schema.type)
            if expected and not isinstance(data, expected):
                errors.append(f"{path}: expected {schema.type}, got {type(data).__name__}")
                return

        if schema.enum and data not in schema.enum:
            errors.append(f"{path}: value not in enum {schema.enum}")

        if schema.minimum is not None and isinstance(data, (int, float)) and data < schema.minimum:
            errors.append(f"{path}: {data} < minimum {schema.minimum}")

        if schema.maximum is not None and isinstance(data, (int, float)) and data > schema.maximum:
            errors.append(f"{path}: {data} > maximum {schema.maximum}")

        if schema.min_length is not None and isinstance(data, (str, list)) and len(data) < schema.min_length:
            errors.append(f"{path}: length {len(data)} < min_length {schema.min_length}")

        if schema.max_length is not None and isinstance(data, (str, list)) and len(data) > schema.max_length:
            errors.append(f"{path}: length {len(data)} > max_length {schema.max_length}")

        if schema.pattern and isinstance(data, str):
            if not re.search(schema.pattern, data):
                errors.append(f"{path}: pattern mismatch")

        if schema.properties and isinstance(data, dict):
            for field_name, field_schema in schema.properties.items():
                if field_name in data:
                    self._validate_recursive(data[field_name], field_schema, f"{path}.{field_name}", errors)
            for required in schema.required:
                if required not in data:
                    errors.append(f"{path}: missing required field '{required}'")

        if schema.items and isinstance(data, list):
            for i, item in enumerate(data):
                self._validate_recursive(item, schema.items, f"{path}[{i}]", errors)

    def stream_parse(self, path: str, chunk_size: int = 1000) -> Iterator[list[Any]]:
        """Stream parse large JSON array file in chunks."""
        with open(path, "r", encoding="utf-8") as f:
            first_char = f.read(1)
            if first_char != "[":
                return
            f.seek(0)
            decoder = json.JSONDecoder()
            buffer = ""
            count = 0
            chunk: list[Any] = []
            while True:
                chunk_str = f.read(chunk_size)
                if not chunk_str:
                    break
                buffer += chunk_str
                while buffer:
                    try:
                        obj, idx = decoder.raw_decode(buffer)
                        chunk.append(obj)
                        buffer = buffer[idx:].lstrip()
                        if len(chunk) >= 100:
                            yield chunk
                            chunk = []
                    except json.JSONDecodeError:
                        break
            if chunk:
                yield chunk

    def flatten(self, data: Any, separator: str = ".") -> dict[str, Any]:
        """Flatten nested JSON to dot-notation keys."""
        result: dict[str, Any] = {}

        def _flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_key = f"{prefix}{separator}{k}" if prefix else k
                    _flatten(v, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    _flatten(item, f"{prefix}[{i}]")
            else:
                result[prefix] = obj

        _flatten(data)
        return result

    def get_error(self) -> Optional[str]:
        return self._last_error


if __name__ == "__main__":
    handler = JSONHandler()
    data = {"store": {"book": [{"title": "Book 1", "price": 10}, {"title": "Book 2", "price": 20}]}}
    titles = handler.query(data, "$.store.book[*].title")
    print(f"Titles: {titles}")
    patched = handler.patch(data, "$.store.book[0].price", 15)
    print(f"Patched: {patched}")
