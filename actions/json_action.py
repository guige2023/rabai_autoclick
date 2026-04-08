"""JSON processing and manipulation utilities.

Handles JSON parsing, validation, transformation,
schema validation, and query operations.
"""

from typing import Any, Optional, Union, Callable
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
import json
import re

logger = logging.getLogger(__name__)


@dataclass
class JSONConfig:
    """Configuration for JSON processing."""
    encoding: str = "utf-8"
    indent: Optional[int] = 2
    ensure_ascii: bool = False
    sort_keys: bool = False
    allow_nan: bool = True


@dataclass
class JSONPatch:
    """Represents a JSON Patch operation."""
    op: str  # add, remove, replace, move, copy, test
    path: str
    value: Any = None
    from_path: Optional[str] = None


@dataclass
class JSONMergePatch:
    """Represents a JSON Merge Patch."""
    patch: dict
    base: dict


class JSONDecodeError(Exception):
    """Raised on JSON decode errors."""
    pass


class JSONValidationError(Exception):
    """Raised on JSON validation errors."""
    pass


class JSONPathError(Exception):
    """Raised on JSONPath errors."""
    pass


class JSONAction:
    """JSON processing utilities."""

    def __init__(self, config: Optional[JSONConfig] = None):
        """Initialize JSON processor with configuration.

        Args:
            config: JSONConfig with processing options
        """
        self.config = config or JSONConfig()
        self._encoder = JSONEncoder(indent=self.config.indent)

    def loads(self, json_string: str) -> Any:
        """Parse JSON string to Python object.

        Args:
            json_string: JSON content as string

        Returns:
            Parsed Python object

        Raises:
            JSONDecodeError: On parse failure
        """
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as e:
            raise JSONDecodeError(f"Parse failed: {e}")

    def dumps(self, obj: Any, **kwargs) -> str:
        """Serialize Python object to JSON string.

        Args:
            obj: Object to serialize
            **kwargs: Override config options

        Returns:
            JSON string
        """
        opts = self._get_dumps_kwargs(kwargs)
        return json.dumps(obj, **opts)

    def load_file(self, file_path: str) -> Any:
        """Load JSON from file.

        Args:
            file_path: Path to JSON file

        Returns:
            Parsed Python object
        """
        with open(file_path, "r", encoding=self.config.encoding) as f:
            return json.load(f)

    def save_file(self, file_path: str, obj: Any, **kwargs) -> bool:
        """Save Python object to JSON file.

        Args:
            file_path: Path to output file
            obj: Object to serialize
            **kwargs: Override config options

        Returns:
            True if successful
        """
        try:
            opts = self._get_dumps_kwargs(kwargs)
            content = json.dumps(obj, **opts)

            with open(file_path, "w", encoding=self.config.encoding) as f:
                f.write(content)

            return True

        except Exception as e:
            logger.error(f"Save file failed: {e}")
            return False

    def validate_schema(self, data: Any, schema: dict) -> bool:
        """Validate data against JSON schema.

        Args:
            data: Data to validate
            schema: JSON schema definition

        Returns:
            True if valid

        Raises:
            JSONValidationError: If invalid
        """
        errors = []

        def validate_recursive(instance: Any, schema_node: dict, path: str) -> None:
            if schema_node is None:
                return

            if "type" in schema_node:
                expected_type = schema_node["type"]
                if not self._check_type(instance, expected_type):
                    errors.append(f"{path}: expected {expected_type}, got {type(instance).__name__}")
                    return

            if expected_type == "object":
                if not isinstance(instance, dict):
                    errors.append(f"{path}: expected object")
                    return

                properties = schema_node.get("properties", {})
                required = schema_node.get("required", [])

                for req in required:
                    if req not in instance:
                        errors.append(f"{path}: missing required field '{req}'")

                for key, value in instance.items():
                    if key in properties:
                        validate_recursive(value, properties[key], f"{path}.{key}")

            elif expected_type == "array":
                if not isinstance(instance, list):
                    errors.append(f"{path}: expected array")
                    return

                items = schema_node.get("items")
                if items:
                    for i, item in enumerate(instance):
                        validate_recursive(item, items, f"{path}[{i}]")

            if "enum" in schema_node:
                if instance not in schema_node["enum"]:
                    errors.append(f"{path}: value not in enum {schema_node['enum']}")

            if "minimum" in schema_node:
                if isinstance(instance, (int, float)) and instance < schema_node["minimum"]:
                    errors.append(f"{path}: below minimum {schema_node['minimum']}")

            if "maximum" in schema_node:
                if isinstance(instance, (int, float)) and instance > schema_node["maximum"]:
                    errors.append(f"{path}: above maximum {schema_node['maximum']}")

            if "pattern" in schema_node:
                if not isinstance(instance, str):
                    errors.append(f"{path}: pattern requires string")
                elif not re.match(schema_node["pattern"], instance):
                    errors.append(f"{path}: pattern mismatch")

        validate_recursive(data, schema, "$")

        if errors:
            raise JSONValidationError("\n".join(errors))

        return True

    def merge_patch(self, base: dict, patch: dict) -> dict:
        """Apply JSON Merge Patch (RFC 7396).

        Args:
            base: Base JSON object
            patch: Merge patch

        Returns:
            Merged object
        """
        result = self._deep_copy(base)

        for key, value in patch.items():
            if value is None:
                if key in result:
                    del result[key]
            elif isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self.merge_patch(result[key], value)
            else:
                result[key] = self._deep_copy(value)

        return result

    def apply_patch(self, data: Any, patches: list[JSONPatch]) -> Any:
        """Apply JSON Patch operations (RFC 6902).

        Args:
            data: Base data
            patches: List of JSONPatch operations

        Returns:
            Patched data
        """
        result = self._deep_copy(data)

        for patch in patches:
            result = self._apply_single_patch(result, patch)

        return result

    def jsonpath(self, data: Any, path: str) -> list:
        """Execute JSONPath query.

        Args:
            data: JSON data
            path: JSONPath expression

        Returns:
            List of matched values
        """
        parts = self._parse_jsonpath(path)
        return self._execute_jsonpath(data, parts)

    def flatten(self, data: Any, separator: str = ".") -> dict:
        """Flatten nested JSON to dot-notation dict.

        Args:
            data: Nested JSON object
            separator: Key separator

        Returns:
            Flattened dict
        """
        result: dict[str, Any] = {}

        def flatten_recursive(obj: Any, prefix: str) -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}{separator}{key}" if prefix else key
                    flatten_recursive(value, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_key = f"{prefix}[{i}]"
                    flatten_recursive(item, new_key)
            else:
                result[prefix] = obj

        flatten_recursive(data, "")
        return result

    def unflatten(self, data: dict, separator: str = ".") -> Any:
        """Unflatten dot-notation dict to nested JSON.

        Args:
            data: Flattened dict
            separator: Key separator

        Returns:
            Nested JSON object
        """
        result: dict[str, Any] = {}

        for flat_key, value in data.items():
            keys = self._split_key(flat_key, separator)
            current = result

            for i, key in enumerate(keys[:-1]):
                if key not in current:
                    next_key = keys[i + 1]
                    if next_key.isdigit():
                        current[key] = []
                    else:
                        current[key] = {}
                current = current[key]

            current[keys[-1]] = value

        return result

    def diff(self, left: Any, right: Any) -> list[dict]:
        """Compute difference between two JSON objects.

        Args:
            left: First object
            right: Second object

        Returns:
            List of difference dicts with path, left, right
        """
        diffs = []

        def diff_recursive(l: Any, r: Any, path: str) -> None:
            if type(l) != type(r):
                diffs.append({"path": path, "left": l, "right": r})
                return

            if isinstance(l, dict):
                all_keys = set(l.keys()) | set(r.keys())
                for key in all_keys:
                    new_path = f"{path}.{key}" if path else key
                    if key not in l:
                        diffs.append({"path": new_path, "left": None, "right": r[key]})
                    elif key not in r:
                        diffs.append({"path": new_path, "left": l[key], "right": None})
                    else:
                        diff_recursive(l[key], r[key], new_path)

            elif isinstance(l, list):
                max_len = max(len(l), len(r))
                for i in range(max_len):
                    new_path = f"{path}[{i}]"
                    if i >= len(l):
                        diffs.append({"path": new_path, "left": None, "right": r[i]})
                    elif i >= len(r):
                        diffs.append({"path": new_path, "left": l[i], "right": None})
                    else:
                        diff_recursive(l[i], r[i], new_path)

            else:
                if l != r:
                    diffs.append({"path": path, "left": l, "right": r})

        diff_recursive(left, right, "$")
        return diffs

    def _get_dumps_kwargs(self, overrides: dict) -> dict:
        """Get kwargs for json.dumps."""
        kwargs = {
            "indent": self.config.indent if "indent" not in overrides else overrides["indent"],
            "ensure_ascii": self.config.ensure_ascii if "ensure_ascii" not in overrides else overrides["ensure_ascii"],
            "sort_keys": self.config.sort_keys if "sort_keys" not in overrides else overrides["sort_keys"],
            "default": self._json_default
        }

        if self.config.allow_nan or overrides.get("allow_nan", True):
            kwargs["allow_nan"] = True

        kwargs.update(overrides)
        return kwargs

    def _json_default(self, obj: Any) -> Any:
        """Default JSON serializer for non-serializable types."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode(self.config.encoding, errors="replace")
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        else:
            return str(obj)

    def _check_type(self, instance: Any, expected_type: str) -> bool:
        """Check if instance matches expected JSON schema type."""
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None)
        }

        expected = type_map.get(expected_type)
        if expected is None:
            return False

        return isinstance(instance, expected)

    def _deep_copy(self, obj: Any) -> Any:
        """Deep copy Python object."""
        if isinstance(obj, dict):
            return {k: self._deep_copy(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_copy(item) for item in obj]
        else:
            return obj

    def _apply_single_patch(self, data: Any, patch: JSONPatch) -> Any:
        """Apply a single JSON Patch operation."""
        if patch.op == "add":
            return self._patch_add(data, patch.path, patch.value)
        elif patch.op == "remove":
            return self._patch_remove(data, patch.path)
        elif patch.op == "replace":
            return self._patch_replace(data, patch.path, patch.value)
        elif patch.op == "move":
            return self._patch_move(data, patch.from_path, patch.path)
        elif patch.op == "copy":
            return self._patch_copy(data, patch.from_path, patch.path)
        elif patch.op == "test":
            if not self._patch_test(data, patch.path, patch.value):
                raise JSONPatchError(f"Test failed at {patch.path}")
            return data

        return data

    def _patch_add(self, data: Any, path: str, value: Any) -> Any:
        """JSON Patch add operation."""
        tokens = self._parse_jsonpath_tokens(path)
        if not tokens:
            return value

        result = self._deep_copy(data)
        current = result

        for i, token in enumerate(tokens[:-1]):
            current = current[token]

        last_token = tokens[-1]
        if last_token.isdigit():
            current.insert(int(last_token), self._deep_copy(value))
        else:
            current[last_token] = self._deep_copy(value)

        return result

    def _patch_remove(self, data: Any, path: str) -> Any:
        """JSON Patch remove operation."""
        tokens = self._parse_jsonpath_tokens(path)
        if not tokens:
            return None

        result = self._deep_copy(data)
        current = result

        for i, token in enumerate(tokens[:-1]):
            current = current[token]

        last_token = tokens[-1]
        if last_token.isdigit():
            del current[int(last_token)]
        else:
            del current[last_token]

        return result

    def _patch_replace(self, data: Any, path: str, value: Any) -> Any:
        """JSON Patch replace operation."""
        return self._patch_remove(self._patch_add(data, path, value), path)

    def _patch_move(self, data: Any, from_path: str, path: str) -> Any:
        """JSON Patch move operation."""
        values = self.jsonpath(data, from_path)
        if not values:
            raise JSONPathError(f"Path not found: {from_path}")
        return self._patch_add(self._patch_remove(data, from_path), path, values[0])

    def _patch_copy(self, data: Any, from_path: str, path: str) -> Any:
        """JSON Patch copy operation."""
        values = self.jsonpath(data, from_path)
        if not values:
            raise JSONPathError(f"Path not found: {from_path}")
        return self._patch_add(data, path, self._deep_copy(values[0]))

    def _patch_test(self, data: Any, path: str, value: Any) -> bool:
        """JSON Patch test operation."""
        values = self.jsonpath(data, path)
        return values and values[0] == value

    def _parse_jsonpath(self, path: str) -> list:
        """Parse JSONPath expression to tokens."""
        path = path.strip()

        if path.startswith("$"):
            path = path[1:]

        if not path.startswith(".") and not path.startswith("["):
            if path:
                return [path]

        tokens = []
        current = ""
        i = 0

        while i < len(path):
            char = path[i]

            if char == ".":
                if current:
                    tokens.append(current)
                    current = ""
                if i + 1 < len(path) and path[i + 1] == ".":
                    i += 1
            elif char == "[":
                if current:
                    tokens.append(current)
                    current = ""
                end = path.find("]", i)
                if end != -1:
                    token = path[i + 1:end]
                    tokens.append(token)
                    i = end
            else:
                current += char

            i += 1

        if current:
            tokens.append(current)

        return tokens

    def _parse_jsonpath_tokens(self, path: str) -> list[str]:
        """Parse path to tokens (for patch operations)."""
        return self._parse_jsonpath(path)

    def _execute_jsonpath(self, data: Any, tokens: list) -> list:
        """Execute parsed JSONPath tokens."""
        if not tokens:
            return [data]

        results = []
        current = [data]

        for token in tokens:
            next_current = []

            for item in current:
                if isinstance(item, dict):
                    if token == "*":
                        next_current.extend(item.values())
                    elif token in item:
                        next_current.append(item[token])
                elif isinstance(item, list):
                    if token == "*":
                        next_current.extend(item)
                    elif token.isdigit():
                        idx = int(token)
                        if 0 <= idx < len(item):
                            next_current.append(item[idx])

            current = next_current

        return current

    def _split_key(self, key: str, separator: str) -> list[str]:
        """Split flat key to nested keys."""
        result = []
        current = ""
        i = 0

        while i < len(key):
            char = key[i]

            if char == separator:
                if current:
                    result.append(current)
                    current = ""
            elif char == "[":
                if current:
                    result.append(current)
                    current = ""
                end = key.find("]", i)
                if end != -1:
                    result.append(key[i + 1:end])
                    i = end
            else:
                current += char

            i += 1

        if current:
            result.append(current)

        return result


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder with extra type support."""

    def __init__(self, indent: Optional[int] = None, **kwargs):
        super().__init__(indent=indent, **kwargs)

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)
