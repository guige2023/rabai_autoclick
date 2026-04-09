"""
Data Patch Action Module.

JSON Patch (RFC 6902) implementation for automation with
apply, diff, and validate operations.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class PatchOperation(Enum):
    """JSON Patch operations."""
    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"
    MOVE = "move"
    COPY = "copy"
    TEST = "test"
    NONE = "none"


@dataclass
class PatchOperation:
    """A single patch operation."""
    op: PatchOperation
    path: str
    value: Any = None
    from_path: Optional[str] = None
    path_tokens: List[str] = field(default_factory=list)


@dataclass
class PatchResult:
    """Result of applying a patch."""
    success: bool
    patched: bool = False
    message: str = ""
    data: Any = None
    error: Optional[str] = None


@dataclass
class PatchStats:
    """Statistics for patch operations."""
    operations_applied: int = 0
    operations_failed: int = 0
    add_count: int = 0
    remove_count: int = 0
    replace_count: int = 0
    move_count: int = 0
    copy_count: int = 0


class DataPatchAction:
    """
    JSON Patch (RFC 6902) implementation for data manipulation.

    Supports applying patches, computing diffs, and validating patches
    against data structures.

    Example:
        patcher = DataPatchAction()

        # Apply patch
        result = patcher.apply(
            data={"name": "Alice", "age": 30},
            patch=[
                {"op": "replace", "path": "/name", "value": "Bob"},
                {"op": "add", "path": "/city", "value": "NYC"},
            ]
        )

        # Compute diff
        patch = patcher.diff(old_data, new_data)
    """

    def __init__(self) -> None:
        self._stats = PatchStats()

    def _parse_path(self, path: str) -> List[str]:
        """Parse a JSON pointer path into tokens."""
        if not path or path == "/":
            return []
        # Remove leading /
        path = path.lstrip("/")
        # Split and unescape
        tokens = path.split("/")
        return [t.replace("~1", "/").replace("~0", "~") for t in tokens]

    def _get_value(self, data: Any, path_tokens: List[str]) -> tuple[Any, Any, str]:
        """Get value at path, returning (parent, value, full_path)."""
        if not path_tokens:
            return None, data, ""

        current = data
        for i, token in enumerate(path_tokens[:-1]):
            if isinstance(current, dict):
                current = current.get(token)
            elif isinstance(current, list):
                try:
                    idx = int(token)
                    current = current[idx]
                except (ValueError, IndexError):
                    return None, None, "/".join(path_tokens[:i+1])
            else:
                return None, None, "/".join(path_tokens[:i])

        final_token = path_tokens[-1]
        if isinstance(current, dict):
            return current, current.get(final_token), "/".join(path_tokens)
        elif isinstance(current, list):
            try:
                idx = int(final_token)
                return current, current[idx], "/".join(path_tokens)
            except (ValueError, IndexError):
                return current, None, "/".join(path_tokens)
        return current, None, "/".join(path_tokens)

    def _set_value(self, data: Any, path_tokens: List[str], value: Any) -> None:
        """Set value at path in data structure (mutates data)."""
        if not path_tokens:
            return

        current = data
        for token in path_tokens[:-1]:
            if isinstance(current, dict):
                current = current.setdefault(token, {})
            elif isinstance(current, list):
                idx = int(token)
                while len(current) <= idx:
                    current.append(None)
                if current[idx] is None:
                    current[idx] = {}
                current = current[idx]

        final_token = path_tokens[-1]
        if isinstance(current, dict):
            current[final_token] = value
        elif isinstance(current, list):
            idx = int(final_token)
            while len(current) <= idx:
                current.append(None)
            current[idx] = value

    def _remove_value(self, data: Any, path_tokens: List[str]) -> None:
        """Remove value at path from data structure."""
        if not path_tokens:
            return

        current = data
        for token in path_tokens[:-1]:
            if isinstance(current, dict):
                current = current.get(token, {})
            elif isinstance(current, list):
                idx = int(token)
                if idx < len(current):
                    current = current[idx]
                else:
                    return
            else:
                return

        final_token = path_tokens[-1]
        if isinstance(current, dict):
            current.pop(final_token, None)
        elif isinstance(current, list):
            idx = int(final_token)
            if idx < len(current):
                current.pop(idx)

    def _clone_data(self, data: Any) -> Any:
        """Deep clone data for patching."""
        import copy
        return copy.deepcopy(data)

    def apply(
        self,
        data: Union[Dict, List],
        patch: List[Dict[str, Any]],
        validate_only: bool = False,
    ) -> PatchResult:
        """Apply a JSON Patch to data."""
        working = self._clone_data(data)
        applied = False

        for op_def in patch:
            try:
                op_str = op_def.get("op", "")
                path = op_def.get("path", "/")
                value = op_def.get("value")
                from_path = op_def.get("from")
                tokens = self._parse_path(path)

                if op_str == "add":
                    self._set_value(working, tokens, value)
                    self._stats.add_count += 1
                    applied = True

                elif op_str == "remove":
                    self._remove_value(working, tokens)
                    self._stats.remove_count += 1
                    applied = True

                elif op_str == "replace":
                    self._set_value(working, tokens, value)
                    self._stats.replace_count += 1
                    applied = True

                elif op_str == "move":
                    if from_path:
                        from_tokens = self._parse_path(from_path)
                        _, val, _ = self._get_value(working, from_tokens)
                        self._remove_value(working, from_tokens)
                        self._set_value(working, tokens, val)
                        self._stats.move_count += 1
                        applied = True

                elif op_str == "copy":
                    if from_path:
                        from_tokens = self._parse_path(from_path)
                        _, val, _ = self._get_value(working, from_tokens)
                        self._set_value(working, tokens, self._clone_data(val))
                        self._stats.copy_count += 1
                        applied = True

                elif op_str == "test":
                    _, val, _ = self._get_value(working, tokens)
                    if val != value:
                        return PatchResult(
                            success=False,
                            patched=False,
                            message=f"Test failed at {path}: expected {value}, got {val}",
                            error="Test operation failed",
                        )

                else:
                    return PatchResult(
                        success=False,
                        patched=False,
                        message=f"Unknown operation: {op_str}",
                        error="Invalid operation",
                    )

                self._stats.operations_applied += 1

            except Exception as e:
                self._stats.operations_failed += 1
                return PatchResult(
                    success=False,
                    patched=False,
                    message=f"Patch failed: {e}",
                    error=str(e),
                )

        if validate_only:
            return PatchResult(success=True, patched=False, data=data)

        return PatchResult(
            success=True,
            patched=applied,
            data=working,
        )

    def diff(
        self,
        old_data: Any,
        new_data: Any,
    ) -> List[Dict[str, Any]]:
        """Compute the patch needed to transform old_data to new_data."""
        patch: List[Dict[str, Any]] = []

        def compute_diff(
            old: Any,
            new: Any,
            path: str,
        ) -> None:
            if old == new:
                return

            if old is None or new is None:
                patch.append({"op": "replace", "path": path or "/", "value": new})
                return

            if isinstance(old, dict) and isinstance(new, dict):
                all_keys = set(old.keys()) | set(new.keys())
                for key in all_keys:
                    child_path = f"{path}/{key}" if path else f"/{key}"
                    if key not in new:
                        patch.append({"op": "remove", "path": child_path})
                    elif key not in old:
                        patch.append({"op": "add", "path": child_path, "value": new[key]})
                    else:
                        compute_diff(old[key], new[key], child_path)

            elif isinstance(old, list) and isinstance(new, list):
                max_len = max(len(old), len(new))
                for i in range(max_len):
                    child_path = f"{path}/{i}" if path else f"/{i}"
                    if i >= len(old):
                        patch.append({"op": "add", "path": child_path, "value": new[i]})
                    elif i >= len(new):
                        patch.append({"op": "remove", "path": child_path})
                    else:
                        compute_diff(old[i], new[i], child_path)

            else:
                patch.append({"op": "replace", "path": path or "/", "value": new})

        compute_diff(old_data, new_data, "")
        return patch

    def validate(
        self,
        patch: List[Dict[str, Any]],
    ) -> tuple[bool, List[str]]:
        """Validate a patch without applying it."""
        errors = []

        for i, op_def in enumerate(patch):
            if "op" not in op_def:
                errors.append(f"Operation {i}: missing 'op' field")

            op = op_def.get("op", "")
            if op not in [e.value for e in PatchOperation]:
                errors.append(f"Operation {i}: unknown op '{op}'")

            if "path" not in op_def:
                errors.append(f"Operation {i}: missing 'path' field")

            if op in ("add", "replace", "test") and "value" not in op_def:
                errors.append(f"Operation {i}: missing 'value' for '{op}'")

            if op == "move" and "from" not in op_def:
                errors.append(f"Operation {i}: missing 'from' for 'move'")

        return len(errors) == 0, errors

    def get_stats(self) -> PatchStats:
        """Get patch operation statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = PatchStats()
