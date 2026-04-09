"""
Data Projector Action Module.

Project and transform data schemas.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Union


class DataProjectorAction:
    """
    Project and transform data schemas.

    Supports field selection, renaming, and computed projections.
    """

    def __init__(self) -> None:
        self._projections: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
        self._renames: Dict[str, str] = {}

    def add_projection(
        self,
        field: str,
        func: Callable[[Dict[str, Any]], Any],
    ) -> "DataProjectorAction":
        """
        Add a computed projection.

        Args:
            field: Output field name
            func: Function to compute value

        Returns:
            Self for chaining
        """
        self._projections[field] = func
        return self

    def add_rename(
        self,
        from_field: str,
        to_field: str,
    ) -> "DataProjectorAction":
        """
        Add a field rename.

        Args:
            from_field: Original field name
            to_field: New field name

        Returns:
            Self for chaining
        """
        self._renames[from_field] = to_field
        return self

    def project(
        self,
        data: List[Dict[str, Any]],
        fields: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        rename: bool = True,
        compute: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Project data to specified schema.

        Args:
            data: Data to project
            fields: Fields to include (None = all)
            exclude: Fields to exclude
            rename: Apply renames
            compute: Compute projections

        Returns:
            Projected data
        """
        result = []

        for record in data:
            projected = self._project_record(
                record,
                fields=fields,
                exclude=exclude,
                rename=rename,
                compute=compute,
            )
            result.append(projected)

        return result

    def _project_record(
        self,
        record: Dict[str, Any],
        fields: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        rename: bool = True,
        compute: bool = True,
    ) -> Dict[str, Any]:
        """Project a single record."""
        result = {}

        exclude_set = set(exclude or [])

        if rename:
            renames_applied = {}
            for from_f, to_f in self._renames.items():
                if from_f in record:
                    renames_applied[from_f] = to_f

            for from_f, to_f in renames_applied.items():
                value = record[from_f]
                if from_f not in exclude_set and (fields is None or to_f in fields):
                    result[to_f] = value

        else:
            for key, value in record.items():
                if key in exclude_set:
                    continue
                if fields is None or key in fields:
                    result[key] = value

        if compute:
            for field_name, func in self._projections.items():
                if field_name not in exclude_set and (fields is None or field_name in fields):
                    try:
                        result[field_name] = func(record)
                    except Exception:
                        result[field_name] = None

        return result

    def select(
        self,
        data: List[Dict[str, Any]],
        fields: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Select specific fields.

        Args:
            data: Data to select from
            fields: Fields to select

        Returns:
            Data with only selected fields
        """
        return self.project(data, fields=fields, rename=False, compute=False)

    def exclude(
        self,
        data: List[Dict[str, Any]],
        fields: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Exclude specific fields.

        Args:
            data: Data to filter
            fields: Fields to exclude

        Returns:
            Data without excluded fields
        """
        return self.project(data, exclude=fields, rename=False, compute=False)

    def flatten(
        self,
        data: List[Dict[str, Any]],
        prefix: str = "",
        separator: str = ".",
    ) -> List[Dict[str, Any]]:
        """
        Flatten nested structures.

        Args:
            data: Data with nested dicts
            prefix: Field name prefix
            separator: Separator for nested keys

        Returns:
            Flattened data
        """
        result = []

        for record in data:
            flattened = self._flatten_record(record, prefix, separator)
            result.append(flattened)

        return result

    def _flatten_record(
        self,
        record: Dict[str, Any],
        prefix: str,
        separator: str,
    ) -> Dict[str, Any]:
        """Flatten a single record."""
        result = {}

        for key, value in record.items():
            new_key = f"{prefix}{separator}{key}" if prefix else key

            if isinstance(value, dict):
                result.update(self._flatten_record(value, new_key, separator))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        result.update(self._flatten_record(item, f"{new_key}[{i}]", separator))
                    else:
                        result[f"{new_key}[{i}]"] = item
            else:
                result[new_key] = value

        return result

    def unflatten(
        self,
        data: List[Dict[str, Any]],
        separator: str = ".",
    ) -> List[Dict[str, Any]]:
        """
        Unflatten flattened structures.

        Args:
            data: Flattened data
            separator: Separator used in keys

        Returns:
            Nested data
        """
        result = []

        for record in data:
            unflattened = self._unflatten_record(record, separator)
            result.append(unflattened)

        return result

    def _unflatten_record(
        self,
        record: Dict[str, Any],
        separator: str,
    ) -> Dict[str, Any]:
        """Unflatten a single record."""
        result: Dict[str, Any] = {}

        for flat_key, value in record.items():
            keys = flat_key.split(separator)
            current = result

            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            current[keys[-1]] = value

        return result

    def cast_types(
        self,
        data: List[Dict[str, Any]],
        type_map: Dict[str, type],
    ) -> List[Dict[str, Any]]:
        """
        Cast fields to specified types.

        Args:
            data: Data to cast
            type_map: Map of field name to target type

        Returns:
            Data with cast values
        """
        result = []

        for record in data:
            new_record = record.copy()

            for field, target_type in type_map.items():
                if field in new_record:
                    try:
                        new_record[field] = target_type(new_record[field])
                    except (ValueError, TypeError):
                        new_record[field] = None

            result.append(new_record)

        return result
