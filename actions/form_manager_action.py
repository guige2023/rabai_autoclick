"""
Form Manager Action Module.

Manages form filling operations including field validation,
auto-complete, field dependencies, and multi-step form flows.
"""

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class FormField:
    """A form field definition."""
    name: str
    field_type: str
    label: str
    required: bool = False
    default_value: Any = None
    validator: Optional[Callable[[Any], bool]] = None
    options: Optional[list[str]] = None
    placeholder: str = ""
    depends_on: Optional[str] = None
    visible_when: Optional[Callable[[dict], bool]] = None


@dataclass
class FieldValue:
    """A field value with validation status."""
    name: str
    value: Any
    valid: bool
    error: Optional[str] = None


class FormManager:
    """Manages form filling and validation."""

    def __init__(self):
        """Initialize form manager."""
        self._fields: dict[str, FormField] = {}
        self._values: dict[str, Any] = {}

    def register_field(self, field: FormField) -> None:
        """
        Register a form field.

        Args:
            field: FormField definition.
        """
        self._fields[field.name] = field
        if field.default_value is not None:
            self._values[field.name] = field.default_value

    def register_fields(self, fields: list[FormField]) -> None:
        """Register multiple form fields."""
        for field in fields:
            self.register_field(field)

    def set_value(self, name: str, value: Any) -> FieldValue:
        """
        Set a field value with validation.

        Args:
            name: Field name.
            value: Value to set.

        Returns:
            FieldValue with validation result.
        """
        field = self._fields.get(name)

        if field is None:
            return FieldValue(name=name, value=value, valid=False, error="Unknown field")

        valid = True
        error = None

        if field.validator:
            try:
                valid = field.validator(value)
                if not valid:
                    error = "Validation failed"
            except Exception as e:
                valid = False
                error = str(e)

        if valid and field.options and value not in field.options:
            valid = False
            error = f"Value must be one of: {', '.join(field.options)}"

        if valid and field.required:
            if value is None or value == "":
                valid = False
                error = "This field is required"

        self._values[name] = value

        return FieldValue(name=name, value=value, valid=valid, error=error)

    def get_value(self, name: str, default: Any = None) -> Any:
        """
        Get a field value.

        Args:
            name: Field name.
            default: Default if not found.

        Returns:
            Field value or default.
        """
        return self._values.get(name, default)

    def get_all_values(self) -> dict[str, Any]:
        """Get all field values."""
        return dict(self._values)

    def validate_all(self) -> list[FieldValue]:
        """
        Validate all field values.

        Returns:
            List of FieldValue objects with validation status.
        """
        results = []
        for name in self._fields:
            value = self._values.get(name)
            field = self._fields[name]

            valid = True
            error = None

            if field.required and (value is None or value == ""):
                valid = False
                error = "This field is required"

            if valid and field.options and value not in field.options:
                valid = False
                error = f"Value must be one of: {', '.join(field.options)}"

            if valid and field.validator:
                try:
                    valid = field.validator(value)
                    if not valid:
                        error = "Validation failed"
                except Exception as e:
                    valid = False
                    error = str(e)

            results.append(FieldValue(name=name, value=value, valid=valid, error=error))

        return results

    def is_valid(self) -> bool:
        """Check if all fields are valid."""
        return all(fv.valid for fv in self.validate_all())

    def get_visible_fields(self) -> list[FormField]:
        """
        Get fields that are currently visible.

        Returns:
            List of visible FormField objects.
        """
        visible = []
        for field in self._fields.values():
            if field.visible_when:
                try:
                    if field.visible_when(self._values):
                        visible.append(field)
                except Exception:
                    pass
            else:
                visible.append(field)
        return visible

    def clear(self) -> None:
        """Clear all field values."""
        self._values.clear()
        for field in self._fields.values():
            if field.default_value is not None:
                self._values[field.name] = field.default_value
