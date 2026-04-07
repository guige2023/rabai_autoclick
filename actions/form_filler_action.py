"""Form filler action for automated form filling and submission.

This module provides form filling capabilities including
field detection, validation, and multi-step form submission.

Example:
    >>> action = FormFillerAction()
    >>> result = action.execute(
    ...     form_data={"username": "user", "password": "pass"},
    ...     submit=True
    ... )
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class FormField:
    """Represents a form field."""
    name: str
    type: str
    selector: str
    value: Any = None
    required: bool = False
    options: list[str] = field(default_factory=list)
    validation: Optional[str] = None


@dataclass
class FormConfig:
    """Configuration for form filling."""
    submit_selector: Optional[str] = None
    submit_method: str = "click"  # click or javascript
    wait_after_submit: float = 2.0
    validate_before_submit: bool = True
    clear_before_fill: bool = True
    fill_delay: float = 0.1


class FormFillerAction:
    """Form filling and submission action.

    Provides automated form filling with field detection,
    validation, and multi-step submission workflows.

    Example:
        >>> action = FormFillerAction()
        >>> result = action.execute(
        ...     operation="fill_and_submit",
        ...     form_data={"email": "test@example.com"},
        ...     submit_selector="#submit-btn"
        ... )
    """

    def __init__(self, config: Optional[FormConfig] = None) -> None:
        """Initialize form filler.

        Args:
            config: Optional form configuration.
        """
        self.config = config or FormConfig()
        self._fields: list[FormField] = []
        self._last_filled: dict[str, Any] = {}

    def execute(
        self,
        operation: str,
        form_data: Optional[dict[str, Any]] = None,
        submit_selector: Optional[str] = None,
        html: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute form operation.

        Args:
            operation: Operation name.
            form_data: Data to fill into form.
            submit_selector: Selector for submit button.
            html: HTML content to parse for form.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "parse":
            if not html:
                raise ValueError("html required for 'parse' operation")
            result.update(self._parse_form(html))

        elif op == "fill":
            if not form_data:
                raise ValueError("form_data required for 'fill' operation")
            result.update(self._fill_form(form_data))

        elif op == "submit":
            result.update(self._submit_form(submit_selector or self.config.submit_selector))

        elif op == "fill_and_submit":
            if not form_data:
                raise ValueError("form_data required")
            result.update(self._fill_form(form_data))
            if result["success"]:
                result.update(self._submit_form(submit_selector or self.config.submit_selector))

        elif op == "validate":
            result.update(self._validate_form())

        elif op == "get_fields":
            result["fields"] = [
                {"name": f.name, "type": f.type, "required": f.required, "options": f.options}
                for f in self._fields
            ]

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _parse_form(self, html: str) -> dict[str, Any]:
        """Parse HTML for form fields.

        Args:
            html: HTML content.

        Returns:
            Parsed form information.
        """
        import re

        fields: list[FormField] = []

        # Parse input fields
        input_pattern = re.compile(
            r'<input[^>]+name=["\']([^"\']+)["\'][^>]*>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in input_pattern.finditer(html):
            attrs = match.group(0)
            name = match.group(1)

            field_type = "text"
            if 'type="' in attrs.lower():
                type_match = re.search(r'type=["\']([^"\']+)["\']', attrs, re.IGNORECASE)
                if type_match:
                    field_type = type_match.group(1).lower()

            required = "required" in attrs.lower()

            selector = f"input[name='{name}']"
            fields.append(FormField(
                name=name,
                type=field_type,
                selector=selector,
                required=required,
            ))

        # Parse select fields
        select_pattern = re.compile(
            r'<select[^>]+name=["\']([^"\']+)["\'][^>]*>(.*?)</select>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in select_pattern.finditer(html):
            name = match.group(1)
            options_html = match.group(2)
            options = re.findall(r'<option[^>]+value=["\']([^"\']+)["\']', options_html, re.IGNORECASE)

            selector = f"select[name='{name}']"
            fields.append(FormField(
                name=name,
                type="select",
                selector=selector,
                options=options,
            ))

        # Parse textarea fields
        textarea_pattern = re.compile(
            r'<textarea[^>]+name=["\']([^"\']+)["\'][^>]*>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in textarea_pattern.finditer(html):
            name = match.group(1)
            selector = f"textarea[name='{name}']"
            fields.append(FormField(
                name=name,
                type="textarea",
                selector=selector,
            ))

        self._fields = fields

        return {
            "field_count": len(fields),
            "fields": [
                {"name": f.name, "type": f.type, "selector": f.selector, "required": f.required}
                for f in fields
            ],
        }

    def _fill_form(self, form_data: dict[str, Any]) -> dict[str, Any]:
        """Fill form with data.

        Args:
            form_data: Data to fill.

        Returns:
            Fill result dictionary.
        """
        try:
            import pyautogui
        except ImportError:
            return {"success": False, "error": "pyautogui not installed"}

        filled: list[str] = []
        errors: list[str] = []

        for name, value in form_data.items():
            # Find field by name
            field = next((f for f in self._fields if f.name == name), None)

            if not field:
                errors.append(f"Field not found: {name}")
                continue

            try:
                if self.config.clear_before_fill:
                    # Select all and delete
                    pyautogui.hotkey("cmd", "a")
                    time.sleep(0.05)

                # Type value
                if isinstance(value, str):
                    pyautogui.write(str(value), interval=self.config.fill_delay)
                elif isinstance(value, int):
                    pyautogui.write(str(value), interval=self.config.fill_delay)

                filled.append(name)
                self._last_filled[name] = value

            except Exception as e:
                errors.append(f"Error filling {name}: {str(e)}")

        return {
            "filled_count": len(filled),
            "filled": filled,
            "errors": errors,
        }

    def _submit_form(self, selector: Optional[str]) -> dict[str, Any]:
        """Submit form.

        Args:
            selector: Submit button selector.

        Returns:
            Submit result dictionary.
        """
        try:
            import pyautogui
        except ImportError:
            return {"success": False, "error": "pyautogui not installed"}

        if not selector and not self._fields:
            # Just press enter to submit
            pyautogui.press("enter")
            time.sleep(self.config.wait_after_submit)
            return {"submitted": True, "method": "enter"}

        if selector:
            # Click submit button (would need browser context in real impl)
            pyautogui.click()
            time.sleep(self.config.wait_after_submit)
            return {"submitted": True, "selector": selector}

        return {"submitted": False, "error": "No submit method specified"}

    def _validate_form(self) -> dict[str, Any]:
        """Validate form fields.

        Returns:
            Validation result dictionary.
        """
        errors: list[str] = []
        required_missing: list[str] = []

        for field in self._fields:
            if field.required:
                if field.name not in self._last_filled:
                    required_missing.append(field.name)

        if required_missing:
            errors.append(f"Required fields missing: {', '.join(required_missing)}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "required_missing": required_missing,
        }

    def set_field_value(self, name: str, value: Any) -> None:
        """Set field value directly.

        Args:
            name: Field name.
            value: Field value.
        """
        for field in self._fields:
            if field.name == name:
                field.value = value
                break

    def get_field(self, name: str) -> Optional[FormField]:
        """Get field by name.

        Args:
            name: Field name.

        Returns:
            FormField or None.
        """
        return next((f for f in self._fields if f.name == name), None)

    def clear_fields(self) -> None:
        """Clear all field values."""
        self._last_filled.clear()
        for field in self._fields:
            field.value = None
