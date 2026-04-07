"""
Form Filler Action Module.

Fills web forms with data, handles various input types,
manages multi-step forms, and validates submission.

Example:
    >>> from form_filler_action import FormFiller
    >>> filler = FormFiller()
    >>> await filler.fill(page, {"username": "alice", "password": "secret"})
    >>> await filler.submit()
"""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class FormField:
    """Represents a form field."""
    selector: str
    type: str
    value: Any = None
    options: list[dict[str, str]] = None


class FormFiller:
    """Fill and submit web forms."""

    def __init__(self):
        self._fields: dict[str, FormField] = {}
        self._current_field_index = 0

    def add_field(
        self,
        selector: str,
        value: Any,
        field_type: Optional[str] = None,
    ) -> "FormFiller":
        """Add a field to the form definition."""
        self._fields[selector] = FormField(
            selector=selector,
            type=field_type or "text",
            value=value,
        )
        return self

    def set_fields(self, fields: dict[str, Any]) -> "FormFiller":
        """Set multiple fields at once."""
        for selector, value in fields.items():
            self.add_field(selector, value)
        return self

    async def fill(self, page: Any, fields: Optional[dict[str, Any]] = None) -> bool:
        """
        Fill form fields on a Playwright page.

        Args:
            page: Playwright page object
            fields: Optional dict of selector->value overrides

        Returns:
            True on success
        """
        all_fields = dict(self._fields)
        if fields:
            for selector, value in fields.items():
                all_fields[selector] = FormField(selector=selector, type="text", value=value)

        for selector, field in all_fields.items():
            try:
                await self._fill_field(page, field)
            except Exception as e:
                return False
        return True

    async def _fill_field(self, page: Any, field: FormField) -> None:
        selector = field.selector
        value = field.value

        if field.type in ("text", "email", "password", "search", "url", "tel", "number"):
            await page.fill(selector, str(value), timeout=5000)
        elif field.type == "textarea":
            await page.fill(selector, str(value), timeout=5000)
        elif field.type == "checkbox":
            if value:
                await page.check(selector)
            else:
                await page.uncheck(selector)
        elif field.type == "radio":
            await page.click(f"{selector}[value='{value}']")
        elif field.type == "select":
            await page.select_option(selector, value)
        elif field.type == "file":
            if isinstance(value, list):
                await page.set_input_files(selector, value)
            else:
                await page.set_input_files(selector, [value])
        elif field.type == "date":
            await page.fill(selector, str(value))
        elif field.type == "contenteditable":
            await page.evaluate(f'document.querySelector("{selector}").innerHTML = "{value}"')
        else:
            await page.fill(selector, str(value), timeout=5000)

    async def submit(
        self,
        page: Any,
        submit_selector: Optional[str] = None,
        wait_for_navigation: bool = True,
    ) -> bool:
        """
        Submit the form.

        Args:
            page: Playwright page
            submit_selector: CSS selector for submit button (auto-detected if None)
            wait_for_navigation: Wait for navigation after submit

        Returns:
            True on success
        """
        if submit_selector:
            if wait_for_navigation:
                async with page.expect_navigation(timeout=10000):
                    await page.click(submit_selector)
            else:
                await page.click(submit_selector)
        else:
            buttons = ["button[type='submit']", "input[type='submit']", ".btn-submit", "[type='submit']"]
            for btn_selector in buttons:
                try:
                    elem = await page.query_selector(btn_selector)
                    if elem and await elem.is_visible():
                        if wait_for_navigation:
                            async with page.expect_navigation(timeout=10000):
                                await elem.click()
                        else:
                            await elem.click()
                        return True
                except Exception:
                    continue
            await page.evaluate("document.querySelector('form').submit()")
        return True

    def detect_fields(self, page: Any) -> dict[str, dict[str, Any]]:
        """Auto-detect form fields from a Playwright page."""
        fields: dict[str, dict[str, Any]] = {}

        selectors = {
            "input[type='text']": "text",
            "input[type='email']": "email",
            "input[type='password']": "password",
            "input[type='search']": "search",
            "input[type='tel']": "tel",
            "input[type='number']": "number",
            "input[type='date']": "date",
            "input[type='checkbox']": "checkbox",
            "input[type='radio']": "radio",
            "input[type='file']": "file",
            "textarea": "textarea",
            "select": "select",
        }

        for selector, field_type in selectors.items():
            try:
                elements = await page.query_selector_all(selector)
                for elem in elements:
                    tag = await elem.evaluate("el => el.tagName.toLowerCase()")
                    name = await elem.get_attribute("name") or await elem.get_attribute("id") or ""
                    selector_str = selector
                    if name:
                        selector_str = f"{selector}[name='{name}']"
                    fields[selector_str] = {
                        "type": field_type,
                        "name": name,
                        "required": await elem.get_attribute("required") is not None,
                        "placeholder": await elem.get_attribute("placeholder") or "",
                    }
            except Exception:
                continue

        return fields

    def validate(self, fields: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate form data."""
        errors: list[str] = []
        for selector, field in self._fields.items():
            value = field.value
            if value is None or value == "":
                continue
            if field.type == "email":
                if not re.match(r"[^@]+@[^@]+\.[^@]+", str(value)):
                    errors.append(f"{selector}: Invalid email format")
            elif field.type == "tel":
                digits = re.sub(r"\D", "", str(value))
                if len(digits) < 10:
                    errors.append(f"{selector}: Phone number too short")
        return len(errors) == 0, errors


if __name__ == "__main__":
    print("FormFiller module loaded")
