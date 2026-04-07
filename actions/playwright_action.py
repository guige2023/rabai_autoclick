"""Playwright browser automation action.

This module provides Playwright-based browser automation with
support for multiple browsers, keyboard/mouse, and network interception.

Example:
    >>> action = PlaywrightAction()
    >>> action.execute(command="goto", url="https://example.com")
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class PlaywrightConfig:
    """Configuration for Playwright browser."""
    browser: str = "chromium"
    headless: bool = True
    slow_mo: float = 0.0
    viewport: dict[str, int] = field(default_factory=lambda: {"width": 1920, "height": 1080})
    timeout: float = 30.0


@dataclass
class PageResult:
    """Result from a page operation."""
    url: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    success: bool = True
    error: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)


class PlaywrightAction:
    """Playwright-based browser automation action.

    Provides high-level browser automation with auto-waiting,
    multiple browsers, and comprehensive event handling.

    Example:
        >>> action = PlaywrightAction()
        >>> result = action.execute(
        ...     command="click",
        ...     selector="button.submit"
        ... )
    """

    def __init__(self, config: Optional[PlaywrightConfig] = None) -> None:
        """Initialize Playwright action.

        Args:
            config: Optional configuration object.
        """
        self.config = config or PlaywrightConfig()
        self._browser: Optional[Any] = None
        self._context: Optional[Any] = None
        self._page: Optional[Any] = None

    def execute(
        self,
        command: str,
        selector: Optional[str] = None,
        value: Optional[str] = None,
        url: Optional[str] = None,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute Playwright command synchronously.

        Args:
            command: Command to execute.
            selector: Element selector.
            value: Command value (text, script, etc.).
            url: URL for navigation.
            timeout: Command timeout.
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return {
                "success": False,
                "error": "Playwright not installed. Run: pip install playwright && playwright install",
            }

        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        with sync_playwright() as p:
            browser_type = getattr(p, self.config.browser)

            if self._browser is None:
                self._browser = browser_type.launch(
                    headless=self.config.headless,
                    slow_mo=self.config.slow_mo,
                )
                self._context = self._browser.new_context(
                    viewport=self.config.viewport,
                )
                self._page = self._context.new_page()
                self._page.set_default_timeout(timeout * 1000)

            if cmd == "goto" or cmd == "get":
                if not url:
                    raise ValueError("URL required for 'goto' command")
                response = self._page.goto(url, timeout=timeout * 1000)
                result["url"] = self._page.url
                result["title"] = self._page.title()
                result["status"] = response.status if response else None

            elif cmd == "click":
                if not selector:
                    raise ValueError("Selector required for 'click' command")
                self._page.click(selector, timeout=timeout * 1000)
                result["clicked"] = selector

            elif cmd in ("fill", "type", "send_keys"):
                if not selector or not value:
                    raise ValueError("Selector and value required")
                self._page.fill(selector, value, timeout=timeout * 1000)
                result["filled"] = selector
                result["value"] = value

            elif cmd == "screenshot":
                path = kwargs.get("path", "/tmp/screenshot.png")
                self._page.screenshot(path=path)
                result["path"] = path

            elif cmd == "inner_text":
                if not selector:
                    raise ValueError("Selector required")
                result["text"] = self._page.inner_text(selector)

            elif cmd == "inner_html":
                if not selector:
                    raise ValueError("Selector required")
                result["html"] = self._page.inner_html(selector)

            elif cmd == "evaluate":
                if not value:
                    raise ValueError("Script required for 'evaluate'")
                result["result"] = self._page.evaluate(value)

            elif cmd == "wait_for_selector":
                if not selector:
                    raise ValueError("Selector required")
                self._page.wait_for_selector(selector, timeout=timeout * 1000)
                result["found"] = selector

            elif cmd == "wait_for_timeout":
                self._page.wait_for_timeout(timeout * 1000)

            elif cmd == "press":
                if not selector or not value:
                    raise ValueError("Selector and key required")
                self._page.press(selector, value, timeout=timeout * 1000)
                result["pressed"] = value

            elif cmd == "select":
                if not selector or not value:
                    raise ValueError("Selector and value required")
                self._page.select_option(selector, value)
                result["selected"] = value

            elif cmd == "reload":
                self._page.reload(timeout=timeout * 1000)
                result["url"] = self._page.url

            elif cmd == "content":
                result["content"] = self._page.content()
                result["url"] = self._page.url
                result["title"] = self._page.title()

            else:
                raise ValueError(f"Unknown command: {command}")

        return result

    def close(self) -> None:
        """Close browser and context."""
        if self._context:
            self._context.close()
            self._context = None
        if self._browser:
            self._browser.close()
            self._browser = None
        self._page = None

    def __del__(self) -> None:
        """Cleanup on destruction."""
        self.close()
