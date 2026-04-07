"""
Browser Automation Action Module.

Provides browser control via Playwright/Selenium for clicking,
typing, navigation, and screenshot capture in web automation workflows.

Example:
    >>> from browser自动化_action import BrowserAction, BrowserConfig
    >>> browser = BrowserAction()
    >>> await browser.start()
    >>> page = await browser.new_page()
    >>> await page.goto("https://example.com")
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class BrowserConfig:
    """Browser launch configuration."""
    browser: str = "chromium"
    headless: bool = True
    slow_mo: float = 0
    viewport_width: int = 1280
    viewport_height: int = 720
    user_agent: Optional[str] = None
    proxy: Optional[str] = None
    timeout: float = 30000


@dataclass
class ElementHandle:
    """Wrapper for a browser DOM element."""
    selector: str
    index: int = 0


class BrowserAction:
    """Browser automation using Playwright."""

    def __init__(self, config: Optional[BrowserConfig] = None):
        self.config = config or BrowserConfig()
        self._browser = None
        self._context = None
        self._page = None
        self._playwright = None

    async def start(self) -> bool:
        """Launch browser and create context."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return False

        self._playwright = await async_playwright().start()
        browser_type = getattr(self._playwright, self.config.browser)

        kwargs: dict[str, Any] = {"headless": self.config.headless}
        if self.config.slow_mo:
            kwargs["slow_mo"] = self.config.slow_mo

        self._browser = await browser_type.launch(**kwargs)

        context_opts: dict[str, Any] = {
            "viewport": {"width": self.config.viewport_width, "height": self.config.viewport_height},
        }
        if self.config.user_agent:
            context_opts["user_agent"] = self.config.user_agent
        if self.config.proxy:
            context_opts["proxy"] = {"server": self.config.proxy}

        self._context = await self._browser.new_context(**context_opts)
        self._page = await self._context.new_page()
        return True

    async def stop(self) -> None:
        """Close browser and cleanup."""
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def goto(self, url: str, wait_until: str = "load") -> bool:
        """Navigate to URL."""
        if not self._page:
            return False
        try:
            await self._page.goto(url, wait_until=wait_until, timeout=self.config.timeout)
            return True
        except Exception:
            return False

    async def click(self, selector: str, timeout: float = 5000) -> bool:
        """Click element by CSS selector."""
        if not self._page:
            return False
        try:
            await self._page.click(selector, timeout=timeout)
            return True
        except Exception:
            return False

    async def type_text(
        self,
        selector: str,
        text: str,
        delay: float = 0,
        timeout: float = 5000,
    ) -> bool:
        """Type text into element."""
        if not self._page:
            return False
        try:
            await self._page.fill(selector, text, timeout=timeout)
            return True
        except Exception:
            try:
                await self._page.click(selector, timeout=timeout)
                await self._page.keyboard.type(text, delay=delay)
                return True
            except Exception:
                return False

    async def press_key(self, selector: str, key: str, timeout: float = 5000) -> bool:
        """Press key while focused on element."""
        if not self._page:
            return False
        try:
            await self._page.focus(selector, timeout=timeout)
            await self._page.keyboard.press(key)
            return True
        except Exception:
            return False

    async def hover(self, selector: str, timeout: float = 5000) -> bool:
        """Hover over element."""
        if not self._page:
            return False
        try:
            await self._page.hover(selector, timeout=timeout)
            return True
        except Exception:
            return False

    async def select_option(
        self,
        selector: str,
        value: Optional[str] = None,
        label: Optional[str] = None,
        index: Optional[int] = None,
    ) -> bool:
        """Select option in dropdown."""
        if not self._page:
            return False
        try:
            opts: dict[str, Any] = {}
            if value is not None:
                opts["value"] = value
            elif label is not None:
                opts["label"] = label
            elif index is not None:
                opts["index"] = index
            await self._page.select_option(selector, **opts)
            return True
        except Exception:
            return False

    async def check(self, selector: str, checked: bool = True) -> bool:
        """Check or uncheck checkbox/radio."""
        if not self._page:
            return False
        try:
            if checked:
                await self._page.check(selector)
            else:
                await self._page.uncheck(selector)
            return True
        except Exception:
            return False

    async def screenshot(
        self,
        path: str,
        selector: Optional[str] = None,
        full_page: bool = False,
    ) -> bool:
        """Take screenshot."""
        if not self._page:
            return False
        try:
            if selector:
                elem = await self._page.query_selector(selector)
                if elem:
                    await elem.screenshot(path)
                else:
                    return False
            else:
                await self._page.screenshot(path=path, full_page=full_page)
            return True
        except Exception:
            return False

    async def evaluate(self, script: str) -> Any:
        """Execute JavaScript in page context."""
        if not self._page:
            return None
        try:
            return await self._page.evaluate(script)
        except Exception:
            return None

    async def wait_for_selector(
        self,
        selector: str,
        state: str = "visible",
        timeout: float = 5000,
    ) -> bool:
        """Wait for element to appear."""
        if not self._page:
            return False
        try:
            await self._page.wait_for_selector(selector, state=state, timeout=timeout)
            return True
        except Exception:
            return False

    async def wait_for_navigation(
        self,
        wait_until: str = "load",
        timeout: float = 30000,
    ) -> bool:
        """Wait for navigation to complete."""
        if not self._page:
            return False
        try:
            await self._page.wait_for_load_state(wait_until, timeout=timeout)
            return True
        except Exception:
            return False

    async def get_text(self, selector: str) -> Optional[str]:
        """Get text content of element."""
        if not self._page:
            return None
        try:
            elem = await self._page.query_selector(selector)
            if elem:
                return await elem.text_content()
            return None
        except Exception:
            return None

    async def get_attribute(self, selector: str, attr: str) -> Optional[str]:
        """Get element attribute value."""
        if not self._page:
            return None
        try:
            return await self._page.get_attribute(selector, attr)
        except Exception:
            return None

    async def is_visible(self, selector: str) -> bool:
        """Check if element is visible."""
        if not self._page:
            return False
        try:
            elem = await self._page.query_selector(selector)
            if elem:
                return await elem.is_visible()
            return False
        except Exception:
            return False

    async def is_enabled(self, selector: str) -> bool:
        """Check if element is enabled."""
        if not self._page:
            return False
        try:
            elem = await self._page.query_selector(selector)
            if elem:
                return await elem.is_enabled()
            return False
        except Exception:
            return False

    async def reload(self, wait_until: str = "load") -> bool:
        """Reload current page."""
        if not self._page:
            return False
        try:
            await self._page.reload(wait_until=wait_until)
            return True
        except Exception:
            return False

    async def go_back(self) -> bool:
        """Navigate back."""
        if not self._page:
            return False
        try:
            await self._page.go_back()
            return True
        except Exception:
            return False

    async def go_forward(self) -> bool:
        """Navigate forward."""
        if not self._page:
            return False
        try:
            await self._page.go_forward()
            return True
        except Exception:
            return False

    async def download_file(
        self,
        url: str,
        output_path: str,
        timeout: float = 30000,
    ) -> bool:
        """Download file from URL."""
        if not self._context:
            return False
        try:
            async with self._context.expect_download(timeout=timeout) as download_info:
                await self._page.goto(url)
            download = await download_info.value
            await download.save_as(output_path)
            return True
        except Exception:
            return False

    async def set_viewport_size(self, width: int, height: int) -> bool:
        """Set viewport dimensions."""
        if not self._context:
            return False
        try:
            await self._context.set_viewport_size({"width": width, "height": height})
            return True
        except Exception:
            return False

    async def add_cookies(self, cookies: list[dict[str, Any]]) -> bool:
        """Add cookies to context."""
        if not self._context:
            return False
        try:
            await self._context.add_cookies(cookies)
            return True
        except Exception:
            return False

    async def clear_cookies(self) -> bool:
        """Clear all cookies."""
        if not self._context:
            return False
        try:
            await self._context.clear_cookies()
            return True
        except Exception:
            return False

    async def set_extra_http_headers(self, headers: dict[str, str]) -> bool:
        """Set custom HTTP headers."""
        if not self._context:
            return False
        try:
            await self._context.set_extra_http_headers(headers)
            return True
        except Exception:
            return False

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()


if __name__ == "__main__":
    async def test():
        browser = BrowserAction(BrowserConfig(headless=True))
        started = await browser.start()
        print(f"Browser started: {started}")
        if started:
            await browser.goto("https://example.com")
            title = await browser.get_text("h1")
            print(f"Page title: {title}")
            await browser.screenshot("/tmp/example.png")
            await browser.stop()

    asyncio.run(test())
