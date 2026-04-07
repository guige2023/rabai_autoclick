"""Browser automation action using Selenium WebDriver.

This module provides Selenium-based browser automation including
element finding, form filling, waiting, and screenshot capture.

Example:
    >>> action = SeleniumAction()
    >>> action.execute(command="get", url="https://example.com")
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class SeleniumConfig:
    """Configuration for Selenium WebDriver."""
    browser: str = "chrome"
    headless: bool = True
    implicit_wait: float = 10.0
    page_load_timeout: float = 30.0
    window_size: tuple[int, int] = (1920, 1080)
    user_agent: Optional[str] = None


@dataclass
class ElementInfo:
    """Information about a found element."""
    tag_name: str
    text: Optional[str]
    attributes: dict[str, str]
    location: tuple[int, int]
    size: tuple[int, int]
    is_displayed: bool
    is_enabled: bool


class SeleniumAction:
    """Selenium WebDriver-based browser automation action.

    Provides comprehensive browser automation including navigation,
    element interaction, waiting, and screenshot capture.

    Example:
        >>> action = SeleniumAction()
        >>> action.execute(
        ...     command="click",
        ...     selector="#submit-button"
        ... )
    """

    def __init__(self, config: Optional[SeleniumConfig] = None) -> None:
        """Initialize Selenium action.

        Args:
            config: Optional configuration object.
        """
        self.config = config or SeleniumConfig()
        self._driver: Optional[Any] = None

    def execute(
        self,
        command: str,
        selector: Optional[str] = None,
        value: Optional[str] = None,
        url: Optional[str] = None,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute Selenium command.

        Args:
            command: Command to execute (get, click, send_keys, etc.).
            selector: CSS or XPath selector.
            value: Value for send_keys or other commands.
            url: URL for navigation commands.
            timeout: Command-specific timeout.
            **kwargs: Additional command parameters.

        Returns:
            Dictionary with command result.

        Raises:
            ValueError: If command or selector is invalid.
            RuntimeError: If WebDriver fails.
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
        except ImportError:
            return {
                "success": False,
                "error": "Selenium not installed. Run: pip install selenium",
            }

        if self._driver is None:
            self._driver = self._create_driver()

        by_map = {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "class": By.CLASS_NAME,
            "tag": By.TAG_NAME,
        }

        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        if cmd == "get":
            if not url:
                raise ValueError("URL required for 'get' command")
            self._driver.get(url)
            result["url"] = self._driver.current_url
            result["title"] = self._driver.title

        elif cmd in ("click", "find_element", "find"):
            if not selector:
                raise ValueError(f"Selector required for '{cmd}' command")
            by = by_map.get(kwargs.get("by", "css"), By.CSS_SELECTOR)
            wait = WebDriverWait(self._driver, timeout)
            element = wait.until(EC.element_to_be_clickable((by, selector)))
            if cmd == "click":
                element.click()
            result["found"] = True
            result["tag"] = element.tag_name

        elif cmd in ("send_keys", "type", "input"):
            if not selector:
                raise ValueError(f"Selector required for '{cmd}' command")
            if not value:
                raise ValueError(f"Value required for '{cmd}' command")
            by = by_map.get(kwargs.get("by", "css"), By.CSS_SELECTOR)
            wait = WebDriverWait(self._driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, selector)))
            element.clear()
            element.send_keys(value)
            result["value"] = value

        elif cmd == "screenshot":
            screenshot_bytes = self._driver.get_screenshot_as_png()
            result["screenshot"] = base64.b64encode(screenshot_bytes).decode()
            result["size"] = len(screenshot_bytes)

        elif cmd == "get_text":
            if not selector:
                raise ValueError("Selector required for 'get_text' command")
            by = by_map.get(kwargs.get("by", "css"), By.CSS_SELECTOR)
            wait = WebDriverWait(self._driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, selector)))
            result["text"] = element.text

        elif cmd == "get_attribute":
            if not selector or not value:
                raise ValueError("Selector and attribute name required for 'get_attribute'")
            by = by_map.get(kwargs.get("by", "css"), By.CSS_SELECTOR)
            wait = WebDriverWait(self._driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, selector)))
            result["attribute"] = value
            result["value"] = element.get_attribute(value)

        elif cmd == "wait":
            time.sleep(timeout)

        elif cmd == "execute_script":
            if not value:
                raise ValueError("Script required for 'execute_script'")
            result["return"] = self._driver.execute_script(value)

        elif cmd == "back":
            self._driver.back()
            result["url"] = self._driver.current_url

        elif cmd == "forward":
            self._driver.forward()
            result["url"] = self._driver.current_url

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def _create_driver(self) -> Any:
        """Create WebDriver instance based on configuration.

        Returns:
            Configured WebDriver instance.
        """
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService

        options = ChromeOptions()
        if self.config.headless:
            options.add_argument("--headless=new")
        options.add_argument(f"--window-size={self.config.window_size[0]},{self.config.window_size[1]}")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        if self.config.user_agent:
            options.add_argument(f"--user-agent={self.config.user_agent}")

        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(self.config.implicit_wait)
        driver.set_page_load_timeout(self.config.page_load_timeout)
        return driver

    def close(self) -> None:
        """Close the WebDriver."""
        if self._driver:
            self._driver.quit()
            self._driver = None

    def __del__(self) -> None:
        """Cleanup on destruction."""
        self.close()
