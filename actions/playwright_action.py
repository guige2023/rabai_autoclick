"""Playwright browser automation action module for RabAI AutoClick.

Provides modern browser automation via Playwright for Python.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PlaywrightNavigateAction(BaseAction):
    """Navigate and interact with web pages using Playwright.

    Supports Chromium, Firefox, and WebKit browsers.
    """
    action_type = "playwright_navigate"
    display_name = "Playwright导航"
    description = "Playwright浏览器自动化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Navigate to URL and perform actions.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Target URL
                - browser: Browser type (chromium, firefox, webkit)
                - action: Action after navigation
                - selector: CSS selector for action
                - value: Value to type
                - headless: Run headless
                - timeout: Timeout in seconds

        Returns:
            ActionResult with navigation results.
        """
        url = params.get('url', '')
        browser = params.get('browser', 'chromium').lower()
        action = params.get('action', 'navigate')
        selector = params.get('selector', '')
        value = params.get('value', '')
        headless = params.get('headless', True)
        timeout = params.get('timeout', 30000)

        if not url and action == 'navigate':
            return ActionResult(success=False, message="url is required")

        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
        except ImportError:
            return ActionResult(success=False, message="playwright not installed. Run: pip install playwright && playwright install")

        start = time.time()
        pw = None
        browser_obj = None
        context_obj = None
        page = None
        try:
            pw = sync_playwright().start()
            browser_obj = pw.chromium.launch(headless=headless) if browser == 'chromium' else (
                pw.firefox.launch(headless=headless) if browser == 'firefox' else
                pw.webkit.launch(headless=headless)
            )
            context_obj = browser_obj.new_context()
            page = context_obj.new_page()
            page.set_default_timeout(timeout)
            page.goto(url)

            result_data: Dict[str, Any] = {'url': page.url, 'title': page.title()}

            if action == 'screenshot':
                ss_path = params.get('screenshot_path', '/tmp/playwright_screenshot.png')
                page.screenshot(path=ss_path)
                result_data['screenshot'] = ss_path
            elif action == 'click' and selector:
                page.click(selector)
                result_data['clicked'] = selector
            elif action == 'type' and selector and value:
                page.fill(selector, value)
                result_data['typed'] = value
            elif action == 'hover' and selector:
                page.hover(selector)
                result_data['hovered'] = selector
            elif action == 'get_text' and selector:
                result_data['text'] = page.text_content(selector)
            elif action == 'get_html':
                result_data['html'] = page.content()

            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Playwright {action} completed",
                data=result_data, duration=duration
            )
        except PlaywrightTimeout:
            return ActionResult(success=False, message=f"Playwright timeout after {timeout}ms")
        except Exception as e:
            return ActionResult(success=False, message=f"Playwright error: {str(e)}")
        finally:
            if page:
                page.close()
            if context_obj:
                context_obj.close()
            if browser_obj:
                browser_obj.close()
            if pw:
                pw.stop()


class PlaywrightScreenshotAction(BaseAction):
    """Take screenshots of web pages using Playwright."""
    action_type = "playwright_screenshot"
    display_name = "Playwright截图"
    description = "Playwright网页截图"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Take screenshot.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: URL to screenshot
                - output_path: Path to save screenshot
                - browser: Browser type
                - full_page: Capture full page
                - viewport: Viewport size dict
                - headless: Run headless

        Returns:
            ActionResult with screenshot path.
        """
        url = params.get('url', '')
        output_path = params.get('output_path', '/tmp/playwright_screenshot.png')
        browser = params.get('browser', 'chromium').lower()
        full_page = params.get('full_page', False)
        viewport = params.get('viewport', None)
        headless = params.get('headless', True)

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return ActionResult(success=False, message="playwright not installed")

        start = time.time()
        pw = None
        try:
            pw = sync_playwright().start()
            browser_obj = pw.chromium.launch(headless=headless) if browser == 'chromium' else (
                pw.firefox.launch(headless=headless) if browser == 'firefox' else
                pw.webkit.launch(headless=headless)
            )
            context_obj = browser_obj.new_context(viewport=viewport or {'width': 1280, 'height': 720})
            page = context_obj.new_page()
            page.goto(url, wait_until='networkidle')
            page.screenshot(path=output_path, full_page=full_page)
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Screenshot saved to {output_path}",
                data={'path': output_path}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Playwright screenshot error: {str(e)}")
        finally:
            if pw:
                pw.stop()
