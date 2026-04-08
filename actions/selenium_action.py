"""Selenium web automation action module for RabAI AutoClick.

Provides browser automation via Selenium WebDriver for web scraping,
testing, and form submission.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SeleniumNavigateAction(BaseAction):
    """Navigate to URLs and interact with web pages using Selenium.

    Supports Chrome, Firefox, Edge, and Safari WebDrivers.
    """
    action_type = "selenium_navigate"
    display_name = "Selenium浏览器导航"
    description = "Selenium网页导航与交互"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Navigate to URL and perform actions.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Target URL to navigate to
                - browser: Browser type (chrome, firefox, edge)
                - action: Action to perform after nav (click, type, screenshot, etc.)
                - selector: CSS selector for action
                - value: Value to type (for type action)
                - headless: Run in headless mode
                - timeout: Page load timeout in seconds

        Returns:
            ActionResult with navigation/action results.
        """
        url = params.get('url', '')
        browser = params.get('browser', 'chrome').lower()
        action = params.get('action', 'navigate')
        selector = params.get('selector', '')
        value = params.get('value', '')
        headless = params.get('headless', True)
        timeout = params.get('timeout', 30)

        if not url and action == 'navigate':
            return ActionResult(success=False, message="url is required")

        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException, NoSuchElementException
        except ImportError:
            return ActionResult(success=False, message="selenium not installed. Run: pip install selenium")

        driver = None
        start = time.time()
        try:
            if browser == 'chrome':
                options = webdriver.ChromeOptions()
                if headless:
                    options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                driver = webdriver.Chrome(options=options)
            elif browser == 'firefox':
                options = webdriver.FirefoxOptions()
                if headless:
                    options.add_argument('--headless')
                driver = webdriver.Firefox(options=options)
            elif browser == 'edge':
                options = webdriver.EdgeOptions()
                if headless:
                    options.add_argument('--headless')
                driver = webdriver.Edge(options=options)
            else:
                return ActionResult(success=False, message=f"Unsupported browser: {browser}")

            driver.set_page_load_timeout(timeout)
            driver.get(url)

            # Wait for page load
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            result_data: Dict[str, Any] = {'url': driver.current_url, 'title': driver.title}

            # Perform action if specified
            if action == 'screenshot':
                screenshot_path = params.get('screenshot_path', '/tmp/selenium_screenshot.png')
                driver.save_screenshot(screenshot_path)
                result_data['screenshot'] = screenshot_path
            elif action in ('click', 'type') and selector:
                element = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                if action == 'click':
                    element.click()
                    result_data['clicked'] = selector
                elif action == 'type':
                    element.clear()
                    element.send_keys(value)
                    result_data['typed'] = value
            elif action == 'get_text' and selector:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                result_data['text'] = element.text
            elif action == 'get_html' and selector:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                result_data['html'] = element.get_attribute('outerHTML')
            elif action == 'get_html':
                result_data['html'] = driver.page_source

            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Selenium {action} completed",
                data=result_data, duration=duration
            )
        except TimeoutException:
            return ActionResult(success=False, message=f"Page load timeout after {timeout}s")
        except NoSuchElementException as e:
            return ActionResult(success=False, message=f"Element not found: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Selenium error: {str(e)}")
        finally:
            if driver:
                driver.quit()


class SeleniumFormAction(BaseAction):
    """Fill and submit forms using Selenium."""
    action_type = "selenium_form"
    display_name = "Selenium表单填写"
    description = "Selenium表单填写与提交"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fill form and submit.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Target URL
                - fields: Dict of {selector: value} to fill
                - submit_selector: Selector for submit button
                - browser: Browser type
                - headless: Run headless

        Returns:
            ActionResult with form submission result.
        """
        url = params.get('url', '')
        fields = params.get('fields', {})
        submit_selector = params.get('submit_selector', '')
        browser = params.get('browser', 'chrome').lower()
        headless = params.get('headless', True)

        if not url or not fields:
            return ActionResult(success=False, message="url and fields are required")

        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
        except ImportError:
            return ActionResult(success=False, message="selenium not installed")

        driver = None
        start = time.time()
        try:
            if browser == 'chrome':
                options = webdriver.ChromeOptions()
                if headless:
                    options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                driver = webdriver.Chrome(options=options)
            elif browser == 'firefox':
                options = webdriver.FirefoxOptions()
                if headless:
                    options.add_argument('--headless')
                driver = webdriver.Firefox(options=options)
            else:
                driver = webdriver.Chrome()

            driver.get(url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            for selector, value in fields.items():
                try:
                    element = driver.find_element(By.CSS_SELECTOR, selector)
                    element.clear()
                    element.send_keys(value)
                except Exception:
                    pass

            if submit_selector:
                submit_btn = driver.find_element(By.CSS_SELECTOR, submit_selector)
                submit_btn.click()
                time.sleep(2)

            duration = time.time() - start
            return ActionResult(
                success=True, message="Form submitted",
                data={'url': driver.current_url, 'title': driver.title},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Selenium form error: {str(e)}")
        finally:
            if driver:
                driver.quit()
