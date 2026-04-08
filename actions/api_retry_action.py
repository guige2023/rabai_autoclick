"""API retry action module for RabAI AutoClick.

Provides automatic retry logic with exponential backoff and jitter
for failing API requests with configurable retry conditions.
"""

import time
import random
import sys
import os
from typing import Any, Callable, Dict, List, Optional, Set, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    retry_on_status: Set[int] = None
    
    def __post_init__(self):
        if self.retry_on_status is None:
            self.retry_on_status = {429, 500, 502, 503, 504}
        else:
            self.retry_on_status = set(self.retry_on_status)


class ApiRetryAction(BaseAction):
    """API retry action with exponential backoff and jitter.
    
    Wraps HTTP requests with automatic retry logic, configurable
    status code retry conditions, and backoff strategies.
    """
    action_type = "api_retry"
    display_name = "API重试"
    description = "带指数退避的API自动重试机制"
    
    def __init__(self):
        super().__init__()
        self._default_config = RetryConfig()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP request with retry logic.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   max_attempts, initial_delay, max_delay, multiplier,
                   jitter, retry_on_status, timeout.
        
        Returns:
            ActionResult with response body and attempt count.
        """
        config = self._build_config(params)
        url = params.get('url', '')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        body = params.get('body')
        timeout = params.get('timeout', 30)
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        last_error = None
        for attempt in range(1, config.max_attempts + 1):
            try:
                response = self._make_request(
                    url, method, headers, body, timeout
                )
                
                status = response.get('status', 200)
                
                if status < 400:
                    return ActionResult(
                        success=True,
                        message=f"Success on attempt {attempt}",
                        data={
                            'body': response.get('body'),
                            'status': status,
                            'attempts': attempt
                        }
                    )
                
                if status in config.retry_on_status and attempt < config.max_attempts:
                    last_error = f"HTTP {status}"
                    delay = self._calculate_delay(config, attempt)
                    time.sleep(delay)
                    continue
                
                return ActionResult(
                    success=False,
                    message=f"HTTP {status} on attempt {attempt}",
                    data={
                        'body': response.get('body'),
                        'status': status,
                        'attempts': attempt
                    }
                )
                
            except HTTPError as e:
                last_error = f"HTTPError: {e.code} - {e.reason}"
                if e.code in config.retry_on_status and attempt < config.max_attempts:
                    delay = self._calculate_delay(config, attempt)
                    time.sleep(delay)
                    continue
                return ActionResult(
                    success=False,
                    message=f"HTTPError: {e.code}",
                    data={'attempts': attempt, 'error': last_error}
                )
            except URLError as e:
                last_error = f"URLError: {str(e.reason)}"
                if attempt < config.max_attempts:
                    delay = self._calculate_delay(config, attempt)
                    time.sleep(delay)
                    continue
                return ActionResult(
                    success=False,
                    message=f"URLError: {str(e.reason)}",
                    data={'attempts': attempt, 'error': last_error}
                )
            except Exception as e:
                last_error = str(e)
                if attempt < config.max_attempts:
                    delay = self._calculate_delay(config, attempt)
                    time.sleep(delay)
                    continue
                return ActionResult(
                    success=False,
                    message=f"Error: {str(e)}",
                    data={'attempts': attempt, 'error': last_error}
                )
        
        return ActionResult(
            success=False,
            message=f"All {config.max_attempts} attempts failed",
            data={'error': last_error, 'attempts': config.max_attempts}
        )
    
    def _make_request(
        self,
        url: str,
        method: str,
        headers: Dict[str, str],
        body: Optional[Union[str, Dict]],
        timeout: int
    ) -> Dict[str, Any]:
        """Make HTTP request and return response."""
        import json
        
        data = None
        if body:
            if isinstance(body, dict):
                data = json.dumps(body).encode('utf-8')
                headers.setdefault('Content-Type', 'application/json')
            else:
                data = body.encode('utf-8') if isinstance(body, str) else body
        
        req = Request(url, data=data, headers=headers, method=method)
        
        with urlopen(req, timeout=timeout) as response:
            body_bytes = response.read()
            return {
                'status': response.status,
                'body': body_bytes.decode('utf-8', errors='replace'),
                'headers': dict(response.headers)
            }
    
    def _build_config(self, params: Dict[str, Any]) -> RetryConfig:
        """Build retry config from params."""
        return RetryConfig(
            max_attempts=params.get('max_attempts', 3),
            initial_delay=params.get('initial_delay', 1.0),
            max_delay=params.get('max_delay', 60.0),
            multiplier=params.get('multiplier', 2.0),
            jitter=params.get('jitter', True),
            retry_on_status=params.get('retry_on_status')
        )
    
    def _calculate_delay(self, config: RetryConfig, attempt: int) -> float:
        """Calculate delay for given attempt with backoff and jitter.
        
        Args:
            config: Retry configuration.
            attempt: Current attempt number (1-indexed).
            
        Returns:
            Delay in seconds.
        """
        delay = min(
            config.initial_delay * (config.multiplier ** (attempt - 1)),
            config.max_delay
        )
        
        if config.jitter:
            delay = delay * (0.5 + random.random())
        
        return delay
