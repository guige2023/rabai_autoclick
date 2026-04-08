"""HTTP retry action module for RabAI AutoClick.

Provides HTTP client actions with built-in retry logic, 
backoff strategies, and error handling for resilient API calls.
"""

import json
import urllib.request
import urllib.error
import sys
import os
import time
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HttpRetryAction(BaseAction):
    """HTTP request with automatic retry on failure.
    
    Implements configurable retry logic with exponential backoff.
    """
    action_type = "http_retry"
    display_name = "HTTP重试请求"
    description = "带重试逻辑的HTTP请求"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP request with retry.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   max_retries, backoff_base, backoff_factor,
                   retry_on_timeout, retry_on_status.
        
        Returns:
            ActionResult with response data.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        body = params.get('body', None)
        max_retries = params.get('max_retries', 3)
        backoff_base = params.get('backoff_base', 2)
        backoff_factor = params.get('backoff_factor', 1)
        retry_on_timeout = params.get('retry_on_timeout', True)
        retry_on_status = params.get('retry_on_status', [408, 429, 500, 502, 503, 504])
        timeout = params.get('timeout', 30)

        if not url:
            return ActionResult(success=False, message="url is required")

        attempt = 0
        last_error = None

        while attempt <= max_retries:
            try:
                data_bytes = None
                if body:
                    if isinstance(body, dict):
                        data_bytes = json.dumps(body).encode('utf-8')
                    elif isinstance(body, str):
                        data_bytes = body.encode('utf-8')

                req = urllib.request.Request(
                    url, 
                    data=data_bytes,
                    headers=headers,
                    method=method
                )

                with urllib.request.urlopen(req, timeout=timeout) as response:
                    response_body = response.read()
                    response_data = {
                        'status_code': response.status,
                        'headers': dict(response.headers),
                        'body': response_body.decode('utf-8', errors='replace'),
                        'attempts': attempt + 1
                    }
                    
                    try:
                        response_data['json'] = json.loads(response_body)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

                    return ActionResult(
                        success=True,
                        message=f"Request successful on attempt {attempt + 1}",
                        data=response_data
                    )

            except urllib.error.HTTPError as e:
                last_error = f"HTTP {e.code}: {e.reason}"
                
                if e.code in retry_on_status:
                    if attempt < max_retries:
                        wait_time = backoff_factor * (backoff_base ** attempt)
                        time.sleep(wait_time)
                        attempt += 1
                        continue
                
                return ActionResult(
                    success=False,
                    message=f"HTTP error after {attempt + 1} attempts: {last_error}",
                    data={'status_code': e.code, 'attempts': attempt + 1}
                )

            except urllib.error.URLError as e:
                last_error = str(e.reason)
                
                if retry_on_timeout and attempt < max_retries:
                    wait_time = backoff_factor * (backoff_base ** attempt)
                    time.sleep(wait_time)
                    attempt += 1
                    continue
                
                return ActionResult(
                    success=False,
                    message=f"Request failed after {attempt + 1} attempts: {last_error}",
                    data={'attempts': attempt + 1}
                )

            except TimeoutError as e:
                last_error = str(e)
                
                if retry_on_timeout and attempt < max_retries:
                    wait_time = backoff_factor * (backoff_base ** attempt)
                    time.sleep(wait_time)
                    attempt += 1
                    continue
                
                return ActionResult(
                    success=False,
                    message=f"Timeout after {attempt + 1} attempts",
                    data={'attempts': attempt + 1}
                )

            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Unexpected error: {str(e)}",
                    data={'attempts': attempt + 1}
                )

        return ActionResult(
            success=False,
            message=f"All {max_retries + 1} attempts failed: {last_error}",
            data={'attempts': attempt, 'last_error': last_error}
        )


class HttpCircuitBreakerAction(BaseAction):
    """HTTP request with circuit breaker pattern.
    
    Prevents cascading failures by opening circuit after threshold.
    """
    action_type = "http_circuit_breaker"
    display_name = "HTTP断路器请求"
    description = "带断路器模式的HTTP请求"

    def __init__(self):
        super().__init__()
        self._circuit_state = 'closed'
        self._failure_count = 0
        self._last_failure_time = 0
        self._success_count = 0

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTTP request with circuit breaker.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   failure_threshold, recovery_timeout,
                   half_open_max_calls.
        
        Returns:
            ActionResult with response or circuit state.
        """
        url = params.get('url', '')
        method = params.get('method', 'GET')
        headers = params.get('headers', {})
        body = params.get('body', None)
        failure_threshold = params.get('failure_threshold', 5)
        recovery_timeout = params.get('recovery_timeout', 60)
        half_open_max_calls = params.get('half_open_max_calls', 3)
        timeout = params.get('timeout', 30)

        if not url:
            return ActionResult(success=False, message="url is required")

        current_time = time.time()
        
        if self._circuit_state == 'open':
            if current_time - self._last_failure_time > recovery_timeout:
                self._circuit_state = 'half-open'
                self._success_count = 0
            else:
                return ActionResult(
                    success=False,
                    message=f"Circuit open. Retry after {int(recovery_timeout - (current_time - self._last_failure_time))}s",
                    data={'circuit_state': 'open'}
                )

        try:
            data_bytes = None
            if body:
                if isinstance(body, dict):
                    data_bytes = json.dumps(body).encode('utf-8')
                elif isinstance(body, str):
                    data_bytes = body.encode('utf-8')

            req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
            
            with urllib.request.urlopen(req, timeout=timeout) as response:
                response_body = response.read()
                
                if self._circuit_state == 'half-open':
                    self._success_count += 1
                    if self._success_count >= half_open_max_calls:
                        self._circuit_state = 'closed'
                        self._failure_count = 0

                return ActionResult(
                    success=True,
                    message="Request successful",
                    data={
                        'circuit_state': self._circuit_state,
                        'status_code': response.status,
                        'body': response_body.decode('utf-8', errors='replace')
                    }
                )

        except Exception as e:
            self._failure_count += 1
            self._last_failure_time = current_time
            
            if self._circuit_state == 'half-open' or self._failure_count >= failure_threshold:
                self._circuit_state = 'open'

            return ActionResult(
                success=False,
                message=f"Request failed: {str(e)}",
                data={
                    'circuit_state': self._circuit_state,
                    'failure_count': self._failure_count
                }
            )


class HttpBatchRetryAction(BaseAction):
    """Batch HTTP requests with retry and concurrency control.
    
    Executes multiple HTTP requests with shared retry logic.
    """
    action_type = "http_batch_retry"
    display_name = "批量HTTP重试"
    description = "批量HTTP请求带重试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch HTTP requests.
        
        Args:
            context: Execution context.
            params: Dict with keys: requests, max_concurrency,
                   max_retries, backoff_base, stop_on_first_error.
        
        Returns:
            ActionResult with all responses.
        """
        requests = params.get('requests', [])
        max_concurrency = params.get('max_concurrency', 5)
        max_retries = params.get('max_retries', 3)
        backoff_base = params.get('backoff_base', 2)
        stop_on_first_error = params.get('stop_on_first_error', False)
        timeout = params.get('timeout', 30)

        if not requests:
            return ActionResult(success=False, message="requests list is empty")

        results = []
        errors = []

        for i, req_config in enumerate(requests):
            url = req_config.get('url', '')
            method = req_config.get('method', 'GET')
            headers = req_config.get('headers', {})
            body = req_config.get('body', None)

            attempt = 0
            success = False

            while attempt <= max_retries and not success:
                try:
                    data_bytes = None
                    if body:
                        if isinstance(body, dict):
                            data_bytes = json.dumps(body).encode('utf-8')
                        elif isinstance(body, str):
                            data_bytes = body.encode('utf-8')

                    req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
                    
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        response_body = response.read()
                        
                        results.append({
                            'index': i,
                            'url': url,
                            'status_code': response.status,
                            'body': response_body.decode('utf-8', errors='replace'),
                            'attempts': attempt + 1,
                            'success': True
                        })
                        success = True

                except Exception as e:
                    last_error = str(e)
                    attempt += 1
                    
                    if attempt <= max_retries:
                        time.sleep((backoff_base ** (attempt - 1)))

                if not success and attempt > max_retries:
                    errors.append({
                        'index': i,
                        'url': url,
                        'error': last_error,
                        'attempts': attempt
                    })
                    
                    if stop_on_first_error:
                        return ActionResult(
                            success=False,
                            message=f"Stopped on first error at request {i}",
                            data={'results': results, 'errors': errors}
                        )

        return ActionResult(
            success=len(errors) == 0,
            message=f"Completed: {len(results)} success, {len(errors)} failed",
            data={'results': results, 'errors': errors, 'total': len(requests)}
        )
