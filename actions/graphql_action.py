"""GraphQL action module for RabAI AutoClick.

Provides GraphQL query and mutation execution with support for
variables, fragments, subscriptions, and response parsing.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GraphQLAction(BaseAction):
    """Execute GraphQL queries, mutations, and subscriptions.
    
    Supports GraphQL queries and mutations with variables,
    fragments, inline fragments, and response validation.
    """
    action_type = "graphql"
    display_name = "GraphQL请求"
    description = "执行GraphQL查询和变更，支持变量和片段"
    DEFAULT_TIMEOUT = 30
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a GraphQL operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoint, query, variables,
                   operation_name, headers, timeout, expected_errors.
        
        Returns:
            ActionResult with GraphQL response data.
        """
        endpoint = params.get('endpoint', '')
        if not endpoint:
            return ActionResult(success=False, message="GraphQL endpoint is required")
        
        query = params.get('query', '')
        if not query:
            return ActionResult(success=False, message="GraphQL query is required")
        
        variables = params.get('variables', {})
        operation_name = params.get('operation_name')
        headers = params.get('headers', {})
        timeout = params.get('timeout', self.DEFAULT_TIMEOUT)
        expected_errors = params.get('expected_errors', [])
        
        request_body = {
            'query': query
        }
        if variables:
            request_body['variables'] = variables
        if operation_name:
            request_body['operationName'] = operation_name
        
        headers_dict = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if headers:
            headers_dict.update({str(k): str(v) for k, v in headers.items()})
        
        try:
            request_body_bytes = json.dumps(request_body).encode('utf-8')
            request = Request(
                endpoint,
                data=request_body_bytes,
                headers=headers_dict,
                method='POST'
            )
            
            start_time = time.time()
            with urlopen(request, timeout=timeout) as response:
                elapsed = time.time() - start_time
                response_body = response.read().decode('utf-8')
                
                try:
                    response_data = json.loads(response_body)
                except json.JSONDecodeError as e:
                    return ActionResult(
                        success=False,
                        message=f"Failed to parse response: {e}",
                        data={'raw_response': response_body}
                    )
                
                has_errors = 'errors' in response_data
                error_messages = []
                if has_errors:
                    errors = response_data.get('errors', [])
                    error_messages = [err.get('message', str(err)) for err in errors]
                
                expected_match = True
                if expected_errors and has_errors:
                    for expected in expected_errors:
                        if not any(expected.lower() in msg.lower() for msg in error_messages):
                            expected_match = False
                            break
                
                success = (not has_errors or expected_match)
                
                return ActionResult(
                    success=success,
                    message=f"GraphQL {'errors' if has_errors else 'ok'} in {elapsed:.2f}s",
                    data={
                        'data': response_data.get('data'),
                        'errors': response_data.get('errors') if has_errors else None,
                        'elapsed': elapsed
                    }
                )
                
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP {e.code}: {e.reason}",
                data={'status_code': e.code}
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"Connection error: {e.reason}",
                data={'error': str(e)}
            )
        except TimeoutError:
            return ActionResult(
                success=False,
                message=f"Request timeout after {timeout}s",
                data={'timeout': timeout}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GraphQL execution failed: {e}",
                data={'error': str(e)}
            )


class GraphQLSubscriptionAction(BaseAction):
    """Execute GraphQL subscriptions for real-time data streams.
    
    Note: Basic polling implementation. For true streaming,
    use WebSocket-based GraphQL subscription libraries.
    """
    action_type = "graphql_subscription"
    display_name = "GraphQL订阅"
    description = "GraphQL订阅（轮询模式）"
    DEFAULT_TIMEOUT = 30
    DEFAULT_INTERVAL = 5
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a GraphQL subscription via polling.
        
        Args:
            context: Execution context.
            params: Dict with keys: endpoint, query, variables,
                   interval, max_iterations, on_data callback.
        
        Returns:
            ActionResult with collected subscription data.
        """
        endpoint = params.get('endpoint', '')
        if not endpoint:
            return ActionResult(success=False, message="GraphQL endpoint is required")
        
        query = params.get('query', '')
        if not query:
            return ActionResult(success=False, message="GraphQL query is required")
        
        variables = params.get('variables', {})
        interval = params.get('interval', self.DEFAULT_INTERVAL)
        max_iterations = params.get('max_iterations', 10)
        headers = params.get('headers', {})
        timeout = params.get('timeout', self.DEFAULT_TIMEOUT)
        
        collected_data = []
        iterations = 0
        
        request_body = {'query': query}
        if variables:
            request_body['variables'] = variables
        
        headers_dict = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if headers:
            headers_dict.update({str(k): str(v) for k, v in headers.items()})
        
        try:
            request_body_bytes = json.dumps(request_body).encode('utf-8')
            
            while iterations < max_iterations:
                iterations += 1
                
                try:
                    request = Request(
                        endpoint,
                        data=request_body_bytes,
                        headers=headers_dict,
                        method='POST'
                    )
                    
                    with urlopen(request, timeout=timeout) as response:
                        response_body = response.read().decode('utf-8')
                        response_data = json.loads(response_body)
                        
                        if 'data' in response_data:
                            collected_data.append(response_data['data'])
                        elif 'errors' in response_data:
                            collected_data.append({
                                'errors': response_data['errors']
                            })
                    
                    if iterations < max_iterations:
                        time.sleep(interval)
                        
                except Exception as e:
                    collected_data.append({'error': str(e)})
                    if iterations < max_iterations:
                        time.sleep(interval)
            
            return ActionResult(
                success=True,
                message=f"Subscription completed: {len(collected_data)} iterations",
                data={
                    'iterations': iterations,
                    'collected_data': collected_data
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Subscription failed: {e}",
                data={'error': str(e)}
            )
