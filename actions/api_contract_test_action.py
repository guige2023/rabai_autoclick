"""API Contract Test action module for RabAI AutoClick.

Tests API contracts against OpenAPI specs and validates
request/response compliance.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiContractTestAction(BaseAction):
    """Test API endpoints against OpenAPI contract.

    Validates request/response against spec definitions
    including status codes, schemas, and headers.
    """
    action_type = "api_contract_test"
    display_name = "API契约测试"
    description = "根据OpenAPI规范测试API契约"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Test API contract.

        Args:
            context: Execution context.
            params: Dict with keys: base_url, spec, test_cases,
                   stop_on_failure.

        Returns:
            ActionResult with test results.
        """
        start_time = time.time()
        try:
            base_url = params.get('base_url', '')
            spec = params.get('spec', {})
            test_cases = params.get('test_cases', [])
            stop_on_failure = params.get('stop_on_failure', False)

            if not base_url or not spec:
                return ActionResult(
                    success=False,
                    message="base_url and spec are required",
                    duration=time.time() - start_time,
                )

            paths = spec.get('paths', {})
            results = []

            for test in test_cases:
                test_name = test.get('name', 'unnamed')
                method = test.get('method', 'GET').upper()
                path = test.get('path', '/')
                expected_status = test.get('expected_status', 200)
                validate_response = test.get('validate_response', True)

                url = base_url.rstrip('/') + path
                headers = test.get('headers', {})
                body = test.get('body')
                test_start = time.time()

                try:
                    body_bytes = None
                    if body:
                        body_bytes = json.dumps(body).encode('utf-8')
                        headers.setdefault('Content-Type', 'application/json')

                    req = Request(url, data=body_bytes, headers=headers, method=method)
                    with urlopen(req, timeout=30) as resp:
                        latency_ms = int((time.time() - test_start) * 1000)
                        status_ok = resp.status == expected_status

                        response_data = json.loads(resp.read())

                        errors = []
                        if validate_response and 'response_schema' in test:
                            schema = test['response_schema']
                            self._validate_response(response_data, schema, errors)

                        results.append({
                            'name': test_name,
                            'method': method,
                            'path': path,
                            'success': status_ok and len(errors) == 0,
                            'status': resp.status,
                            'expected_status': expected_status,
                            'latency_ms': latency_ms,
                            'validation_errors': errors,
                        })

                except HTTPError as e:
                    latency_ms = int((time.time() - test_start) * 1000)
                    results.append({
                        'name': test_name,
                        'method': method,
                        'path': path,
                        'success': e.code == expected_status,
                        'status': e.code,
                        'expected_status': expected_status,
                        'latency_ms': latency_ms,
                        'error': str(e),
                    })
                except Exception as e:
                    results.append({
                        'name': test_name,
                        'method': method,
                        'path': path,
                        'success': False,
                        'error': str(e),
                    })

                if stop_on_failure and results[-1].get('success') is False:
                    break

            passed = sum(1 for r in results if r.get('success'))
            failed = len(results) - passed

            duration = time.time() - start_time
            return ActionResult(
                success=failed == 0,
                message=f"Contract tests: {passed} passed, {failed} failed",
                data={
                    'results': results,
                    'passed': passed,
                    'failed': failed,
                    'total': len(results),
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Contract test error: {str(e)}",
                duration=duration,
            )

    def _validate_response(self, data: Any, schema: Dict, errors: List) -> None:
        """Validate response data against schema."""
        if 'type' in schema:
            expected = schema['type']
            actual_type = type(data).__name__
            if expected == 'object' and not isinstance(data, dict):
                errors.append(f"Expected object, got {actual_type}")
            elif expected == 'array' and not isinstance(data, list):
                errors.append(f"Expected array, got {actual_type}")
        if 'required' in schema and isinstance(data, dict):
            for field in schema['required']:
                if field not in data:
                    errors.append(f"Missing required field: {field}")


class ApiContractVerifierAction(BaseAction):
    """Verify API implementation matches OpenAPI spec.

    Scans endpoints and compares with spec definitions.
    """
    action_type = "api_contract_verifier"
    display_name = "API契约验证"
    description = "验证API实现与OpenAPI规范一致性"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Verify API contract.

        Args:
            context: Execution context.
            params: Dict with keys: base_url, spec, strict.

        Returns:
            ActionResult with verification results.
        """
        start_time = time.time()
        try:
            base_url = params.get('base_url', '')
            spec = params.get('spec', {})
            strict = params.get('strict', False)

            if not base_url or not spec:
                return ActionResult(
                    success=False,
                    message="base_url and spec are required",
                    duration=time.time() - start_time,
                )

            paths = spec.get('paths', {})
            verification_results = []

            for path, methods in paths.items():
                for method, details in methods.items():
                    if method.upper() not in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE'):
                        continue

                    url = base_url.rstrip('/') + path
                    test_start = time.time()

                    try:
                        body = None
                        if method.upper() in ('POST', 'PUT', 'PATCH'):
                            body = json.dumps({}).encode('utf-8')

                        req = Request(url, data=body, method=method.upper())
                        with urlopen(req, timeout=10) as resp:
                            latency = int((time.time() - test_start) * 1000)
                            defined_statuses = list(details.get('responses', {}).keys())
                            actual_status = str(resp.status)
                            is_defined = actual_status in [str(s) for s in defined_statuses]

                            verification_results.append({
                                'path': path,
                                'method': method.upper(),
                                'url': url,
                                'status': resp.status,
                                'defined_statuses': defined_statuses,
                                'defined': is_defined,
                                'latency_ms': latency,
                                'success': is_defined if strict else True,
                            })

                    except HTTPError as e:
                        defined_statuses = list(details.get('responses', {}).keys())
                        is_defined = str(e.code) in [str(s) for s in defined_statuses]
                        verification_results.append({
                            'path': path,
                            'method': method.upper(),
                            'url': url,
                            'status': e.code,
                            'defined_statuses': defined_statuses,
                            'defined': is_defined,
                            'success': is_defined if strict else True,
                        })
                    except Exception as e:
                        verification_results.append({
                            'path': path,
                            'method': method.upper(),
                            'url': url,
                            'success': False,
                            'error': str(e),
                        })

            all_defined = all(r.get('defined', False) for r in verification_results)
            duration = time.time() - start_time

            return ActionResult(
                success=all_defined,
                message=f"Verified {len(verification_results)} endpoints",
                data={
                    'verification': verification_results,
                    'all_defined': all_defined,
                    'total': len(verification_results),
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Verification error: {str(e)}",
                duration=duration,
            )
