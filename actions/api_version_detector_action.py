"""API Version Detector action module for RabAI AutoClick.

Detects API versions from endpoints, headers, and response fields.
"""

import json
import time
import sys
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiVersionDetectorAction(BaseAction):
    """Detect API version from various sources.

    Checks URL paths, Accept headers, APIKey prefixes,
    and response schemas to determine version.
    """
    action_type = "api_version_detector"
    display_name = "API版本检测"
    description = "从URL、Header和响应中检测API版本"

    VERSION_PATTERNS = [
        (r'/v(\d+(?:\.\d+)?)/', 'path'),
        (r'/api/v(\d+(?:\.\d+)?)/', 'path'),
        (r'-v(\d+(?:\.\d+)?)', 'path'),
        (r'api-version["\']?\s*[:=]\s*["\']?(\d+(?:\.\d+)?)', 'header'),
        (r'X-API-Version:\s*(\d+(?:\.\d+)?)', 'header'),
        (r'version["\']?\s*[:=]\s*["\']?(\d+(?:\.\d+)?)', 'field'),
    ]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Detect API version.

        Args:
            context: Execution context.
            params: Dict with keys: url, headers, response_body,
                   check_endpoints (list), timeout.

        Returns:
            ActionResult with detected version info.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            headers = params.get('headers', {})
            response_body = params.get('response_body', None)
            check_endpoints = params.get('check_endpoints', ['/health', '/version', '/api'])
            timeout = params.get('timeout', 10)

            detected_versions = []
            sources = []

            # Check URL path
            if url:
                for pattern, source in self.VERSION_PATTERNS:
                    match = re.search(pattern, url)
                    if match:
                        version = match.group(1)
                        detected_versions.append(version)
                        sources.append(f'url:{source}')

            # Check headers
            for header_name, header_value in headers.items():
                if isinstance(header_value, str):
                    for pattern, source in self.VERSION_PATTERNS:
                        match = re.search(pattern, header_value)
                        if match:
                            version = match.group(1)
                            if version not in detected_versions:
                                detected_versions.append(version)
                                sources.append(f'header:{header_name}:{source}')

            # Check response body
            if response_body:
                body_str = json.dumps(response_body) if isinstance(response_body, dict) else str(response_body)
                for pattern, source in self.VERSION_PATTERNS:
                    match = re.search(pattern, body_str)
                    if match:
                        version = match.group(1)
                        if version not in detected_versions:
                            detected_versions.append(version)
                            sources.append(f'body:{source}')

            # Probe endpoints
            probed_versions = []
            for endpoint in check_endpoints:
                probe_url = url.rstrip('/') + '/' + endpoint.lstrip('/')
                try:
                    req = Request(probe_url, headers=headers)
                    with urlopen(req, timeout=timeout) as resp:
                        content = resp.read().decode('utf-8', errors='ignore')
                        for pattern, source in self.VERSION_PATTERNS:
                            match = re.search(pattern, content)
                            if match:
                                version = match.group(1)
                                probed_versions.append({'version': version, 'endpoint': endpoint, 'source': source})
                except Exception:
                    pass

            # Determine best version
            best_version = detected_versions[0] if detected_versions else None
            confidence = 'low'
            if detected_versions and probed_versions:
                confidence = 'high'
            elif detected_versions or probed_versions:
                confidence = 'medium'

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Detected version: {best_version} (confidence: {confidence})",
                data={
                    'version': best_version,
                    'confidence': confidence,
                    'detected_from': sources,
                    'probed': probed_versions,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Version detection failed: {str(e)}",
                duration=duration,
            )


class ApiVersionComparatorAction(BaseAction):
    """Compare API versions and check compatibility.

    Analyzes version numbers, checks changelog entries,
    and determines upgrade paths.
    """
    action_type = "api_version_comparator"
    display_name = "API版本比较"
    description = "比较API版本并判断兼容性"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare two API versions.

        Args:
            context: Execution context.
            params: Dict with keys: version_a, version_b,
                   changelog (optional).

        Returns:
            ActionResult with comparison result.
        """
        start_time = time.time()
        try:
            version_a = params.get('version_a', '')
            version_b = params.get('version_b', '')
            changelog = params.get('changelog', [])

            if not version_a or not version_b:
                return ActionResult(
                    success=False,
                    message="Both version_a and version_b are required",
                    duration=time.time() - start_time,
                )

            # Parse versions
            def parse_version(v: str) -> Tuple[int, ...]:
                return tuple(int(x) for x in re.sub(r'[^0-9.].*', '', v).split('.') if x)

            v_a = parse_version(version_a)
            v_b = parse_version(version_b)

            if v_a < v_b:
                direction = 'upgrade'
                diff = 'newer'
            elif v_a > v_b:
                direction = 'downgrade'
                diff = 'older'
            else:
                direction = 'same'
                diff = 'identical'

            # Breaking change detection
            breaking = False
            if changelog:
                for entry in changelog:
                    if isinstance(entry, dict):
                        change_type = entry.get('type', '')
                        change_version = entry.get('version', '')
                        if change_type == 'breaking' and parse_version(change_version) > v_a:
                            breaking = True

            compatibility = 'compatible'
            if breaking:
                compatibility = 'breaking'
            elif direction == 'upgrade' and v_b[0] > v_a[0]:
                compatibility = 'major_upgrade'

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Version comparison: {version_a} vs {version_b}",
                data={
                    'version_a': version_a,
                    'version_b': version_b,
                    'direction': direction,
                    'diff': diff,
                    'compatibility': compatibility,
                    'breaking_change': breaking,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Version comparison failed: {str(e)}",
                duration=duration,
            )
