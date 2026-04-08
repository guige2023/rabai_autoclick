"""LaunchDarkly integration for RabAI AutoClick.

Provides actions to manage feature flags, flag settings, and user segments.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LaunchDarklyFlagAction(BaseAction):
    """Manage LaunchDarkly feature flags - create, update, toggle flags.

    Handles flag lifecycle and targeting rules.
    """
    action_type = "launchdarkly_flag"
    display_name = "LaunchDarkly特性开关"
    description = "管理LaunchDarkly特性开关：创建、更新、切换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage LaunchDarkly feature flags.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: LaunchDarkly API key
                - project_key: Project key
                - environment_key: Environment key
                - operation: list | get | toggle | create | update
                - flag_key: Feature flag key
                - value: Flag value (for toggle)
                - variation: Variation index (for toggle)
                - comment: Change comment

        Returns:
            ActionResult with flag data.
        """
        api_key = params.get('api_key') or os.environ.get('LAUNCHDARKLY_API_KEY')
        project_key = params.get('project_key') or os.environ.get('LAUNCHDARKLY_PROJECT_KEY', 'default')
        env_key = params.get('environment_key') or os.environ.get('LAUNCHDARKLY_ENV_KEY', 'production')

        if not api_key:
            return ActionResult(success=False, message="LAUNCHDARKLY_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': api_key,
            'Content-Type': 'application/json',
            'LD-API-Version': 'beta'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'list':
                url = f'https://app.launchdarkly.com/api/v2/flags/{project_key}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                flags = result.get('items', [])
                return ActionResult(success=True, message=f"Found {len(flags)} flags", data={'flags': flags})

            elif operation == 'get':
                flag_key = params.get('flag_key')
                if not flag_key:
                    return ActionResult(success=False, message="flag_key is required")

                url = f'https://app.launchdarkly.com/api/v2/flags/{project_key}/{flag_key}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Flag retrieved", data=result)

            elif operation == 'toggle':
                flag_key = params.get('flag_key')
                value = params.get('value')
                variation = params.get('variation', 0)

                if not flag_key:
                    return ActionResult(success=False, message="flag_key is required")

                patch = [{
                    'op': 'replace',
                    'path': f'/environments/{env_key}/on',
                    'value': value if value is not None else True
                }]

                url = f'https://app.launchdarkly.com/api/v2/flags/{project_key}/{flag_key}'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(patch).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Flag {flag_key} toggled", data=result)

            elif operation == 'create':
                flag_key = params.get('flag_key')
                if not flag_key:
                    return ActionResult(success=False, message="flag_key is required")

                payload = {
                    'key': flag_key,
                    'name': params.get('name', flag_key),
                    'description': params.get('description', ''),
                    'kind': params.get('kind', 'boolean'),
                    'variations': params.get('variations', [True, False]),
                }

                req = urllib.request.Request(
                    f'https://app.launchdarkly.com/api/v2/flags/{project_key}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Flag {flag_key} created", data={'key': result.get('key')})

            elif operation == 'update':
                flag_key = params.get('flag_key')
                if not flag_key:
                    return ActionResult(success=False, message="flag_key is required")

                patch = []
                for key in ['name', 'description']:
                    if key in params:
                        patch.append({'op': 'replace', 'path': f'/{key}', 'value': params[key]})

                if not patch:
                    return ActionResult(success=False, message="No fields to update")

                url = f'https://app.launchdarkly.com/api/v2/flags/{project_key}/{flag_key}'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(patch).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Flag {flag_key} updated", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"LaunchDarkly API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"LaunchDarkly error: {str(e)}")


class LaunchDarklySegmentAction(BaseAction):
    """Manage LaunchDarkly user segments.

    Handles segment creation and member management.
    """
    action_type = "launchdarkly_segment"
    display_name = "LaunchDarkly用户群体"
    description = "管理LaunchDarkly用户群体"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage LaunchDarkly segments.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: LaunchDarkly API key
                - project_key: Project key
                - environment_key: Environment key
                - operation: list | get | create | update | add_users | remove_users
                - segment_key: Segment key
                - name: Segment name
                - description: Segment description
                - included: List of included user keys
                - excluded: List of excluded user keys

        Returns:
            ActionResult with segment data.
        """
        api_key = params.get('api_key') or os.environ.get('LAUNCHDARKLY_API_KEY')
        project_key = params.get('project_key') or os.environ.get('LAUNCHDARKLY_PROJECT_KEY', 'default')
        env_key = params.get('environment_key') or os.environ.get('LAUNCHDARKLY_ENV_KEY', 'production')

        if not api_key:
            return ActionResult(success=False, message="LAUNCHDARKLY_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': api_key,
            'Content-Type': 'application/json',
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'list':
                url = f'https://app.launchdarkly.com/api/v2/segments/{project_key}/{env_key}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                segments = result.get('items', [])
                return ActionResult(success=True, message=f"Found {len(segments)} segments", data={'segments': segments})

            elif operation == 'get':
                segment_key = params.get('segment_key')
                if not segment_key:
                    return ActionResult(success=False, message="segment_key is required")

                url = f'https://app.launchdarkly.com/api/v2/segments/{project_key}/{env_key}/{segment_key}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Segment retrieved", data=result)

            elif operation == 'create':
                segment_key = params.get('segment_key')
                if not segment_key:
                    return ActionResult(success=False, message="segment_key is required")

                payload = {
                    'key': segment_key,
                    'name': params.get('name', segment_key),
                    'description': params.get('description', ''),
                    'included': params.get('included', []),
                    'excluded': params.get('excluded', []),
                }

                req = urllib.request.Request(
                    f'https://app.launchdarkly.com/api/v2/segments/{project_key}/{env_key}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Segment {segment_key} created", data={'key': result.get('key')})

            elif operation == 'add_users':
                segment_key = params.get('segment_key')
                included = params.get('included', [])

                if not segment_key:
                    return ActionResult(success=False, message="segment_key is required")

                patch = [{'op': 'add', 'path': '/included/-', 'value': u} for u in included]

                url = f'https://app.launchdarkly.com/api/v2/segments/{project_key}/{env_key}/{segment_key}'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(patch).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Added {len(included)} users to segment", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"LaunchDarkly API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"LaunchDarkly error: {str(e)}")
