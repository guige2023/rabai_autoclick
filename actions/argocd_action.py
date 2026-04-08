"""ArgoCD integration for RabAI AutoClick.

Provides actions to manage ArgoCD applications, sync status, and health checks.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ArgoCDAppAction(BaseAction):
    """Manage ArgoCD applications - create, sync, rollback.

    Provides application lifecycle management in ArgoCD.
    """
    action_type = "argocd_app"
    display_name = "ArgoCD应用"
    description = "管理ArgoCD应用：创建、同步、回滚"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage ArgoCD applications.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: ArgoCD server URL
                - token: ArgoCD auth token
                - operation: create | sync | get | delete | list | rollback
                - app_name: Application name
                - repo_url: Git repository URL
                - path: Path in repo
                - revision: Git revision (branch/tag/commit)
                - project: ArgoCD project name
                - namespace: Target namespace
                - server: Target cluster server
                - prune: Whether to prune resources
                - force: Force sync even if hash unchanged
                - revision_history: Revision to rollback to

        Returns:
            ActionResult with operation result.
        """
        url = params.get('url') or os.environ.get('ARGOCD_URL', 'https://localhost:8080')
        token = params.get('token') or os.environ.get('ARGOCD_TOKEN')
        operation = params.get('operation', 'list')

        if not token:
            return ActionResult(success=False, message="ARGOCD_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'create':
                app_name = params.get('app_name')
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                payload = {
                    'metadata': {'name': app_name, 'namespace': 'argocd'},
                    'spec': {
                        'project': params.get('project', 'default'),
                        'source': {
                            'repoURL': params.get('repo_url', ''),
                            'path': params.get('path', ''),
                            'targetRevision': params.get('revision', 'HEAD'),
                        },
                        'destination': {
                            'server': params.get('server', 'https://kubernetes.default.svc'),
                            'namespace': params.get('namespace', 'default'),
                        },
                        'syncPolicy': params.get('sync_policy', {'automated': None}),
                    }
                }

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"App {app_name} created", data=result)

            elif operation == 'sync':
                app_name = params.get('app_name')
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                payload = {
                    'prune': params.get('prune', False),
                    'dryRun': params.get('dry_run', False),
                    'revision': params.get('revision'),
                    'force': params.get('force', False),
                }

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}/sync',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"App {app_name} synced", data=result)

            elif operation == 'get':
                app_name = params.get('app_name')
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="App retrieved", data=result)

            elif operation == 'delete':
                app_name = params.get('app_name')
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8')) if resp.status != 200 else {}
                return ActionResult(success=True, message=f"App {app_name} deleted", data=result)

            elif operation == 'list':
                req = urllib.request.Request(
                    f'{url}/api/v1/applications',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                apps = result.get('items', [])
                return ActionResult(success=True, message=f"Found {len(apps)} apps", data={'applications': apps})

            elif operation == 'rollback':
                app_name = params.get('app_name')
                revision = params.get('revision_history')
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                payload = {'id': revision} if revision else {}

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}/rollback',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"App {app_name} rolled back", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"ArgoCD API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"ArgoCD error: {str(e)}")


class ArgoCDSyncAction(BaseAction):
    """Monitor and manage ArgoCD sync status and health.

    Tracks application sync state and resource health.
    """
    action_type = "argocd_sync"
    display_name = "ArgoCD同步状态"
    description = "监控ArgoCD同步状态和健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Monitor ArgoCD sync status.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: ArgoCD server URL
                - token: ArgoCD auth token
                - operation: get_status | watch | list_resources | get_resource
                - app_name: Application name
                - resource_namespace: Resource namespace
                - resource_kind: Resource kind
                - resource_name: Resource name

        Returns:
            ActionResult with sync status data.
        """
        url = params.get('url') or os.environ.get('ARGOCD_URL', 'https://localhost:8080')
        token = params.get('token') or os.environ.get('ARGOCD_TOKEN')

        if not token:
            return ActionResult(success=False, message="ARGOCD_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            app_name = params.get('app_name')
            operation = params.get('operation', 'get_status')

            if operation == 'get_status':
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                status = result.get('status', {})
                health = status.get('health', {})
                sync = status.get('sync', {})

                return ActionResult(
                    success=True,
                    message=f"Status: {sync.get('status', 'Unknown')}, Health: {health.get('status', 'Unknown')}",
                    data={
                        'sync_status': sync.get('status'),
                        'health_status': health.get('status'),
                        'history': status.get('history', []),
                    }
                )

            elif operation == 'list_resources':
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/applications/{app_name}/resource-tree',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resources = json.loads(resp.read().decode('utf-8'))
                return ActionResult(
                    success=True,
                    message=f"Found {len(resources)} resources",
                    data={'resources': resources}
                )

            elif operation == 'get_resource':
                if not app_name:
                    return ActionResult(success=False, message="app_name is required")

                query_params = {
                    'namespace': params.get('resource_namespace', ''),
                    'kind': params.get('resource_kind', ''),
                    'name': params.get('resource_name', ''),
                }

                url2 = f'{url}/api/v1/applications/{app_name}/resource?' + '&'.join(f'{k}={v}' for k, v in query_params.items() if v)
                req = urllib.request.Request(url2, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Resource retrieved", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"ArgoCD API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"ArgoCD error: {str(e)}")
