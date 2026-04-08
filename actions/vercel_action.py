"""Vercel integration for RabAI AutoClick.

Provides actions to deploy projects, manage domains, and monitor deployments on Vercel.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VercelDeployAction(BaseAction):
    """Deploy projects to Vercel.

    Supports git-based deployments and direct file uploads.
    """
    action_type = "vercel_deploy"
    display_name = "Vercel部署"
    description = "向Vercel部署项目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Deploy to Vercel.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Vercel API token
                - project_name: Project name
                - operation: deploy | get | list | cancel
                - deployment_id: Deployment ID (for get/cancel)
                - git_source: Dict with type, repo, branch
                - target_state: ready | production | development

        Returns:
            ActionResult with deployment data.
        """
        api_token = params.get('api_token') or os.environ.get('VERCEL_API_TOKEN')
        operation = params.get('operation', 'list')

        if not api_token:
            return ActionResult(success=False, message="VERCEL_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        try:
            if operation == 'deploy':
                project_name = params.get('project_name')
                if not project_name:
                    return ActionResult(success=False, message="project_name is required for deploy")

                payload = {
                    'name': project_name,
                    'gitSource': params.get('git_source', {}),
                    'target': params.get('target_state', 'production'),
                }

                req = urllib.request.Request(
                    'https://api.vercel.com/v13/deployments',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                return ActionResult(
                    success=True,
                    message=f"Deployment triggered for {project_name}",
                    data={'deployment_id': result.get('id'), 'url': result.get('url'), 'status': result.get('status')}
                )

            elif operation == 'get':
                deployment_id = params.get('deployment_id')
                if not deployment_id:
                    return ActionResult(success=False, message="deployment_id is required for get")

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/deployments/{deployment_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Deployment retrieved", data=result)

            elif operation == 'list':
                project_name = params.get('project_name')
                url = f'https://api.vercel.com/v13/deployments?limit={params.get("limit", 20)}'
                if project_name:
                    url += f'&projectId={project_name}'

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                deployments = result.get('deployments', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(deployments)} deployments",
                    data={'deployments': deployments}
                )

            elif operation == 'cancel':
                deployment_id = params.get('deployment_id')
                if not deployment_id:
                    return ActionResult(success=False, message="deployment_id is required for cancel")

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/deployments/{deployment_id}/cancel',
                    data=b'{}',
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Deployment cancelled", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Vercel API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Vercel error: {str(e)}")


class VercelProjectAction(BaseAction):
    """Manage Vercel projects and configurations.

    Supports project CRUD and environment variable management.
    """
    action_type = "vercel_project"
    display_name = "Vercel项目"
    description = "管理Vercel项目配置和环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Vercel projects.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Vercel API token
                - operation: create | get | update | delete | list
                - project_name: Project name (for create/get/delete)
                - framework: Framework preset (nextjs, react, etc.)
                - build_command: Build command
                - output_directory: Output directory
                - env_vars: List of {key, value, secret} dicts
                - git_repository: Git repository URL

        Returns:
            ActionResult with project data.
        """
        api_token = params.get('api_token') or os.environ.get('VERCEL_API_TOKEN')
        operation = params.get('operation', 'list')

        if not api_token:
            return ActionResult(success=False, message="VERCEL_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        try:
            if operation == 'create':
                project_name = params.get('project_name')
                if not project_name:
                    return ActionResult(success=False, message="project_name is required for create")

                payload = {
                    'name': project_name,
                    'framework': params.get('framework', None),
                    'buildCommand': params.get('build_command'),
                    'outputDirectory': params.get('output_directory'),
                    'gitRepository': params.get('git_repository'),
                }
                payload = {k: v for k, v in payload.items() if v is not None}

                req = urllib.request.Request(
                    'https://api.vercel.com/v13/projects',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                # Add environment variables if provided
                if params.get('env_vars'):
                    for var in params['env_vars']:
                        env_payload = {
                            'key': var['key'],
                            'value': var['value'],
                            'secret': var.get('secret', False),
                        }
                        req2 = urllib.request.Request(
                            f'https://api.vercel.com/v13/projects/{result["id"]}/env',
                            data=json.dumps(env_payload).encode('utf-8'),
                            method='POST',
                            headers=headers
                        )
                        try:
                            urllib.request.urlopen(req2, timeout=15)
                        except Exception:
                            pass

                return ActionResult(success=True, message="Project created", data=result)

            elif operation == 'get':
                project_name = params.get('project_name')
                if not project_name:
                    return ActionResult(success=False, message="project_name is required for get")
                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Project retrieved", data=result)

            elif operation == 'update':
                project_name = params.get('project_name')
                if not project_name:
                    return ActionResult(success=False, message="project_name is required for update")

                payload = {}
                for key in ['framework', 'buildCommand', 'outputDirectory']:
                    if params.get(key):
                        payload[key] = params[key]

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Project updated", data=result)

            elif operation == 'delete':
                project_name = params.get('project_name')
                if not project_name:
                    return ActionResult(success=False, message="project_name is required for delete")
                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Project deleted", data=result)

            elif operation == 'list':
                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects?limit={params.get("limit", 20)}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                projects = result.get('projects', [])
                return ActionResult(success=True, message=f"Found {len(projects)} projects", data={'projects': projects})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Vercel API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Vercel error: {str(e)}")


class VercelDomainAction(BaseAction):
    """Manage domains on Vercel projects.

    Handles domain addition, verification, and SSL certificate management.
    """
    action_type = "vercel_domain"
    display_name = "Vercel域名"
    description = "管理Vercel项目域名和SSL证书"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Vercel domains.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Vercel API token
                - project_name: Project name
                - operation: add_domain | verify | remove | list
                - domain: Domain name
                - redirect: Redirect domain

        Returns:
            ActionResult with domain data.
        """
        api_token = params.get('api_token') or os.environ.get('VERCEL_API_TOKEN')
        project_name = params.get('project_name')

        if not api_token:
            return ActionResult(success=False, message="VERCEL_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'add_domain':
                if not project_name or not params.get('domain'):
                    return ActionResult(success=False, message="project_name and domain are required")

                payload = {'name': params['domain']}
                if params.get('redirect'):
                    payload['redirect'] = params['redirect']

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}/domains',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Domain added", data=result)

            elif operation == 'verify':
                if not project_name or not params.get('domain'):
                    return ActionResult(success=False, message="project_name and domain are required")

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}/domains/{params["domain"]}/verify',
                    data=b'{}',
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(
                    success=result.get('verified', False),
                    message="Domain verified" if result.get('verified') else "Domain not verified",
                    data=result
                )

            elif operation == 'remove':
                if not project_name or not params.get('domain'):
                    return ActionResult(success=False, message="project_name and domain are required")

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}/domains/{params["domain"]}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Domain removed", data=result)

            elif operation == 'list':
                if not project_name:
                    return ActionResult(success=False, message="project_name is required")
                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/projects/{project_name}/domains',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                domains = result.get('domains', [])
                return ActionResult(success=True, message=f"Found {len(domains)} domains", data={'domains': domains})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Vercel API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Vercel error: {str(e)}")


class VercelSecretAction(BaseAction):
    """Manage Vercel secrets and environment variables.

    Handles encrypted environment variable management across projects.
    """
    action_type = "vercel_secret"
    display_name = "Vercel密钥"
    description = "管理Vercel加密密钥和环境变量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Vercel secrets.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_token: Vercel API token
                - operation: create | list | delete
                - secret_name: Name of the secret
                - secret_value: Value to encrypt and store

        Returns:
            ActionResult with secret data.
        """
        api_token = params.get('api_token') or os.environ.get('VERCEL_API_TOKEN')

        if not api_token:
            return ActionResult(success=False, message="VERCEL_API_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'create':
                secret_name = params.get('secret_name')
                secret_value = params.get('secret_value')

                if not secret_name or not secret_value:
                    return ActionResult(success=False, message="secret_name and secret_value are required")

                payload = {'name': secret_name, 'value': secret_value}

                req = urllib.request.Request(
                    'https://api.vercel.com/v13/secrets',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Secret created", data={'name': secret_name})

            elif operation == 'list':
                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/secrets?limit={params.get("limit", 20)}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                secrets = result.get('secrets', [])
                return ActionResult(success=True, message=f"Found {len(secrets)} secrets", data={'secrets': secrets})

            elif operation == 'delete':
                secret_name = params.get('secret_name')
                if not secret_name:
                    return ActionResult(success=False, message="secret_name is required")

                req = urllib.request.Request(
                    f'https://api.vercel.com/v13/secrets/{secret_name}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Secret deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Vercel API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Vercel error: {str(e)}")
