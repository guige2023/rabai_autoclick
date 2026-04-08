"""Sentry integration for RabAI AutoClick.

Provides actions to manage Sentry projects, issues, and events.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SentryIssueAction(BaseAction):
    """Manage Sentry issues - list, update, resolve, assign.

    Handles issue lifecycle and tracking.
    """
    action_type = "sentry_issue"
    display_name = "Sentry问题"
    description = "管理Sentry问题：列表、更新、解决、分配"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Sentry issues.

        Args:
            context: Execution context.
            params: Dict with keys:
                - auth_token: Sentry auth token
                - organization: Organization slug
                - operation: list | get | update | resolve | assign
                - project: Project slug
                - issue_id: Issue ID
                - status: Issue status (resolved, unresolved, ignored)
                - assigned_to: User ID or team to assign to
                - tags: Dict of tags to update

        Returns:
            ActionResult with issue data.
        """
        auth_token = params.get('auth_token') or os.environ.get('SENTRY_AUTH_TOKEN')
        organization = params.get('organization') or os.environ.get('SENTRY_ORG')

        if not auth_token or not organization:
            return ActionResult(success=False, message="auth_token and organization are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')
            project = params.get('project')

            if operation == 'list':
                url = f'https://sentry.io/api/0/organizations/{organization}/issues/?'
                query_params = []
                if params.get('status'):
                    query_params.append(f'status={params["status"]}')
                if project:
                    query_params.append(f'project={project}')
                if params.get('query'):
                    query_params.append(f'query={params["query"]}')
                if params.get('limit'):
                    query_params.append(f'limit={params["limit"]}')
                url += '&'.join(query_params)

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    issues = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(issues)} issues", data={'issues': issues})

            elif operation == 'get':
                issue_id = params.get('issue_id')
                if not issue_id:
                    return ActionResult(success=False, message="issue_id is required")

                url = f'https://sentry.io/api/0/organizations/{organization}/issues/{issue_id}/'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    issue = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Issue retrieved", data=issue)

            elif operation == 'update':
                issue_id = params.get('issue_id')
                if not issue_id:
                    return ActionResult(success=False, message="issue_id is required")

                payload = {}
                for key in ['status', 'assigned_to', 'tags']:
                    if key in params:
                        payload[key] = params[key]

                url = f'https://sentry.io/api/0/organizations/{organization}/issues/{issue_id}/'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Issue updated", data=result)

            elif operation == 'resolve':
                issue_id = params.get('issue_id')
                if not issue_id:
                    return ActionResult(success=False, message="issue_id is required")

                payload = {'status': 'resolved'}
                if params.get('statusDetails'):
                    payload['statusDetails'] = params['statusDetails']

                url = f'https://sentry.io/api/0/organizations/{organization}/issues/{issue_id}/'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Issue resolved", data=result)

            elif operation == 'assign':
                issue_id = params.get('issue_id')
                if not issue_id:
                    return ActionResult(success=False, message="issue_id is required")

                assigned_to = params.get('assigned_to', '')
                payload = {'assignedTo': assigned_to}

                url = f'https://sentry.io/api/0/organizations/{organization}/issues/{issue_id}/'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Issue assigned", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Sentry API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Sentry error: {str(e)}")


class SentryProjectAction(BaseAction):
    """Manage Sentry projects and teams.

    Handles project CRUD and team management.
    """
    action_type = "sentry_project"
    display_name = "Sentry项目"
    description = "管理Sentry项目和团队"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Sentry projects.

        Args:
            context: Execution context.
            params: Dict with keys:
                - auth_token: Sentry auth token
                - organization: Organization slug
                - operation: list | get | create | delete
                - project: Project slug
                - team: Team slug
                - name: Project name
                - platform: Project platform

        Returns:
            ActionResult with project data.
        """
        auth_token = params.get('auth_token') or os.environ.get('SENTRY_AUTH_TOKEN')
        organization = params.get('organization') or os.environ.get('SENTRY_ORG')

        if not auth_token or not organization:
            return ActionResult(success=False, message="auth_token and organization are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'list':
                url = f'https://sentry.io/api/0/organizations/{organization}/projects/'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    projects = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(projects)} projects", data={'projects': projects})

            elif operation == 'get':
                project = params.get('project')
                if not project:
                    return ActionResult(success=False, message="project slug is required")

                url = f'https://sentry.io/api/0/projects/{organization}/{project}/'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Project retrieved", data=result)

            elif operation == 'create':
                name = params.get('name')
                team = params.get('team')
                if not name:
                    return ActionResult(success=False, message="name is required")

                payload = {
                    'name': name,
                    'platform': params.get('platform', 'python'),
                }
                if team:
                    payload['team'] = team

                url = f'https://sentry.io/api/0/organizations/{organization}/projects/'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Project {name} created", data=result)

            elif operation == 'delete':
                project = params.get('project')
                if not project:
                    return ActionResult(success=False, message="project slug is required")

                url = f'https://sentry.io/api/0/projects/{organization}/{project}/'
                req = urllib.request.Request(url, method='DELETE', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8')) if resp.status != 204 else {}
                return ActionResult(success=True, message=f"Project {project} deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Sentry API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Sentry error: {str(e)}")


class SentryReleaseAction(BaseAction):
    """Manage Sentry releases and deployment tracking.

    Handles release creation, artifact management, and deploy tracking.
    """
    action_type = "sentry_release"
    display_name = "Sentry发布"
    description = "管理Sentry发布版本和部署追踪"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Sentry releases.

        Args:
            context: Execution context.
            params: Dict with keys:
                - auth_token: Sentry auth token
                - organization: Organization slug
                - operation: create | list | deploy | get_deploy
                - version: Release version
                - projects: List of project slugs
                - ref: Git ref
                - Commits: List of commit dicts {sha, repository, message}
                - environment: Deploy environment
                - started_at: Deploy start time (ISO)

        Returns:
            ActionResult with release data.
        """
        auth_token = params.get('auth_token') or os.environ.get('SENTRY_AUTH_TOKEN')
        organization = params.get('organization') or os.environ.get('SENTRY_ORG')

        if not auth_token or not organization:
            return ActionResult(success=False, message="auth_token and organization are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'create':
                version = params.get('version')
                if not version:
                    return ActionResult(success=False, message="version is required")

                projects = params.get('projects', [])
                payload = {
                    'version': version,
                    'refs': params.get('refs', []),
                    'commits': params.get('commits', []),
                    'projects': projects if projects else ['default'],
                }

                url = f'https://sentry.io/api/0/organizations/{organization}/releases/'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Release {version} created", data=result)

            elif operation == 'list':
                url = f'https://sentry.io/api/0/organizations/{organization}/releases/?'
                query_params = []
                if params.get('project'):
                    query_params.append(f'project={params["project"]}')
                if params.get('limit'):
                    query_params.append(f'limit={params["limit"]}')
                url += '&'.join(query_params)

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    releases = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(releases)} releases", data={'releases': releases})

            elif operation == 'deploy':
                version = params.get('version')
                environment = params.get('environment', 'production')
                if not version:
                    return ActionResult(success=False, message="version is required")

                payload = {
                    'environment': environment,
                    'startedAt': params.get('started_at'),
                }

                url = f'https://sentry.io/api/0/organizations/{organization}/releases/{version}/deploys/'
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Deploy created for {version}", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Sentry API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Sentry error: {str(e)}")
