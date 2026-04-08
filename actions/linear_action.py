"""Linear action module for RabAI AutoClick.

Provides Linear API operations for issues, projects, and teams.
"""

import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LinearAction(BaseAction):
    """Linear issue tracking operations.
    
    Supports creating, updating, and managing Linear issues,
    projects, and team workflows.
    """
    action_type = "linear"
    display_name = "Linear工单管理"
    description = "Linear工单创建、项目与团队管理"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Linear operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'create_issue', 'get_issue', 'update_issue', 'list_issues', 'create_project'
                - api_key: Linear API key (or env LINEAR_API_KEY)
                - team_id: Linear team ID
                - issue_id: Issue ID for operations
                - title: Issue title
                - description: Issue description
                - state: Issue state (todo, in_progress, done)
                - priority: Priority (0-4)
        
        Returns:
            ActionResult with operation result.
        """
        api_key = params.get('api_key') or os.environ.get('LINEAR_API_KEY')
        if not api_key:
            return ActionResult(success=False, message="Linear API key required (set LINEAR_API_KEY env)")
        
        command = params.get('command', 'list_issues')
        team_id = params.get('team_id')
        issue_id = params.get('issue_id')
        title = params.get('title')
        description = params.get('description', '')
        state = params.get('state', 'triage')
        priority = params.get('priority', 0)
        
        base_url = "https://api.linear.app/graphql/v1"
        headers = {'Authorization': f'{api_key}', 'Content-Type': 'application/json'}
        
        if command == 'create_issue':
            if not team_id or not title:
                return ActionResult(success=False, message="team_id and title required for create_issue")
            return self._linear_create_issue(base_url, headers, team_id, title, description, priority, state)
        
        if command == 'get_issue':
            if not issue_id:
                return ActionResult(success=False, message="issue_id required for get_issue")
            return self._linear_get_issue(base_url, headers, issue_id)
        
        if command == 'update_issue':
            if not issue_id:
                return ActionResult(success=False, message="issue_id required for update_issue")
            return self._linear_update_issue(base_url, headers, issue_id, params)
        
        if command == 'list_issues':
            if not team_id:
                return ActionResult(success=False, message="team_id required for list_issues")
            return self._linear_list_issues(base_url, headers, team_id, params.get('limit', 10))
        
        if command == 'create_project':
            return self._linear_create_project(base_url, headers, params.get('name', 'Untitled'), params.get('description', ''))
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _linear_graphql(self, base_url: str, headers: Dict, query: str, variables: Optional[Dict] = None) -> Dict:
        """Execute GraphQL query against Linear API."""
        from urllib.request import Request, urlopen
        
        payload: Dict[str, Any] = {'query': query}
        if variables:
            payload['variables'] = variables
        
        data = json.dumps(payload).encode('utf-8')
        request = Request(base_url, data=data, headers=headers, method='POST')
        with urlopen(request, timeout=15) as resp:
            return json.loads(resp.read().decode())
    
    def _linear_create_issue(self, base_url: str, headers: Dict, team_id: str, title: str, description: str, priority: int, state: str) -> ActionResult:
        """Create Linear issue."""
        query = """
        mutation CreateIssue($teamId: String!, $title: String!, $description: String, $priority: Int, $state: String) {
            issueCreate(input: {
                teamId: $teamId,
                title: $title,
                description: $description,
                priority: $priority,
                stateId: $state
            }) {
                success
                issue {
                    id
                    identifier
                    title
                }
            }
        }
        """
        variables = {
            'teamId': team_id,
            'title': title,
            'description': description,
            'priority': priority,
            'state': state
        }
        
        try:
            result = self._linear_graphql(base_url, headers, query, variables)
            data = result.get('data', {}).get('issueCreate', {})
            if data.get('success'):
                issue = data.get('issue', {})
                return ActionResult(
                    success=True,
                    message=f"Created issue {issue.get('identifier')}: {title}",
                    data={'issue_id': issue.get('id'), 'identifier': issue.get('identifier')}
                )
            return ActionResult(success=False, message="Failed to create issue")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create issue: {e}")
    
    def _linear_get_issue(self, base_url: str, headers: Dict, issue_id: str) -> ActionResult:
        """Get Linear issue."""
        query = """
        query GetIssue($id: String!) {
            issue(id: $id) {
                id
                identifier
                title
                description
                priority
                state {
                    name
                }
                assignee {
                    name
                    email
                }
                createdAt
                updatedAt
            }
        }
        """
        try:
            result = self._linear_graphql(base_url, headers, query, {'id': issue_id})
            issue = result.get('data', {}).get('issue')
            if issue:
                return ActionResult(success=True, message=f"Retrieved {issue.get('identifier')}", data={'issue': issue})
            return ActionResult(success=False, message="Issue not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get issue: {e}")
    
    def _linear_update_issue(self, base_url: str, headers: Dict, issue_id: str, params: Dict) -> ActionResult:
        """Update Linear issue."""
        query = """
        mutation UpdateIssue($id: String!, $title: String, $priority: Int, $description: String) {
            issueUpdate(id: $id, input: {
                title: $title,
                priority: $priority,
                description: $description
            }) {
                success
            }
        }
        """
        variables = {
            'id': issue_id,
            'title': params.get('title'),
            'priority': params.get('priority'),
            'description': params.get('description')
        }
        try:
            result = self._linear_graphql(base_url, headers, query, variables)
            success = result.get('data', {}).get('issueUpdate', {}).get('success', False)
            return ActionResult(success=success, message=f"Updated issue {issue_id[:8]}...")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to update issue: {e}")
    
    def _linear_list_issues(self, base_url: str, headers: Dict, team_id: str, limit: int) -> ActionResult:
        """List Linear issues for a team."""
        query = """
        query ListIssues($teamId: String!, $limit: Int) {
            issues(filter: {team: {id: {eq: $teamId}}}, first: $limit) {
                nodes {
                    id
                    identifier
                    title
                    priority
                    state {
                        name
                    }
                    assignee {
                        name
                    }
                    createdAt
                }
            }
        }
        """
        try:
            result = self._linear_graphql(base_url, headers, query, {'teamId': team_id, 'limit': limit})
            issues = result.get('data', {}).get('issues', {}).get('nodes', [])
            return ActionResult(
                success=True,
                message=f"Listed {len(issues)} issues",
                data={'issues': issues, 'count': len(issues)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to list issues: {e}")
    
    def _linear_create_project(self, base_url: str, headers: Dict, name: str, description: str) -> ActionResult:
        """Create Linear project."""
        query = """
        mutation CreateProject($name: String!, $description: String) {
            projectCreate(input: {name: $name, description: $description}) {
                success
                project {
                    id
                    name
                }
            }
        }
        """
        try:
            result = self._linear_graphql(base_url, headers, query, {'name': name, 'description': description})
            data = result.get('data', {}).get('projectCreate', {})
            if data.get('success'):
                project = data.get('project', {})
                return ActionResult(success=True, message=f"Created project: {name}", data={'project_id': project.get('id'), 'name': project.get('name')})
            return ActionResult(success=False, message="Failed to create project")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create project: {e}")
