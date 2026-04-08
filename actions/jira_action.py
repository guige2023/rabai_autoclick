"""Jira action module for RabAI AutoClick.

Provides Jira Cloud/Server operations including issue management,
project access, and workflow transitions.
"""

import os
import sys
import time
import base64
import json
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JiraClient:
    """Jira API client for Cloud and Server.
    
    Provides methods for interacting with Jira issues, projects, and workflows.
    """
    
    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        cloud: bool = True
    ) -> None:
        """Initialize Jira client.
        
        Args:
            url: Jira server URL (e.g., 'https://yourcompany.atlassian.net').
            username: Username for basic auth (for Server).
            password: Password or API token for authentication.
            token: API token (for Cloud, preferred over password).
            cloud: Whether this is Jira Cloud (True) or Server (False).
        """
        self.url = url.rstrip("/")
        self.username = username
        self.token = token or password
        self.cloud = cloud
        self._api_base = f"{self.url}/rest/api/3"
        
        if self.cloud:
            self._auth = base64.b64encode(
                f"{username}:{self.token}".encode()
            ).decode() if username and self.token else ""
        else:
            import urllib.parse
            self._auth = base64.b64encode(
                f"{username}:{password}".encode()
            ).decode() if username and password else ""
    
    def _headers(self) -> Dict[str, str]:
        """Build request headers."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        if self._auth:
            headers["Authorization"] = f"Basic {self._auth}"
        return headers
    
    def _request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make an authenticated API request.
        
        Args:
            method: HTTP method.
            path: API path (e.g., '/issue/ABC-123').
            data: Optional request body.
            params: Optional URL query parameters.
            
        Returns:
            Parsed JSON response.
        """
        url = f"{self._api_base}{path}"
        
        if params:
            import urllib.parse
            url = f"{url}?{urllib.parse.urlencode(params)}"
        
        body = json.dumps(data).encode("utf-8") if data else None
        
        headers = self._headers()
        if body:
            headers["Content-Type"] = "application/json"
        
        req = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(req, timeout=30) as response:
                if response.status == 204:
                    return {}
                return json.loads(response.read().decode("utf-8"))
        
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            try:
                error_data = json.loads(error_body)
                message = error_data.get("errorMessages", ["Unknown error"])[0]
            except (json.JSONDecodeError, IndexError):
                message = f"HTTP {e.code}: {error_body[:500]}"
            raise Exception(f"Jira API error: {message}")
    
    def get_myself(self) -> Dict[str, Any]:
        """Get current user information.
        
        Returns:
            Current user data.
        """
        return self._request("GET", "/myself")
    
    def get_issue(
        self,
        issue_key: str,
        fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get an issue by key.
        
        Args:
            issue_key: Issue key (e.g., 'ABC-123').
            fields: Optional list of fields to return.
            
        Returns:
            Issue data dictionary.
        """
        params = {}
        if fields:
            params["fields"] = ",".join(fields)
        
        return self._request("GET", f"/issue/{issue_key}", params=params)
    
    def create_issue(
        self,
        project_key: str,
        issue_type: str,
        summary: str,
        description: Optional[str] = None,
        priority: Optional[str] = None,
        labels: Optional[List[str]] = None,
        assignee: Optional[str] = None,
        reporter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new issue.
        
        Args:
            project_key: Project key (e.g., 'ABC').
            issue_type: Issue type (e.g., 'Task', 'Bug', 'Story').
            summary: Issue summary/title.
            description: Optional issue description.
            priority: Optional priority name.
            labels: Optional list of labels.
            assignee: Optional assignee username.
            reporter: Optional reporter username.
            
        Returns:
            Created issue data with key.
        """
        fields: Dict[str, Any] = {
            "project": {"key": project_key},
            "summary": summary,
            "issuetype": {"name": issue_type}
        }
        
        if description:
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [{
                    "type": "paragraph",
                    "content": [{"type": "text", "text": description}]
                }]
            }
        
        if priority:
            fields["priority"] = {"name": priority}
        
        if labels:
            fields["labels"] = labels
        
        if assignee:
            fields["assignee"] = {"name": assignee}
        
        if reporter:
            fields["reporter"] = {"name": reporter}
        
        return self._request("POST", "/issue", data={"fields": fields})
    
    def update_issue(
        self,
        issue_key: str,
        fields: Dict[str, Any],
        transition: Optional[str] = None
    ) -> bool:
        """Update an issue.
        
        Args:
            issue_key: Issue key to update.
            fields: Fields to update.
            transition: Optional transition name or ID.
            
        Returns:
            True if successful.
        """
        if transition:
            transitions = self.get_transitions(issue_key)
            transition_id = None
            
            for t in transitions:
                if str(t.get("name")) == str(transition) or str(t.get("id")) == str(transition):
                    transition_id = t["id"]
                    break
            
            if transition_id:
                self._request(
                    "POST",
                    f"/issue/{issue_key}/transitions",
                    data={"transition": {"id": transition_id}}
                )
        
        if fields:
            self._request("PUT", f"/issue/{issue_key}", data={"fields": fields})
        
        return True
    
    def delete_issue(self, issue_key: str) -> bool:
        """Delete an issue.
        
        Args:
            issue_key: Issue key to delete.
            
        Returns:
            True if deleted successfully.
        """
        self._request("DELETE", f"/issue/{issue_key}")
        return True
    
    def search_issues(
        self,
        jql: str,
        fields: Optional[List[str]] = None,
        max_results: int = 50,
        start_at: int = 0
    ) -> Dict[str, Any]:
        """Search for issues using JQL.
        
        Args:
            jql: JQL query string.
            fields: Optional list of fields to return.
            max_results: Maximum results to return.
            start_at: Result offset for pagination.
            
        Returns:
            Search results with issues and total count.
        """
        params = {
            "jql": jql,
            "maxResults": max_results,
            "startAt": start_at
        }
        
        if fields:
            params["fields"] = ",".join(fields)
        
        return self._request("GET", "/search", params=params)
    
    def add_comment(
        self,
        issue_key: str,
        body: str,
        visibility: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Add a comment to an issue.
        
        Args:
            issue_key: Issue key to comment on.
            body: Comment body text.
            visibility: Optional visibility restrictions.
            
        Returns:
            Created comment data.
        """
        data: Dict[str, Any] = {
            "type": "doc",
            "version": 1,
            "content": [{
                "type": "paragraph",
                "content": [{"type": "text", "text": body}]
            }]
        }
        
        if visibility:
            data["visibility"] = visibility
        
        return self._request("POST", f"/issue/{issue_key}/comment", data=data)
    
    def get_comments(self, issue_key: str) -> List[Dict[str, Any]]:
        """Get all comments for an issue.
        
        Args:
            issue_key: Issue key.
            
        Returns:
            List of comment dictionaries.
        """
        data = self._request("GET", f"/issue/{issue_key}/comment")
        return data.get("comments", [])
    
    def get_transitions(self, issue_key: str) -> List[Dict[str, Any]]:
        """Get available transitions for an issue.
        
        Args:
            issue_key: Issue key.
            
        Returns:
            List of transition dictionaries.
        """
        data = self._request("GET", f"/issue/{issue_key}/transitions")
        return data.get("transitions", [])
    
    def transition_issue(
        self,
        issue_key: str,
        transition: Union[str, int]
    ) -> bool:
        """Transition an issue to a new status.
        
        Args:
            issue_key: Issue key.
            transition: Transition name or ID.
            
        Returns:
            True if transitioned successfully.
        """
        transitions = self.get_transitions(issue_key)
        transition_id = None
        
        for t in transitions:
            if str(t.get("name")) == str(transition) or str(t.get("id")) == str(transition):
                transition_id = t["id"]
                break
        
        if not transition_id:
            raise Exception(f"Transition '{transition}' not found")
        
        self._request(
            "POST",
            f"/issue/{issue_key}/transitions",
            data={"transition": {"id": transition_id}}
        )
        return True
    
    def get_projects(self) -> List[Dict[str, Any]]:
        """Get all accessible projects.
        
        Returns:
            List of project dictionaries.
        """
        data = self._request("GET", "/project")
        return data if isinstance(data, list) else []
    
    def get_project(self, project_key: str) -> Dict[str, Any]:
        """Get a project by key.
        
        Args:
            project_key: Project key.
            
        Returns:
            Project data dictionary.
        """
        return self._request("GET", f"/project/{project_key}")
    
    def get_issue_types(self) -> List[Dict[str, Any]]:
        """Get all issue types.
        
        Returns:
            List of issue type dictionaries.
        """
        data = self._request("GET", "/issuetype")
        return data if isinstance(data, list) else []
    
    def get_priorities(self) -> List[Dict[str, Any]]:
        """Get all priorities.
        
        Returns:
            List of priority dictionaries.
        """
        data = self._request("GET", "/priority")
        return data if isinstance(data, list) else []


class JiraAction(BaseAction):
    """Jira action for issue and project management.
    
    Supports creating, updating, searching issues and managing projects.
    """
    action_type: str = "jira"
    display_name: str = "Jira动作"
    description: str = "Jira问题管理，支持创建、更新、搜索和管理项目"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[JiraClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Jira operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                self._client = None
                return ActionResult(
                    success=True,
                    message="Jira client disconnected",
                    duration=time.time() - start_time
                )
            elif operation == "get_issue":
                return self._get_issue(params, start_time)
            elif operation == "create_issue":
                return self._create_issue(params, start_time)
            elif operation == "update_issue":
                return self._update_issue(params, start_time)
            elif operation == "delete_issue":
                return self._delete_issue(params, start_time)
            elif operation == "search":
                return self._search_issues(params, start_time)
            elif operation == "add_comment":
                return self._add_comment(params, start_time)
            elif operation == "transition":
                return self._transition(params, start_time)
            elif operation == "list_transitions":
                return self._list_transitions(params, start_time)
            elif operation == "get_projects":
                return self._get_projects(start_time)
            elif operation == "get_issue_types":
                return self._get_issue_types(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Jira operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Jira."""
        url = params.get("url", "")
        username = params.get("username")
        token = params.get("token")
        cloud = params.get("cloud", True)
        
        if not url:
            return ActionResult(
                success=False,
                message="Jira URL is required",
                duration=time.time() - start_time
            )
        
        if not token and not username:
            return ActionResult(
                success=False,
                message="Token or username is required",
                duration=time.time() - start_time
            )
        
        self._client = JiraClient(
            url=url,
            username=username,
            token=token,
            cloud=cloud
        )
        
        try:
            me = self._client.get_myself()
            display_name = me.get("displayName", "unknown")
            
            return ActionResult(
                success=True,
                message=f"Connected to Jira as {display_name}",
                data={"user": display_name},
                duration=time.time() - start_time
            )
        except Exception as e:
            self._client = None
            return ActionResult(
                success=False,
                message=f"Failed to connect: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _require_client(self) -> JiraClient:
        """Ensure a Jira client exists."""
        if not self._client:
            raise RuntimeError("Not connected to Jira. Use 'connect' operation first.")
        return self._client
    
    def _get_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get an issue."""
        client = self._require_client()
        issue_key = params.get("issue_key", "")
        fields = params.get("fields")
        
        if not issue_key:
            return ActionResult(
                success=False,
                message="issue_key is required",
                duration=time.time() - start_time
            )
        
        try:
            issue = client.get_issue(issue_key, fields=fields)
            
            return ActionResult(
                success=True,
                message=f"Retrieved issue: {issue_key}",
                data=issue,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an issue."""
        client = self._require_client()
        project_key = params.get("project_key", "")
        issue_type = params.get("issue_type", "Task")
        summary = params.get("summary", "")
        
        if not project_key or not summary:
            return ActionResult(
                success=False,
                message="project_key and summary are required",
                duration=time.time() - start_time
            )
        
        try:
            issue = client.create_issue(
                project_key=project_key,
                issue_type=issue_type,
                summary=summary,
                description=params.get("description"),
                priority=params.get("priority"),
                labels=params.get("labels"),
                assignee=params.get("assignee"),
                reporter=params.get("reporter")
            )
            
            return ActionResult(
                success=True,
                message=f"Created issue: {issue.get('key')}",
                data=issue,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to create issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _update_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update an issue."""
        client = self._require_client()
        issue_key = params.get("issue_key", "")
        fields = params.get("fields", {})
        transition = params.get("transition")
        
        if not issue_key:
            return ActionResult(
                success=False,
                message="issue_key is required",
                duration=time.time() - start_time
            )
        
        try:
            client.update_issue(issue_key, fields, transition=transition)
            
            return ActionResult(
                success=True,
                message=f"Updated issue: {issue_key}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to update issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _delete_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete an issue."""
        client = self._require_client()
        issue_key = params.get("issue_key", "")
        
        if not issue_key:
            return ActionResult(
                success=False,
                message="issue_key is required",
                duration=time.time() - start_time
            )
        
        try:
            client.delete_issue(issue_key)
            
            return ActionResult(
                success=True,
                message=f"Deleted issue: {issue_key}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to delete issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _search_issues(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Search for issues."""
        client = self._require_client()
        jql = params.get("jql", "")
        fields = params.get("fields")
        max_results = params.get("max_results", 50)
        start_at = params.get("start_at", 0)
        
        if not jql:
            return ActionResult(
                success=False,
                message="jql is required",
                duration=time.time() - start_time
            )
        
        try:
            results = client.search_issues(
                jql=jql,
                fields=fields,
                max_results=max_results,
                start_at=start_at
            )
            
            issues = results.get("issues", [])
            
            return ActionResult(
                success=True,
                message=f"Found {results.get('total', 0)} issues",
                data={
                    "issues": issues,
                    "total": results.get("total", 0),
                    "start_at": results.get("startAt", 0),
                    "max_results": results.get("maxResults", 0)
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Search failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _add_comment(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a comment to an issue."""
        client = self._require_client()
        issue_key = params.get("issue_key", "")
        body = params.get("body", "")
        
        if not issue_key or not body:
            return ActionResult(
                success=False,
                message="issue_key and body are required",
                duration=time.time() - start_time
            )
        
        try:
            comment = client.add_comment(issue_key, body)
            
            return ActionResult(
                success=True,
                message=f"Added comment to {issue_key}",
                data=comment,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to add comment: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _transition(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Transition an issue."""
        client = self._require_client()
        issue_key = params.get("issue_key", "")
        transition = params.get("transition", "")
        
        if not issue_key or not transition:
            return ActionResult(
                success=False,
                message="issue_key and transition are required",
                duration=time.time() - start_time
            )
        
        try:
            client.transition_issue(issue_key, transition)
            
            return ActionResult(
                success=True,
                message=f"Transitioned {issue_key} to '{transition}'",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to transition: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_transitions(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List available transitions."""
        client = self._require_client()
        issue_key = params.get("issue_key", "")
        
        if not issue_key:
            return ActionResult(
                success=False,
                message="issue_key is required",
                duration=time.time() - start_time
            )
        
        try:
            transitions = client.get_transitions(issue_key)
            
            return ActionResult(
                success=True,
                message=f"Found {len(transitions)} transitions",
                data={"transitions": transitions},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get transitions: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_projects(self, start_time: float) -> ActionResult:
        """Get all projects."""
        client = self._require_client()
        
        try:
            projects = client.get_projects()
            
            return ActionResult(
                success=True,
                message=f"Found {len(projects)} projects",
                data={"projects": projects},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get projects: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_issue_types(self, start_time: float) -> ActionResult:
        """Get all issue types."""
        client = self._require_client()
        
        try:
            issue_types = client.get_issue_types()
            
            return ActionResult(
                success=True,
                message=f"Found {len(issue_types)} issue types",
                data={"issue_types": issue_types},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get issue types: {str(e)}",
                duration=time.time() - start_time
            )
