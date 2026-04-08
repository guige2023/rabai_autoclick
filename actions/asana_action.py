"""Asana action module for RabAI AutoClick.

Provides integration with Asana API for task management,
project operations, and team workflows.
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


class AsanaAction(BaseAction):
    """Asana API integration for task and project management.

    Supports task CRUD, project management, workspace operations,
    and team collaboration features.

    Args:
        config: Asana configuration containing access_token and workspace_gid
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.access_token = self.config.get("access_token", "")
        self.workspace_gid = self.config.get("workspace_gid", "")
        self.api_base = "https://app.asana.com/api/1.0"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Asana API."""
        url = f"{self.api_base}/{endpoint}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result.get("data", result)
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def list_workspaces(self) -> ActionResult:
        """List all workspaces accessible to the user.

        Returns:
            ActionResult with workspaces list
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("GET", "workspaces")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"workspaces": result})

    def list_projects(
        self,
        workspace: Optional[str] = None,
        team: Optional[str] = None,
    ) -> ActionResult:
        """List projects in a workspace or team.

        Args:
            workspace: Workspace GID (uses config default if not provided)
            team: Team GID to filter by

        Returns:
            ActionResult with projects list
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        workspace = workspace or self.workspace_gid
        endpoint = f"workspaces/{workspace}/projects"
        params = {}
        if team:
            params["team"] = team

        result = self._make_request("GET", endpoint, params=params)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"projects": result})

    def create_task(
        self,
        name: str,
        project_gid: str,
        notes: Optional[str] = None,
        assignee: Optional[str] = None,
        due_on: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> ActionResult:
        """Create a new task in a project.

        Args:
            name: Task name
            project_gid: Project GID
            notes: Task description
            assignee: User GID to assign
            due_on: Due date (YYYY-MM-DD)
            tags: List of tag names

        Returns:
            ActionResult with created task
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        data = {
            "data": {
                "name": name,
                "projects": [project_gid],
            }
        }
        if notes:
            data["data"]["notes"] = notes
        if assignee:
            data["data"]["assignee"] = assignee
        if due_on:
            data["data"]["due_on"] = due_on
        if tags:
            data["data"]["tags"] = tags

        result = self._make_request("POST", "tasks", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def get_task(self, task_gid: str) -> ActionResult:
        """Get a task by GID.

        Args:
            task_gid: Task GID

        Returns:
            ActionResult with task data
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("GET", f"tasks/{task_gid}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def update_task(
        self, task_gid: str, **kwargs
    ) -> ActionResult:
        """Update a task.

        Args:
            task_gid: Task GID
            **kwargs: Fields to update (name, notes, assignee, due_on, completed)

        Returns:
            ActionResult with updated task
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        data = {"data": kwargs}
        result = self._make_request("PUT", f"tasks/{task_gid}", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def complete_task(self, task_gid: str) -> ActionResult:
        """Mark a task as completed.

        Args:
            task_gid: Task GID

        Returns:
            ActionResult with updated task
        """
        return self.update_task(task_gid, completed=True)

    def delete_task(self, task_gid: str) -> ActionResult:
        """Delete a task.

        Args:
            task_gid: Task GID

        Returns:
            ActionResult with deletion confirmation
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("DELETE", f"tasks/{task_gid}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def search_tasks(
        self,
        workspace: Optional[str] = None,
        project: Optional[str] = None,
        assignee: Optional[str] = None,
        completed: Optional[bool] = None,
        query: Optional[str] = None,
    ) -> ActionResult:
        """Search for tasks.

        Args:
            workspace: Workspace GID
            project: Project GID to filter by
            assignee: Assignee GID
            completed: Filter by completion status
            query: Text search query

        Returns:
            ActionResult with matching tasks
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        workspace = workspace or self.workspace_gid
        endpoint = f"workspaces/{workspace}/tasks/search"
        params = {"limit": 100}
        if project:
            params["projects"] = project
        if assignee:
            params["assignee"] = assignee
        if completed is not None:
            params["completed"] = completed
        if query:
            params["text"] = query

        result = self._make_request("GET", endpoint, params=params)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"tasks": result})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Asana operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "list_workspaces": self.list_workspaces,
            "list_projects": self.list_projects,
            "create_task": self.create_task,
            "get_task": self.get_task,
            "update_task": self.update_task,
            "complete_task": self.complete_task,
            "delete_task": self.delete_task,
            "search_tasks": self.search_tasks,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
