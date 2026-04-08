"""Sentry action module for RabAI AutoClick.

Provides Sentry error tracking operations including
issue management, event data retrieval, and project configuration.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class SentryIssue:
    """Represents a Sentry issue.
    
    Attributes:
        id: Issue ID.
        title: Issue title.
        status: Issue status.
        platform: Platform.
        last_seen: Last event timestamp.
    """
    id: str
    title: str
    status: str
    platform: str
    last_seen: str


class SentryClient:
    """Sentry client for error tracking operations.
    
    Provides methods for interacting with Sentry API
    including issue management and event retrieval.
    """
    
    def __init__(
        self,
        dsn: str = "",
        organization: str = "",
        project: str = "",
        token: str = ""
    ) -> None:
        """Initialize Sentry client.
        
        Args:
            dsn: Sentry DSN URL.
            organization: Organization slug.
            project: Project slug.
            token: API authentication token.
        """
        self.dsn = dsn
        self.organization = organization
        self.project = project
        self.token = token
        self._session: Optional[Any] = None
        self._base_url = "https://sentry.io/api"
    
    def connect(self) -> bool:
        """Test connection to Sentry API.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        if not self.token:
            return False
        
        try:
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            })
            
            response = self._session.get(
                f"{self._base_url}/0/organizations/{self.organization}/",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Sentry session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects in the organization.
        
        Returns:
            List of project information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            response = self._session.get(
                f"{self._base_url}/0/organizations/{self.organization}/projects/",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List projects failed: {str(e)}")
    
    def list_issues(
        self,
        project: Optional[str] = None,
        status: Optional[str] = None,
        query: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List issues in a project.
        
        Args:
            project: Optional project slug override.
            status: Optional status filter ('unresolved', 'resolved', 'ignored').
            query: Optional search query.
            limit: Maximum number of results.
            
        Returns:
            List of issue information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            proj = project or self.project
            
            url = f"{self._base_url}/0/projects/{self.organization}/{proj}/issues/"
            
            params: Dict[str, Any] = {"limit": limit}
            
            if status:
                params["status"] = status
            
            if query:
                params["query"] = query
            
            response = self._session.get(url, params=params, timeout=30)
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List issues failed: {str(e)}")
    
    def get_issue(self, issue_id: str) -> Optional[Dict[str, Any]]:
        """Get a single issue by ID.
        
        Args:
            issue_id: Issue ID.
            
        Returns:
            Issue information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            response = self._session.get(
                f"{self._base_url}/0/issues/{issue_id}/",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def update_issue(
        self,
        issue_id: str,
        status: Optional[str] = None,
        assigned_to: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> bool:
        """Update an issue.
        
        Args:
            issue_id: Issue ID.
            status: New status ('resolved', 'unresolved', 'ignored').
            assigned_to: Username or email to assign.
            tags: Tags to set.
            
        Returns:
            True if updated successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            data: Dict[str, Any] = {}
            
            if status:
                data["status"] = status
            
            if assigned_to:
                data["assignedTo"] = assigned_to
            
            if tags:
                for key, value in tags.items():
                    data[f"tags[{key}]"] = value
            
            response = self._session.put(
                f"{self._base_url}/0/issues/{issue_id}/",
                json=data,
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def resolve_issue(self, issue_id: str) -> bool:
        """Resolve an issue.
        
        Args:
            issue_id: Issue ID.
            
        Returns:
            True if resolved successfully.
        """
        return self.update_issue(issue_id, status="resolved")
    
    def resolve批量(
        self,
        project: Optional[str] = None,
        query: Optional[str] = None
    ) -> int:
        """Bulk resolve issues matching a query.
        
        Args:
            project: Optional project slug.
            query: Optional search query.
            
        Returns:
            Number of resolved issues.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            proj = project or self.project
            
            url = f"{self._base_url}/0/projects/{self.organization}/{proj}/issues/"
            
            params: Dict[str, Any] = {"status": "unresolved", "limit": 100}
            if query:
                params["query"] = query
            
            response = self._session.get(url, params=params, timeout=30)
            
            if response.status_code not in (200, 201):
                return 0
            
            issues = response.json()
            count = 0
            
            for issue in issues:
                if self.resolve_issue(issue.get("id", "")):
                    count += 1
            
            return count
        
        except Exception:
            return 0
    
    def get_events(
        self,
        issue_id: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get events for an issue.
        
        Args:
            issue_id: Issue ID.
            limit: Maximum number of events.
            
        Returns:
            List of event information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            response = self._session.get(
                f"{self._base_url}/0/issues/{issue_id}/events/",
                params={"limit": limit},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"Get events failed: {str(e)}")
    
    def get_event_detail(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about an event.
        
        Args:
            event_id: Event ID.
            
        Returns:
            Event details or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            proj = self.project
            
            response = self._session.get(
                f"{self._base_url}/0/projects/{self.organization}/{proj}/events/{event_id}/",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def list_releases(
        self,
        project: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List releases for a project.
        
        Args:
            project: Optional project slug.
            limit: Maximum number of releases.
            
        Returns:
            List of release information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            proj = project or self.project
            
            response = self._session.get(
                f"{self._base_url}/0/organizations/{self.organization}/releases/",
                params={"project": proj, "limit": limit},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List releases failed: {str(e)}")
    
    def create_release(
        self,
        version: str,
        project: Optional[str] = None,
        ref: Optional[str] = None,
        commits: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """Create a new release.
        
        Args:
            version: Release version.
            project: Optional project slug.
            ref: Git ref.
            commits: Optional commit data.
            
        Returns:
            True if created successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            data: Dict[str, Any] = {
                "version": version,
                "projects": [project or self.project]
            }
            
            if ref:
                data["ref"] = ref
            
            if commits:
                data["commits"] = commits
            
            response = self._session.post(
                f"{self._base_url}/0/organizations/{self.organization}/releases/",
                json=data,
                timeout=30
            )
            
            return response.status_code in (200, 201, 202)
        
        except Exception:
            return False
    
    def get_stats(
        self,
        project: Optional[str] = None,
        stat: str = "handled",
        since: Optional[int] = None
    ) -> List[List[int]]:
        """Get project statistics.
        
        Args:
            project: Optional project slug.
            stat: Stat type ('handled', 'unhandled', 'project_stats').
            since: Unix timestamp for start.
            
        Returns:
            Stats data as list of [timestamp, value] pairs.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            proj = project or self.project
            
            params: Dict[str, Any] = {"stat": stat}
            if since:
                params["since"] = since
            
            response = self._session.get(
                f"{self._base_url}/0/projects/{self.organization}/{proj}/stats/",
                params=params,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"Get stats failed: {str(e)}")
    
    def get_user_feedback(
        self,
        issue_id: str
    ) -> List[Dict[str, Any]]:
        """Get user feedback for an issue.
        
        Args:
            issue_id: Issue ID.
            
        Returns:
            List of user feedback.
        """
        if not self._session:
            raise RuntimeError("Not connected to Sentry")
        
        try:
            response = self._session.get(
                f"{self._base_url}/0/issues/{issue_id}/user-feedback/",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"Get user feedback failed: {str(e)}")


class SentryAction(BaseAction):
    """Sentry action for error tracking operations.
    
    Supports issue management, event retrieval, and project stats.
    """
    action_type: str = "sentry"
    display_name: str = "Sentry动作"
    description: str = "Sentry错误追踪和项目管理操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[SentryClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Sentry operation.
        
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
                return self._disconnect(start_time)
            elif operation == "list_projects":
                return self._list_projects(start_time)
            elif operation == "list_issues":
                return self._list_issues(params, start_time)
            elif operation == "get_issue":
                return self._get_issue(params, start_time)
            elif operation == "update_issue":
                return self._update_issue(params, start_time)
            elif operation == "resolve_issue":
                return self._resolve_issue(params, start_time)
            elif operation == "resolve_batch":
                return self._resolve_batch(params, start_time)
            elif operation == "get_events":
                return self._get_events(params, start_time)
            elif operation == "get_event_detail":
                return self._get_event_detail(params, start_time)
            elif operation == "list_releases":
                return self._list_releases(params, start_time)
            elif operation == "create_release":
                return self._create_release(params, start_time)
            elif operation == "get_stats":
                return self._get_stats(params, start_time)
            elif operation == "get_user_feedback":
                return self._get_user_feedback(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Sentry operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Sentry."""
        token = params.get("token", "")
        organization = params.get("organization", "")
        project = params.get("project", "")
        
        if not token or not organization:
            return ActionResult(
                success=False,
                message="token and organization are required",
                duration=time.time() - start_time
            )
        
        self._client = SentryClient(
            organization=organization,
            project=project,
            token=token
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Sentry" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Sentry."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Sentry",
            duration=time.time() - start_time
        )
    
    def _list_projects(self, start_time: float) -> ActionResult:
        """List all projects."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            projects = self._client.list_projects()
            return ActionResult(
                success=True,
                message=f"Found {len(projects)} projects",
                data={"projects": projects, "count": len(projects)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_issues(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List issues in a project."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project = params.get("project")
        status = params.get("status")
        query = params.get("query")
        limit = params.get("limit", 100)
        
        try:
            issues = self._client.list_issues(
                project=project,
                status=status,
                query=query,
                limit=limit
            )
            return ActionResult(
                success=True,
                message=f"Found {len(issues)} issues",
                data={"issues": issues, "count": len(issues)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a single issue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        issue_id = params.get("issue_id", "")
        if not issue_id:
            return ActionResult(success=False, message="issue_id is required", duration=time.time() - start_time)
        
        try:
            issue = self._client.get_issue(issue_id)
            return ActionResult(
                success=issue is not None,
                message=f"Found issue: {issue_id}" if issue else f"Issue not found: {issue_id}",
                data={"issue": issue},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _update_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update an issue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        issue_id = params.get("issue_id", "")
        if not issue_id:
            return ActionResult(success=False, message="issue_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.update_issue(
                issue_id,
                status=params.get("status"),
                assigned_to=params.get("assigned_to"),
                tags=params.get("tags")
            )
            return ActionResult(
                success=success,
                message=f"Issue updated: {issue_id}" if success else "Update failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _resolve_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Resolve an issue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        issue_id = params.get("issue_id", "")
        if not issue_id:
            return ActionResult(success=False, message="issue_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.resolve_issue(issue_id)
            return ActionResult(
                success=success,
                message=f"Issue resolved: {issue_id}" if success else "Resolve failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _resolve_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Bulk resolve issues."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            count = self._client.resolve批量(
                project=params.get("project"),
                query=params.get("query")
            )
            return ActionResult(
                success=True,
                message=f"Resolved {count} issues",
                data={"resolved": count},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_events(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get events for an issue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        issue_id = params.get("issue_id", "")
        limit = params.get("limit", 100)
        
        if not issue_id:
            return ActionResult(success=False, message="issue_id is required", duration=time.time() - start_time)
        
        try:
            events = self._client.get_events(issue_id, limit)
            return ActionResult(
                success=True,
                message=f"Found {len(events)} events",
                data={"events": events, "count": len(events)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_event_detail(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get detailed event information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        event_id = params.get("event_id", "")
        if not event_id:
            return ActionResult(success=False, message="event_id is required", duration=time.time() - start_time)
        
        try:
            event = self._client.get_event_detail(event_id)
            return ActionResult(
                success=event is not None,
                message=f"Event found: {event_id}" if event else f"Event not found: {event_id}",
                data={"event": event},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_releases(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List releases for a project."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            releases = self._client.list_releases(
                project=params.get("project"),
                limit=params.get("limit", 50)
            )
            return ActionResult(
                success=True,
                message=f"Found {len(releases)} releases",
                data={"releases": releases, "count": len(releases)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_release(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new release."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        version = params.get("version", "")
        if not version:
            return ActionResult(success=False, message="version is required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_release(
                version=version,
                project=params.get("project"),
                ref=params.get("ref"),
                commits=params.get("commits")
            )
            return ActionResult(
                success=success,
                message=f"Release created: {version}" if success else "Create release failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get project statistics."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            stats = self._client.get_stats(
                project=params.get("project"),
                stat=params.get("stat", "handled"),
                since=params.get("since")
            )
            return ActionResult(
                success=True,
                message="Stats retrieved",
                data={"stats": stats},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_user_feedback(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get user feedback for an issue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        issue_id = params.get("issue_id", "")
        if not issue_id:
            return ActionResult(success=False, message="issue_id is required", duration=time.time() - start_time)
        
        try:
            feedback = self._client.get_user_feedback(issue_id)
            return ActionResult(
                success=True,
                message=f"Found {len(feedback)} feedback items",
                data={"feedback": feedback, "count": len(feedback)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
