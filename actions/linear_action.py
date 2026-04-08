"""Linear API integration for issue and project management.

Handles Linear workspace operations including issue creation/updates,
project management, workflow states, and team operations.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


class LinearPriority(Enum):
    """Issue priority levels."""
    NO_PRIORITY = 0
    URGENT = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


class LinearStateType(Enum):
    """State type classification."""
    BACKLOG = "backlog"
    UNSTARTED = "unstarted"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class LinearConfig:
    """Configuration for Linear API client."""
    api_key: str
    team_id: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3


@dataclass
class LinearIssue:
    """Represents a Linear issue."""
    id: str
    identifier: str
    title: str
    description: Optional[str] = None
    state: Optional[str] = None
    priority: int = 0
    assignee_id: Optional[str] = None
    team_id: Optional[str] = None
    project_id: Optional[str] = None
    labels: list[str] = field(default_factory=list)
    due_date: Optional[str] = None
    estimate: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    url: str = ""


@dataclass
class LinearProject:
    """Represents a Linear project."""
    id: str
    name: str
    description: Optional[str] = None
    state: str = "active"
    lead_id: Optional[str] = None
    member_ids: list[str] = field(default_factory=list)
    created_at: Optional[str] = None
    url: str = ""


class LinearAPIError(Exception):
    """Raised when Linear API returns an error."""
    def __init__(self, message: str, errors: Optional[list] = None):
        super().__init__(message)
        self.errors = errors or []


class LinearAction:
    """Linear API client for issue and project management."""

    BASE_URL = "https://api.linear.app/graphql"

    def __init__(self, config: LinearConfig):
        """Initialize Linear client with API key.

        Args:
            config: LinearConfig with API key and optional team ID
        """
        if requests is None:
            raise ImportError("requests library required: pip install requests")

        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json"
        })

    def _graphql(self, query: str, variables: Optional[dict] = None) -> dict:
        """Execute GraphQL query against Linear API.

        Args:
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            Parsed JSON response

        Raises:
            LinearAPIError: On GraphQL errors
        """
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        retries = self.config.max_retries

        while retries > 0:
            try:
                response = self.session.post(
                    self.BASE_URL,
                    json=payload,
                    timeout=self.config.timeout
                )

                data = response.json()

                if "errors" in data:
                    raise LinearAPIError(
                        message=data["errors"][0].get("message", "GraphQL error"),
                        errors=data.get("errors")
                    )

                return data.get("data", {})

            except requests.RequestException as e:
                retries -= 1
                if retries == 0:
                    raise LinearAPIError(f"Request failed: {e}")

    def create_issue(self, title: str, team_id: Optional[str] = None,
                     description: Optional[str] = None,
                     state_id: Optional[str] = None,
                     priority: int = 3,
                     assignee_id: Optional[str] = None,
                     project_id: Optional[str] = None,
                     label_ids: Optional[list[str]] = None,
                     due_date: Optional[str] = None) -> LinearIssue:
        """Create a new issue.

        Args:
            title: Issue title
            team_id: Team ID (uses config default if not provided)
            description: Issue description (markdown supported)
            state_id: Initial state ID
            priority: Priority level (0-4, lower = higher priority)
            assignee_id: User ID to assign
            project_id: Project ID to add issue to
            label_ids: List of label IDs
            due_date: ISO date string for due date

        Returns:
            Created LinearIssue object
        """
        query = """
        mutation IssueCreate($input: IssueCreateInput!) {
            issueCreate(input: $input) {
                success
                issue {
                    id
                    identifier
                    title
                    description
                    priority
                    dueDate
                    url
                    createdAt
                    updatedAt
                    state { id name }
                    assignee { id name }
                    project { id name }
                    labels { nodes { id name } }
                }
            }
        }
        """

        input_data: dict[str, Any] = {"title": title}

        team_id = team_id or self.config.team_id
        if team_id:
            input_data["teamId"] = team_id

        if description:
            input_data["description"] = description

        if state_id:
            input_data["stateId"] = state_id

        input_data["priority"] = priority

        if assignee_id:
            input_data["assigneeId"] = assignee_id

        if project_id:
            input_data["projectId"] = project_id

        if label_ids:
            input_data["labelIds"] = label_ids

        if due_date:
            input_data["dueDate"] = due_date

        variables = {"input": input_data}
        data = self._graphql(query, variables)

        result = data.get("issueCreate", {}).get("issue", {})

        return LinearIssue(
            id=result.get("id", ""),
            identifier=result.get("identifier", ""),
            title=result.get("title", ""),
            description=result.get("description"),
            priority=result.get("priority", 0),
            due_date=result.get("dueDate"),
            created_at=result.get("createdAt"),
            updated_at=result.get("updatedAt"),
            url=result.get("url", ""),
            state=result.get("state", {}).get("name") if result.get("state") else None,
            assignee_id=result.get("assignee", {}).get("id") if result.get("assignee") else None,
            project_id=result.get("project", {}).get("id") if result.get("project") else None,
            labels=[l.get("name") for l in result.get("labels", {}).get("nodes", [])]
        )

    def update_issue(self, issue_id: str, **kwargs) -> bool:
        """Update an existing issue.

        Args:
            issue_id: Issue ID to update
            **kwargs: Fields to update (title, description, stateId,
                      priority, assigneeId, projectId, dueDate, etc.)

        Returns:
            True if update successful
        """
        query = """
        mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                success
            }
        }
        """

        input_data = {k: v for k, v in kwargs.items() if v is not None}
        variables = {"id": issue_id, "input": input_data}

        data = self._graphql(query, variables)
        return data.get("issueUpdate", {}).get("success", False)

    def get_issue(self, issue_id: str) -> LinearIssue:
        """Get issue by ID.

        Args:
            issue_id: Linear issue ID or identifier (e.g., 'KEY-123')

        Returns:
            LinearIssue object
        """
        query = """
        query Issue($id: String!) {
            issue(id: $id) {
                id
                identifier
                title
                description
                priority
                dueDate
                url
                createdAt
                updatedAt
                state { id name }
                assignee { id name }
                project { id name }
                labels { nodes { id name } }
                team { id name }
            }
        }
        """

        data = self._graphql(query, {"id": issue_id})
        result = data.get("issue", {})

        if not result:
            raise LinearAPIError(f"Issue not found: {issue_id}")

        return LinearIssue(
            id=result.get("id", ""),
            identifier=result.get("identifier", ""),
            title=result.get("title", ""),
            description=result.get("description"),
            priority=result.get("priority", 0),
            due_date=result.get("dueDate"),
            created_at=result.get("createdAt"),
            updated_at=result.get("updatedAt"),
            url=result.get("url", ""),
            state=result.get("state", {}).get("name") if result.get("state") else None,
            assignee_id=result.get("assignee", {}).get("id") if result.get("assignee") else None,
            project_id=result.get("project", {}).get("id") if result.get("project") else None,
            team_id=result.get("team", {}).get("id") if result.get("team") else None,
            labels=[l.get("name") for l in result.get("labels", {}).get("nodes", [])]
        )

    def list_issues(self, team_id: Optional[str] = None,
                    assignee_id: Optional[str] = None,
                    project_id: Optional[str] = None,
                    state: Optional[str] = None,
                    limit: int = 50) -> list[LinearIssue]:
        """List issues with optional filters.

        Args:
            team_id: Filter by team
            assignee_id: Filter by assignee
            project_id: Filter by project
            state: Filter by state (started, completed, etc.)
            limit: Number of results (max 100)

        Returns:
            List of LinearIssue objects
        """
        query = """
        query Issues($filter: IssueFilter, $first: Int) {
            issues(filter: $filter, first: $first) {
                nodes {
                    id
                    identifier
                    title
                    description
                    priority
                    dueDate
                    url
                    createdAt
                    updatedAt
                    state { id name }
                    assignee { id name }
                    project { id name }
                    labels { nodes { id name } }
                    team { id name }
                }
            }
        }
        """

        filter_cond: dict[str, Any] = {}

        if team_id:
            filter_cond["team"] = {"id": {"eq": team_id}}

        if assignee_id:
            filter_cond["assignee"] = {"id": {"eq": assignee_id}}

        if project_id:
            filter_cond["project"] = {"id": {"eq": project_id}}

        if state:
            filter_cond["state"] = {"type": {"eq": state}}

        variables = {
            "filter": filter_cond if filter_cond else None,
            "first": min(limit, 100)
        }

        data = self._graphql(query, variables)
        results = data.get("issues", {}).get("nodes", [])

        issues = []
        for result in results:
            issues.append(LinearIssue(
                id=result.get("id", ""),
                identifier=result.get("identifier", ""),
                title=result.get("title", ""),
                description=result.get("description"),
                priority=result.get("priority", 0),
                due_date=result.get("dueDate"),
                created_at=result.get("createdAt"),
                updated_at=result.get("updatedAt"),
                url=result.get("url", ""),
                state=result.get("state", {}).get("name") if result.get("state") else None,
                assignee_id=result.get("assignee", {}).get("id") if result.get("assignee") else None,
                project_id=result.get("project", {}).get("id") if result.get("project") else None,
                team_id=result.get("team", {}).get("id") if result.get("team") else None,
                labels=[l.get("name") for l in result.get("labels", {}).get("nodes", [])]
            ))

        return issues

    def create_project(self, name: str, team_id: Optional[str] = None,
                       description: Optional[str] = None,
                       lead_id: Optional[str] = None,
                       member_ids: Optional[list[str]] = None) -> LinearProject:
        """Create a new project.

        Args:
            name: Project name
            team_id: Associated team ID
            description: Project description
            lead_id: Project lead user ID
            member_ids: List of member user IDs

        Returns:
            Created LinearProject object
        """
        query = """
        mutation ProjectCreate($input: ProjectCreateInput!) {
            projectCreate(input: $input) {
                success
                project {
                    id
                    name
                    description
                    state
                    url
                    createdAt
                    lead { id name }
                    members { nodes { id name } }
                }
            }
        }
        """

        input_data: dict[str, Any] = {"name": name}

        if team_id:
            input_data["teamId"] = team_id

        if description:
            input_data["description"] = description

        if lead_id:
            input_data["leadId"] = lead_id

        if member_ids:
            input_data["memberIds"] = member_ids

        data = self._graphql(query, {"input": input_data})
        result = data.get("projectCreate", {}).get("project", {})

        return LinearProject(
            id=result.get("id", ""),
            name=result.get("name", ""),
            description=result.get("description"),
            state=result.get("state", "active"),
            lead_id=result.get("lead", {}).get("id") if result.get("lead") else None,
            member_ids=[m.get("id") for m in result.get("members", {}).get("nodes", [])],
            created_at=result.get("createdAt"),
            url=result.get("url", "")
        )

    def get_teams(self) -> list[dict]:
        """Get all teams in workspace.

        Returns:
            List of team objects with id, name, key
        """
        query = """
        query Teams {
            teams {
                nodes {
                    id
                    name
                    key
                    description
                }
            }
        }
        """

        data = self._graphql(query)
        return data.get("teams", {}).get("nodes", [])

    def get_workflow_states(self, team_id: Optional[str] = None) -> list[dict]:
        """Get workflow states for a team.

        Args:
            team_id: Team ID (uses config default if not provided)

        Returns:
            List of state objects with id, name, type
        """
        team_id = team_id or self.config.team_id

        if not team_id:
            raise LinearAPIError("team_id required")

        query = """
        query Team($id: String!) {
            team(id: $id) {
                states {
                    nodes {
                        id
                        name
                        type
                        color
                    }
                }
            }
        }
        """

        data = self._graphql(query, {"id": team_id})
        return data.get("team", {}).get("states", {}).get("nodes", [])
