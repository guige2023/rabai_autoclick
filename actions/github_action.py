"""GitHub action module for RabAI AutoClick.

Provides GitHub API operations including repository management,
issues, pull requests, and workflow control via the GitHub REST API.
"""

import os
import sys
import time
import json
import base64
from typing import Any, Dict, List, Optional, Union, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GitHubClient:
    """GitHub REST API client with token authentication.
    
    Provides methods for common GitHub operations including
    repository, issue, and pull request management.
    """
    
    API_BASE = "https://api.github.com"
    
    def __init__(self, token: str, owner: Optional[str] = None, repo: Optional[str] = None) -> None:
        """Initialize GitHub client.
        
        Args:
            token: GitHub personal access token or OAuth token.
            owner: Default repository owner (for convenience methods).
            repo: Default repository name (for convenience methods).
        """
        self.token = token
        self.default_owner = owner
        self.default_repo = repo
        self._rate_limit_remaining: Optional[int] = None
        self._rate_limit_reset: Optional[float] = None
    
    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build request headers with authentication."""
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "RabAI-AutoClick/1.0"
        }
        if extra:
            headers.update(extra)
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
            method: HTTP method (GET, POST, PATCH, DELETE).
            path: API path (e.g., '/repos/owner/name/issues').
            data: Optional request body data.
            params: Optional URL query parameters.
            
        Returns:
            Parsed JSON response.
            
        Raises:
            HTTPError: If the request fails.
        """
        url = f"{self.API_BASE}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        
        body = json.dumps(data).encode("utf-8") if data else None
        
        headers = self._headers(
            {"Content-Type": "application/json"} if body else {}
        )
        
        req = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(req, timeout=30) as response:
                self._rate_limit_remaining = int(
                    response.headers.get("X-RateLimit-Remaining", "")
                )
                self._rate_limit_reset = float(
                    response.headers.get("X-RateLimit-Reset", "0")
                )
                
                if response.status == 204:
                    return {}
                
                return json.loads(response.read().decode("utf-8"))
        
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            try:
                error_data = json.loads(error_body)
                message = error_data.get("message", str(e))
            except (json.JSONDecodeError, UnicodeDecodeError):
                message = f"HTTP {e.code}: {error_body[:500]}"
            
            raise HTTPError(
                url, e.code, message, e.headers, e.fp
            ) from e
    
    def get_rate_limit(self) -> Dict[str, Any]:
        """Get current rate limit status.
        
        Returns:
            Rate limit information.
        """
        data = self._request("GET", "/rate_limit")
        return data.get("rate", {})
    
    def get_repo(self, owner: Optional[str] = None, repo: Optional[str] = None) -> Dict[str, Any]:
        """Get repository information.
        
        Args:
            owner: Repository owner (uses default if not provided).
            repo: Repository name (uses default if not provided).
            
        Returns:
            Repository data dictionary.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        return self._request("GET", f"/repos/{owner}/{repo}")
    
    def create_issue(
        self,
        title: str,
        body: Optional[str] = None,
        labels: Optional[List[str]] = None,
        assignees: Optional[List[str]] = None,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new issue.
        
        Args:
            title: Issue title.
            body: Optional issue body/description.
            labels: Optional list of label names.
            assignees: Optional list of assignee usernames.
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            Created issue data.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        data: Dict[str, Any] = {"title": title}
        if body:
            data["body"] = body
        if labels:
            data["labels"] = labels
        if assignees:
            data["assignees"] = assignees
        
        return self._request("POST", f"/repos/{owner}/{repo}/issues", data=data)
    
    def list_issues(
        self,
        state: str = "open",
        labels: Optional[str] = None,
        sort: str = "created",
        direction: str = "desc",
        per_page: int = 30,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List issues in a repository.
        
        Args:
            state: Filter by state ('open', 'closed', 'all').
            labels: Comma-separated label names to filter by.
            sort: Sort field ('created', 'updated', 'comments').
            direction: Sort direction ('asc', 'desc').
            per_page: Number of results per page (max 100).
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            List of issue dictionaries.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        params: Dict[str, Any] = {
            "state": state,
            "sort": sort,
            "direction": direction,
            "per_page": min(per_page, 100)
        }
        if labels:
            params["labels"] = labels
        
        return self._request(
            "GET",
            f"/repos/{owner}/{repo}/issues",
            params=params
        )
    
    def get_issue(
        self,
        issue_number: int,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get a single issue by number.
        
        Args:
            issue_number: Issue number.
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            Issue data dictionary.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        return self._request("GET", f"/repos/{owner}/{repo}/issues/{issue_number}")
    
    def update_issue(
        self,
        issue_number: int,
        title: Optional[str] = None,
        body: Optional[str] = None,
        state: Optional[str] = None,
        labels: Optional[List[str]] = None,
        assignees: Optional[List[str]] = None,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update an existing issue.
        
        Args:
            issue_number: Issue number to update.
            title: New title.
            body: New body.
            state: New state ('open' or 'closed').
            labels: New label list.
            assignees: New assignee list.
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            Updated issue data.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        data: Dict[str, Any] = {}
        if title is not None:
            data["title"] = title
        if body is not None:
            data["body"] = body
        if state is not None:
            data["state"] = state
        if labels is not None:
            data["labels"] = labels
        if assignees is not None:
            data["assignees"] = assignees
        
        return self._request(
            "PATCH",
            f"/repos/{owner}/{repo}/issues/{issue_number}",
            data=data
        )
    
    def create_pull_request(
        self,
        title: str,
        head: str,
        base: str,
        body: Optional[str] = None,
        draft: bool = False,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new pull request.
        
        Args:
            title: PR title.
            head: Branch containing changes.
            base: Target branch to merge into.
            body: Optional PR description.
            draft: Whether to create as draft PR.
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            Created PR data.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        data: Dict[str, Any] = {
            "title": title,
            "head": head,
            "base": base,
            "draft": draft
        }
        if body:
            data["body"] = body
        
        return self._request("POST", f"/repos/{owner}/{repo}/pulls", data=data)
    
    def list_pull_requests(
        self,
        state: str = "open",
        sort: str = "created",
        direction: str = "desc",
        per_page: int = 30,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List pull requests in a repository.
        
        Args:
            state: Filter by state ('open', 'closed', 'all').
            sort: Sort field ('created', 'updated', 'popularity').
            direction: Sort direction ('asc', 'desc').
            per_page: Number of results per page (max 100).
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            List of PR dictionaries.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        params = {
            "state": state,
            "sort": sort,
            "direction": direction,
            "per_page": min(per_page, 100)
        }
        
        return self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls",
            params=params
        )
    
    def get_file_content(
        self,
        path: str,
        ref: Optional[str] = None,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> Tuple[str, str]:
        """Get the content of a file from the repository.
        
        Args:
            path: Path to the file in the repository.
            ref: Git ref (branch, tag, or commit SHA).
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            Tuple of (content as string, SHA of the file).
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        path_params = f"/repos/{owner}/{repo}/contents/{path}"
        if ref:
            path_params += f"?ref={ref}"
        
        data = self._request("GET", path_params)
        
        content_b64 = data.get("content", "")
        content = base64.b64decode(content_b64.replace("\n", "")).decode("utf-8")
        sha = data.get("sha", "")
        
        return content, sha
    
    def create_file(
        self,
        path: str,
        content: str,
        message: str,
        branch: Optional[str] = None,
        owner: Optional[str] = None,
        repo: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create or update a file in the repository.
        
        Args:
            path: Path where to create the file.
            content: File content as string.
            message: Commit message.
            branch: Optional branch to commit to.
            owner: Repository owner.
            repo: Repository name.
            
        Returns:
            Created file commit data.
        """
        owner = owner or self.default_owner
        repo = repo or self.default_repo
        
        if not owner or not repo:
            raise ValueError("Owner and repo are required")
        
        data = {
            "message": message,
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8")
        }
        if branch:
            data["branch"] = branch
        
        return self._request(
            "PUT",
            f"/repos/{owner}/{repo}/contents/{path}",
            data=data
        )


class GitHubAction(BaseAction):
    """GitHub action for repository and issue management.
    
    Supports creating issues, PRs, listing repositories, and file operations.
    """
    action_type: str = "github"
    display_name: str = "GitHub动作"
    description: str = "GitHub仓库和Issue管理，支持创建Issue、PR和文件操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[GitHubClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GitHub operation.
        
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
                    message="GitHub client disconnected",
                    duration=time.time() - start_time
                )
            elif operation == "get_repo":
                return self._get_repo(params, start_time)
            elif operation == "create_issue":
                return self._create_issue(params, start_time)
            elif operation == "list_issues":
                return self._list_issues(params, start_time)
            elif operation == "get_issue":
                return self._get_issue(params, start_time)
            elif operation == "update_issue":
                return self._update_issue(params, start_time)
            elif operation == "create_pr":
                return self._create_pr(params, start_time)
            elif operation == "list_prs":
                return self._list_prs(params, start_time)
            elif operation == "get_file":
                return self._get_file(params, start_time)
            elif operation == "create_file":
                return self._create_file(params, start_time)
            elif operation == "rate_limit":
                return self._rate_limit(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"GitHub API error: {e.description}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GitHub operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _require_client(self) -> GitHubClient:
        """Ensure a GitHub client exists."""
        if not self._client:
            raise RuntimeError("Not connected to GitHub. Use 'connect' operation first.")
        return self._client
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to GitHub API."""
        token = params.get("token", "")
        owner = params.get("owner")
        repo = params.get("repo")
        
        if not token:
            return ActionResult(
                success=False,
                message="GitHub token is required",
                duration=time.time() - start_time
            )
        
        self._client = GitHubClient(token=token, owner=owner, repo=repo)
        
        try:
            rate_limit = self._client.get_rate_limit()
            return ActionResult(
                success=True,
                message="Connected to GitHub",
                data={"rate_limit": rate_limit},
                duration=time.time() - start_time
            )
        except Exception as e:
            self._client = None
            return ActionResult(
                success=False,
                message=f"Failed to connect: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_repo(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get repository information."""
        client = self._require_client()
        owner = params.get("owner")
        repo = params.get("repo")
        
        try:
            data = client.get_repo(owner=owner, repo=repo)
            return ActionResult(
                success=True,
                message=f"Retrieved repo: {data.get('full_name', '')}",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get repo: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new issue."""
        client = self._require_client()
        
        title = params.get("title", "")
        if not title:
            return ActionResult(
                success=False,
                message="Issue title is required",
                duration=time.time() - start_time
            )
        
        try:
            data = client.create_issue(
                title=title,
                body=params.get("body"),
                labels=params.get("labels"),
                assignees=params.get("assignees"),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Created issue #{data.get('number')}: {data.get('title')}",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to create issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_issues(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List repository issues."""
        client = self._require_client()
        
        try:
            issues = client.list_issues(
                state=params.get("state", "open"),
                labels=params.get("labels"),
                sort=params.get("sort", "created"),
                direction=params.get("direction", "desc"),
                per_page=params.get("per_page", 30),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Found {len(issues)} issues",
                data={"issues": issues, "count": len(issues)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list issues: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a single issue."""
        client = self._require_client()
        issue_number = params.get("issue_number")
        
        if not issue_number:
            return ActionResult(
                success=False,
                message="issue_number is required",
                duration=time.time() - start_time
            )
        
        try:
            data = client.get_issue(
                issue_number=int(issue_number),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Retrieved issue #{data.get('number')}",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _update_issue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update an issue."""
        client = self._require_client()
        issue_number = params.get("issue_number")
        
        if not issue_number:
            return ActionResult(
                success=False,
                message="issue_number is required",
                duration=time.time() - start_time
            )
        
        try:
            data = client.update_issue(
                issue_number=int(issue_number),
                title=params.get("title"),
                body=params.get("body"),
                state=params.get("state"),
                labels=params.get("labels"),
                assignees=params.get("assignees"),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Updated issue #{data.get('number')}",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to update issue: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_pr(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a pull request."""
        client = self._require_client()
        
        title = params.get("title", "")
        head = params.get("head", "")
        base = params.get("base", "main")
        
        if not title or not head:
            return ActionResult(
                success=False,
                message="title and head are required",
                duration=time.time() - start_time
            )
        
        try:
            data = client.create_pull_request(
                title=title,
                head=head,
                base=base,
                body=params.get("body"),
                draft=params.get("draft", False),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Created PR #{data.get('number')}: {data.get('title')}",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to create PR: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_prs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List pull requests."""
        client = self._require_client()
        
        try:
            prs = client.list_pull_requests(
                state=params.get("state", "open"),
                sort=params.get("sort", "created"),
                direction=params.get("direction", "desc"),
                per_page=params.get("per_page", 30),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Found {len(prs)} pull requests",
                data={"pull_requests": prs, "count": len(prs)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list PRs: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_file(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get file content from repository."""
        client = self._require_client()
        path = params.get("path", "")
        
        if not path:
            return ActionResult(
                success=False,
                message="path is required",
                duration=time.time() - start_time
            )
        
        try:
            content, sha = client.get_file_content(
                path=path,
                ref=params.get("ref"),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Retrieved file: {path}",
                data={"path": path, "content": content, "sha": sha},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get file: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_file(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create or update a file in repository."""
        client = self._require_client()
        path = params.get("path", "")
        content = params.get("content", "")
        message = params.get("message", "")
        
        if not path or content is None or not message:
            return ActionResult(
                success=False,
                message="path, content, and message are required",
                duration=time.time() - start_time
            )
        
        try:
            data = client.create_file(
                path=path,
                content=content,
                message=message,
                branch=params.get("branch"),
                owner=params.get("owner"),
                repo=params.get("repo")
            )
            
            return ActionResult(
                success=True,
                message=f"Created/updated file: {path}",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to create file: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _rate_limit(self, start_time: float) -> ActionResult:
        """Get current rate limit status."""
        client = self._require_client()
        
        try:
            data = client.get_rate_limit()
            return ActionResult(
                success=True,
                message="Rate limit status retrieved",
                data=data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get rate limit: {str(e)}",
                duration=time.time() - start_time
            )
