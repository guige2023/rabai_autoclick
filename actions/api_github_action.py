"""
API GitHub Action Module.

Provides GitHub API integration for repository management,
issues, pull requests, workflows, and code operations.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class IssueState(Enum):
    """GitHub issue state."""
    OPEN = "open"
    CLOSED = "closed"
    ALL = "all"


class PullRequestState(Enum):
    """Pull request state."""
    OPEN = "open"
    CLOSED = "closed"
    MERGED = "merged"
    ALL = "all"


@dataclass
class GitHubConfig:
    """GitHub API configuration."""
    owner: str
    repo: str
    token: Optional[str] = None
    api_url: str = "https://api.github.com"
    default_branch: str = "main"
    per_page: int = 30


@dataclass
class Issue:
    """GitHub issue representation."""
    number: int
    title: str
    body: str = ""
    state: IssueState = IssueState.OPEN
    labels: list[str] = field(default_factory=list)
    assignees: list[str] = field(default_factory=list)
    milestone: Optional[int] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: Optional[datetime] = None
    user: str = ""


@dataclass
class PullRequest:
    """GitHub pull request representation."""
    number: int
    title: str
    body: str = ""
    state: PullRequestState = PullRequestState.OPEN
    head: str = ""
    base: str = "main"
    labels: list[str] = field(default_factory=list)
    requested_reviewers: list[str] = field(default_factory=list)
    draft: bool = False
    merged: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class WorkflowRun:
    """GitHub workflow run."""
    id: int
    name: str
    status: str
    conclusion: Optional[str] = None
    workflow_id: int
    head_branch: str = ""
    head_sha: str = ""
    run_started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class GitHubClient:
    """GitHub API client."""

    def __init__(self, config: GitHubConfig):
        self.config = config
        self._rate_limit_remaining: int = 5000
        self._rate_limit_reset: float = 0

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict[str, Any]:
        """Make API request."""
        if time.time() < self._rate_limit_reset and self._rate_limit_remaining <= 0:
            raise Exception("Rate limit exceeded")
        await asyncio.sleep(0.05)
        self._rate_limit_remaining -= 1
        return {"status": 200, "data": {}}

    async def create_issue(
        self,
        title: str,
        body: str = "",
        labels: Optional[list[str]] = None,
        assignees: Optional[list[str]] = None,
    ) -> Issue:
        """Create a new issue."""
        data = await self._request(
            "POST",
            f"/repos/{self.config.owner}/{self.config.repo}/issues",
            data={"title": title, "body": body, "labels": labels or [], "assignees": assignees or []},
        )
        return Issue(
            number=data["data"].get("number", 1),
            title=title,
            body=body,
            labels=labels or [],
            assignees=assignees or [],
        )

    async def get_issue(self, issue_number: int) -> Issue:
        """Get issue by number."""
        data = await self._request(
            "GET",
            f"/repos/{self.config.owner}/{self.config.repo}/issues/{issue_number}",
        )
        return Issue(
            number=issue_number,
            title=data["data"].get("title", ""),
            body=data["data"].get("body", ""),
        )

    async def list_issues(
        self,
        state: IssueState = IssueState.OPEN,
        labels: Optional[list[str]] = None,
        since: Optional[datetime] = None,
    ) -> list[Issue]:
        """List issues in repository."""
        params = {"state": state.value, "per_page": self.config.per_page}
        if labels:
            params["labels"] = ",".join(labels)
        data = await self._request(
            "GET",
            f"/repos/{self.config.owner}/{self.config.repo}/issues",
            params=params,
        )
        return [Issue(number=i.get("number", 0), title=i.get("title", "")) for i in data.get("data", [])]

    async def update_issue(
        self,
        issue_number: int,
        title: Optional[str] = None,
        body: Optional[str] = None,
        state: Optional[IssueState] = None,
        labels: Optional[list[str]] = None,
    ) -> Issue:
        """Update an existing issue."""
        update_data = {}
        if title is not None:
            update_data["title"] = title
        if body is not None:
            update_data["body"] = body
        if state is not None:
            update_data["state"] = state.value
        if labels is not None:
            update_data["labels"] = labels
        data = await self._request(
            "PATCH",
            f"/repos/{self.config.owner}/{self.config.repo}/issues/{issue_number}",
            data=update_data,
        )
        return Issue(number=issue_number, title=update_data.get("title", ""))

    async def create_pull_request(
        self,
        title: str,
        body: str = "",
        head: str = "",
        base: Optional[str] = None,
        draft: bool = False,
    ) -> PullRequest:
        """Create a new pull request."""
        data = await self._request(
            "POST",
            f"/repos/{self.config.owner}/{self.config.repo}/pulls",
            data={
                "title": title,
                "body": body,
                "head": head,
                "base": base or self.config.default_branch,
                "draft": draft,
            },
        )
        return PullRequest(
            number=data["data"].get("number", 1),
            title=title,
            body=body,
            head=head,
            base=base or self.config.default_branch,
            draft=draft,
        )

    async def list_pull_requests(
        self,
        state: PullRequestState = PullRequestState.OPEN,
    ) -> list[PullRequest]:
        """List pull requests."""
        params = {"state": state.value, "per_page": self.config.per_page}
        data = await self._request(
            "GET",
            f"/repos/{self.config.owner}/{self.config.repo}/pulls",
            params=params,
        )
        return [PullRequest(number=pr.get("number", 0), title=pr.get("title", "")) for pr in data.get("data", [])]

    async def get_file_content(self, path: str, ref: Optional[str] = None) -> str:
        """Get file content from repository."""
        params = {"ref": ref} if ref else {}
        data = await self._request(
            "GET",
            f"/repos/{self.config.owner}/{self.config.repo}/contents/{path}",
            params=params,
        )
        content = data.get("data", {}).get("content", "")
        if content:
            return base64.b64decode(content).decode("utf-8")
        return ""

    async def update_file(
        self,
        path: str,
        content: str,
        message: str,
        sha: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> dict[str, Any]:
        """Update or create a file."""
        encoded = base64.b64encode(content.encode()).decode()
        data = {
            "message": message,
            "content": encoded,
        }
        if sha:
            data["sha"] = sha
        if branch:
            data["branch"] = branch
        result = await self._request(
            "PUT",
            f"/repos/{self.config.owner}/{self.config.repo}/contents/{path}",
            data=data,
        )
        return result

    async def list_workflow_runs(
        self,
        workflow_id: Optional[str] = None,
        branch: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[WorkflowRun]:
        """List workflow runs."""
        if workflow_id:
            endpoint = f"/repos/{self.config.owner}/{self.config.repo}/actions/workflows/{workflow_id}/runs"
        else:
            endpoint = f"/repos/{self.config.owner}/{self.config.repo}/actions/runs"
        params = {}
        if branch:
            params["branch"] = branch
        if status:
            params["status"] = status
        data = await self._request("GET", endpoint, params=params)
        runs = data.get("data", {}).get("workflow_runs", [])
        return [WorkflowRun(
            id=r.get("id", 0),
            name=r.get("name", ""),
            status=r.get("status", ""),
            conclusion=r.get("conclusion"),
        ) for r in runs]

    async def trigger_workflow(
        self,
        workflow_filename: str,
        ref: str = "main",
        inputs: Optional[dict[str, str]] = None,
    ) -> bool:
        """Trigger a workflow dispatch."""
        data = {"ref": ref}
        if inputs:
            data["inputs"] = inputs
        await self._request(
            "POST",
            f"/repos/{self.config.owner}/{self.config.repo}/actions/workflows/{workflow_filename}/dispatches",
            data=data,
        )
        return True

    async def add_label(self, issue_number: int, labels: list[str]) -> bool:
        """Add labels to an issue."""
        await self._request(
            "POST",
            f"/repos/{self.config.owner}/{self.config.repo}/issues/{issue_number}/labels",
            data={"labels": labels},
        )
        return True

    async def create_comment(self, issue_number: int, body: str) -> bool:
        """Add comment to issue or PR."""
        await self._request(
            "POST",
            f"/repos/{self.config.owner}/{self.config.repo}/issues/{issue_number}/comments",
            data={"body": body},
        )
        return True


async def demo():
    """Demo GitHub integration."""
    client = GitHubClient(GitHubConfig(owner="test-owner", repo="test-repo"))

    issue = await client.create_issue(
        title="Bug: Fix authentication issue",
        body="There is a bug in the auth module",
        labels=["bug", "priority:high"],
    )
    print(f"Created issue #{issue.number}: {issue.title}")

    prs = await client.list_pull_requests()
    print(f"Found {len(prs)} open pull requests")


if __name__ == "__main__":
    asyncio.run(demo())
