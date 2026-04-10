"""
AWS CodeCommit Integration Module for Workflow System

Implements a CodeCommitIntegration class with:
1. Repository management: Create/manage CodeCommit repositories
2. Branch management: Create/manage branches
3. Commit operations: Create/manage commits
4. File operations: Get/update files
5. Pull requests: Manage pull requests
6. Approval rules: Configure approval rules
7. Notifications: Configure notifications
8. Merge: Merge branches
9. Compare: Compare branches/commits
10. Clone: Clone repositories locally

Commit: 'feat(aws-codecommit): add AWS CodeCommit integration with repository management, branches, commits, files, pull requests, approval rules, notifications, merge, compare'
"""

import uuid
import json
import threading
import time
import logging
import os
import subprocess
import base64
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union, BinaryIO
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        BotoCoreError
    )
    from botocore.config import Config as BotoConfig
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None
    BotoConfig = None


logger = logging.getLogger(__name__)


class RepositoryStatus(Enum):
    """CodeCommit repository status."""
    ACTIVE = "ACTIVE"
    CREATING = "CREATING"
    DELETING = "DELETING"


class PullRequestStatus(Enum):
    """Pull request status."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    MERGED = "MERGED"


class MergeOption(Enum):
    """Merge options for pull requests."""
    FAST_FORWARD_MERGE = "FAST_FORWARD_MERGE"
    SQUASH_MERGE = "SQUASH_MERGE"
    THREE_WAY_MERGE = "THREE_WAY_MERGE"


class ApprovalRuleEventType(Enum):
    """Events that trigger approval rule notifications."""
    PULLREQUEST_CREATED = "PULLREQUEST_CREATED"
    PULLREQUEST_UPDATED = "PULLREQUEST_UPDATED"
    PULLREQUEST_APPROVAL_RULE_CREATED = "PULLREQUEST_APPROVAL_RULE_CREATED"
    PULLREQUEST_APPROVAL_RULE_DELETED = "PULLREQUEST_APPROVAL_RULE_DELETED"
    PULLREQUEST_APPROVAL_RULE_UPDATED = "PULLREQUEST_APPROVAL_RULE_UPDATED"
    PULLREQUEST_APPROVAL_REVOKED = "PULLREQUEST_APPROVAL_REVOKED"
    PULLREQUEST_APPROVED = "PULLREQUEST_APPROVED"


@dataclass
class RepositoryConfig:
    """Configuration for a CodeCommit repository."""
    repository_name: str
    description: Optional[str] = None
    region: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    kms_key_id: Optional[str] = None
    encryption_enabled: bool = True
    notification_config: Optional[Dict[str, Any]] = None


@dataclass
class BranchConfig:
    """Configuration for a branch."""
    branch_name: str
    repository_name: str
    commit_id: Optional[str] = None


@dataclass
class CommitConfig:
    """Configuration for a commit."""
    repository_name: str
    branch_name: str
    commit_message: str
    author_name: str
    author_email: str
    parent_commit_id: Optional[str] = None
    keep_empty_files: bool = False


@dataclass
class FileConfig:
    """Configuration for file operations."""
    repository_name: str
    branch_name: str
    file_path: str
    file_content: Union[str, bytes]
    commit_message: str
    author_name: str
    author_email: str


@dataclass
class PullRequestConfig:
    """Configuration for a pull request."""
    title: str
    description: str
    source_branch: str
    target_branch: str
    repository_name: str
    author: Optional[str] = None
    targets: Optional[List[Dict[str, Any]]] = None


@dataclass
class ApprovalRuleConfig:
    """Configuration for an approval rule."""
    name: str
    repository_name: str
    approval_pool_size: Optional[int] = None
    branch_name: Optional[str] = None
    template_id: Optional[str] = None
    rules: Optional[List[Dict[str, Any]]] = None


@dataclass
class NotificationConfig:
    """Configuration for notifications."""
    repository_name: str
    rule_id: str
    destination_arn: str
    events: List[str]
    branch_filter: Optional[str] = None


@dataclass
class CloneConfig:
    """Configuration for cloning a repository."""
    repository_name: str
    local_path: str
    branch: Optional[str] = None
    depth: Optional[int] = None
    use_git_credentials: bool = True


class CodeCommitIntegration:
    """
    AWS CodeCommit Integration for workflow automation.
    
    Provides comprehensive management for:
    - Repository lifecycle
    - Branches and commits
    - File operations
    - Pull requests
    - Approval rules
    - Notifications
    - Merge operations
    - Comparison
    - Local cloning
    """
    
    def __init__(
        self,
        region: Optional[str] = None,
        profile_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        config: Optional[BotoConfig] = None
    ):
        """
        Initialize the CodeCommit integration.
        
        Args:
            region: AWS region (defaults to config or us-east-1)
            profile_name: AWS profile name
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
            endpoint_url: Custom endpoint URL
            config: Botocore Config object
        """
        self.region = region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.config = config
        
        self._clients: Dict[str, Any] = {}
        self._resource_lock = threading.Lock()
        
        if BOTO3_AVAILABLE:
            self._init_clients()
    
    def _init_clients(self):
        """Initialize AWS clients."""
        init_params = {
            'region_name': self.region
        }
        
        if self.profile_name:
            init_params['profile_name'] = self.profile_name
        elif self.aws_access_key_id and self.aws_secret_access_key:
            init_params['aws_access_key_id'] = self.aws_access_key_id
            init_params['aws_secret_access_key'] = self.aws_secret_access_key
        
        if self.endpoint_url:
            init_params['endpoint_url'] = self.endpoint_url
        
        if self.config:
            init_params['config'] = self.config
        
        self._clients['codecommit'] = boto3.client('codecommit', **init_params)
        self._clients['sns'] = boto3.client('sns', **init_params)
        self._clients['sts'] = boto3.client('sts', **init_params)
        self._clients['iam'] = boto3.client('iam', **init_params)
    
    @property
    def codecommit_client(self):
        """Get the CodeCommit client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for CodeCommit operations")
        return self._clients.get('codecommit')
    
    @property
    def sns_client(self):
        """Get the SNS client."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for SNS operations")
        return self._clients.get('sns')
    
    def _get_client(self, service_name: str):
        """Get a specific AWS client."""
        if service_name not in self._clients:
            if BOTO3_AVAILABLE:
                self._clients[service_name] = boto3.client(
                    service_name,
                    region_name=self.region
                )
        return self._clients.get(service_name)
    
    # ========================================================================
    # Repository Management
    # ========================================================================
    
    def create_repository(
        self,
        config: RepositoryConfig
    ) -> Dict[str, Any]:
        """
        Create a new CodeCommit repository.
        
        Args:
            config: RepositoryConfig with repository settings
            
        Returns:
            Dictionary with repository details
        """
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 not available, returning mock data")
            return {
                'repository_metadata': {
                    'repository_name': config.repository_name,
                    'repository_id': str(uuid.uuid4()),
                    'repository_description': config.description,
                    'account_id': '123456789012',
                    'arn': f'arn:aws:codecommit:{self.region}:123456789012:{config.repository_name}',
                    'created_date': datetime.now().isoformat(),
                    'last_modified_date': datetime.now().isoformat(),
                },
                'status': RepositoryStatus.CREATING.value
            }
        
        try:
            kwargs = {
                'repositoryName': config.repository_name,
            }
            
            if config.description:
                kwargs['repositoryDescription'] = config.description
            
            if config.encryption_enabled and config.kms_key_id:
                kwargs['kmsKeyId'] = config.kms_key_id
            elif config.encryption_enabled:
                kwargs['encrypt'] = True
            
            if config.tags:
                kwargs['tags'] = config.tags
            
            response = self.codecommit_client.create_repository(**kwargs)
            
            logger.info(f"Created repository: {config.repository_name}")
            return {
                'repository_metadata': response.get('repositoryMetadata', {}),
                'status': RepositoryStatus.ACTIVE.value
            }
            
        except ClientError as e:
            logger.error(f"Failed to create repository: {e}")
            raise
    
    def get_repository(
        self,
        repository_name: str
    ) -> Dict[str, Any]:
        """
        Get details of a repository.
        
        Args:
            repository_name: Name of the repository
            
        Returns:
            Dictionary with repository details
        """
        if not BOTO3_AVAILABLE:
            return {
                'repository_name': repository_name,
                'repository_id': str(uuid.uuid4()),
                'description': '',
                'arn': f'arn:aws:codecommit:{self.region}:123456789012:{repository_name}',
            }
        
        try:
            response = self.codecommit_client.get_repository(
                repositoryName=repository_name
            )
            return response.get('repositoryMetadata', {})
        except ClientError as e:
            logger.error(f"Failed to get repository: {e}")
            raise
    
    def list_repositories(
        self,
        sort_by: str = 'repositoryName',
        order: str = 'ascending'
    ) -> List[Dict[str, Any]]:
        """
        List all repositories.
        
        Args:
            sort_by: Sort criteria (repositoryName or lastModifiedDate)
            order: Sort order (ascending or descending)
            
        Returns:
            List of repository details
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            repositories = []
            paginator = self.codecommit_client.get_paginator('list_repositories')
            
            page_iterator = paginator.paginate(
                sortBy=sort_by,
                order=order
            )
            
            for page in page_iterator:
                repositories.extend(page.get('repositories', []))
            
            return repositories
            
        except ClientError as e:
            logger.error(f"Failed to list repositories: {e}")
            raise
    
    def delete_repository(
        self,
        repository_name: str,
        force_delete: bool = False
    ) -> Dict[str, Any]:
        """
        Delete a repository.
        
        Args:
            repository_name: Name of the repository to delete
            force_delete: Force delete even if there are unmerged pull requests
            
        Returns:
            Dictionary with deletion status
        """
        if not BOTO3_AVAILABLE:
            return {'status': 'deleted', 'repository_name': repository_name}
        
        try:
            kwargs = {'repositoryName': repository_name}
            
            if force_delete:
                kwargs['force'] = True
            
            self.codecommit_client.delete_repository(**kwargs)
            
            logger.info(f"Deleted repository: {repository_name}")
            return {
                'status': 'deleted',
                'repository_name': repository_name
            }
            
        except ClientError as e:
            logger.error(f"Failed to delete repository: {e}")
            raise
    
    def update_repository(
        self,
        repository_name: str,
        description: Optional[str] = None,
        default_branch: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update repository settings.
        
        Args:
            repository_name: Name of the repository
            description: New description
            default_branch: New default branch name
            
        Returns:
            Updated repository details
        """
        if not BOTO3_AVAILABLE:
            return {'repository_name': repository_name, 'updated': True}
        
        try:
            kwargs = {'repositoryName': repository_name}
            
            if description is not None:
                kwargs['repositoryDescription'] = description
            
            if default_branch is not None:
                kwargs['defaultBranch'] = default_branch
            
            response = self.codecommit_client.update_repository(**kwargs)
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to update repository: {e}")
            raise
    
    # ========================================================================
    # Branch Management
    # ========================================================================
    
    def create_branch(
        self,
        repository_name: str,
        branch_name: str,
        commit_id: str
    ) -> Dict[str, Any]:
        """
        Create a new branch.
        
        Args:
            repository_name: Name of the repository
            branch_name: Name of the new branch
            commit_id: Commit ID to create branch from
            
        Returns:
            Branch creation status
        """
        if not BOTO3_AVAILABLE:
            return {
                'branch': {
                    'repository_name': repository_name,
                    'branch_name': branch_name,
                    'commit_id': commit_id
                },
                'status': 'created'
            }
        
        try:
            response = self.codecommit_client.create_branch(
                repositoryName=repository_name,
                branchName=branch_name,
                commitId=commit_id
            )
            
            logger.info(f"Created branch {branch_name} in {repository_name}")
            return {
                'branch': response,
                'status': 'created'
            }
            
        except ClientError as e:
            logger.error(f"Failed to create branch: {e}")
            raise
    
    def get_branch(
        self,
        repository_name: str,
        branch_name: str
    ) -> Dict[str, Any]:
        """
        Get details of a branch.
        
        Args:
            repository_name: Name of the repository
            branch_name: Name of the branch
            
        Returns:
            Branch details including commit ID
        """
        if not BOTO3_AVAILABLE:
            return {
                'branch_name': branch_name,
                'commit_id': 'abc123'
            }
        
        try:
            response = self.codecommit_client.get_branch(
                repositoryName=repository_name,
                branchName=branch_name
            )
            
            return response.get('branch', {})
            
        except ClientError as e:
            logger.error(f"Failed to get branch: {e}")
            raise
    
    def list_branches(
        self,
        repository_name: str,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List all branches in a repository.
        
        Args:
            repository_name: Name of the repository
            max_results: Maximum number of branches to return
            
        Returns:
            List of branch details
        """
        if not BOTO3_AVAILABLE:
            return [{'branch_name': 'main'}, {'branch_name': 'develop'}]
        
        try:
            branches = []
            kwargs = {'repositoryName': repository_name}
            
            if max_results:
                kwargs['maxResults'] = max_results
            
            paginator = self.codecommit_client.get_paginator('list_branches')
            page_iterator = paginator.paginate(**kwargs)
            
            for page in page_iterator:
                for branch_name in page.get('branches', []):
                    branches.append({'branch_name': branch_name})
            
            return branches
            
        except ClientError as e:
            logger.error(f"Failed to list branches: {e}")
            raise
    
    def delete_branch(
        self,
        repository_name: str,
        branch_name: str
    ) -> Dict[str, Any]:
        """
        Delete a branch.
        
        Args:
            repository_name: Name of the repository
            branch_name: Name of the branch to delete
            
        Returns:
            Deletion status
        """
        if not BOTO3_AVAILABLE:
            return {'status': 'deleted', 'branch_name': branch_name}
        
        try:
            response = self.codecommit_client.delete_branch(
                repositoryName=repository_name,
                branchName=branch_name
            )
            
            logger.info(f"Deleted branch {branch_name} from {repository_name}")
            return {'status': 'deleted', 'branch_name': branch_name}
            
        except ClientError as e:
            logger.error(f"Failed to delete branch: {e}")
            raise
    
    # ========================================================================
    # Commit Operations
    # ========================================================================
    
    def create_commit(
        self,
        config: CommitConfig
    ) -> Dict[str, Any]:
        """
        Create a new commit.
        
        Args:
            config: CommitConfig with commit details
            
        Returns:
            Created commit details
        """
        if not BOTO3_AVAILABLE:
            return {
                'commit_id': str(uuid.uuid4()),
                'tree_id': str(uuid.uuid4()),
                'message': config.commit_message,
                'author': {
                    'name': config.author_name,
                    'email': config.author_email
                }
            }
        
        try:
            kwargs = {
                'repositoryName': config.repository_name,
                'branchName': config.branch_name,
                'commitMessage': config.commit_message,
                'name': config.author_name,
                'email': config.author_email,
            }
            
            if config.parent_commit_id:
                kwargs['parentCommitId'] = config.parent_commit_id
            
            kwargs['keepEmptyFiles'] = config.keep_empty_files
            
            response = self.codecommit_client.create_commit(**kwargs)
            
            logger.info(f"Created commit in {config.repository_name}/{config.branch_name}")
            return {
                'commit_id': response.get('commitId'),
                'tree_id': response.get('treeId'),
                'message': config.commit_message,
                'author': {
                    'name': config.author_name,
                    'email': config.author_email
                }
            }
            
        except ClientError as e:
            logger.error(f"Failed to create commit: {e}")
            raise
    
    def get_commit(
        self,
        repository_name: str,
        commit_id: str
    ) -> Dict[str, Any]:
        """
        Get details of a commit.
        
        Args:
            repository_name: Name of the repository
            commit_id: Commit ID
            
        Returns:
            Commit details
        """
        if not BOTO3_AVAILABLE:
            return {
                'commit_id': commit_id,
                'message': 'Sample commit',
                'author': {'name': 'user', 'email': 'user@example.com'},
                'committer': {'name': 'user', 'email': 'user@example.com'},
                'parents': []
            }
        
        try:
            response = self.codecommit_client.get_commit(
                repositoryName=repository_name,
                commitId=commit_id
            )
            
            return response.get('commit', {})
            
        except ClientError as e:
            logger.error(f"Failed to get commit: {e}")
            raise
    
    def list_commits(
        self,
        repository_name: str,
        branch_name: Optional[str] = None,
        commit_id: Optional[str] = None,
        max_results: Optional[int] = None,
        author: Optional[str] = None,
        date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List commits in a repository.
        
        Args:
            repository_name: Name of the repository
            branch_name: Filter by branch
            commit_id: Start from this commit
            max_results: Maximum number of commits to return
            author: Filter by author
            date: Filter by date (ISO 8601 format)
            
        Returns:
            List of commits
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            kwargs = {'repositoryName': repository_name}
            
            if branch_name:
                kwargs['branchName'] = branch_name
            
            if commit_id:
                kwargs['commitId'] = commit_id
            
            if max_results:
                kwargs['maxResults'] = max_results
            
            if author:
                kwargs['author'] = author
            
            if date:
                kwargs['DateRange'] = date
            
            commits = []
            paginator = self.codecommit_client.get_paginator('list_commits')
            page_iterator = paginator.paginate(**kwargs)
            
            for page in page_iterator:
                commits.extend(page.get('commits', []))
            
            return commits
            
        except ClientError as e:
            logger.error(f"Failed to list commits: {e}")
            raise
    
    # ========================================================================
    # File Operations
    # ========================================================================
    
    def get_file(
        self,
        repository_name: str,
        file_path: str,
        commit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a file from the repository.
        
        Args:
            repository_name: Name of the repository
            file_path: Path to the file
            commit_id: Specific commit ID (defaults to head)
            
        Returns:
            File content and metadata
        """
        if not BOTO3_AVAILABLE:
            return {
                'file_path': file_path,
                'file_content': base64.b64encode(b'Sample content').decode(),
                'commit_id': commit_id or 'abc123',
                'blob_id': str(uuid.uuid4())
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'filePath': file_path
            }
            
            if commit_id:
                kwargs['commitId'] = commit_id
            
            response = self.codecommit_client.get_file(**kwargs)
            
            return {
                'file_path': response.get('filePath'),
                'file_content': response.get('fileContent'),
                'commit_id': response.get('commitId'),
                'blob_id': response.get('blobId'),
                'tree_id': response.get('treeId'),
                'file_sha': response.get('fileSha')
            }
            
        except ClientError as e:
            logger.error(f"Failed to get file: {e}")
            raise
    
    def get_folder(
        self,
        repository_name: str,
        folder_path: str,
        commit_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a folder (directory) contents.
        
        Args:
            repository_name: Name of the repository
            folder_path: Path to the folder
            commit_id: Specific commit ID
            
        Returns:
            Folder contents
        """
        if not BOTO3_AVAILABLE:
            return {
                'folder_path': folder_path,
                'files': [],
                'sub_folders': []
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'folderPath': folder_path
            }
            
            if commit_id:
                kwargs['commitId'] = commit_id
            
            response = self.codecommit_client.get_folder(**kwargs)
            
            return {
                'folder_path': folder_path,
                'files': response.get('files', []),
                'sub_folders': response.get('subFolders', []),
                'commit_id': response.get('commitId')
            }
            
        except ClientError as e:
            logger.error(f"Failed to get folder: {e}")
            raise
    
    def create_file(
        self,
        config: FileConfig
    ) -> Dict[str, Any]:
        """
        Create or update a file in the repository.
        
        Args:
            config: FileConfig with file details
            
        Returns:
            Created file details
        """
        if not BOTO3_AVAILABLE:
            return {
                'file_path': config.file_path,
                'blob_id': str(uuid.uuid4()),
                'commit_id': str(uuid.uuid4()),
                'status': 'created'
            }
        
        try:
            file_content = config.file_content
            if isinstance(file_content, str):
                file_content = file_content.encode('utf-8')
            
            kwargs = {
                'repositoryName': config.repository_name,
                'branchName': config.branch_name,
                'fileContent': file_content,
                'filePath': config.file_path,
                'commitMessage': config.commit_message,
                'name': config.author_name,
                'email': config.author_email
            }
            
            response = self.codecommit_client.create_file(**kwargs)
            
            logger.info(f"Created/updated file {config.file_path}")
            return {
                'file_path': response.get('fileMetadata', {}).get('filePath'),
                'blob_id': response.get('fileMetadata', {}).get('blobId'),
                'commit_id': response.get('commitId'),
                'status': 'created'
            }
            
        except ClientError as e:
            logger.error(f"Failed to create file: {e}")
            raise
    
    def update_file(
        self,
        config: FileConfig,
        file_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update an existing file in the repository.
        
        Args:
            config: FileConfig with file details
            file_id: File ID from get_file
            
        Returns:
            Updated file details
        """
        if not BOTO3_AVAILABLE:
            return {
                'file_path': config.file_path,
                'blob_id': str(uuid.uuid4()),
                'commit_id': str(uuid.uuid4()),
                'status': 'updated'
            }
        
        try:
            file_content = config.file_content
            if isinstance(file_content, str):
                file_content = file_content.encode('utf-8')
            
            kwargs = {
                'repositoryName': config.repository_name,
                'branchName': config.branch_name,
                'fileContent': file_content,
                'filePath': config.file_path,
                'commitMessage': config.commit_message,
                'name': config.author_name,
                'email': config.author_email
            }
            
            if file_id:
                kwargs['fileId'] = file_id
            
            response = self.codecommit_client.update_file(**kwargs)
            
            logger.info(f"Updated file {config.file_path}")
            return {
                'file_path': response.get('fileMetadata', {}).get('filePath'),
                'blob_id': response.get('fileMetadata', {}).get('blobId'),
                'commit_id': response.get('commitId'),
                'status': 'updated'
            }
            
        except ClientError as e:
            logger.error(f"Failed to update file: {e}")
            raise
    
    def delete_file(
        self,
        repository_name: str,
        branch_name: str,
        file_path: str,
        commit_message: str,
        author_name: str,
        author_email: str,
        file_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete a file from the repository.
        
        Args:
            repository_name: Name of the repository
            branch_name: Branch name
            file_path: Path to the file
            commit_message: Commit message
            author_name: Author name
            author_email: Author email
            file_id: File ID from get_file
            
        Returns:
            Deletion status
        """
        if not BOTO3_AVAILABLE:
            return {
                'file_path': file_path,
                'commit_id': str(uuid.uuid4()),
                'status': 'deleted'
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'branchName': branch_name,
                'filePath': file_path,
                'commitMessage': commit_message,
                'name': author_name,
                'email': author_email
            }
            
            if file_id:
                kwargs['fileId'] = file_id
            
            response = self.codecommit_client.delete_file(**kwargs)
            
            logger.info(f"Deleted file {file_path}")
            return {
                'file_path': file_path,
                'commit_id': response.get('commitId'),
                'blob_id': response.get('blobId'),
                'status': 'deleted'
            }
            
        except ClientError as e:
            logger.error(f"Failed to delete file: {e}")
            raise
    
    # ========================================================================
    # Pull Request Operations
    # ========================================================================
    
    def create_pull_request(
        self,
        config: PullRequestConfig
    ) -> Dict[str, Any]:
        """
        Create a new pull request.
        
        Args:
            config: PullRequestConfig with PR details
            
        Returns:
            Created pull request details
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': str(uuid.uuid4()),
                'title': config.title,
                'description': config.description,
                'source_branch': config.source_branch,
                'target_branch': config.target_branch,
                'status': PullRequestStatus.OPEN.value
            }
        
        try:
            kwargs = {
                'title': config.title,
                'description': config.description,
                'targets': [
                    {
                        'repositoryName': config.repository_name,
                        'sourceReference': config.source_branch,
                        'destinationReference': config.target_branch
                    }
                ]
            }
            
            if config.author:
                kwargs['authorArn'] = config.author
            
            if config.targets:
                kwargs['targets'] = config.targets
            
            response = self.codecommit_client.create_pull_request(**kwargs)
            pr_metadata = response.get('pullRequest', {})
            
            logger.info(f"Created pull request {pr_metadata.get('pullRequestId')}")
            return {
                'pull_request_id': pr_metadata.get('pullRequestId'),
                'title': pr_metadata.get('title'),
                'description': pr_metadata.get('description'),
                'source_branch': config.source_branch,
                'target_branch': config.target_branch,
                'status': pr_metadata.get('pullRequestStatus'),
                'author_arn': pr_metadata.get('authorArn')
            }
            
        except ClientError as e:
            logger.error(f"Failed to create pull request: {e}")
            raise
    
    def get_pull_request(
        self,
        repository_name: str,
        pull_request_id: str
    ) -> Dict[str, Any]:
        """
        Get pull request details.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            
        Returns:
            Pull request details
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': pull_request_id,
                'title': 'Sample PR',
                'status': PullRequestStatus.OPEN.value
            }
        
        try:
            response = self.codecommit_client.get_pull_request(
                repositoryName=repository_name,
                pullRequestId=pull_request_id
            )
            
            return response.get('pullRequest', {})
            
        except ClientError as e:
            logger.error(f"Failed to get pull request: {e}")
            raise
    
    def list_pull_requests(
        self,
        repository_name: str,
        status: Optional[str] = None,
        author_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List pull requests in a repository.
        
        Args:
            repository_name: Name of the repository
            status: Filter by status (OPEN, CLOSED, MERGED)
            author_id: Filter by author ARN
            
        Returns:
            List of pull requests
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            kwargs = {'repositoryName': repository_name}
            
            if status:
                kwargs['pullRequestStatus'] = status
            
            if author_id:
                kwargs['authorArn'] = author_id
            
            response = self.codecommit_client.list_pull_requests(**kwargs)
            
            return response.get('pullRequestIds', [])
            
        except ClientError as e:
            logger.error(f"Failed to list pull requests: {e}")
            raise
    
    def update_pull_request(
        self,
        repository_name: str,
        pull_request_id: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update a pull request.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            title: New title
            description: New description
            status: New status
            
        Returns:
            Updated pull request details
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': pull_request_id,
                'title': title,
                'status': status
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'pullRequestId': pull_request_id
            }
            
            if title:
                kwargs['title'] = title
            
            if description:
                kwargs['description'] = description
            
            if status:
                kwargs['pullRequestStatus'] = status
            
            response = self.codecommit_client.update_pull_request(**kwargs)
            
            logger.info(f"Updated pull request {pull_request_id}")
            return response.get('pullRequest', {})
            
        except ClientError as e:
            logger.error(f"Failed to update pull request: {e}")
            raise
    
    def close_pull_request(
        self,
        repository_name: str,
        pull_request_id: str
    ) -> Dict[str, Any]:
        """
        Close a pull request.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            
        Returns:
            Closed pull request details
        """
        return self.update_pull_request(
            repository_name=repository_name,
            pull_request_id=pull_request_id,
            status=PullRequestStatus.CLOSED.value
        )
    
    # ========================================================================
    # Approval Rules
    # ========================================================================
    
    def create_approval_rule(
        self,
        config: ApprovalRuleConfig
    ) -> Dict[str, Any]:
        """
        Create an approval rule for a pull request.
        
        Args:
            config: ApprovalRuleConfig with rule details
            
        Returns:
            Created approval rule details
        """
        if not BOTO3_AVAILABLE:
            return {
                'approval_rule_id': str(uuid.uuid4()),
                'name': config.name,
                'repository_name': config.repository_name,
                'status': 'active'
            }
        
        try:
            kwargs = {
                'repositoryName': config.repository_name,
                'approvalRuleContent': json.dumps({
                    'version': '1',
                    'statements': [
                        {
                            'type': 'Approvers',
                            'numberOfApprovers': config.approval_pool_size or 1,
                            'approvalPoolMembers': ['*']
                        }
                    ]
                }),
                'approvalRuleName': config.name
            }
            
            if config.branch_name:
                kwargs['ruleContentSha1'] = hashlib.sha1(
                    json.dumps({'branch': config.branch_name}).encode()
                ).hexdigest()
            
            response = self.codecommit_client.create_approval_rule(**kwargs)
            
            logger.info(f"Created approval rule {config.name}")
            return {
                'approval_rule_id': response.get('approvalRule', {}).get('approvalRuleId'),
                'name': response.get('approvalRule', {}).get('approvalRuleName'),
                'repository_name': config.repository_name,
                'status': 'active'
            }
            
        except ClientError as e:
            logger.error(f"Failed to create approval rule: {e}")
            raise
    
    def get_approval_rule(
        self,
        repository_name: str,
        approval_rule_name: str
    ) -> Dict[str, Any]:
        """
        Get approval rule details.
        
        Args:
            repository_name: Name of the repository
            approval_rule_name: Name of the approval rule
            
        Returns:
            Approval rule details
        """
        if not BOTO3_AVAILABLE:
            return {
                'approval_rule_name': approval_rule_name,
                'approval_rule_id': str(uuid.uuid4())
            }
        
        try:
            response = self.codecommit_client.get_approval_rule(
                repositoryName=repository_name,
                approvalRuleName=approval_rule_name
            )
            
            return response.get('approvalRule', {})
            
        except ClientError as e:
            logger.error(f"Failed to get approval rule: {e}")
            raise
    
    def list_approval_rules(
        self,
        repository_name: str
    ) -> List[Dict[str, Any]]:
        """
        List all approval rules in a repository.
        
        Args:
            repository_name: Name of the repository
            
        Returns:
            List of approval rules
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            response = self.codecommit_client.list_approval_rules(
                repositoryName=repository_name
            )
            
            return response.get('approvalRules', [])
            
        except ClientError as e:
            logger.error(f"Failed to list approval rules: {e}")
            raise
    
    def update_approval_rule(
        self,
        repository_name: str,
        approval_rule_name: str,
        new_rule_content: str
    ) -> Dict[str, Any]:
        """
        Update an approval rule.
        
        Args:
            repository_name: Name of the repository
            approval_rule_name: Name of the rule to update
            new_rule_content: New rule content as JSON string
            
        Returns:
            Updated approval rule details
        """
        if not BOTO3_AVAILABLE:
            return {
                'approval_rule_name': approval_rule_name,
                'updated': True
            }
        
        try:
            response = self.codecommit_client.update_approval_rule(
                repositoryName=repository_name,
                approvalRuleName=approval_rule_name,
                newRuleContent=new_rule_content
            )
            
            logger.info(f"Updated approval rule {approval_rule_name}")
            return {
                'approval_rule_name': response.get('approvalRule', {}).get('approvalRuleName'),
                'updated': True
            }
            
        except ClientError as e:
            logger.error(f"Failed to update approval rule: {e}")
            raise
    
    def delete_approval_rule(
        self,
        repository_name: str,
        approval_rule_name: str
    ) -> Dict[str, Any]:
        """
        Delete an approval rule.
        
        Args:
            repository_name: Name of the repository
            approval_rule_name: Name of the rule to delete
            
        Returns:
            Deletion status
        """
        if not BOTO3_AVAILABLE:
            return {'status': 'deleted', 'rule_name': approval_rule_name}
        
        try:
            response = self.codecommit_client.delete_approval_rule(
                repositoryName=repository_name,
                approvalRuleName=approval_rule_name
            )
            
            logger.info(f"Deleted approval rule {approval_rule_name}")
            return {
                'status': 'deleted',
                'rule_name': approval_rule_name
            }
            
        except ClientError as e:
            logger.error(f"Failed to delete approval rule: {e}")
            raise
    
    # ========================================================================
    # Approvals
    # ========================================================================
    
    def approve_pull_request(
        self,
        repository_name: str,
        pull_request_id: str,
        approval_rule_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Approve a pull request.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            approval_rule_name: Optional specific rule to approve
            
        Returns:
            Approval status
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': pull_request_id,
                'approved': True
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'pullRequestId': pull_request_id
            }
            
            if approval_rule_name:
                kwargs['approvalRuleName'] = approval_rule_name
            
            self.codecommit_client.post_approval_rule_approval(**kwargs)
            
            logger.info(f"Approved pull request {pull_request_id}")
            return {
                'pull_request_id': pull_request_id,
                'approved': True
            }
            
        except ClientError as e:
            logger.error(f"Failed to approve pull request: {e}")
            raise
    
    def revoke_approval(
        self,
        repository_name: str,
        pull_request_id: str,
        approval_rule_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Revoke approval on a pull request.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            approval_rule_name: Optional specific rule to revoke
            
        Returns:
            Revocation status
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': pull_request_id,
                'revoked': True
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'pullRequestId': pull_request_id
            }
            
            if approval_rule_name:
                kwargs['approvalRuleName'] = approval_rule_name
            
            self.codecommit_client.post_approval_rule_revoke(**kwargs)
            
            logger.info(f"Revoked approval on pull request {pull_request_id}")
            return {
                'pull_request_id': pull_request_id,
                'revoked': True
            }
            
        except ClientError as e:
            logger.error(f"Failed to revoke approval: {e}")
            raise
    
    def get_approval_state(
        self,
        repository_name: str,
        pull_request_id: str
    ) -> Dict[str, Any]:
        """
        Get approval state of a pull request.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            
        Returns:
            Approval state details
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': pull_request_id,
                'approvals': []
            }
        
        try:
            response = self.codecommit_client.get_approval_state(
                repositoryName=repository_name,
                pullRequestId=pull_request_id
            )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get approval state: {e}")
            raise
    
    # ========================================================================
    # Notifications
    # ========================================================================
    
    def create_notification_rule(
        self,
        config: NotificationConfig
    ) -> Dict[str, Any]:
        """
        Create a notification rule for repository events.
        
        Args:
            config: NotificationConfig with notification details
            
        Returns:
            Created notification rule details
        """
        if not BOTO3_AVAILABLE:
            return {
                'rule_id': config.rule_id,
                'repository_name': config.repository_name,
                'destination_arn': config.destination_arn,
                'status': 'active'
            }
        
        try:
            kwargs = {
                'repositoryName': config.repository_name,
                'rule': {
                    'destination': config.destination_arn,
                    'events': config.events
                },
                'ruleName': config.rule_id
            }
            
            if config.branch_filter:
                kwargs['rule']['branchFilter'] = config.branch_filter
            
            response = self.codecommit_client.create_notification_rule(**kwargs)
            
            logger.info(f"Created notification rule {config.rule_id}")
            return {
                'rule_id': response.get('ruleId'),
                'repository_name': config.repository_name,
                'destination_arn': config.destination_arn,
                'status': 'active'
            }
            
        except ClientError as e:
            logger.error(f"Failed to create notification rule: {e}")
            raise
    
    def list_notification_rules(
        self,
        repository_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List notification rules.
        
        Args:
            repository_name: Optional repository name filter
            
        Returns:
            List of notification rules
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            kwargs = {}
            
            if repository_name:
                kwargs['repositoryName'] = repository_name
            
            response = self.codecommit_client.list_notification_rules(**kwargs)
            
            return response.get('notificationRules', [])
            
        except ClientError as e:
            logger.error(f"Failed to list notification rules: {e}")
            raise
    
    def delete_notification_rule(
        self,
        rule_id: str
    ) -> Dict[str, Any]:
        """
        Delete a notification rule.
        
        Args:
            rule_id: Notification rule ID
            
        Returns:
            Deletion status
        """
        if not BOTO3_AVAILABLE:
            return {'status': 'deleted', 'rule_id': rule_id}
        
        try:
            self.codecommit_client.delete_notification_rule(ruleId=rule_id)
            
            logger.info(f"Deleted notification rule {rule_id}")
            return {'status': 'deleted', 'rule_id': rule_id}
            
        except ClientError as e:
            logger.error(f"Failed to delete notification rule: {e}")
            raise
    
    # ========================================================================
    # Merge Operations
    # ========================================================================
    
    def merge_branches(
        self,
        repository_name: str,
        source_branch: str,
        destination_branch: str,
        merge_option: MergeOption = MergeOption.FAST_FORWARD_MERGE,
        author_name: Optional[str] = None,
        author_email: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Merge branches in a repository.
        
        Args:
            repository_name: Name of the repository
            source_branch: Source branch name
            destination_branch: Destination branch name
            merge_option: Merge strategy
            author_name: Author name for the merge commit
            author_email: Author email for the merge commit
            
        Returns:
            Merge result
        """
        if not BOTO3_AVAILABLE:
            return {
                'merge_commit_id': str(uuid.uuid4()),
                'source_branch': source_branch,
                'destination_branch': destination_branch,
                'status': 'merged'
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'sourceCommitId': source_branch,
                'destinationCommitId': destination_branch,
                'mergeOption': merge_option.value
            }
            
            if author_name:
                kwargs['authorName'] = author_name
            
            if author_email:
                kwargs['email'] = author_email
            
            response = self.codecommit_client.merge_branches_by_fast_forward(**kwargs)
            
            logger.info(f"Merged {source_branch} into {destination_branch}")
            return {
                'merge_commit_id': response.get('mergeCommitId'),
                'source_branch': source_branch,
                'destination_branch': destination_branch,
                'status': 'merged'
            }
            
        except ClientError as e:
            logger.error(f"Failed to merge branches: {e}")
            raise
    
    def merge_pull_request(
        self,
        repository_name: str,
        pull_request_id: str,
        merge_option: MergeOption = MergeOption.FAST_FORWARD_MERGE
    ) -> Dict[str, Any]:
        """
        Merge a pull request.
        
        Args:
            repository_name: Name of the repository
            pull_request_id: Pull request ID
            merge_option: Merge strategy
            
        Returns:
            Merge result
        """
        if not BOTO3_AVAILABLE:
            return {
                'pull_request_id': pull_request_id,
                'status': 'merged'
            }
        
        try:
            response = self.codecommit_client.merge_pull_request_by_fast_forward(
                repositoryName=repository_name,
                pullRequestId=pull_request_id,
                mergeOption=merge_option.value
            )
            
            logger.info(f"Merged pull request {pull_request_id}")
            return {
                'pull_request_id': pull_request_id,
                'merge_commit_id': response.get('mergeCommitId'),
                'status': 'merged'
            }
            
        except ClientError as e:
            logger.error(f"Failed to merge pull request: {e}")
            raise
    
    # ========================================================================
    # Compare Operations
    # ========================================================================
    
    def compare_commits(
        self,
        repository_name: str,
        base_commit: str,
        head_commit: str,
        max_results: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Compare two commits.
        
        Args:
            repository_name: Name of the repository
            base_commit: Base commit ID
            head_commit: Head commit ID
            max_results: Maximum number of differences to return
            
        Returns:
            Comparison results
        """
        if not BOTO3_AVAILABLE:
            return {
                'base_commit': base_commit,
                'head_commit': head_commit,
                'differences': []
            }
        
        try:
            kwargs = {
                'repositoryName': repository_name,
                'baseCommitId': base_commit,
                'afterCommitId': head_commit
            }
            
            if max_results:
                kwargs['MaxResults'] = max_results
            
            response = self.codecommit_client.compare_commits(**kwargs)
            
            return {
                'base_commit': base_commit,
                'head_commit': head_commit,
                'differences': response.get('differences', []),
                'status': response.get('comparisonStatus'),
                'merge_base_commit_id': response.get('mergeBaseCommitId')
            }
            
        except ClientError as e:
            logger.error(f"Failed to compare commits: {e}")
            raise
    
    def compare_branches(
        self,
        repository_name: str,
        source_branch: str,
        target_branch: str
    ) -> Dict[str, Any]:
        """
        Compare two branches.
        
        Args:
            repository_name: Name of the repository
            source_branch: Source branch name
            target_branch: Target branch name
            
        Returns:
            Branch comparison results
        """
        if not BOTO3_AVAILABLE:
            return {
                'source_branch': source_branch,
                'target_branch': target_branch,
                'difference_type': 'DIFFERENT'
            }
        
        try:
            response = self.codecommit_client.compare_branches(
                repositoryName=repository_name,
                sourceBranchName=source_branch,
                targetBranchName=target_branch
            )
            
            return {
                'source_branch': source_branch,
                'target_branch': target_branch,
                'difference_type': response.get('differenceType'),
                'merge_base_commit_id': response.get('mergeBaseCommitId')
            }
            
        except ClientError as e:
            logger.error(f"Failed to compare branches: {e}")
            raise
    
    # ========================================================================
    # Clone Operations
    # ========================================================================
    
    def get_repository_clone_url(
        self,
        repository_name: str,
        connection_type: str = 'HTTPS'
    ) -> str:
        """
        Get the clone URL for a repository.
        
        Args:
            repository_name: Name of the repository
            connection_type: Connection type (HTTPS, SSH, HTTPS_GRC)
            
        Returns:
            Clone URL
        """
        if not BOTO3_AVAILABLE:
            return f"https://git-codecommit.{self.region}.amazonaws.com/v1/repos/{repository_name}"
        
        try:
            response = self.codecommit_client.get_repository(
                repositoryName=repository_name
            )
            
            clone_url = None
            if connection_type == 'HTTPS':
                clone_url = response['repositoryMetadata'].get('cloneUrlHttp')
            elif connection_type == 'SSH':
                clone_url = response['repositoryMetadata'].get('cloneUrlSsh')
            elif connection_type == 'HTTPS_GRC':
                clone_url = response['repositoryMetadata'].get('cloneUrlHttp')
            
            return clone_url or f"https://git-codecommit.{self.region}.amazonaws.com/v1/repos/{repository_name}"
            
        except ClientError as e:
            logger.error(f"Failed to get clone URL: {e}")
            raise
    
    def clone_repository(
        self,
        config: CloneConfig
    ) -> Dict[str, Any]:
        """
        Clone a repository locally using git.
        
        Args:
            config: CloneConfig with clone settings
            
        Returns:
            Clone status
        """
        try:
            clone_url = self.get_repository_clone_url(
                config.repository_name,
                'HTTPS' if config.use_git_credentials else 'SSH'
            )
            
            os.makedirs(config.local_path, exist_ok=True)
            
            cmd = ['git', 'clone', '--mirror', clone_url, config.local_path]
            
            if config.depth:
                cmd.insert(2, f'--depth={config.depth}')
            
            if config.branch:
                cmd.extend(['--branch', config.branch])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info(f"Cloned repository {config.repository_name} to {config.local_path}")
                return {
                    'status': 'cloned',
                    'repository_name': config.repository_name,
                    'local_path': config.local_path,
                    'branch': config.branch
                }
            else:
                raise RuntimeError(f"Git clone failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("Clone operation timed out")
            raise
        except Exception as e:
            logger.error(f"Failed to clone repository: {e}")
            raise
    
    def get_git_credentials(self, username: str) -> Dict[str, Any]:
        """
        Get Git credentials for CodeCommit.
        
        Args:
            username: IAM user name
            
        Returns:
            Git credentials
        """
        if not BOTO3_AVAILABLE:
            return {
                'username': username,
                'password': 'dummy-password'
            }
        
        try:
            response = self._clients['iam'].create_service_account_credential(
                ServiceName='codecommit.amazonaws.com',
                UserName=username
            )
            
            return {
                'username': response.get('ServiceUserName'),
                'password': response.get('ServicePassword')
            }
            
        except ClientError as e:
            logger.error(f"Failed to get git credentials: {e}")
            raise
    
    # ========================================================================
    # Batch Operations
    # ========================================================================
    
    def batch_get_files(
        self,
        repository_name: str,
        file_paths: List[str],
        commit_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get multiple files at once.
        
        Args:
            repository_name: Name of the repository
            file_paths: List of file paths
            commit_id: Optional commit ID
            
        Returns:
            List of file contents
        """
        if not BOTO3_AVAILABLE:
            return [{'file_path': p, 'content': ''} for p in file_paths]
        
        results = []
        for path in file_paths:
            try:
                result = self.get_file(repository_name, path, commit_id)
                results.append(result)
            except Exception as e:
                logger.warning(f"Failed to get file {path}: {e}")
                results.append({'file_path': path, 'error': str(e)})
        
        return results
    
    def batch_create_files(
        self,
        files: List[FileConfig]
    ) -> List[Dict[str, Any]]:
        """
        Create multiple files in a single commit.
        
        Args:
            files: List of FileConfig objects
            
        Returns:
            List of created file results
        """
        if not BOTO3_AVAILABLE:
            return [{'file_path': f.file_path, 'status': 'created'} for f in files]
        
        if not files:
            return []
        
        first_file = files[0]
        commit_config = CommitConfig(
            repository_name=first_file.repository_name,
            branch_name=first_file.branch_name,
            commit_message=f"Add {len(files)} files",
            author_name=first_file.author_name,
            author_email=first_file.author_email
        )
        
        try:
            self.create_commit(commit_config)
        except Exception as e:
            logger.error(f"Batch file creation failed: {e}")
            raise
        
        results = []
        for file_config in files:
            try:
                result = self.create_file(file_config)
                results.append(result)
            except Exception as e:
                logger.warning(f"Failed to create file {file_config.file_path}: {e}")
                results.append({'file_path': file_config.file_path, 'error': str(e)})
        
        return results
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def get_repository_usage(self, repository_name: str) -> Dict[str, Any]:
        """
        Get usage statistics for a repository.
        
        Args:
            repository_name: Name of the repository
            
        Returns:
            Usage statistics
        """
        if not BOTO3_AVAILABLE:
            return {
                'repository_name': repository_name,
                'total_commits': 0,
                'total_branches': 0,
                'total_pull_requests': 0
            }
        
        try:
            branches = self.list_branches(repository_name)
            commits = self.list_commits(repository_name, max_results=100)
            
            return {
                'repository_name': repository_name,
                'total_commits': len(commits),
                'total_branches': len(branches),
                'last_modified': datetime.now().isoformat()
            }
            
        except ClientError as e:
            logger.error(f"Failed to get repository usage: {e}")
            raise
    
    def validate_repository_name(self, name: str) -> bool:
        """
        Validate a repository name against CodeCommit naming rules.
        
        Args:
            name: Repository name to validate
            
        Returns:
            True if valid, False otherwise
        """
        import re
        
        pattern = r'^[a-zA-Z0-9_\.-]{1,100}$'
        return bool(re.match(pattern, name))
    
    def get_repository_arn(self, repository_name: str) -> str:
        """
        Get the ARN of a repository.
        
        Args:
            repository_name: Name of the repository
            
        Returns:
            Repository ARN
        """
        if not BOTO3_AVAILABLE:
            return f'arn:aws:codecommit:{self.region}:123456789012:{repository_name}'
        
        try:
            response = self.codecommit_client.get_repository(
                repositoryName=repository_name
            )
            return response['repositoryMetadata']['arn']
            
        except ClientError as e:
            logger.error(f"Failed to get repository ARN: {e}")
            raise
    
    def describe_events(
        self,
        repository_name: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get repository events.
        
        Args:
            repository_name: Name of the repository
            start_date: Start date for events
            end_date: End date for events
            max_results: Maximum number of events
            
        Returns:
            List of repository events
        """
        if not BOTO3_AVAILABLE:
            return []
        
        try:
            kwargs = {'repositoryName': repository_name}
            
            if start_date:
                kwargs['startDate'] = start_date.isoformat()
            
            if end_date:
                kwargs['endDate'] = end_date.isoformat()
            
            if max_results:
                kwargs['maxResults'] = max_results
            
            response = self.codecommit_client.describe_repository_events(**kwargs)
            
            return response.get('repositoryEvents', [])
            
        except ClientError as e:
            logger.error(f"Failed to describe repository events: {e}")
            raise


class CodeCommitWorkflowMixin:
    """
    Mixin class to add CodeCommit functionality to existing workflow classes.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._codecommit: Optional[CodeCommitIntegration] = None
    
    @property
    def codecommit(self) -> CodeCommitIntegration:
        """Get or create the CodeCommit integration instance."""
        if self._codecommit is None:
            self._codecommit = CodeCommitIntegration()
        return self._codecommit
    
    def setup_codecommit(
        self,
        region: Optional[str] = None,
        profile_name: Optional[str] = None,
        **kwargs
    ):
        """Setup the CodeCommit integration."""
        self._codecommit = CodeCommitIntegration(
            region=region,
            profile_name=profile_name,
            **kwargs
        )
