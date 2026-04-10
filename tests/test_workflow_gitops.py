"""Tests for Workflow GitOps Module.

Comprehensive tests for GitOps workflow management including
Git repository sync, branch management, pull request workflow,
review process, rollback, environment promotion, secrets management,
Git hooks, CI/CD integration, and drift detection.
"""

import unittest
import sys
import json
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')

from workflow_gitops import (
    GitOpsManager, GitOpsConfig, Environment, PullRequestStatus,
    PullRequest, ReviewStatus, Review, EnvironmentPromotion,
    GitHook, SecretFile, DriftStatus, DriftReport, CICDJob
)


class TestGitOpsConfig(unittest.TestCase):
    """Tests for GitOpsConfig dataclass."""

    def test_config_creation_defaults(self):
        """Test GitOpsConfig creation with defaults."""
        config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.assertEqual(config.repo_url, "https://github.com/test/repo")
        self.assertEqual(config.default_branch, "main")
        self.assertEqual(config.workflows_path, "workflows")
        self.assertEqual(config.secrets_path, ".secrets")
        self.assertEqual(config.required_reviewers, 1)
        self.assertFalse(config.require_signed_commits)
        self.assertEqual(config.environments, ["development", "staging", "production"])
        self.assertEqual(config.protect_branches, ["main", "production"])

    def test_config_creation_custom(self):
        """Test GitOpsConfig creation with custom values."""
        config = GitOpsConfig(
            repo_url="https://github.com/test/repo",
            default_branch="develop",
            workflows_path="custom/workflows",
            secrets_path=".custom-secrets",
            environments=["dev", "staging", "prod"],
            required_reviewers=2,
            require_signed_commits=True,
            protect_branches=["main", "master", "production"]
        )
        self.assertEqual(config.default_branch, "develop")
        self.assertEqual(config.workflows_path, "custom/workflows")
        self.assertEqual(config.required_reviewers, 2)
        self.assertTrue(config.require_signed_commits)


class TestEnums(unittest.TestCase):
    """Tests for GitOps enums."""

    def test_environment_enum(self):
        """Test Environment enum values."""
        self.assertEqual(Environment.DEVELOPMENT.value, "development")
        self.assertEqual(Environment.STAGING.value, "staging")
        self.assertEqual(Environment.PRODUCTION.value, "production")

    def test_pull_request_status_enum(self):
        """Test PullRequestStatus enum values."""
        self.assertEqual(PullRequestStatus.OPEN.value, "open")
        self.assertEqual(PullRequestStatus.MERGED.value, "merged")
        self.assertEqual(PullRequestStatus.CLOSED.value, "closed")
        self.assertEqual(PullRequestStatus.DRAFT.value, "draft")

    def test_review_status_enum(self):
        """Test ReviewStatus enum values."""
        self.assertEqual(ReviewStatus.PENDING.value, "pending")
        self.assertEqual(ReviewStatus.APPROVED.value, "approved")
        self.assertEqual(ReviewStatus.CHANGES_REQUESTED.value, "changes_requested")
        self.assertEqual(ReviewStatus.COMMENTED.value, "commented")

    def test_drift_status_enum(self):
        """Test DriftStatus enum values."""
        self.assertEqual(DriftStatus.IN_SYNC.value, "in_sync")
        self.assertEqual(DriftStatus.DRIFTED.value, "drifted")
        self.assertEqual(DriftStatus.UNKNOWN.value, "unknown")


class TestPullRequest(unittest.TestCase):
    """Tests for PullRequest dataclass."""

    def test_pull_request_creation(self):
        """Test PullRequest creation."""
        pr = PullRequest(
            id="test-123",
            title="Test PR",
            description="Test description",
            source_branch="feature",
            target_branch="main",
            author="testuser",
            status=PullRequestStatus.OPEN,
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00",
            workflow_ids=["wf-1", "wf-2"]
        )
        self.assertEqual(pr.id, "test-123")
        self.assertEqual(pr.title, "Test PR")
        self.assertEqual(pr.status, PullRequestStatus.OPEN)
        self.assertEqual(len(pr.workflow_ids), 2)

    def test_pull_request_to_dict(self):
        """Test PullRequest to_dict conversion."""
        pr = PullRequest(
            id="test-123",
            title="Test PR",
            description="Test description",
            source_branch="feature",
            target_branch="main",
            author="testuser",
            status=PullRequestStatus.OPEN,
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00"
        )
        data = pr.to_dict()
        self.assertEqual(data['id'], "test-123")
        self.assertEqual(data['status'], "open")

    def test_pull_request_from_dict(self):
        """Test PullRequest from_dict creation."""
        data = {
            'id': 'test-456',
            'title': 'Test PR 2',
            'description': 'Description',
            'source_branch': 'feature',
            'target_branch': 'main',
            'author': 'user',
            'status': 'merged',
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-02T00:00:00'
        }
        pr = PullRequest.from_dict(data)
        self.assertEqual(pr.id, "test-456")
        self.assertEqual(pr.status, PullRequestStatus.MERGED)


class TestReview(unittest.TestCase):
    """Tests for Review dataclass."""

    def test_review_creation(self):
        """Test Review creation."""
        review = Review(
            id="rev-123",
            pull_request_id="pr-456",
            reviewer="reviewer1",
            status=ReviewStatus.APPROVED,
            comment="LGTM",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00"
        )
        self.assertEqual(review.id, "rev-123")
        self.assertEqual(review.pull_request_id, "pr-456")
        self.assertEqual(review.status, ReviewStatus.APPROVED)

    def test_review_to_dict(self):
        """Test Review to_dict conversion."""
        review = Review(
            id="rev-123",
            pull_request_id="pr-456",
            reviewer="reviewer1",
            status=ReviewStatus.CHANGES_REQUESTED,
            comment="Needs work",
            created_at="2024-01-01T00:00:00",
            updated_at="2024-01-01T00:00:00"
        )
        data = review.to_dict()
        self.assertEqual(data['status'], "changes_requested")

    def test_review_from_dict(self):
        """Test Review from_dict creation."""
        data = {
            'id': 'rev-789',
            'pull_request_id': 'pr-123',
            'reviewer': 'user2',
            'status': 'commented',
            'comment': 'Nice work',
            'created_at': '2024-01-01T00:00:00',
            'updated_at': '2024-01-01T00:00:00'
        }
        review = Review.from_dict(data)
        self.assertEqual(review.status, ReviewStatus.COMMENTED)


class TestEnvironmentPromotion(unittest.TestCase):
    """Tests for EnvironmentPromotion dataclass."""

    def test_promotion_creation(self):
        """Test EnvironmentPromotion creation."""
        promo = EnvironmentPromotion(
            id="promo-123",
            workflow_id="wf-1",
            from_environment="development",
            to_environment="staging",
            commit_hash="abc123",
            promoted_by="user",
            promoted_at="2024-01-01T00:00:00",
            status="completed"
        )
        self.assertEqual(promo.id, "promo-123")
        self.assertEqual(promo.from_environment, "development")
        self.assertEqual(promo.to_environment, "staging")
        self.assertTrue(promo.rollback_available)

    def test_promotion_to_dict(self):
        """Test EnvironmentPromotion to_dict conversion."""
        promo = EnvironmentPromotion(
            id="promo-123",
            workflow_id="wf-1",
            from_environment="staging",
            to_environment="production",
            commit_hash="def456",
            promoted_by="admin",
            promoted_at="2024-01-01T00:00:00",
            status="completed"
        )
        data = promo.to_dict()
        self.assertEqual(data['workflow_id'], "wf-1")

    def test_promotion_from_dict(self):
        """Test EnvironmentPromotion from_dict creation."""
        data = {
            'id': 'promo-456',
            'workflow_id': 'wf-2',
            'from_environment': 'dev',
            'to_environment': 'staging',
            'commit_hash': 'xyz789',
            'promoted_by': 'user',
            'promoted_at': '2024-01-01T00:00:00',
            'status': 'completed',
            'rollback_available': True
        }
        promo = EnvironmentPromotion.from_dict(data)
        self.assertEqual(promo.workflow_id, "wf-2")


class TestGitHook(unittest.TestCase):
    """Tests for GitHook dataclass."""

    def test_git_hook_creation(self):
        """Test GitHook creation."""
        hook = GitHook(
            name="pre-commit-hook",
            hook_type="pre-commit",
            script_content="#!/bin/bash\necho 'test'",
            enabled=True
        )
        self.assertEqual(hook.name, "pre-commit-hook")
        self.assertEqual(hook.hook_type, "pre-commit")
        self.assertTrue(hook.enabled)

    def test_git_hook_to_dict(self):
        """Test GitHook to_dict conversion."""
        hook = GitHook(
            name="pre-push-hook",
            hook_type="pre-push",
            script_content="#!/bin/bash\necho 'push'",
            enabled=False
        )
        data = hook.to_dict()
        self.assertEqual(data['hook_type'], "pre-push")
        self.assertFalse(data['enabled'])

    def test_git_hook_from_dict(self):
        """Test GitHook from_dict creation."""
        data = {
            'name': 'custom-hook',
            'hook_type': 'pre-rebase',
            'script_content': '#!/bin/bash',
            'enabled': True
        }
        hook = GitHook.from_dict(data)
        self.assertEqual(hook.hook_type, "pre-rebase")


class TestSecretFile(unittest.TestCase):
    """Tests for SecretFile dataclass."""

    def test_secret_file_creation(self):
        """Test SecretFile creation."""
        secret = SecretFile(
            id="secret-123",
            name="api-keys",
            encrypted_path="/path/to/enc/file.enc",
            original_path=".secrets/api-keys",
            last_modified="2024-01-01T00:00:00"
        )
        self.assertEqual(secret.name, "api-keys")
        self.assertTrue(secret.encrypted)

    def test_secret_file_to_dict(self):
        """Test SecretFile to_dict conversion."""
        secret = SecretFile(
            id="secret-456",
            name="db-creds",
            encrypted_path="/path/to/db.enc",
            original_path=".secrets/db-creds",
            last_modified="2024-01-01T00:00:00"
        )
        data = secret.to_dict()
        self.assertEqual(data['name'], "db-creds")
        self.assertTrue(data['encrypted'])


class TestDriftReport(unittest.TestCase):
    """Tests for DriftReport dataclass."""

    def test_drift_report_creation(self):
        """Test DriftReport creation."""
        report = DriftReport(
            workflow_id="wf-1",
            environment="production",
            status=DriftStatus.IN_SYNC,
            checked_at="2024-01-01T00:00:00",
            git_commit_hash="abc123",
            current_commit_hash="abc123"
        )
        self.assertEqual(report.workflow_id, "wf-1")
        self.assertEqual(report.status, DriftStatus.IN_SYNC)

    def test_drift_report_to_dict(self):
        """Test DriftReport to_dict conversion."""
        report = DriftReport(
            workflow_id="wf-2",
            environment="staging",
            status=DriftStatus.DRIFTED,
            checked_at="2024-01-01T00:00:00",
            git_commit_hash="old123",
            current_commit_hash="new456",
            differences={"field": "changed"}
        )
        data = report.to_dict()
        self.assertEqual(data['status'], "drifted")
        self.assertIn('field', data['differences'])


class TestCICDJob(unittest.TestCase):
    """Tests for CICDJob dataclass."""

    def test_cicd_job_creation(self):
        """Test CICDJob creation."""
        job = CICDJob(
            id="job-123",
            workflow_id="wf-1",
            environment="production",
            status="success",
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:05:00"
        )
        self.assertEqual(job.workflow_id, "wf-1")
        self.assertEqual(job.status, "success")

    def test_cicd_job_to_dict(self):
        """Test CICDJob to_dict conversion."""
        job = CICDJob(
            id="job-456",
            workflow_id="wf-2",
            environment="staging",
            status="failure",
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:03:00",
            logs="Error: build failed"
        )
        data = job.to_dict()
        self.assertEqual(data['status'], "failure")
        self.assertIn("Error", data['logs'])


class TestGitOpsManagerInit(unittest.TestCase):
    """Tests for GitOpsManager initialization."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_manager_init_with_custom_workdir(self):
        """Test GitOpsManager initialization with custom workdir."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        self.assertEqual(manager.config, self.config)
        self.assertEqual(str(manager.workdir), self.workdir)

    def test_manager_init_default_workdir(self):
        """Test GitOpsManager initialization with default workdir."""
        manager = GitOpsManager(self.config)
        self.assertIsNotNone(manager.workdir)

    def test_manager_directories_created(self):
        """Test that all required directories are created."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()
        self.assertTrue(os.path.exists(manager.git_dir))
        self.assertTrue(os.path.exists(manager.workflows_dir))


class TestGitOpsManagerRepositorySync(unittest.TestCase):
    """Tests for GitOpsManager repository sync operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_sync_from_git_empty(self):
        """Test sync_from_git with no workflows."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()
        result = manager.sync_from_git(branch="main")

        self.assertEqual(result['branch'], "main")
        self.assertEqual(result['synced_count'], 0)
        self.assertIn('synced_at', result)

    def test_sync_to_git(self):
        """Test sync_to_git method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        workflow_data = {"id": "wf-1", "name": "Test Workflow"}
        commit_hash = manager.sync_to_git("wf-1", workflow_data, branch="main")

        self.assertIsNotNone(commit_hash)
        self.assertEqual(len(commit_hash), 12)

        # Verify file was created
        workflow_file = manager.workflows_dir / "wf-1.json"
        self.assertTrue(workflow_file.exists())

    def test_clone_repo(self):
        """Test clone_repo method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.clone_repo("https://github.com/test/repo")

        self.assertTrue(result['success'])
        self.assertEqual(result['repo_url'], "https://github.com/test/repo")
        self.assertIn('cloned_at', result)

    def test_push_changes(self):
        """Test push_changes method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.push_changes(branch="main")

        self.assertTrue(result['success'])
        self.assertEqual(result['branch'], "main")
        self.assertIn('pushed_at', result)

    def test_pull_changes(self):
        """Test pull_changes method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.pull_changes(branch="main")

        self.assertTrue(result['success'])
        self.assertEqual(result['branch'], "main")


class TestGitOpsManagerBranchManagement(unittest.TestCase):
    """Tests for GitOpsManager branch management operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_create_branch(self):
        """Test create_branch method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.create_branch("feature-1", source_branch="main")

        self.assertEqual(result['name'], "feature-1")
        self.assertEqual(result['source'], "main")
        self.assertFalse(result['protected'])

    def test_create_branch_protected(self):
        """Test create_branch with protected branch."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.create_branch("main", source_branch="develop")

        self.assertTrue(result['protected'])

    def test_switch_branch(self):
        """Test switch_branch method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.switch_branch("feature-1")

        self.assertTrue(result['success'])
        self.assertEqual(result['branch'], "feature-1")

    def test_delete_branch_success(self):
        """Test delete_branch successful deletion."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.delete_branch("feature-1")

        self.assertTrue(result['deleted'])

    def test_delete_branch_protected(self):
        """Test delete_branch protected branch."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.delete_branch("main")

        self.assertFalse(result['deleted'])
        self.assertIn('error', result)

    def test_list_branches(self):
        """Test list_branches method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        branches = manager.list_branches()

        self.assertIsInstance(branches, list)
        self.assertEqual(len(branches), 1)
        self.assertEqual(branches[0]['name'], "main")
        self.assertTrue(branches[0]['protected'])

    def test_merge_branch(self):
        """Test merge_branch method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.merge_branch("feature-1", target_branch="main")

        self.assertTrue(result['success'])
        self.assertEqual(result['source'], "feature-1")
        self.assertEqual(result['target'], "main")


class TestGitOpsManagerPullRequest(unittest.TestCase):
    """Tests for GitOpsManager pull request operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_create_pull_request(self):
        """Test create_pull_request method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request(
            title="Test PR",
            description="Test description",
            source_branch="feature",
            target_branch="main",
            author="testuser"
        )

        self.assertIsInstance(pr, PullRequest)
        self.assertEqual(pr.title, "Test PR")
        self.assertEqual(pr.status, PullRequestStatus.OPEN)

    def test_get_pull_request(self):
        """Test get_pull_request method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request(
            title="Test PR",
            description="Test description",
            source_branch="feature"
        )

        retrieved = manager.get_pull_request(pr.id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.id, pr.id)

    def test_get_pull_request_not_found(self):
        """Test get_pull_request with non-existent ID."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        retrieved = manager.get_pull_request("non-existent")
        self.assertIsNone(retrieved)

    def test_list_pull_requests(self):
        """Test list_pull_requests method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        manager.create_pull_request("PR 1", "Desc 1", "feature-1")
        manager.create_pull_request("PR 2", "Desc 2", "feature-2")

        prs = manager.list_pull_requests()
        self.assertEqual(len(prs), 2)

    def test_list_pull_requests_filtered_by_status(self):
        """Test list_pull_requests with status filter."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr1 = manager.create_pull_request("PR 1", "Desc 1", "feature-1")
        manager.create_pull_request("PR 2", "Desc 2", "feature-2")

        manager.merge_pull_request(pr1.id)

        open_prs = manager.list_pull_requests(status=PullRequestStatus.OPEN)
        merged_prs = manager.list_pull_requests(status=PullRequestStatus.MERGED)

        self.assertEqual(len(open_prs), 1)
        self.assertEqual(len(merged_prs), 1)

    def test_merge_pull_request(self):
        """Test merge_pull_request method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Test PR", "Description", "feature")

        result = manager.merge_pull_request(pr.id)

        self.assertTrue(result['success'])
        self.assertEqual(result['pr_id'], pr.id)

    def test_close_pull_request(self):
        """Test close_pull_request method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Test PR", "Description", "feature")

        result = manager.close_pull_request(pr.id)

        self.assertTrue(result['success'])

    def test_update_pull_request(self):
        """Test update_pull_request method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Original Title", "Description", "feature")

        updated = manager.update_pull_request(pr.id, title="Updated Title")

        self.assertIsNotNone(updated)
        self.assertEqual(updated.title, "Updated Title")


class TestGitOpsManagerReview(unittest.TestCase):
    """Tests for GitOpsManager review operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_submit_review(self):
        """Test submit_review method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Test PR", "Description", "feature")

        review = manager.submit_review(
            pull_request_id=pr.id,
            reviewer="reviewer1",
            status=ReviewStatus.APPROVED,
            comment="LGTM"
        )

        self.assertIsInstance(review, Review)
        self.assertEqual(review.status, ReviewStatus.APPROVED)

    def test_get_review(self):
        """Test get_review method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Test PR", "Description", "feature")
        review = manager.submit_review(
            pull_request_id=pr.id,
            reviewer="reviewer1",
            status=ReviewStatus.APPROVED
        )

        retrieved = manager.get_review(review.id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.id, review.id)

    def test_list_reviews(self):
        """Test list_reviews method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Test PR", "Description", "feature")
        manager.submit_review(pr.id, "reviewer1", ReviewStatus.APPROVED)
        manager.submit_review(pr.id, "reviewer2", ReviewStatus.COMMENTED)

        reviews = manager.list_reviews(pull_request_id=pr.id)
        self.assertEqual(len(reviews), 2)

    def test_require_reviewers(self):
        """Test require_reviewers method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        pr = manager.create_pull_request("Test PR", "Description", "feature")

        self.assertFalse(manager.require_reviewers(pr.id, required_count=1))

        manager.submit_review(pr.id, "reviewer1", ReviewStatus.APPROVED)

        self.assertTrue(manager.require_reviewers(pr.id, required_count=1))


class TestGitOpsManagerRollback(unittest.TestCase):
    """Tests for GitOpsManager rollback operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_rollback(self):
        """Test rollback method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.rollback("wf-1", target_commit="abc123")

        self.assertTrue(result['success'])
        self.assertEqual(result['workflow_id'], "wf-1")
        self.assertEqual(result['target_commit'], "abc123")

    def test_rollback_environment(self):
        """Test rollback_environment method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.rollback_environment("production", workflow_id="wf-1")

        self.assertTrue(result['success'])
        self.assertEqual(result['environment'], "production")

    def test_list_rollback_points(self):
        """Test list_rollback_points method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        rollback_points = manager.list_rollback_points()
        self.assertIsInstance(rollback_points, list)


class TestGitOpsManagerPromotion(unittest.TestCase):
    """Tests for GitOpsManager environment promotion operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_promote_workflow(self):
        """Test promote_workflow method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        # Create a workflow file first
        workflow_data = {"id": "wf-1", "name": "Test"}
        manager.sync_to_git("wf-1", workflow_data)

        promo = manager.promote_workflow("wf-1", "development", "staging")

        self.assertIsInstance(promo, EnvironmentPromotion)
        self.assertEqual(promo.from_environment, "development")
        self.assertEqual(promo.to_environment, "staging")

    def test_list_promotions(self):
        """Test list_promotions method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        # Create workflows
        manager.sync_to_git("wf-1", {"id": "wf-1"})
        manager.sync_to_git("wf-2", {"id": "wf-2"})

        manager.promote_workflow("wf-1", "development", "staging")
        manager.promote_workflow("wf-1", "staging", "production")

        promos = manager.list_promotions(workflow_id="wf-1")
        self.assertEqual(len(promos), 2)

    def test_get_environment_status(self):
        """Test get_environment_status method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        status = manager.get_environment_status("production")

        self.assertEqual(status['environment'], "production")
        self.assertIn('workflow_count', status)


class TestGitOpsManagerSecrets(unittest.TestCase):
    """Tests for GitOpsManager secrets management operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_encrypt_secret(self):
        """Test encrypt_secret method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        secret = manager.encrypt_secret("api-keys", {"key": "value"})

        self.assertIsInstance(secret, SecretFile)
        self.assertEqual(secret.name, "api-keys")
        self.assertTrue(secret.encrypted)

    def test_decrypt_secret(self):
        """Test decrypt_secret method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        secret = manager.encrypt_secret("test-secret", {"api_key": "secret123"})

        decrypted = manager.decrypt_secret(secret.id)
        self.assertIsNotNone(decrypted)
        self.assertEqual(decrypted['api_key'], "secret123")

    def test_list_secrets(self):
        """Test list_secrets method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        manager.encrypt_secret("secret-1", {"key": "value1"})
        manager.encrypt_secret("secret-2", {"key": "value2"})

        secrets = manager.list_secrets()
        self.assertEqual(len(secrets), 2)

    def test_init_git_crypt(self):
        """Test init_git_crypt method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.init_git_crypt()

        self.assertTrue(result['initialized'])
        self.assertIn('key_path', result)

    def test_lock_secrets(self):
        """Test lock_secrets method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.lock_secrets()

        self.assertTrue(result['locked'])

    def test_unlock_secrets(self):
        """Test unlock_secrets method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        result = manager.unlock_secrets()

        self.assertTrue(result['unlocked'])


class TestGitOpsManagerHooks(unittest.TestCase):
    """Tests for GitOpsManager Git hooks operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_create_hook(self):
        """Test create_hook method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        hook = manager.create_hook(
            hook_type="pre-commit",
            script_content="#!/bin/bash\necho 'test'",
            name="test-hook"
        )

        self.assertIsInstance(hook, GitHook)
        self.assertEqual(hook.hook_type, "pre-commit")

    def test_get_hook(self):
        """Test get_hook method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        created = manager.create_hook("pre-commit", "echo 'test'", name="my-hook")
        retrieved = manager.get_hook("my-hook")

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "my-hook")

    def test_list_hooks(self):
        """Test list_hooks method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        manager.create_hook("pre-commit", "echo '1'", name="hook-1")
        manager.create_hook("pre-push", "echo '2'", name="hook-2")

        hooks = manager.list_hooks()
        self.assertEqual(len(hooks), 2)

    def test_delete_hook(self):
        """Test delete_hook method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        created = manager.create_hook("pre-commit", "echo 'test'", name="delete-me")
        result = manager.delete_hook("delete-me")

        self.assertTrue(result)

    def test_install_pre_commit_hook(self):
        """Test install_pre_commit_hook method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        hook = manager.install_pre_commit_hook()

        self.assertIsInstance(hook, GitHook)
        self.assertEqual(hook.hook_type, "pre-commit")

    def test_install_pre_push_hook(self):
        """Test install_pre_push_hook method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        hook = manager.install_pre_push_hook()

        self.assertIsInstance(hook, GitHook)
        self.assertEqual(hook.hook_type, "pre-push")


class TestGitOpsManagerDriftDetection(unittest.TestCase):
    """Tests for GitOpsManager drift detection operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = GitOpsConfig(repo_url="https://github.com/test/repo")
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir)

    def test_detect_drift(self):
        """Test detect_drift method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        # Create a workflow first
        manager.sync_to_git("wf-1", {"id": "wf-1", "name": "Test"})

        result = manager.detect_drift("wf-1", "production")

        self.assertIsInstance(result, DriftReport)
        self.assertEqual(result.workflow_id, "wf-1")
        self.assertEqual(result.environment, "production")

    def test_get_drift_report(self):
        """Test get_drift_report method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        # Create and detect drift
        manager.sync_to_git("wf-1", {"id": "wf-1"})
        manager.detect_drift("wf-1", "production")

        report = manager.get_drift_report("wf-1", "production")
        self.assertIsNotNone(report)
        self.assertEqual(report.workflow_id, "wf-1")

    def test_list_drift_reports(self):
        """Test list_drift_reports method."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        # Create workflows and detect drift
        manager.sync_to_git("wf-1", {"id": "wf-1"})
        manager.detect_drift("wf-1", "production")

        reports = manager.list_drift_reports()
        self.assertEqual(len(reports), 1)
        self.assertIsInstance(reports[0], DriftReport)

    def test_list_drift_reports_filtered(self):
        """Test list_drift_reports with status filter."""
        manager = GitOpsManager(self.config, workdir=self.workdir)
        manager._ensure_initialized()

        manager.sync_to_git("wf-1", {"id": "wf-1"})
        manager.detect_drift("wf-1", "production")

        reports = manager.list_drift_reports(status=DriftStatus.IN_SYNC)
        self.assertEqual(len(reports), 1)


if __name__ == '__main__':
    unittest.main()
