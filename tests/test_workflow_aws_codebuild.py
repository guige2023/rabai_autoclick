"""
Tests for workflow_aws_codebuild module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types

# Create mock boto3 module before importing workflow_aws_codebuild
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

# Create mock botocore config
mock_boto3_config = types.ModuleType('botocore.config')
mock_boto3_config.Config = MagicMock()

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions
sys.modules['botocore.config'] = mock_boto3_config

# Now we can import the module
from src.workflow_aws_codebuild import (
    CodeBuildIntegration,
    CodeBuildConfig,
    SourceConfig,
    GitHubSourceConfig,
    CodeCommitSourceConfig,
    S3SourceConfig,
    EnvironmentConfig,
    ArtifactsConfig,
    CacheConfig,
    LogsConfig,
    WebhookConfig,
    ReportGroupConfig,
    ProjectConfig,
    ProjectInfo,
    BuildInfo,
    ReportGroupInfo,
    SourceType,
    SourceAuthType,
    EnvironmentType,
    ComputeType,
    CacheType,
    LocalCacheMode,
    BuildStatus,
    ReportType,
    ReportStatus,
    ArtifactType,
    EncryptionKeyType,
    WebhookFilterType,
    WebhookBuildType,
    BOTO3_AVAILABLE,
)


class TestSourceType(unittest.TestCase):
    """Test SourceType enum"""

    def test_source_type_values(self):
        self.assertEqual(SourceType.CODECOMMIT.value, "CODECOMMIT")
        self.assertEqual(SourceType.CODEPIPELINE.value, "CODEPIPELINE")
        self.assertEqual(SourceType.GITHUB.value, "GITHUB")
        self.assertEqual(SourceType.GITHUB_ENTERPRISE.value, "GITHUB_ENTERPRISE")
        self.assertEqual(SourceType.BITBUCKET.value, "BITBUCKET")
        self.assertEqual(SourceType.S3.value, "S3")
        self.assertEqual(SourceType.NO_SOURCE.value, "NO_SOURCE")

    def test_source_type_is_string(self):
        self.assertIsInstance(SourceType.GITHUB.value, str)


class TestEnvironmentType(unittest.TestCase):
    """Test EnvironmentType enum"""

    def test_environment_type_values(self):
        self.assertEqual(EnvironmentType.LINUX_CONTAINER.value, "LINUX_CONTAINER")
        self.assertEqual(EnvironmentType.LINUX_GPU_CONTAINER.value, "LINUX_GPU_CONTAINER")
        self.assertEqual(EnvironmentType.ARM_CONTAINER.value, "ARM_CONTAINER")
        self.assertEqual(EnvironmentType.WINDOWS_CONTAINER.value, "WINDOWS_CONTAINER")
        self.assertEqual(EnvironmentType.WINDOWS_SERVER_2019_CONTAINER.value, "WINDOWS_SERVER_2019_CONTAINER")
        self.assertEqual(EnvironmentType.LINUX_EC2.value, "LINUX_EC2")
        self.assertEqual(EnvironmentType.ARM_EC2.value, "ARM_EC2")
        self.assertEqual(EnvironmentType.WINDOWS_EC2.value, "WINDOWS_EC2")


class TestComputeType(unittest.TestCase):
    """Test ComputeType enum"""

    def test_compute_type_values(self):
        self.assertEqual(ComputeType.BUILD_GENERAL_SMALL.value, "BUILD_GENERAL_SMALL")
        self.assertEqual(ComputeType.BUILD_GENERAL_MEDIUM.value, "BUILD_GENERAL_MEDIUM")
        self.assertEqual(ComputeType.BUILD_GENERAL_LARGE.value, "BUILD_GENERAL_LARGE")
        self.assertEqual(ComputeType.BUILD_GENERAL_XLARGE.value, "BUILD_GENERAL_XLARGE")
        self.assertEqual(ComputeType.BUILD_GENERAL_2XLARGE.value, "BUILD_GENERAL_2XLARGE")


class TestCacheType(unittest.TestCase):
    """Test CacheType enum"""

    def test_cache_type_values(self):
        self.assertEqual(CacheType.NO_CACHE.value, "NO_CACHE")
        self.assertEqual(CacheType.S3.value, "S3")
        self.assertEqual(CacheType.LOCAL.value, "LOCAL")


class TestBuildStatus(unittest.TestCase):
    """Test BuildStatus enum"""

    def test_build_status_values(self):
        self.assertEqual(BuildStatus.SUCCEEDED.value, "SUCCEEDED")
        self.assertEqual(BuildStatus.FAILED.value, "FAILED")
        self.assertEqual(BuildStatus.FAULT.value, "FAULT")
        self.assertEqual(BuildStatus.TIMED_OUT.value, "TIMED_OUT")
        self.assertEqual(BuildStatus.IN_PROGRESS.value, "IN_PROGRESS")
        self.assertEqual(BuildStatus.STOPPED.value, "STOPPED")


class TestReportType(unittest.TestCase):
    """Test ReportType enum"""

    def test_report_type_values(self):
        self.assertEqual(ReportType.TEST.value, "TEST")
        self.assertEqual(ReportType.CODE_COVERAGE.value, "CODE_COVERAGE")


class TestArtifactType(unittest.TestCase):
    """Test ArtifactType enum"""

    def test_artifact_type_values(self):
        self.assertEqual(ArtifactType.CODEPIPELINE.value, "CODEPIPELINE")
        self.assertEqual(ArtifactType.S3.value, "S3")
        self.assertEqual(ArtifactType.NO_ARTIFACTS.value, "NO_ARTIFACTS")


class TestWebhookFilterType(unittest.TestCase):
    """Test WebhookFilterType enum"""

    def test_webhook_filter_type_values(self):
        self.assertEqual(WebhookFilterType.EVENT.value, "EVENT")
        self.assertEqual(WebhookFilterType.BASE_REF.value, "BASE_REF")
        self.assertEqual(WebhookFilterType.HEAD_REF.value, "HEAD_REF")
        self.assertEqual(WebhookFilterType.ACTOR_ACCOUNT_ID.value, "ACTOR_ACCOUNT_ID")
        self.assertEqual(WebhookFilterType.FILE_PATH.value, "FILE_PATH")
        self.assertEqual(WebhookFilterType.COMMIT_MESSAGE.value, "COMMIT_MESSAGE")


class TestCodeBuildConfig(unittest.TestCase):
    """Test CodeBuildConfig dataclass"""

    def test_codebuild_config_defaults(self):
        config = CodeBuildConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertIsNone(config.aws_session_token)
        self.assertIsNone(config.profile_name)
        self.assertIsNone(config.endpoint_url)
        self.assertTrue(config.verify_ssl)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)

    def test_codebuild_config_custom(self):
        config = CodeBuildConfig(
            region_name="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            profile_name="my-profile",
            endpoint_url="http://localhost:4566",
            verify_ssl=False,
            timeout=60,
            max_retries=5
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.profile_name, "my-profile")
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_retries, 5)


class TestSourceConfig(unittest.TestCase):
    """Test SourceConfig dataclass"""

    def test_source_config_defaults(self):
        config = SourceConfig(source_type=SourceType.GITHUB)
        self.assertEqual(config.source_type, SourceType.GITHUB)
        self.assertIsNone(config.location)
        self.assertIsNone(config.git_clone_depth)
        self.assertFalse(config.report_build_status)

    def test_source_config_custom(self):
        config = SourceConfig(
            source_type=SourceType.CODECOMMIT,
            location="https://git-codecommit.us-east-1.amazonaws.com/v1/repos/my-repo",
            git_clone_depth=1,
            report_build_status=True
        )
        self.assertEqual(config.source_type, SourceType.CODECOMMIT)
        self.assertEqual(config.git_clone_depth, 1)
        self.assertTrue(config.report_build_status)


class TestGitHubSourceConfig(unittest.TestCase):
    """Test GitHubSourceConfig dataclass"""

    def test_github_source_config(self):
        config = GitHubSourceConfig(
            owner="my-org",
            repo="my-repo",
            branch="main",
            oauth_token="gho_xxxxxxxxxxxx",
            webhook_secret="my-secret"
        )
        self.assertEqual(config.owner, "my-org")
        self.assertEqual(config.repo, "my-repo")
        self.assertEqual(config.branch, "main")
        self.assertEqual(config.oauth_token, "gho_xxxxxxxxxxxx")
        self.assertEqual(config.webhook_secret, "my-secret")


class TestCodeCommitSourceConfig(unittest.TestCase):
    """Test CodeCommitSourceConfig dataclass"""

    def test_codecommit_source_config(self):
        config = CodeCommitSourceConfig(
            repository="my-repo",
            branch="main"
        )
        self.assertEqual(config.repository, "my-repo")
        self.assertEqual(config.branch, "main")


class TestS3SourceConfig(unittest.TestCase):
    """Test S3SourceConfig dataclass"""

    def test_s3_source_config(self):
        config = S3SourceConfig(
            bucket="my-bucket",
            path="path/to/source"
        )
        self.assertEqual(config.bucket, "my-bucket")
        self.assertEqual(config.path, "path/to/source")


class TestEnvironmentConfig(unittest.TestCase):
    """Test EnvironmentConfig dataclass"""

    def test_environment_config_defaults(self):
        config = EnvironmentConfig()
        self.assertEqual(config.environment_type, EnvironmentType.LINUX_CONTAINER)
        self.assertEqual(config.image, "aws/codebuild/standard:5.0")
        self.assertEqual(config.compute_type, ComputeType.BUILD_GENERAL_SMALL)
        self.assertFalse(config.privileged_mode)
        self.assertEqual(config.environment_variables, {})
        self.assertIsNone(config.registry_credential)

    def test_environment_config_custom(self):
        config = EnvironmentConfig(
            environment_type=EnvironmentType.ARM_CONTAINER,
            image="aws/codebuild/amazonlinux2-aarch64-standard:2.0",
            compute_type=ComputeType.BUILD_GENERAL_LARGE,
            privileged_mode=True,
            environment_variables={"ENV": "prod", "DEBUG": "false"}
        )
        self.assertEqual(config.environment_type, EnvironmentType.ARM_CONTAINER)
        self.assertEqual(config.compute_type, ComputeType.BUILD_GENERAL_LARGE)
        self.assertEqual(config.environment_variables["ENV"], "prod")


class TestArtifactsConfig(unittest.TestCase):
    """Test ArtifactsConfig dataclass"""

    def test_artifacts_config_defaults(self):
        config = ArtifactsConfig()
        self.assertEqual(config.artifacts_type, ArtifactType.NO_ARTIFACTS)
        self.assertIsNone(config.location)
        self.assertEqual(config.namespace_type, "BUILD_ID")
        self.assertEqual(config.packaging, "NONE")
        self.assertFalse(config.encryption_disabled)

    def test_artifacts_config_s3(self):
        config = ArtifactsConfig(
            artifacts_type=ArtifactType.S3,
            location="my-bucket/artifacts",
            name="my-artifacts",
            packaging="ZIP"
        )
        self.assertEqual(config.artifacts_type, ArtifactType.S3)
        self.assertEqual(config.location, "my-bucket/artifacts")
        self.assertEqual(config.packaging, "ZIP")


class TestCacheConfig(unittest.TestCase):
    """Test CacheConfig dataclass"""

    def test_cache_config_defaults(self):
        config = CacheConfig()
        self.assertEqual(config.cache_type, CacheType.NO_CACHE)
        self.assertIsNone(config.location)
        self.assertEqual(config.modes, [])

    def test_cache_config_s3(self):
        config = CacheConfig(
            cache_type=CacheType.S3,
            location="my-bucket/cache"
        )
        self.assertEqual(config.cache_type, CacheType.S3)
        self.assertEqual(config.location, "my-bucket/cache")


class TestLogsConfig(unittest.TestCase):
    """Test LogsConfig dataclass"""

    def test_logs_config_defaults(self):
        config = LogsConfig()
        self.assertTrue(config.cloud_watch_logs_enabled)
        self.assertIsNone(config.cloud_watch_logs_group_name)
        self.assertEqual(config.cloud_watch_logs_status, "ENABLED")
        self.assertFalse(config.s3_logs_enabled)
        self.assertEqual(config.s3_logs_status, "DISABLED")

    def test_logs_config_custom(self):
        config = LogsConfig(
            cloud_watch_logs_enabled=True,
            cloud_watch_logs_group_name="/aws/codebuild/my-project",
            s3_logs_enabled=True,
            s3_logs_location="my-bucket/logs"
        )
        self.assertTrue(config.cloud_watch_logs_enabled)
        self.assertEqual(config.cloud_watch_logs_group_name, "/aws/codebuild/my-project")
        self.assertTrue(config.s3_logs_enabled)


class TestWebhookConfig(unittest.TestCase):
    """Test WebhookConfig dataclass"""

    def test_webhook_config_defaults(self):
        config = WebhookConfig()
        self.assertEqual(config.filter_groups, [])
        self.assertIsNone(config.webhook_secret)
        self.assertEqual(config.retry_limit, 2)

    def test_webhook_config_custom(self):
        config = WebhookConfig(
            filter_groups=[[{"filterType": "EVENT", "pattern": "PUSH"}]],
            webhook_secret="my-secret",
            retry_limit=3
        )
        self.assertEqual(len(config.filter_groups), 1)
        self.assertEqual(config.retry_limit, 3)


class TestReportGroupConfig(unittest.TestCase):
    """Test ReportGroupConfig dataclass"""

    def test_report_group_config(self):
        config = ReportGroupConfig(
            name="my-report-group",
            report_type=ReportType.TEST,
            tags={"env": "prod"},
            delete_reports=True
        )
        self.assertEqual(config.name, "my-report-group")
        self.assertEqual(config.report_type, ReportType.TEST)
        self.assertEqual(config.delete_reports, True)


class TestProjectConfig(unittest.TestCase):
    """Test ProjectConfig dataclass"""

    def test_project_config_defaults(self):
        config = ProjectConfig(name="my-project")
        self.assertEqual(config.name, "my-project")
        self.assertEqual(config.description, "")
        self.assertIsNone(config.source)
        self.assertIsNone(config.environment)
        self.assertEqual(config.timeout_in_minutes, 60)
        self.assertEqual(config.queued_timeout_in_minutes, 480)
        self.assertEqual(config.tags, {})
        self.assertFalse(config.badge_enabled)

    def test_project_config_full(self):
        config = ProjectConfig(
            name="my-project",
            description="My CodeBuild project",
            source=SourceConfig(source_type=SourceType.GITHUB),
            environment=EnvironmentConfig(),
            artifacts=ArtifactsConfig(artifacts_type=ArtifactType.S3),
            cache=CacheConfig(cache_type=CacheType.S3),
            logs=LogsConfig(),
            timeout_in_minutes=120,
            tags={"env": "prod"},
            badge_enabled=True
        )
        self.assertEqual(config.name, "my-project")
        self.assertEqual(config.timeout_in_minutes, 120)
        self.assertTrue(config.badge_enabled)


class TestProjectInfo(unittest.TestCase):
    """Test ProjectInfo dataclass"""

    def test_project_info(self):
        info = ProjectInfo(
            name="my-project",
            arn="arn:aws:codebuild:us-east-1:123456789012:project/my-project",
            description="Test project",
            source={},
            environment={},
            artifacts={},
            cache={},
            logs_config={},
            created="2024-01-01T00:00:00Z",
            last_modified="2024-01-01T00:00:00Z",
            tags={"env": "prod"}
        )
        self.assertEqual(info.name, "my-project")
        self.assertIn("codebuild", info.arn)


class TestBuildInfo(unittest.TestCase):
    """Test BuildInfo dataclass"""

    def test_build_info(self):
        info = BuildInfo(
            id="arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456",
            arn="arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456",
            project_name="my-project",
            build_number=1,
            build_status=BuildStatus.IN_PROGRESS,
            source_version="main"
        )
        self.assertEqual(info.project_name, "my-project")
        self.assertEqual(info.build_status, BuildStatus.IN_PROGRESS)


class TestCodeBuildIntegration(unittest.TestCase):
    """Test CodeBuildIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_codebuild_client = MagicMock()
        self.integration = CodeBuildIntegration(
            region_name="us-east-1",
            codebuild_client=self.mock_codebuild_client
        )

    def test_initialization(self):
        """Test integration initialization"""
        integration = CodeBuildIntegration()
        self.assertEqual(integration.region_name, "us-east-1")

    def test_initialization_with_custom_client(self):
        """Test initialization with custom client"""
        mock_client = MagicMock()
        integration = CodeBuildIntegration(codebuild_client=mock_client)
        self.assertEqual(integration.codebuild, mock_client)

    def test_codebuild_property(self):
        """Test codebuild property getter"""
        mock_client = MagicMock()
        integration = CodeBuildIntegration(codebuild_client=mock_client)
        self.assertEqual(integration.codebuild, mock_client)

    def test_create_project(self):
        """Test create_project method"""
        mock_response = {
            'project': {
                'name': 'my-project',
                'arn': 'arn:aws:codebuild:us-east-1:123456789012:project/my-project',
                'description': 'Test project',
                'source': {'type': 'GITHUB'},
                'environment': {'type': 'LINUX_CONTAINER', 'image': 'aws/codebuild/standard:5.0'},
                'artifacts': {'type': 'NO_ARTIFACTS'},
                'cache': {'type': 'NO_CACHE'},
                'logsConfig': {'cloudWatchLogs': {'status': 'ENABLED'}},
                'timeoutInMinutes': 60,
                'queuedTimeoutInMinutes': 480,
                'created': '2024-01-01T00:00:00Z',
                'lastModified': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_codebuild_client.create_project.return_value = mock_response

        config = ProjectConfig(
            name="my-project",
            description="Test project"
        )
        result = self.integration.create_project(config)

        self.assertEqual(result.name, "my-project")
        self.mock_codebuild_client.create_project.assert_called_once()

    def test_get_project(self):
        """Test get_project method"""
        mock_response = {
            'projects': [{
                'name': 'my-project',
                'arn': 'arn:aws:codebuild:us-east-1:123456789012:project/my-project',
                'description': 'Test project',
                'source': {'type': 'GITHUB'},
                'environment': {'type': 'LINUX_CONTAINER'},
                'artifacts': {'type': 'NO_ARTIFACTS'},
                'cache': {'type': 'NO_CACHE'},
                'logsConfig': {'cloudWatchLogs': {'status': 'ENABLED'}},
                'timeoutInMinutes': 60,
                'queuedTimeoutInMinutes': 480,
                'created': '2024-01-01T00:00:00Z',
                'lastModified': '2024-01-01T00:00:00Z'
            }]
        }
        self.mock_codebuild_client.batch_get_projects.return_value = mock_response

        result = self.integration.get_project("my-project")

        self.assertEqual(result.name, "my-project")
        self.mock_codebuild_client.batch_get_projects.assert_called_once()

    def test_list_projects(self):
        """Test list_projects method"""
        mock_response = {
            'projects': ['project-1', 'project-2', 'project-3']
        }
        self.mock_codebuild_client.list_projects.return_value = mock_response

        result = self.integration.list_projects()

        self.assertEqual(len(result), 3)
        self.assertIn('project-1', result)

    def test_delete_project(self):
        """Test delete_project method"""
        self.mock_codebuild_client.delete_project.return_value = {}

        result = self.integration.delete_project("my-project")

        self.assertTrue(result)
        self.mock_codebuild_client.delete_project.assert_called_once_with(name="my-project")

    def test_start_build(self):
        """Test start_build method"""
        mock_response = {
            'build': {
                'id': 'arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456',
                'arn': 'arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456',
                'projectName': 'my-project',
                'buildNumber': 1,
                'buildStatus': 'IN_PROGRESS',
                'currentPhase': 'SUBMITTED',
                'source': {'type': 'GITHUB'},
                'environment': {'type': 'LINUX_CONTAINER'},
                'phases': []
            }
        }
        self.mock_codebuild_client.start_build.return_value = mock_response

        result = self.integration.start_build(project_name="my-project", source_version="main")

        self.assertEqual(result.project_name, "my-project")
        self.assertEqual(result.build_status, "IN_PROGRESS")

    def test_stop_build(self):
        """Test stop_build method"""
        mock_response = {
            'build': {
                'id': 'arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456',
                'arn': 'arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456',
                'projectName': 'my-project',
                'buildNumber': 1,
                'buildStatus': 'STOPPED',
                'phases': []
            }
        }
        self.mock_codebuild_client.stop_build.return_value = mock_response

        result = self.integration.stop_build("arn:aws:codebuild:us-east-1:123456789012:build/my-project:123456")

        self.assertEqual(result.build_status, "STOPPED")

    def test_batch_get_projects(self):
        """Test batch_get_projects method"""
        mock_response = {
            'projects': [
                {
                    'name': 'project-1',
                    'arn': 'arn:aws:codebuild:us-east-1:123456789012:project/project-1',
                    'description': 'Project 1',
                    'source': {'type': 'GITHUB'},
                    'environment': {'type': 'LINUX_CONTAINER'},
                    'artifacts': {'type': 'NO_ARTIFACTS'},
                    'cache': {'type': 'NO_CACHE'},
                    'logsConfig': {'cloudWatchLogs': {'status': 'ENABLED'}},
                    'timeoutInMinutes': 60,
                    'queuedTimeoutInMinutes': 480,
                    'created': '2024-01-01T00:00:00Z',
                    'lastModified': '2024-01-01T00:00:00Z'
                },
                {
                    'name': 'project-2',
                    'arn': 'arn:aws:codebuild:us-east-1:123456789012:project/project-2',
                    'description': 'Project 2',
                    'source': {'type': 'CODECOMMIT'},
                    'environment': {'type': 'LINUX_CONTAINER'},
                    'artifacts': {'type': 'S3'},
                    'cache': {'type': 'S3'},
                    'logsConfig': {'cloudWatchLogs': {'status': 'ENABLED'}},
                    'timeoutInMinutes': 120,
                    'queuedTimeoutInMinutes': 480,
                    'created': '2024-01-02T00:00:00Z',
                    'lastModified': '2024-01-02T00:00:00Z'
                }
            ]
        }
        self.mock_codebuild_client.batch_get_projects.return_value = mock_response

        result = self.integration.batch_get_projects(['project-1', 'project-2'])

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, 'project-1')
        self.assertEqual(result[1].name, 'project-2')


class TestCodeBuildIntegrationBuilds(unittest.TestCase):
    """Test CodeBuildIntegration builds-related methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_codebuild_client = MagicMock()
        self.integration = CodeBuildIntegration(
            region_name="us-east-1",
            codebuild_client=self.mock_codebuild_client
        )

    def test_list_builds(self):
        """Test list_builds method"""
        mock_response = {
            'ids': [
                'arn:aws:codebuild:us-east-1:123456789012:build/my-project:1',
                'arn:aws:codebuild:us-east-1:123456789012:build/my-project:2'
            ]
        }
        self.mock_codebuild_client.list_builds_for_project.return_value = mock_response

        result = self.integration.list_builds("my-project")

        self.assertEqual(len(result), 2)

    def test_list_builds_with_multiple_pages(self):
        """Test list_builds with pagination"""
        mock_response = {
            'ids': ['arn:aws:codebuild:us-east-1:123456789012:build/my-project:1'],
            'nextToken': 'token123'
        }
        self.mock_codebuild_client.list_builds_for_project.return_value = mock_response

        result = self.integration.list_builds("my-project")

        self.assertEqual(len(result), 1)


class TestCodeBuildIntegrationReports(unittest.TestCase):
    """Test CodeBuildIntegration report-related methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_codebuild_client = MagicMock()
        self.integration = CodeBuildIntegration(
            region_name="us-east-1",
            codebuild_client=self.mock_codebuild_client
        )

    def test_create_report_group(self):
        """Test create_report_group method"""
        mock_response = {
            'reportGroup': {
                'arn': 'arn:aws:codebuild:us-east-1:123456789012:report-group/my-report-group',
                'name': 'my-report-group',
                'type': 'TEST',
                'exportConfig': {'exportConfigType': 'S3'},
                'created': '2024-01-01T00:00:00Z',
                'lastModified': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_codebuild_client.create_report_group.return_value = mock_response

        config = ReportGroupConfig(name="my-report-group")
        result = self.integration.create_report_group(config)

        self.assertEqual(result.name, "my-report-group")

    def test_list_report_groups(self):
        """Test list_report_groups method"""
        mock_response = {
            'reportGroups': ['report-group-1', 'report-group-2']
        }
        self.mock_codebuild_client.list_report_groups.return_value = mock_response

        result = self.integration.list_report_groups()

        self.assertEqual(len(result), 2)


class TestBoto3Availability(unittest.TestCase):
    """Test BOTO3_AVAILABLE flag"""

    def test_boto3_available(self):
        """Test BOTO3_AVAILABLE is set correctly"""
        # The mock should make BOTO3_AVAILABLE True since we set it up
        self.assertTrue(BOTO3_AVAILABLE)


if __name__ == '__main__':
    unittest.main()
