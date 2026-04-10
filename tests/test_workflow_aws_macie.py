"""
Tests for workflow_aws_macie module
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

# Create mock boto3 module before importing workflow_aws_macie
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Now we can import the module
from src.workflow_aws_macie import (
    MacieIntegration,
    FindingSeverity,
    JobStatus,
    MacieAccountConfig,
    ClassificationJobConfig,
    FindingFilter,
    CustomDataIdentifierConfig,
    AllowListConfig,
    BlockListConfig,
)


class TestFindingSeverity(unittest.TestCase):
    """Test FindingSeverity enum"""

    def test_finding_severity_values(self):
        self.assertEqual(FindingSeverity.LOW.value, "LOW")
        self.assertEqual(FindingSeverity.MEDIUM.value, "MEDIUM")
        self.assertEqual(FindingSeverity.HIGH.value, "HIGH")
        self.assertEqual(FindingSeverity.CRITICAL.value, "CRITICAL")

    def test_finding_severity_count(self):
        self.assertEqual(len(FindingSeverity), 4)


class TestJobStatus(unittest.TestCase):
    """Test JobStatus enum"""

    def test_job_status_values(self):
        self.assertEqual(JobStatus.IDLE.value, "IDLE")
        self.assertEqual(JobStatus.RUNNING.value, "RUNNING")
        self.assertEqual(JobStatus.PAUSED.value, "PAUSED")
        self.assertEqual(JobStatus.CANCELLED.value, "CANCELLED")
        self.assertEqual(JobStatus.COMPLETE.value, "COMPLETE")

    def test_job_status_count(self):
        self.assertEqual(len(JobStatus), 5)


class TestMacieAccountConfig(unittest.TestCase):
    """Test MacieAccountConfig dataclass"""

    def test_account_config_defaults(self):
        config = MacieAccountConfig()
        self.assertEqual(config.region, "us-east-1")
        self.assertTrue(config.enable)
        self.assertEqual(config.finding_publishing_frequency, "FIFTEEN_MINUTES")

    def test_account_config_custom(self):
        config = MacieAccountConfig(
            region="us-west-2",
            enable=False,
            finding_publishing_frequency="ONE_HOUR"
        )
        self.assertEqual(config.region, "us-west-2")
        self.assertFalse(config.enable)
        self.assertEqual(config.finding_publishing_frequency, "ONE_HOUR")


class TestClassificationJobConfig(unittest.TestCase):
    """Test ClassificationJobConfig dataclass"""

    def test_classification_job_config_defaults(self):
        config = ClassificationJobConfig(name="test-job")
        self.assertEqual(config.name, "test-job")
        self.assertEqual(config.job_type, "ONE_TIME")
        self.assertEqual(config.sampling_depth, 1000)
        self.assertEqual(config.status, JobStatus.IDLE)

    def test_classification_job_config_custom(self):
        config = ClassificationJobConfig(
            name="custom-job",
            description="Test job",
            job_type="SCHEDULED",
            sampling_depth=2000,
            custom_data_identifier_ids=["cdi-123"],
        )
        self.assertEqual(config.name, "custom-job")
        self.assertEqual(config.job_type, "SCHEDULED")
        self.assertEqual(config.sampling_depth, 2000)
        self.assertEqual(config.custom_data_identifier_ids, ["cdi-123"])


class TestFindingFilter(unittest.TestCase):
    """Test FindingFilter dataclass"""

    def test_finding_filter_defaults(self):
        f = FindingFilter()
        self.assertIsNone(f.severity)
        self.assertIsNone(f.finding_type)
        self.assertEqual(f.page_size, 50)

    def test_finding_filter_custom(self):
        f = FindingFilter(
            severity=FindingSeverity.HIGH,
            finding_type="SensitiveData",
            resource_type="S3Bucket",
            page_size=100,
        )
        self.assertEqual(f.severity, FindingSeverity.HIGH)
        self.assertEqual(f.finding_type, "SensitiveData")
        self.assertEqual(f.resource_type, "S3Bucket")
        self.assertEqual(f.page_size, 100)


class TestCustomDataIdentifierConfig(unittest.TestCase):
    """Test CustomDataIdentifierConfig dataclass"""

    def test_custom_data_identifier_defaults(self):
        config = CustomDataIdentifierConfig(name="test-cdi")
        self.assertEqual(config.name, "test-cdi")
        self.assertEqual(config.severity, "MEDIUM")
        self.assertEqual(config.maximum_match_distance, 0)

    def test_custom_data_identifier_with_regex(self):
        config = CustomDataIdentifierConfig(
            name="pii-detector",
            regex_pattern=r"\d{3}-\d{2}-\d{4}",
            keywords=["SSN", "social security"],
            severity="HIGH",
        )
        self.assertEqual(config.regex_pattern, r"\d{3}-\d{2}-\d{4}")
        self.assertEqual(config.keywords, ["SSN", "social security"])
        self.assertEqual(config.severity, "HIGH")


class TestAllowListConfig(unittest.TestCase):
    """Test AllowListConfig dataclass"""

    def test_allow_list_config_defaults(self):
        config = AllowListConfig(name="test-allow")
        self.assertEqual(config.name, "test-allow")

    def test_allow_list_config_with_regex(self):
        config = AllowListConfig(
            name="test-allow",
            description="Test allow list",
            regex_pattern=r"safe-pattern-.*",
        )
        self.assertEqual(config.name, "test-allow")
        self.assertEqual(config.regex_pattern, r"safe-pattern-.*")


class TestBlockListConfig(unittest.TestCase):
    """Test BlockListConfig dataclass"""

    def test_block_list_config_defaults(self):
        config = BlockListConfig(name="test-block")
        self.assertEqual(config.name, "test-block")

    def test_block_list_config_with_s3(self):
        config = BlockListConfig(
            name="test-block",
            s3_words_file={"Bucket": "my-bucket", "Key": "words.txt"},
        )
        self.assertEqual(config.s3_words_file["Bucket"], "my-bucket")


class TestMacieIntegration(unittest.TestCase):
    """Test MacieIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_macie2_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_macie2_client,
            self.mock_cloudwatch_client,
        ]

    def test_integration_initialization(self):
        """Test MacieIntegration initialization"""
        integration = MacieIntegration(
            region="us-east-1",
            profile_name="test-profile"
        )
        self.assertEqual(integration.region, "us-east-1")
        self.assertEqual(integration.profile_name, "test-profile")

    def test_integration_with_account_config(self):
        """Test MacieIntegration with custom account config"""
        config = MacieAccountConfig(region="us-west-2", enable=True)
        integration = MacieIntegration(account_config=config)
        self.assertEqual(integration.account_config.region, "us-west-2")

    # =========================================================================
    # Account Management Tests
    # =========================================================================

    def test_enable_macie_success(self):
        """Test enabling Macie successfully"""
        integration = MacieIntegration()
        self.mock_macie2_client.enable_macie.return_value = {}

        result = integration.enable_macie()

        self.assertEqual(result["status"], "enabled")
        self.mock_macie2_client.enable_macie.assert_called_once()

    def test_enable_macie_error(self):
        """Test enabling Macie with error"""
        integration = MacieIntegration()
        self.mock_macie2_client.enable_macie.side_effect = Exception("Failed to enable")

        with self.assertRaises(Exception):
            integration.enable_macie()

    def test_disable_macie_success(self):
        """Test disabling Macie successfully"""
        integration = MacieIntegration()
        self.mock_macie2_client.disable_macie.return_value = {}

        result = integration.disable_macie()

        self.assertEqual(result["status"], "disabled")
        self.mock_macie2_client.disable_macie.assert_called_once()

    def test_get_macie_session(self):
        """Test getting Macie session"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_macie_session.return_value = {
            "status": "ENABLED",
            "findingPublishingFrequency": "FIFTEEN_MINUTES"
        }

        result = integration.get_macie_session()

        self.assertEqual(result["status"], "ENABLED")

    def test_update_macie_session(self):
        """Test updating Macie session"""
        integration = MacieIntegration()
        self.mock_macie2_client.update_macie_session.return_value = {}

        result = integration.update_macie_session(
            finding_publishing_frequency="ONE_HOUR",
            status="PAUSED"
        )

        self.mock_macie2_client.update_macie_session.assert_called_once_with(
            findingPublishingFrequency="ONE_HOUR",
            status="PAUSED"
        )

    def test_get_master_account(self):
        """Test getting master account"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_master_account.return_value = {
            "master": {"AccountId": "123456789012", "Status": "ENABLED"}
        }

        result = integration.get_master_account()

        self.assertEqual(result["AccountId"], "123456789012")

    # =========================================================================
    # Classification Jobs Tests
    # =========================================================================

    def test_create_classification_job(self):
        """Test creating a classification job"""
        integration = MacieIntegration()
        self.mock_macie2_client.create_classification_job.return_value = {
            "jobId": "job-123",
            "jobStatus": "RUNNING",
            "createdAt": "2024-01-01T00:00:00Z"
        }

        config = ClassificationJobConfig(
            name="test-job",
            description="Test classification job",
            job_type="ONE_TIME",
            s3_job_definition={"bucketDefinitions": []},
            sampling_depth=1000,
        )

        result = integration.create_classification_job(config)

        self.assertEqual(result["job_id"], "job-123")
        self.assertEqual(result["status"], "RUNNING")
        self.mock_macie2_client.create_classification_job.assert_called_once()

    def test_create_classification_job_with_schedule(self):
        """Test creating a scheduled classification job"""
        integration = MacieIntegration()
        self.mock_macie2_client.create_classification_job.return_value = {
            "jobId": "job-456",
            "jobStatus": "RUNNING",
            "createdAt": "2024-01-01T00:00:00Z"
        }

        config = ClassificationJobConfig(
            name="scheduled-job",
            job_type="SCHEDULED",
            schedule_frequency={"daily": {}},
        )

        result = integration.create_classification_job(config)

        self.assertEqual(result["job_id"], "job-456")
        call_kwargs = self.mock_macie2_client.create_classification_job.call_args[1]
        self.assertEqual(call_kwargs["scheduleFrequency"], {"daily": {}})

    def test_list_classification_jobs(self):
        """Test listing classification jobs"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_classification_jobs.return_value = {
            "jobDefinitions": [
                {"jobId": "job-1", "name": "job-1"},
                {"jobId": "job-2", "name": "job-2"},
            ],
            "nextToken": "token-123"
        }

        result = integration.list_classification_jobs(job_type="ONE_TIME")

        self.assertEqual(len(result["jobs"]), 2)
        self.assertEqual(result["next_token"], "token-123")

    def test_describe_classification_job(self):
        """Test describing a classification job"""
        integration = MacieIntegration()
        self.mock_macie2_client.describe_classification_job.return_value = {
            "jobId": "job-123",
            "jobStatus": "RUNNING",
            "description": "Test job"
        }

        result = integration.describe_classification_job("job-123")

        self.assertEqual(result["jobId"], "job-123")
        self.mock_macie2_client.describe_classification_job.assert_called_once_with(jobId="job-123")

    def test_update_classification_job(self):
        """Test updating a classification job"""
        integration = MacieIntegration()
        self.mock_macie2_client.update_classification_job.return_value = {}

        config = ClassificationJobConfig(
            name="updated-job",
            description="Updated description",
            sampling_depth=2000,
        )

        result = integration.update_classification_job("job-123", config)

        self.assertEqual(result["job_id"], "job-123")
        self.assertEqual(result["status"], "updated")

    def test_delete_classification_job(self):
        """Test deleting a classification job"""
        integration = MacieIntegration()
        self.mock_macie2_client.delete_classification_job.return_value = {}

        result = integration.delete_classification_job("job-123")

        self.assertEqual(result["job_id"], "job-123")
        self.assertEqual(result["status"], "deleted")

    def test_run_classification_job(self):
        """Test running a classification job"""
        integration = MacieIntegration()
        self.mock_macie2_client.run_classification_job.return_value = {}

        result = integration.run_classification_job("job-123")

        self.assertEqual(result["job_id"], "job-123")
        self.assertEqual(result["status"], "running")

    # =========================================================================
    # Findings Tests
    # =========================================================================

    def test_list_findings(self):
        """Test listing findings"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_findings.return_value = {
            "findings": [{"id": "finding-1"}, {"id": "finding-2"}],
            "nextToken": "token-123",
            "totalCount": 2
        }

        result = integration.list_findings()

        self.assertEqual(len(result["findings"]), 2)
        self.assertEqual(result["total_count"], 2)

    def test_list_findings_with_filter(self):
        """Test listing findings with filter"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_findings.return_value = {
            "findings": [],
            "totalCount": 0
        }

        filter_criteria = FindingFilter(
            severity=FindingSeverity.HIGH,
            finding_type="SensitiveData",
            page_size=100
        )

        integration.list_findings(filter_criteria=filter_criteria)

        call_kwargs = self.mock_macie2_client.list_findings.call_args[1]
        self.assertEqual(call_kwargs["maxResults"], 100)

    def test_get_findings(self):
        """Test getting specific findings"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_findings.return_value = {
            "findings": [{"id": "finding-1"}],
            "unprocessedFindings": []
        }

        result = integration.get_findings(["finding-1", "finding-2"])

        self.assertEqual(len(result["findings"]), 1)
        self.assertEqual(len(result["unprocessed"]), 0)

    def test_describe_findings(self):
        """Test describing findings"""
        integration = MacieIntegration()
        self.mock_macie2_client.describe_findings.return_value = {
            "findings": [{"id": "finding-1", "type": "SensitiveData"}]
        }

        result = integration.describe_findings(["finding-1"])

        self.mock_macie2_client.describe_findings.assert_called_once_with(
            findingIds=["finding-1"],
            locale="en"
        )

    def test_build_finding_filter(self):
        """Test building finding filter expression"""
        integration = MacieIntegration()
        f = FindingFilter(
            severity=FindingSeverity.CRITICAL,
            finding_type="PolicyViolation",
            resource_type="S3Bucket"
        )

        result = integration._build_finding_filter(f)

        self.assertIn("filterExpressions", result)
        self.assertEqual(len(result["filterExpressions"]), 3)

    # =========================================================================
    # Filters Tests
    # =========================================================================

    def test_create_filter(self):
        """Test creating a findings filter"""
        integration = MacieIntegration()
        self.mock_macie2_client.create_filter.return_value = {
            "filterArn": "arn:aws:macie:...filter-123"
        }

        finding_filter = FindingFilter(severity=FindingSeverity.HIGH)
        result = integration.create_filter("high-severity-filter", finding_filter)

        self.assertEqual(result["name"], "high-severity-filter")
        self.assertEqual(result["action"], "ARCHIVE")

    def test_list_filters(self):
        """Test listing filters"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_filters.return_value = {
            "filters": [{"name": "filter-1"}, {"name": "filter-2"}]
        }

        result = integration.list_filters()

        self.assertEqual(len(result["filters"]), 2)

    def test_delete_filter(self):
        """Test deleting a filter"""
        integration = MacieIntegration()
        self.mock_macie2_client.delete_filter.return_value = {}

        result = integration.delete_filter("arn:aws:macie:...filter-123")

        self.assertEqual(result["status"], "deleted")

    # =========================================================================
    # Custom Data Identifiers Tests
    # =========================================================================

    def test_create_custom_data_identifier(self):
        """Test creating a custom data identifier"""
        integration = MacieIntegration()
        self.mock_macie2_client.create_custom_data_identifier.return_value = {
            "customDataIdentifierId": "cdi-123",
            "arn": "arn:aws:macie:...cdi-123"
        }

        config = CustomDataIdentifierConfig(
            name="pii-detector",
            regex_pattern=r"\d{3}-\d{2}-\d{4}",
            severity="HIGH"
        )

        result = integration.create_custom_data_identifier(config)

        self.assertEqual(result["id"], "cdi-123")

    def test_list_custom_data_identifiers(self):
        """Test listing custom data identifiers"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_custom_data_identifiers.return_value = {
            "customDataIdentifiers": [{"id": "cdi-1"}, {"id": "cdi-2"}]
        }

        result = integration.list_custom_data_identifiers()

        self.assertEqual(len(result["identifiers"]), 2)

    def test_get_custom_data_identifier(self):
        """Test getting a custom data identifier"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_custom_data_identifier.return_value = {
            "customDataIdentifierId": "cdi-123",
            "name": "pii-detector"
        }

        result = integration.get_custom_data_identifier("cdi-123")

        self.assertEqual(result["customDataIdentifierId"], "cdi-123")

    def test_delete_custom_data_identifier(self):
        """Test deleting a custom data identifier"""
        integration = MacieIntegration()
        self.mock_macie2_client.delete_custom_data_identifier.return_value = {}

        result = integration.delete_custom_data_identifier("cdi-123")

        self.assertEqual(result["id"], "cdi-123")
        self.assertEqual(result["status"], "deleted")

    # =========================================================================
    # Allow Lists Tests
    # =========================================================================

    def test_create_allow_list(self):
        """Test creating an allow list"""
        integration = MacieIntegration()
        self.mock_macie2_client.create_allow_list.return_value = {
            "arn": "arn:aws:macie:...allowlist-123",
            "id": "allowlist-123",
            "name": "safe-patterns"
        }

        config = AllowListConfig(
            name="safe-patterns",
            description="Safe patterns to ignore",
            regex_pattern=r"safe-.*"
        )

        result = integration.create_allow_list(config)

        self.assertEqual(result["id"], "allowlist-123")

    def test_list_allow_lists(self):
        """Test listing allow lists"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_allow_lists.return_value = {
            "allowLists": [{"id": "al-1"}, {"id": "al-2"}]
        }

        result = integration.list_allow_lists()

        self.assertEqual(len(result["allow_lists"]), 2)

    def test_get_allow_list(self):
        """Test getting an allow list"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_allow_list.return_value = {
            "id": "allowlist-123",
            "name": "safe-patterns"
        }

        result = integration.get_allow_list("allowlist-123")

        self.assertEqual(result["id"], "allowlist-123")

    def test_update_allow_list(self):
        """Test updating an allow list"""
        integration = MacieIntegration()
        self.mock_macie2_client.update_allow_list.return_value = {}

        config = AllowListConfig(
            name="updated-safe-patterns",
            regex_pattern=r"updated-safe-.*"
        )

        result = integration.update_allow_list("allowlist-123", config)

        self.assertEqual(result["id"], "allowlist-123")
        self.assertEqual(result["status"], "updated")

    def test_delete_allow_list(self):
        """Test deleting an allow list"""
        integration = MacieIntegration()
        self.mock_macie2_client.delete_allow_list.return_value = {}

        result = integration.delete_allow_list("allowlist-123")

        self.assertEqual(result["id"], "allowlist-123")
        self.assertEqual(result["status"], "deleted")

    # =========================================================================
    # Block Lists Tests
    # =========================================================================

    def test_create_block_list(self):
        """Test creating a block list"""
        integration = MacieIntegration()
        self.mock_macie2_client.create_block_list.return_value = {
            "arn": "arn:aws:macie:...blocklist-123",
            "id": "blocklist-123",
            "name": "blocked-patterns"
        }

        config = BlockListConfig(
            name="blocked-patterns",
            regex_pattern=r"blocked-.*"
        )

        result = integration.create_block_list(config)

        self.assertEqual(result["id"], "blocklist-123")

    def test_list_block_lists(self):
        """Test listing block lists"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_block_lists.return_value = {
            "blockLists": [{"id": "bl-1"}]
        }

        result = integration.list_block_lists()

        self.assertEqual(len(result["block_lists"]), 1)

    def test_get_block_list(self):
        """Test getting a block list"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_block_list.return_value = {
            "id": "blocklist-123",
            "name": "blocked-patterns"
        }

        result = integration.get_block_list("blocklist-123")

        self.assertEqual(result["id"], "blocklist-123")

    def test_update_block_list(self):
        """Test updating a block list"""
        integration = MacieIntegration()
        self.mock_macie2_client.update_block_list.return_value = {}

        config = BlockListConfig(
            name="updated-blocked-patterns",
            regex_pattern=r"updated-blocked-.*"
        )

        result = integration.update_block_list("blocklist-123", config)

        self.assertEqual(result["id"], "blocklist-123")
        self.assertEqual(result["status"], "updated")

    def test_delete_block_list(self):
        """Test deleting a block list"""
        integration = MacieIntegration()
        self.mock_macie2_client.delete_block_list.return_value = {}

        result = integration.delete_block_list("blocklist-123")

        self.assertEqual(result["id"], "blocklist-123")
        self.assertEqual(result["status"], "deleted")

    # =========================================================================
    # Sensitive Data Discoveries Tests
    # =========================================================================

    def test_list_sensitive_data_findings(self):
        """Test listing sensitive data findings"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_sensitive_data_findings.return_value = {
            "findings": [{"id": "sdf-1"}],
            "nextToken": "token-123",
            "totalCount": 1
        }

        result = integration.list_sensitive_data_findings(max_results=50)

        self.assertEqual(len(result["findings"]), 1)
        self.assertEqual(result["total_count"], 1)

    def test_get_sensitive_data_findings_statistics(self):
        """Test getting sensitive data findings statistics"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_sensitive_data_findings_statistics.return_value = {
            "countsByCategory": {"FINANCIAL": 5, "PERSONAL": 10}
        }

        result = integration.get_sensitive_data_findings_statistics()

        self.assertIn("countsByCategory", result)

    def test_describe_buckets(self):
        """Test describing S3 buckets monitored by Macie"""
        integration = MacieIntegration()
        self.mock_macie2_client.describe_buckets.return_value = {
            "buckets": [
                {"bucketName": "bucket-1", "classificationResult": {"status": "COMPLETE"}}
            ],
            "nextToken": None
        }

        result = integration.describe_buckets()

        self.assertEqual(len(result["buckets"]), 1)

    # =========================================================================
    # Member Accounts Tests
    # =========================================================================

    def test_list_member_accounts(self):
        """Test listing member accounts"""
        integration = MacieIntegration()
        self.mock_macie2_client.list_members.return_value = {
            "members": [{"accountId": "123456789012"}, {"accountId": "999999999999"}],
            "nextToken": None
        }

        result = integration.list_member_accounts()

        self.assertEqual(len(result["members"]), 2)

    def test_get_member_account(self):
        """Test getting a member account"""
        integration = MacieIntegration()
        self.mock_macie2_client.get_member_account.return_value = {
            "accountId": "123456789012",
            "status": "ENABLED"
        }

        result = integration.get_member_account("123456789012")

        self.assertEqual(result["accountId"], "123456789012")


if __name__ == "__main__":
    unittest.main()
