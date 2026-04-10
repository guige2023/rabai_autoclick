"""
Tests for workflow_aws_mwaa module
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

# Create mock boto3 module before importing workflow_aws_mwaa
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Import the module
import src.workflow_aws_mwaa as mwaa_module

MWAAIntegration = mwaa_module.MWAAIntegration
MWAAEnvironment = mwaa_module.MWAAEnvironment
MWAAEnvironmentConfig = mwaa_module.MWAAEnvironmentConfig
MWAAAccessPolicy = mwaa_module.MWAAAccessPolicy
MWAADAGUpload = mwaa_module.MWAADAGUpload
MWAADAGRun = mwaa_module.MWAADAGRun
MWAAEnvironmentStatus = mwaa_module.MWAAEnvironmentStatus
MWAAWebServerStatus = mwaa_module.MWAAWebServerStatus
MWAAExecutionStatus = mwaa_module.MWAAExecutionStatus


class TestMWAAEnvironment(unittest.TestCase):
    """Test MWAAEnvironment dataclass"""

    def test_mwaa_environment_defaults(self):
        env = MWAAEnvironment(name="test-env")
        self.assertEqual(env.name, "test-env")
        self.assertEqual(env.status, "UNKNOWN")
        self.assertEqual(env.region, "")

    def test_mwaa_environment_creation(self):
        env = MWAAEnvironment(
            name="test-env",
            arn="arn:aws:airflow:us-east-1:123456789012:environment/test-env",
            region="us-east-1",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role",
            status="CREATE_COMPLETE"
        )
        self.assertEqual(env.name, "test-env")
        self.assertEqual(env.status, "CREATE_COMPLETE")


class TestMWAAEnvironmentConfig(unittest.TestCase):
    """Test MWAAEnvironmentConfig dataclass"""

    def test_mwaa_environment_config_defaults(self):
        config = MWAAEnvironmentConfig(
            name="test-env",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role",
            dag_s3_bucket="test-bucket"
        )
        self.assertEqual(config.name, "test-env")
        self.assertEqual(config.airflow_version, "2.7.2")
        self.assertEqual(config.min_workers, 1)
        self.assertEqual(config.max_workers, 3)
        self.assertTrue(config.logging_enabled)


class TestMWAAAccessPolicy(unittest.TestCase):
    """Test MWAAAccessPolicy dataclass"""

    def test_mwaa_access_policy_creation(self):
        policy = MWAAAccessPolicy(
            name="test-policy",
            arn="arn:aws:airflow:us-east-1:123456789012:policy/test-policy"
        )
        self.assertEqual(policy.name, "test-policy")


class TestMWAADAGUpload(unittest.TestCase):
    """Test MWAADAGUpload dataclass"""

    def test_mwaa_dag_upload_creation(self):
        upload = MWAADAGUpload(
            dag_id="test-dag",
            file_path="/path/to/dag.py",
            s3_key="dags/dag.py",
            uploaded_at=datetime.now(),
            checksum="abc123",
            size_bytes=1024
        )
        self.assertEqual(upload.dag_id, "test-dag")
        self.assertEqual(upload.size_bytes, 1024)


class TestMWAADAGRun(unittest.TestCase):
    """Test MWAADAGRun dataclass"""

    def test_mwaa_dag_run_creation(self):
        run = MWAADAGRun(
            dag_id="test-dag",
            run_id="run-123",
            state="running",
            execution_date=datetime.now()
        )
        self.assertEqual(run.dag_id, "test-dag")
        self.assertEqual(run.state, "running")


class TestMWAAEnvironmentStatus(unittest.TestCase):
    """Test MWAAEnvironmentStatus enum"""

    def test_mwaa_environment_status_values(self):
        self.assertEqual(MWAAEnvironmentStatus.CREATING.value, "CREATING")
        self.assertEqual(MWAAEnvironmentStatus.CREATE_COMPLETE.value, "CREATE_COMPLETE")
        self.assertEqual(MWAAEnvironmentStatus.UPDATING.value, "UPDATING")
        self.assertEqual(MWAAEnvironmentStatus.ERROR.value, "ERROR")


class TestMWAAWebServerStatus(unittest.TestCase):
    """Test MWAAWebServerStatus enum"""

    def test_mwaa_web_server_status_values(self):
        self.assertEqual(MWAAWebServerStatus.AVAILABLE.value, "AVAILABLE")
        self.assertEqual(MWAAWebServerStatus.UNAVAILABLE.value, "UNAVAILABLE")


class TestMWAAExecutionStatus(unittest.TestCase):
    """Test MWAAExecutionStatus enum"""

    def test_mwaa_execution_status_values(self):
        self.assertEqual(MWAAExecutionStatus.SUCCESS.value, "SUCCESS")
        self.assertEqual(MWAAExecutionStatus.FAILED.value, "FAILED")
        self.assertEqual(MWAAExecutionStatus.RUNNING.value, "RUNNING")


class TestMWAAIntegration(unittest.TestCase):
    """Test MWAAIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mwaa_client = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_iam_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_cloudwatch_logs_client = MagicMock()
        self.mock_sts_client = MagicMock()

        with patch.object(MWAAIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = MWAAIntegration()
            self.integration.region = "us-east-1"
            self.integration.web_server_url = None
            self.integration.web_server_auth_token = None
            self.integration.mwaa_client = self.mock_mwaa_client
            self.integration.s3_client = self.mock_s3_client
            self.integration.iam_client = self.mock_iam_client
            self.integration.cloudwatch_client = self.mock_cloudwatch_client
            self.integration.cloudwatch_logs_client = self.mock_cloudwatch_logs_client
            self.integration.sts_client = self.mock_sts_client
            self.integration._environments_cache = {}
            self.integration._cache_timestamp = None
            self.integration._cache_ttl_seconds = 300

    def test_init_with_boto3(self):
        """Test initialization with boto3 session"""
        with patch.object(MWAAIntegration, '__init__', lambda x, **kwargs: None):
            integration = MWAAIntegration()
            integration.region = "us-east-1"
            integration.web_server_url = None
            integration.web_server_auth_token = None
            integration.mwaa_client = None
            integration.s3_client = None
            integration.iam_client = None
            integration.cloudwatch_client = None
            integration.cloudwatch_logs_client = None
            integration.sts_client = None
            integration._environments_cache = {}
            integration._cache_timestamp = None
            integration._cache_ttl_seconds = 300

    def test_is_cache_valid(self):
        """Test cache validity check"""
        self.integration._cache_timestamp = datetime.now()
        self.assertTrue(self.integration._is_cache_valid())

    def test_is_cache_expired(self):
        """Test cache expiry"""
        self.integration._cache_timestamp = datetime.now() - timedelta(seconds=400)
        self.assertFalse(self.integration._is_cache_valid())

    def test_parse_datetime(self):
        """Test datetime parsing"""
        dt_str = "2024-01-15T10:30:00Z"
        result = self.integration._parse_datetime(dt_str)
        self.assertIsInstance(result, datetime)

    def test_parse_datetime_invalid(self):
        """Test datetime parsing with invalid input"""
        result = self.integration._parse_datetime(None)
        self.assertIsNone(result)

        result = self.integration._parse_datetime("invalid")
        self.assertIsNone(result)


class TestMWAAEnvironmentManagement(unittest.TestCase):
    """Test MWAA environment management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mwaa_client = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_sts_client = MagicMock()

        with patch.object(MWAAIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = MWAAIntegration()
            self.integration.region = "us-east-1"
            self.integration.mwaa_client = self.mock_mwaa_client
            self.integration.s3_client = self.mock_s3_client
            self.integration.cloudwatch_client = self.mock_cloudwatch_client
            self.integration.sts_client = self.mock_sts_client
            self.integration._environments_cache = {}
            self.integration._cache_timestamp = None
            self.integration._cache_ttl_seconds = 300

    def test_create_environment(self):
        """Test creating an MWAA environment"""
        mock_response = {
            "Environment": {
                "Name": "test-env",
                "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env",
                "Status": "CREATING"
            }
        }
        self.mock_mwaa_client.create_environment.return_value = mock_response

        config = MWAAEnvironmentConfig(
            name="test-env",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role",
            dag_s3_bucket="test-bucket"
        )

        result = self.integration.create_environment(config, wait_for_completion=False)

        self.assertEqual(result.name, "test-env")

    def test_get_environment(self):
        """Test getting an MWAA environment"""
        mock_response = {
            "Environment": {
                "Name": "test-env",
                "Arn": "arn:aws:airflow:us-east-1:123456789012:environment/test-env",
                "Status": "CREATE_COMPLETE",
                "WebServerUrl": "https://test.airflow.amazonaws.com"
            }
        }
        self.mock_mwaa_client.get_environment.return_value = mock_response

        result = self.integration.get_environment("test-env", use_cache=False)

        self.assertEqual(result.name, "test-env")

    def test_get_environment_not_found(self):
        """Test getting non-existent environment"""
        error = Exception("ResourceNotFoundException")
        error.response = {"Error": {"Code": "ResourceNotFoundException"}}
        self.mock_mwaa_client.get_environment.side_effect = error

        result = self.integration.get_environment("non-existent")
        self.assertIsNone(result)

    def test_list_environments(self):
        """Test listing MWAA environments"""
        mock_response = {
            "Environments": ["env1", "env2"]
        }
        self.mock_mwaa_client.list_environments.return_value = mock_response

        result = self.integration.list_environments()

        self.assertEqual(len(result), 2)

    def test_update_environment(self):
        """Test updating an MWAA environment"""
        mock_response = {
            "Environment": {
                "Name": "test-env",
                "Status": "UPDATE_COMPLETE",
                "CreatedAt": "2024-01-01T00:00:00Z",
                "LastUpdatedAt": "2024-01-01T00:00:00Z"
            }
        }
        self.mock_mwaa_client.update_environment.return_value = mock_response
        self.mock_mwaa_client.get_environment.return_value = mock_response

        result = self.integration.update_environment("test-env", min_workers=2, wait_for_completion=False)

        self.assertEqual(result.name, "test-env")

    def test_delete_environment(self):
        """Test deleting an MWAA environment"""
        self.mock_mwaa_client.delete_environment.return_value = {}

        result = self.integration.delete_environment("test-env", wait_for_completion=False)

        self.assertTrue(result)


class TestMWAADAGManagement(unittest.TestCase):
    """Test MWAA DAG management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mwaa_client = MagicMock()
        self.mock_s3_client = MagicMock()

        with patch.object(MWAAIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = MWAAIntegration()
            self.integration.region = "us-east-1"
            self.integration.mwaa_client = self.mock_mwaa_client
            self.integration.s3_client = self.mock_s3_client
            self.integration._environments_cache = {}
            self.integration._cache_timestamp = None
            self.integration._cache_ttl_seconds = 300

    def test_extract_bucket_from_s3_path(self):
        """Test extracting bucket name from S3 path"""
        result = self.integration._extract_bucket_from_s3_path("s3://test-bucket/dags")
        self.assertEqual(result, "test-bucket")

        result = self.integration._extract_bucket_from_s3_path("test-bucket/dags")
        self.assertEqual(result, "test-bucket")

    def test_calculate_file_checksum(self):
        """Test file checksum calculation"""
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test content")
            temp_path = f.name

        try:
            result = self.integration._calculate_file_checksum(temp_path)
            self.assertEqual(len(result), 64)  # SHA256 produces 64 hex chars
        finally:
            os.unlink(temp_path)

    def test_list_dags_in_s3(self):
        """Test listing DAGs in S3"""
        # Use get_environment with use_cache=False since we're mocking the cache behavior
        mock_env = MWAAEnvironment(
            name="test-env",
            dag_s3_path="s3://test-bucket/dags"
        )
        self.integration._environments_cache["test-env"] = mock_env
        self.integration._cache_timestamp = datetime.now()

        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "dags/dag1.py", "Size": 1024, "LastModified": "2024-01-01T00:00:00Z", "ETag": "\"abc123\""},
                {"Key": "dags/dag2.py", "Size": 2048, "LastModified": "2024-01-02T00:00:00Z", "ETag": "\"def456\""}
            ]
        }

        result = self.integration.list_dags_in_s3("test-env")

        self.assertEqual(len(result), 2)

    def test_delete_dag_from_s3(self):
        """Test deleting a DAG from S3"""
        mock_env = MWAAEnvironment(
            name="test-env",
            dag_s3_path="s3://test-bucket/dags"
        )
        self.integration._environments_cache["test-env"] = mock_env
        self.integration._cache_timestamp = datetime.now()

        result = self.integration.delete_dag_from_s3("test-env", "dags/dag1.py")

        self.assertTrue(result)


class TestMWAAAccessManagement(unittest.TestCase):
    """Test MWAA access management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mwaa_client = MagicMock()
        self.mock_iam_client = MagicMock()
        self.mock_sts_client = MagicMock()

        with patch.object(MWAAIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = MWAAIntegration()
            self.integration.region = "us-east-1"
            self.integration.mwaa_client = self.mock_mwaa_client
            self.integration.iam_client = self.mock_iam_client
            self.integration.sts_client = self.mock_sts_client
            self.integration._environments_cache = {}
            self.integration._cache_timestamp = None
            self.integration._cache_ttl_seconds = 300

    def test_get_account_id(self):
        """Test getting AWS account ID"""
        self.mock_sts_client.get_caller_identity.return_value = {
            "Account": "123456789012"
        }

        result = self.integration._get_account_id()

        self.assertEqual(result, "123456789012")

    def test_create_access_policy(self):
        """Test creating an access policy"""
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "airflow:CreateCliToken",
                    "Resource": "*"
                }
            ]
        }

        result = self.integration.create_access_policy("test-policy", policy_doc)

        self.assertEqual(result.name, "test-policy")

    def test_get_access_policy(self):
        """Test getting an access policy"""
        self.mock_iam_client.get_policy.return_value = {
            "Policy": {
                "PolicyName": "test-policy",
                "Arn": "arn:aws:iam::123456789012:policy/test-policy"
            }
        }
        self.mock_sts_client.get_caller_identity.return_value = {
            "Account": "123456789012"
        }

        result = self.integration.get_access_policy("test-policy")

        self.assertIsNotNone(result)

    def test_validate_web_server_access_policy(self):
        """Test validating web server access policy"""
        mock_env = MWAAEnvironment(
            name="test-env",
            web_server_url="https://test.airflow.amazonaws.com",
            execution_role_arn="arn:aws:iam::123456789012:role/test-role"
        )
        self.integration._environments_cache["test-env"] = mock_env
        self.integration._cache_timestamp = datetime.now()

        result = self.integration.validate_web_server_access_policy("test-env")

        self.assertEqual(result["environment_name"], "test-env")


class TestMWAAMonitoring(unittest.TestCase):
    """Test MWAA monitoring methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudwatch_client = MagicMock()
        self.mock_mwaa_client = MagicMock()

        with patch.object(MWAAIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = MWAAIntegration()
            self.integration.region = "us-east-1"
            self.integration.cloudwatch_client = self.mock_cloudwatch_client
            self.integration.mwaa_client = self.mock_mwaa_client
            self.integration._environments_cache = {}
            self.integration._cache_timestamp = None
            self.integration._cache_ttl_seconds = 300

    def test_get_environment_metrics(self):
        """Test getting environment metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Average": 1.0, "Maximum": 5.0, "Minimum": 0.0, "Timestamp": "2024-01-01T00:00:00Z"}
            ]
        }

        result = self.integration.get_environment_metrics("test-env")

        self.assertEqual(result["environment_name"], "test-env")
        self.assertIn("metrics", result)

    def test_get_web_server_metrics(self):
        """Test getting web server metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Datapoints": []
        }

        result = self.integration.get_web_server_metrics("test-env")

        # The result contains the metrics data keyed by namespace/metric_name
        self.assertIn("AmazonMWAA/WebServerHealthyHostCount", result)


if __name__ == '__main__':
    unittest.main()
