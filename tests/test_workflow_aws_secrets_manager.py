"""
Tests for workflow_aws_secrets_manager module
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

# Create mock boto3 module before importing workflow_aws_secrets_manager
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
from src.workflow_aws_secrets_manager import (
    SecretsManagerIntegration,
    SecretType,
    RotationStatus,
    ReplicationStatus,
    SecretConfig,
    SecretVersion,
    ReplicationConfig,
    RotationConfig,
    ResourcePolicy,
    DatabaseSecretConfig,
)


class TestSecretType(unittest.TestCase):
    """Test SecretType enum"""

    def test_secret_type_values(self):
        self.assertEqual(SecretType.GENERIC.value, "generic")
        self.assertEqual(SecretType.DATABASE_CREDENTIALS.value, "database_credentials")
        self.assertEqual(SecretType.API_KEY.value, "***")
        self.assertEqual(SecretType.CERTIFICATE.value, "certificate")
        self.assertEqual(SecretType.OTHER.value, "other")

    def test_secret_type_count(self):
        self.assertEqual(len(SecretType), 6)


class TestRotationStatus(unittest.TestCase):
    """Test RotationStatus enum"""

    def test_rotation_status_values(self):
        self.assertEqual(RotationStatus.ENABLED.value, "enabled")
        self.assertEqual(RotationStatus.DISABLED.value, "disabled")
        self.assertEqual(RotationStatus.ROTATING.value, "rotating")
        self.assertEqual(RotationStatus.FAILED.value, "failed")


class TestReplicationStatus(unittest.TestCase):
    """Test ReplicationStatus enum"""

    def test_replication_status_values(self):
        self.assertEqual(ReplicationStatus.REPLICATING.value, "replicating")
        self.assertEqual(ReplicationStatus.REPLICATED.value, "replicated")
        self.assertEqual(ReplicationStatus.FAILED.value, "failed")


class TestSecretConfig(unittest.TestCase):
    """Test SecretConfig dataclass"""

    def test_secret_config_defaults(self):
        config = SecretConfig(name="test-secret")
        self.assertEqual(config.name, "test-secret")
        self.assertEqual(config.secret_type, SecretType.GENERIC)
        self.assertFalse(config.enable_rotation)
        self.assertEqual(config.rotation_days, 30)
        self.assertEqual(len(config.replica_regions), 0)

    def test_secret_config_custom(self):
        config = SecretConfig(
            name="custom-secret",
            secret_type=SecretType.DATABASE_CREDENTIALS,
            description="Test secret",
            kms_key_id="kms-key-123",
            secret_string="secret-value",
            tags={"Environment": "Production"},
            enable_rotation=True,
            rotation_lambda_arn="arn:aws:lambda:us-east-1:123456789:function:rotation-function",
            rotation_days=60,
            replica_regions=["us-west-2", "eu-west-1"]
        )
        self.assertEqual(config.name, "custom-secret")
        self.assertEqual(config.secret_type, SecretType.DATABASE_CREDENTIALS)
        self.assertEqual(config.kms_key_id, "kms-key-123")
        self.assertEqual(config.rotation_days, 60)
        self.assertEqual(len(config.replica_regions), 2)

    def test_secret_config_with_policy(self):
        policy_json = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"secretsmanager:GetSecretValue","Resource":"*"}]}'
        config = SecretConfig(
            name="test-secret",
            policy=policy_json
        )
        self.assertEqual(config.policy, policy_json)


class TestSecretVersion(unittest.TestCase):
    """Test SecretVersion dataclass"""

    def test_secret_version_creation(self):
        version = SecretVersion(
            version_id="v12345678-1234-1234-1234-123456789012",
            version_stages=["AWSCURRENT", "AWSPREVIOUS"],
            created_date=datetime.now()
        )
        self.assertEqual(version.version_id, "v12345678-1234-1234-1234-123456789012")
        self.assertEqual(len(version.version_stages), 2)
        self.assertEqual(version.secret_value, None)
        self.assertEqual(version.secret_binary, None)

    def test_secret_version_with_values(self):
        version = SecretVersion(
            version_id="v123",
            version_stages=["AWSCURRENT"],
            created_date=datetime.now(),
            secret_value="my-secret-value"
        )
        self.assertEqual(version.secret_value, "my-secret-value")


class TestReplicationConfig(unittest.TestCase):
    """Test ReplicationConfig dataclass"""

    def test_replication_config_creation(self):
        config = ReplicationConfig(region="us-west-2")
        self.assertEqual(config.region, "us-west-2")
        self.assertEqual(config.status, ReplicationStatus.REPLICATING)

    def test_replication_config_custom(self):
        config = ReplicationConfig(
            region="eu-west-1",
            kms_key_id="kms-key-eu",
            status=ReplicationStatus.REPLICATED
        )
        self.assertEqual(config.kms_key_id, "kms-key-eu")
        self.assertEqual(config.status, ReplicationStatus.REPLICATED)


class TestRotationConfig(unittest.TestCase):
    """Test RotationConfig dataclass"""

    def test_rotation_config_creation(self):
        config = RotationConfig(
            lambda_arn="arn:aws:lambda:us-east-1:123456789:function:my-function",
            rotation_days=30,
            automatically_after_days=30,
            duration_hours=1
        )
        self.assertEqual(config.lambda_arn, "arn:aws:lambda:us-east-1:123456789:function:my-function")
        self.assertEqual(config.rotation_days, 30)
        self.assertEqual(config.duration_hours, 1)

    def test_rotation_config_defaults(self):
        config = RotationConfig(lambda_arn="arn:aws:lambda:us-east-1:123456789:function:my-function")
        self.assertEqual(config.rotation_days, 30)
        self.assertEqual(config.automatically_after_days, 30)


class TestResourcePolicy(unittest.TestCase):
    """Test ResourcePolicy dataclass"""

    def test_resource_policy_defaults(self):
        policy = ResourcePolicy()
        self.assertEqual(policy.version, "2012-10-17")
        self.assertEqual(len(policy.statement), 0)

    def test_resource_policy_with_statement(self):
        policy = ResourcePolicy(
            statement=[
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789:root"},
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*"
                }
            ]
        )
        self.assertEqual(len(policy.statement), 1)
        self.assertEqual(policy.statement[0]["Effect"], "Allow")


class TestDatabaseSecretConfig(unittest.TestCase):
    """Test DatabaseSecretConfig dataclass"""

    def test_database_secret_config_creation(self):
        config = DatabaseSecretConfig(
            db_type="mysql",
            db_host="localhost",
            db_port=3306,
            db_name="mydb",
            db_username="admin"
        )
        self.assertEqual(config.db_type, "mysql")
        self.assertEqual(config.db_host, "localhost")
        self.assertEqual(config.db_port, 3306)
        self.assertEqual(config.db_username, "admin")
        self.assertEqual(config.db_password_length, 32)
        self.assertTrue(config.db_password_special_chars)

    def test_database_secret_config_custom_password(self):
        config = DatabaseSecretConfig(
            db_type="postgres",
            db_host="db.example.com",
            db_port=5432,
            db_name="production",
            db_username="dbuser",
            db_password_length=64,
            db_password_special_chars=False,
            db_password_exclude_ambiguous=True
        )
        self.assertEqual(config.db_password_length, 64)
        self.assertFalse(config.db_password_special_chars)
        self.assertTrue(config.db_password_exclude_ambiguous)


class TestSecretsManagerIntegration(unittest.TestCase):
    """Test SecretsManagerIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_integration_initialization(self):
        """Test SecretsManagerIntegration initialization"""
        integration = SecretsManagerIntegration(
            region_name="us-east-1",
            profile_name="test-profile"
        )
        self.assertEqual(integration.region_name, "us-east-1")
        self.assertEqual(integration.profile_name, "test-profile")

    def test_integration_with_kwargs(self):
        """Test SecretsManagerIntegration with additional kwargs"""
        integration = SecretsManagerIntegration(
            region_name="us-west-2",
            profile_name="prod-profile",
            endpoint_url="https://secretsmanager.us-west-2.amazonaws.com"
        )
        self.assertEqual(integration.region_name, "us-west-2")
        self.assertEqual(integration.kwargs.get("endpoint_url"), "https://secretsmanager.us-west-2.amazonaws.com")

    def test_client_property(self):
        """Test client property getter"""
        integration = SecretsManagerIntegration()
        # Should not raise if boto3 is mocked
        # Client will be None if not properly initialized in test

    def test_metrics_initialization(self):
        """Test metrics are initialized"""
        integration = SecretsManagerIntegration()
        self.assertIn("api_calls", integration._metrics)
        self.assertIn("errors", integration._metrics)
        self.assertEqual(integration._metrics["cache_hits"], 0)
        self.assertEqual(integration._metrics["cache_misses"], 0)

    def test_cache_ttl_default(self):
        """Test default cache TTL"""
        integration = SecretsManagerIntegration()
        self.assertEqual(integration._cache_ttl, 300)


class TestSecretManagementOperations(unittest.TestCase):
    """Test secret management operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_create_secret_string(self):
        """Test creating a string secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.create_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test-secret",
            "Name": "test-secret",
            "VersionId": "v12345678-1234-1234-1234-123456789012",
            "CreatedDate": datetime.now()
        }

        result = integration.create_secret(
            name="test-secret",
            secret_value="my-secret-value",
            description="Test secret"
        )

        self.assertEqual(result["status"], "created")
        self.assertEqual(result["secret_name"], "test-secret")
        self.mock_client.create_secret.assert_called_once()

    def test_create_secret_binary(self):
        """Test creating a binary secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.create_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test-binary",
            "Name": "test-binary",
            "VersionId": "v123"
        }

        result = integration.create_secret(
            name="test-binary",
            secret_value=b"binary-secret-data",
            description="Binary secret"
        )

        call_kwargs = self.mock_client.create_secret.call_args[1]
        self.assertIn("SecretBinary", call_kwargs)

    def test_create_secret_with_tags(self):
        """Test creating a secret with tags"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.create_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test",
            "VersionId": "v123"
        }

        result = integration.create_secret(
            name="test-secret",
            secret_value="value",
            tags={"Environment": "Production", "Application": "Web"}
        )

        call_kwargs = self.mock_client.create_secret.call_args[1]
        self.assertEqual(len(call_kwargs["Tags"]), 2)

    def test_create_secret_with_replica_regions(self):
        """Test creating a secret with replica regions"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.create_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test",
            "VersionId": "v123"
        }

        result = integration.create_secret(
            name="test-secret",
            secret_value="value",
            add_replica_regions=[
                {"Region": "us-west-2"},
                {"Region": "eu-west-1"}
            ]
        )

        call_kwargs = self.mock_client.create_secret.call_args[1]
        self.assertEqual(len(call_kwargs["AddReplicaRegions"]), 2)

    def test_create_secret_force_overwrite(self):
        """Test creating a secret with force overwrite"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.create_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test",
            "VersionId": "v123"
        }

        result = integration.create_secret(
            name="test-secret",
            secret_value="new-value",
            force_overwrite=True
        )

        call_kwargs = self.mock_client.create_secret.call_args[1]
        self.assertTrue(call_kwargs["ForceOverwriteReplicaSecret"])

    def test_get_secret_string(self):
        """Test getting a string secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v123",
            "VersionStages": ["AWSCURRENT"],
            "SecretString": "my-secret-value",
            "CreatedDate": datetime.now()
        }

        result = integration.get_secret("test-secret")

        self.assertEqual(result["status"], "retrieved")
        self.assertEqual(result["secret_value"], "my-secret-value")

    def test_get_secret_binary(self):
        """Test getting a binary secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v123",
            "SecretBinary": b"binary-data"
        }

        result = integration.get_secret("test-secret")

        self.assertIn("secret_binary", result)

    def test_get_secret_specific_version(self):
        """Test getting a secret with specific version"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v-old",
            "SecretString": "old-value"
        }

        result = integration.get_secret("test-secret", version_id="v-old")

        call_kwargs = self.mock_client.get_secret_value.call_args[1]
        self.assertEqual(call_kwargs["VersionId"], "v-old")

    def test_get_secret_specific_stage(self):
        """Test getting a secret with specific stage"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_secret_value.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v123",
            "VersionStages": ["AWSPENDING"],
            "SecretString": "pending-value"
        }

        result = integration.get_secret("test-secret", version_stage="AWSPENDING")

        call_kwargs = self.mock_client.get_secret_value.call_args[1]
        self.assertEqual(call_kwargs["VersionStage"], "AWSPENDING")

    def test_get_secret_force_refresh(self):
        """Test getting a secret with force refresh (skip cache)"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        # First call returns one value
        self.mock_client.get_secret_value.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v123",
            "SecretString": "value"
        }

        result = integration.get_secret("test-secret", force_refresh=True)

        self.mock_client.get_secret_value.assert_called()

    def test_get_secret_not_found(self):
        """Test getting a non-existent secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        error_response = {"Error": {"Code": "ResourceNotFoundException", "Message": "Secret not found"}}
        self.mock_client.get_secret_value.side_effect = Exception(error_response["Error"]["Code"])

        with self.assertRaises(ValueError):
            integration.get_secret("non-existent-secret")

    def test_update_secret_string(self):
        """Test updating a string secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.update_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v-new-123"
        }

        result = integration.update_secret(
            name="test-secret",
            new_secret_value="new-secret-value"
        )

        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["version_id"], "v-new-123")

    def test_update_secret_binary(self):
        """Test updating a binary secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.update_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v-new"
        }

        result = integration.update_secret(
            name="test-secret",
            new_secret_value=b"new-binary-data"
        )

        call_kwargs = self.mock_client.update_secret.call_args[1]
        self.assertIn("SecretBinary", call_kwargs)

    def test_update_secret_with_description(self):
        """Test updating secret description"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.update_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v123"
        }

        result = integration.update_secret(
            name="test-secret",
            new_secret_value="value",
            new_description="New description"
        )

        call_kwargs = self.mock_client.update_secret.call_args[1]
        self.assertEqual(call_kwargs["Description"], "New description")

    def test_update_secret_with_kms_key(self):
        """Test updating secret with new KMS key"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.update_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionId": "v123"
        }

        result = integration.update_secret(
            name="test-secret",
            new_secret_value="value",
            kms_key_id="new-kms-key"
        )

        call_kwargs = self.mock_client.update_secret.call_args[1]
        self.assertEqual(call_kwargs["KmsKeyId"], "new-kms-key")

    def test_delete_secret_default_recovery(self):
        """Test deleting a secret with default recovery window"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.delete_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "DeletionDate": "2024-02-01T00:00:00.000Z"
        }

        result = integration.delete_secret("test-secret")

        self.assertEqual(result["status"], "simulated")

    def test_delete_secret_custom_recovery(self):
        """Test deleting a secret with custom recovery window"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.delete_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "DeletionDate": "2024-02-10T00:00:00.000Z"
        }

        result = integration.delete_secret("test-secret", recovery_window_days=7)

        call_kwargs = self.mock_client.delete_secret.call_args[1]
        self.assertEqual(call_kwargs["RecoveryWindowDays"], 7)

    def test_delete_secret_force(self):
        """Test force deleting a secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.delete_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test"
        }

        result = integration.delete_secret("test-secret", force_delete_without_recovery=True)

        call_kwargs = self.mock_client.delete_secret.call_args[1]
        self.assertTrue(call_kwargs["ForceDeleteWithoutRecovery"])


class TestSecretVersionOperations(unittest.TestCase):
    """Test secret version operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_list_secret_versions(self):
        """Test listing secret versions"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.list_secret_version_ids.return_value = {
            "Versions": [
                {
                    "VersionId": "v1",
                    "VersionStages": ["AWSCURRENT"],
                    "CreatedDate": datetime.now()
                },
                {
                    "VersionId": "v2",
                    "VersionStages": ["AWSPREVIOUS"],
                    "CreatedDate": datetime.now()
                }
            ]
        }

        versions = integration.list_secret_versions("test-secret")

        self.assertEqual(len(versions), 2)

    def test_get_secret_version_stages(self):
        """Test getting version stages for a secret version"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.describe_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "VersionIdsToStages": {
                "v1": ["AWSCURRENT"],
                "v2": ["AWSPREVIOUS", "AWSCURRENT"]
            }
        }

        stages = integration.get_secret_version_stages("test-secret", "v1")

        self.assertEqual(stages["v1"], ["AWSCURRENT"])


class TestSecretRotationOperations(unittest.TestCase):
    """Test secret rotation operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_enable_rotation(self):
        """Test enabling rotation"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.rotate_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789:function:my-function",
            "RotationStarted": True
        }

        result = integration.enable_rotation(
            secret_name="test-secret",
            rotation_lambda_arn="arn:aws:lambda:us-east-1:123456789:function:my-function"
        )

        self.assertTrue(result["RotationStarted"])

    def test_enable_rotation_automatically(self):
        """Test enabling rotation with automatic schedule"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.rotate_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "RotationStarted": True
        }

        result = integration.enable_rotation_automatically(
            secret_name="test-secret",
            automatically_after_days=30
        )

        call_kwargs = self.mock_client.rotate_secret.call_args[1]
        self.assertEqual(call_kwargs["RotationLambdaARN"], "arn:aws:lambda:us-east-1:123456789:function:AWSSECRET-rotation-lambda")

    def test_disable_rotation(self):
        """Test disabling rotation"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.rotate_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "RotationEnabled": False
        }

        result = integration.disable_rotation("test-secret")

        self.assertFalse(result["RotationEnabled"])

    def test_cancel_rotation(self):
        """Test cancelling an in-progress rotation"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.rotate_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test"
        }

        result = integration.cancel_rotation("test-secret")

        self.mock_client.rotate_secret.assert_called_once()

    def test_get_rotation_status(self):
        """Test getting rotation status"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.describe_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test-secret",
            "RotationEnabled": True,
            "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789:function:my-function",
            "LastRotatedDate": datetime.now(),
            "LastChangedDate": datetime.now(),
            "NextRotationDate": datetime.now() + timedelta(days=30)
        }

        status = integration.get_rotation_status("test-secret")

        self.assertTrue(status["RotationEnabled"])
        self.assertIn("NextRotationDate", status)


class TestSecretReplicationOperations(unittest.TestCase):
    """Test secret replication operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_replicate_secret_to_region(self):
        """Test replicating secret to another region"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.replicate_secret_from_primary.return_value = {
            "ARN": "arn:aws:secretsmanager:us-west-2:123456789:secret:test",
            "ReplicationStatus": [
                {"Region": "us-west-2", "Status": "InSync"}
            ]
        }

        result = integration.replicate_secret_to_region(
            secret_id="test-secret",
            target_region="us-west-2",
            kms_key_id="kms-key-usw2"
        )

        self.assertIn("ReplicationStatus", result)

    def test_remove_regional_replication(self):
        """Test removing regional replication"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.stop_replication_to_replica.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test"
        }

        result = integration.remove_regional_replication("test-secret")

        self.assertIn("ARN", result)

    def test_get_replication_status(self):
        """Test getting replication status"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.describe_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "ReplicationStatus": [
                {"Region": "us-west-2", "Status": "InSync", "KmsKeyId": "kms-key-usw2"},
                {"Region": "eu-west-1", "Status": "InProgress"}
            ]
        }

        status = integration.get_replication_status("test-secret")

        self.assertEqual(len(status), 2)


class TestSecretPolicyOperations(unittest.TestCase):
    """Test secret policy operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_put_secret_policy(self):
        """Test putting a resource policy"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.put_resource_policy.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test"
        }

        policy_json = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789:root"},"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}'

        result = integration.put_secret_policy("test-secret", policy_json)

        self.assertIn("ARN", result)
        self.mock_client.put_resource_policy.assert_called_once()

    def test_get_secret_policy(self):
        """Test getting a resource policy"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_resource_policy.return_value = {
            "ResourcePolicy": '{"Version":"2012-10-17","Statement":[]}'
        }

        policy = integration.get_secret_policy("test-secret")

        self.assertIn("Statement", policy)

    def test_delete_secret_policy(self):
        """Test deleting a resource policy"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.delete_resource_policy.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test"
        }

        result = integration.delete_secret_policy("test-secret")

        self.assertIn("ARN", result)


class TestSecretTaggingOperations(unittest.TestCase):
    """Test secret tagging operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_tag_secret(self):
        """Test tagging a secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.tag_resource.return_value = {}

        result = integration.tag_secret(
            secret_id="test-secret",
            tags={"Environment": "Production", "Application": "Web"}
        )

        self.assertTrue(result)
        self.mock_client.tag_resource.assert_called_once()

    def test_untag_secret(self):
        """Test removing tags from a secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.untag_resource.return_value = {}

        result = integration.untag_secret(
            secret_id="test-secret",
            tag_keys=["Environment", "Application"]
        )

        self.assertTrue(result)

    def test_list_secret_tags(self):
        """Test listing secret tags"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.list_tags_for_resource.return_value = {
            "Tags": [
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Application", "Value": "Web"}
            ]
        }

        tags = integration.list_secret_tags("test-secret")

        self.assertEqual(len(tags), 2)
        self.assertEqual(tags["Environment"], "Production")


class TestSecretQueryOperations(unittest.TestCase):
    """Test secret query operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_list_secrets(self):
        """Test listing secrets"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.list_secrets.return_value = {
            "SecretList": [
                {
                    "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test1",
                    "Name": "test-secret-1",
                    "Description": "First test secret"
                },
                {
                    "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test2",
                    "Name": "test-secret-2",
                    "Description": "Second test secret"
                }
            ]
        }

        secrets = integration.list_secrets(max_results=10)

        self.assertEqual(len(secrets), 2)

    def test_list_secrets_with_filter(self):
        """Test listing secrets with filter"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.list_secrets.return_value = {
            "SecretList": [
                {
                    "Name": "prod-secret",
                    "Tags": [{"Key": "Environment", "Value": "Production"}]
                }
            ]
        }

        secrets = integration.list_secrets(
            filter_key="tag-key",
            filter_value="Environment"
        )

        call_kwargs = self.mock_client.list_secrets.call_args[1]
        self.assertEqual(call_kwargs["Filter"][0]["Key"], "tag-key")

    def test_search_secrets(self):
        """Test searching secrets"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.list_secrets.return_value = {
            "SecretList": [
                {"Name": "database-password", "Description": "Database credentials"}
            ]
        }

        results = integration.search_secrets("database")

        self.assertEqual(len(results), 1)


class TestRandomSecretGeneration(unittest.TestCase):
    """Test random secret generation"""

    def test_generate_random_secret(self):
        """Test generating a random secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "Abc123!@#$%",
            "RandomBytes": b"random-bytes"
        }

        result = integration.generate_random_secret(
            password_length=32,
            exclude_punctuation=False,
            exclude_uppercase=False
        )

        self.assertIn("RandomPassword", result)

    def test_generate_random_secret_string(self):
        """Test generating a random secret string"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "abc123ABC!@#"
        }

        result = integration.generate_random_secret_string(
            length=20,
            include_special_chars=True
        )

        self.assertIn("RandomPassword", result)


class TestDatabaseSecretGeneration(unittest.TestCase):
    """Test database secret generation"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_generate_database_secret_mysql(self):
        """Test generating a MySQL database secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "GeneratedP@ssw0rd!"
        }

        config = DatabaseSecretConfig(
            db_type="mysql",
            db_host="mysql.example.com",
            db_port=3306,
            db_name="mydb",
            db_username="admin"
        )

        secret = integration.generate_database_secret(config)

        self.assertIn("username", secret)
        self.assertIn("password", secret)
        self.assertIn("host", secret)
        self.assertEqual(secret["username"], "admin")
        self.assertEqual(secret["host"], "mysql.example.com:3306")

    def test_generate_database_secret_postgres(self):
        """Test generating a PostgreSQL database secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "GeneratedP@ssw0rd!"
        }

        config = DatabaseSecretConfig(
            db_type="postgres",
            db_host="postgres.example.com",
            db_port=5432,
            db_name="production",
            db_username="dbuser"
        )

        secret = integration.generate_database_secret(config)

        self.assertEqual(secret["username"], "dbuser")

    def test_generate_database_secret_oracle(self):
        """Test generating an Oracle database secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "GeneratedP@ssw0rd!"
        }

        config = DatabaseSecretConfig(
            db_type="oracle",
            db_host="oracle.example.com",
            db_port=1521,
            db_name="ORCL",
            db_username="system"
        )

        secret = integration.generate_database_secret(config)

        self.assertEqual(secret["username"], "system")

    def test_generate_database_secret_sqlserver(self):
        """Test generating a SQL Server database secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "GeneratedP@ssw0rd!"
        }

        config = DatabaseSecretConfig(
            db_type="sqlserver",
            db_host="sqlserver.example.com",
            db_port=1433,
            db_name="mydb",
            db_username="sa"
        )

        secret = integration.generate_database_secret(config)

        self.assertEqual(secret["dbname"], "mydb")

    def test_generate_database_secret_aurora(self):
        """Test generating an Aurora database secret"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.get_random_password.return_value = {
            "RandomPassword": "GeneratedP@ssw0rd!"
        }

        config = DatabaseSecretConfig(
            db_type="aurora",
            db_host="aurora.example.com",
            db_port=3306,
            db_name="mydb",
            db_username="admin"
        )

        secret = integration.generate_database_secret(config)

        self.assertIn("engine", secret)


class TestSecretBatchOperations(unittest.TestCase):
    """Test batch operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_batch_get_secret_values(self):
        """Test batch getting secret values"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.batch_get_secret_value.return_value = {
            "SecretValues": [
                {"Name": "secret1", "SecretString": "value1"},
                {"Name": "secret2", "SecretString": "value2"}
            ]
        }

        result = integration.batch_get_secret_values(["secret1", "secret2"])

        self.assertEqual(len(result), 2)

    def test_batch_create_secrets(self):
        """Test batch creating secrets"""
        integration = SecretsManagerIntegration()
        integration._client = self.mock_client

        self.mock_client.create_secret.return_value = {
            "ARN": "arn:aws:secretsmanager:us-east-1:123456789:secret:test",
            "Name": "test",
            "VersionId": "v123"
        }

        secrets_to_create = [
            {"Name": "secret1", "SecretString": "value1"},
            {"Name": "secret2", "SecretString": "value2"}
        ]

        results = integration.batch_create_secrets(secrets_to_create)

        self.assertEqual(len(results), 2)


class TestCacheOperations(unittest.TestCase):
    """Test cache operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_cache_initialization(self):
        """Test cache is initialized"""
        integration = SecretsManagerIntegration()
        self.assertEqual(len(integration._secret_cache), 0)
        self.assertEqual(len(integration._last_cache_update), 0)

    def test_update_cache(self):
        """Test updating cache"""
        integration = SecretsManagerIntegration()
        integration._update_cache("test-secret", {"secret_value": "value"})

        self.assertIn("test-secret", integration._secret_cache)

    def test_get_cached_secret(self):
        """Test getting cached secret"""
        integration = SecretsManagerIntegration()
        integration._update_cache("test-secret", {"secret_value": "cached-value"})

        cached = integration._get_cached("test-secret")

        self.assertIsNotNone(cached)
        self.assertEqual(cached["secret_value"], "cached-value")

    def test_cache_invalidation(self):
        """Test cache invalidation"""
        integration = SecretsManagerIntegration()
        integration._update_cache("test-secret", {"secret_value": "value"})

        integration._invalidate_cache("test-secret")

        self.assertNotIn("test-secret", integration._secret_cache)

    def test_cache_expiration(self):
        """Test cache expiration"""
        integration = SecretsManagerIntegration()
        integration._cache_ttl = 0  # Immediate expiration

        integration._update_cache("test-secret", {"secret_value": "value"})

        cached = integration._get_cached("test-secret")

        self.assertIsNone(cached)


class TestMetricsOperations(unittest.TestCase):
    """Test metrics operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def test_log_metric(self):
        """Test logging a metric"""
        integration = SecretsManagerIntegration()

        integration._log_metric("test_operation", 5)

        self.assertEqual(integration._metrics["api_calls"]["test_operation"], 5)

    def test_log_error(self):
        """Test logging an error"""
        integration = SecretsManagerIntegration()

        integration._log_error("TestError", "Something went wrong")

        self.assertEqual(integration._metrics["errors"]["TestError"], 1)

    def test_cache_hit_metric(self):
        """Test cache hit metric"""
        integration = SecretsManagerIntegration()

        integration._update_cache("test", {"value": "test"})
        integration._get_cached("test")

        self.assertEqual(integration._metrics["cache_hits"], 1)

    def test_cache_miss_metric(self):
        """Test cache miss metric"""
        integration = SecretsManagerIntegration()

        integration._get_cached("nonexistent")

        self.assertEqual(integration._metrics["cache_misses"], 1)


class TestSecretsManagerIntegrationNoBoto3(unittest.TestCase):
    """Test SecretsManagerIntegration without boto3 available"""

    def test_create_secret_without_boto3(self):
        """Test creating secret when boto3 is not available"""
        integration = SecretsManagerIntegration()
        # Force no boto3 path
        import sys
        original_boto3 = sys.modules.get('boto3')
        try:
            if 'boto3' in sys.modules:
                del sys.modules['boto3']

            integration = SecretsManagerIntegration()
            result = integration.create_secret("test", "value")

            # Should return simulated response
            self.assertEqual(result["status"], "simulated")
        finally:
            if original_boto3:
                sys.modules['boto3'] = original_boto3

    def test_get_secret_without_boto3(self):
        """Test getting secret when boto3 is not available"""
        import sys
        original_boto3 = sys.modules.get('boto3')
        try:
            if 'boto3' in sys.modules:
                del sys.modules['boto3']

            integration = SecretsManagerIntegration()
            result = integration.get_secret("test")

            self.assertEqual(result["status"], "simulated")
        finally:
            if original_boto3:
                sys.modules['boto3'] = original_boto3


if __name__ == '__main__':
    unittest.main()
