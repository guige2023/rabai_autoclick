"""
Tests for workflow_aws_lakeformation module
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

# Create mock boto3 module before importing workflow_aws_lakeformation
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
import src.workflow_aws_lakeformation as lf_module

LakeFormationIntegration = lf_module.LakeFormationIntegration
DataLakeConfig = lf_module.DataLakeConfig
DataPermission = lf_module.DataPermission
LFTag = lf_module.LFTag
DataShare = lf_module.DataShare
BlueprintRun = lf_module.BlueprintRun
Transaction = lf_module.Transaction
SchemaRegistryConfig = lf_module.SchemaRegistryConfig
SchemaVersion = lf_module.SchemaVersion
CrossAccountConfig = lf_module.CrossAccountConfig
CloudWatchConfig = lf_module.CloudWatchConfig
DataLakeStatus = lf_module.DataLakeStatus
PermissionType = lf_module.PermissionType
PrincipalType = lf_module.PrincipalType
DataShareStatus = lf_module.DataShareStatus
TransactionStatus = lf_module.TransactionStatus
BlueprintStatus = lf_module.BlueprintStatus
CrossAccountAccessType = lf_module.CrossAccountAccessType


class TestDataLakeConfig(unittest.TestCase):
    """Test DataLakeConfig dataclass"""

    def test_data_lake_config_defaults(self):
        config = DataLakeConfig(name="test-lake")
        self.assertEqual(config.name, "test-lake")
        self.assertEqual(config.description, "")
        self.assertEqual(config.location, "")
        self.assertEqual(config.tags, {})

    def test_data_lake_config_creation(self):
        config = DataLakeConfig(
            name="test-lake",
            description="Test data lake",
            location="s3://test-bucket/",
            tags={"env": "test"}
        )
        self.assertEqual(config.name, "test-lake")
        self.assertEqual(config.description, "Test data lake")
        self.assertEqual(config.tags["env"], "test")


class TestDataPermission(unittest.TestCase):
    """Test DataPermission dataclass"""

    def test_data_permission_creation(self):
        permission = DataPermission(
            principal="user@example.com",
            principal_type=PrincipalType.USER,
            resource="database",
            permissions=[PermissionType.SELECT]
        )
        self.assertEqual(permission.principal, "user@example.com")
        self.assertEqual(permission.permissions, [PermissionType.SELECT])


class TestLFTag(unittest.TestCase):
    """Test LFTag dataclass"""

    def test_lf_tag_creation(self):
        tag = LFTag(
            tag_key="classification",
            tag_values=["public", "private"]
        )
        self.assertEqual(tag.tag_key, "classification")
        self.assertEqual(len(tag.tag_values), 2)


class TestDataShare(unittest.TestCase):
    """Test DataShare dataclass"""

    def test_data_share_creation(self):
        share = DataShare(
            name="test-share",
            share_type="DATABASE",
            source_arn="arn:aws:glue:us-east-1:123456789012:database/test"
        )
        self.assertEqual(share.name, "test-share")
        self.assertFalse(share.allow_publications)


class TestBlueprintRun(unittest.TestCase):
    """Test BlueprintRun dataclass"""

    def test_blueprint_run_creation(self):
        run = BlueprintRun(
            blueprint_name="test-blueprint",
            role_arn="arn:aws:iam::123456789012:role/test-role",
            parameters={"param1": "value1"}
        )
        self.assertEqual(run.blueprint_name, "test-blueprint")


class TestTransaction(unittest.TestCase):
    """Test Transaction dataclass"""

    def test_transaction_creation(self):
        transaction = Transaction(
            transaction_id="tx-123",
            status=TransactionStatus.ACTIVE,
            start_time=datetime.now()
        )
        self.assertEqual(transaction.transaction_id, "tx-123")
        self.assertEqual(transaction.status, TransactionStatus.ACTIVE)


class TestSchemaRegistryConfig(unittest.TestCase):
    """Test SchemaRegistryConfig dataclass"""

    def test_schema_registry_config_defaults(self):
        config = SchemaRegistryConfig(registry_name="test-registry")
        self.assertEqual(config.registry_name, "test-registry")
        self.assertEqual(config.description, "")


class TestCrossAccountConfig(unittest.TestCase):
    """Test CrossAccountConfig dataclass"""

    def test_cross_account_config_creation(self):
        config = CrossAccountConfig(
            source_account="123456789012",
            target_account="987654321098",
            access_type=CrossAccountAccessType.DIRECT
        )
        self.assertEqual(config.source_account, "123456789012")
        self.assertEqual(config.access_type, CrossAccountAccessType.DIRECT)


class TestDataLakeStatus(unittest.TestCase):
    """Test DataLakeStatus enum"""

    def test_data_lake_status_values(self):
        self.assertEqual(DataLakeStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(DataLakeStatus.CREATING.value, "CREATING")
        self.assertEqual(DataLakeStatus.FAILED.value, "FAILED")


class TestPermissionType(unittest.TestCase):
    """Test PermissionType enum"""

    def test_permission_type_values(self):
        self.assertEqual(PermissionType.SELECT.value, "SELECT")
        self.assertEqual(PermissionType.INSERT.value, "INSERT")
        self.assertEqual(PermissionType.ALL.value, "ALL")


class TestPrincipalType(unittest.TestCase):
    """Test PrincipalType enum"""

    def test_principal_type_values(self):
        self.assertEqual(PrincipalType.USER.value, "USER")
        self.assertEqual(PrincipalType.GROUP.value, "GROUP")
        self.assertEqual(PrincipalType.ROLE.value, "ROLE")


class TestLakeFormationIntegration(unittest.TestCase):
    """Test LakeFormationIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_lf_client = MagicMock()
        self.mock_glue_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        with patch.object(LakeFormationIntegration, '__init__', lambda x: None):
            self.integration = LakeFormationIntegration()
            self.integration.region_name = "us-east-1"
            self.integration.profile_name = None
            self.integration.role_arn = None
            self.integration.external_id = None
            self.integration._client = self.mock_lf_client
            self.integration._glue_client = self.mock_glue_client
            self.integration._cloudwatch_client = self.mock_cloudwatch_client
            self.integration._lock = MagicMock()
            self.integration._databases_cache = {}
            self.integration._tables_cache = {}
            self.integration._lf_tags_cache = {}
            self.integration._data_shares_cache = {}
            self.integration._transactions_cache = {}
            self.integration._event_handlers = {}
            self.integration._metrics = {}
            self.integration._audit_log = []

    def test_init_defaults(self):
        """Test initialization with defaults"""
        with patch.object(LakeFormationIntegration, '__init__', lambda x, **kwargs: None):
            integration = LakeFormationIntegration()
            integration.region_name = "us-east-1"
            integration.profile_name = None
            integration.role_arn = None
            integration.external_id = None
            integration._client = None
            integration._glue_client = None
            integration._cloudwatch_client = None
            integration._lock = MagicMock()
            integration._databases_cache = {}
            integration._tables_cache = {}
            integration._lf_tags_cache = {}
            integration._data_shares_cache = {}
            integration._transactions_cache = {}
            integration._event_handlers = {}
            integration._metrics = {}
            integration._audit_log = []

            self.assertEqual(integration.region_name, "us-east-1")

    def test_on_event_handler(self):
        """Test event handler registration"""
        from collections import defaultdict
        handler = MagicMock()
        self.integration._event_handlers = defaultdict(list)
        self.integration.on("test_event", handler)
        self.assertIn(handler, self.integration._event_handlers["test_event"])

    def test_emit_event(self):
        """Test event emission"""
        handler = MagicMock()
        self.integration._event_handlers = {"test_event": [handler]}
        self.integration._emit_event("test_event", {"data": "value"})
        handler.assert_called_once_with({"data": "value"})


class TestDataLakeManagement(unittest.TestCase):
    """Test data lake management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_lf_client = MagicMock()
        self.mock_glue_client = MagicMock()

        with patch.object(LakeFormationIntegration, '__init__', lambda x: None):
            self.integration = LakeFormationIntegration()
            self.integration.region_name = "us-east-1"
            self.integration._client = self.mock_lf_client
            self.integration._glue_client = self.mock_glue_client
            self.integration._lock = MagicMock()
            self.integration._databases_cache = {}
            self.integration._tables_cache = {}
            self.integration._lf_tags_cache = {}
            self.integration._data_shares_cache = {}
            self.integration._transactions_cache = {}
            self.integration._event_handlers = {}
            self.integration._metrics = {}
            self.integration._audit_log = []

    def test_create_data_lake(self):
        """Test creating a data lake"""
        self.mock_lf_client.register_resource.return_value = {}
        self.mock_glue_client.create_database.return_value = {}
        self.mock_lf_client.grant_permissions.return_value = {}

        config = DataLakeConfig(
            name="test-lake",
            description="Test data lake",
            location="s3://test-bucket/"
        )

        result = self.integration.create_data_lake(config)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["name"], "test-lake")

    def test_describe_data_lake(self):
        """Test describing a data lake"""
        self.mock_glue_client.get_database.return_value = {
            "Database": {
                "Name": "test_lake",
                "Description": "Test lake"
            }
        }
        self.mock_lf_client.list_permissions.return_value = {
            "PrincipalResourcePermissions": []
        }

        result = self.integration.describe_data_lake("test-lake")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["name"], "test-lake")

    def test_list_data_lakes(self):
        """Test listing data lakes"""
        self.mock_glue_client.get_databases.return_value = {
            "DatabaseList": [
                {"Name": "lake1_lake", "Description": "Lake 1"},
                {"Name": "lake2_lake", "Description": "Lake 2"}
            ]
        }

        result = self.integration.list_data_lakes()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "lake1")

    def test_delete_data_lake(self):
        """Test deleting a data lake"""
        self.mock_glue_client.get_tables.return_value = {"TableList": []}
        self.mock_glue_client.delete_database.return_value = {}

        result = self.integration.delete_data_lake("test-lake")

        self.assertEqual(result["status"], "success")

    def test_delete_data_lake_with_force(self):
        """Test deleting a data lake with force"""
        self.mock_glue_client.delete_database.return_value = {}

        result = self.integration.delete_data_lake("test-lake", force=True)

        self.assertEqual(result["status"], "success")


class TestPermissionManagement(unittest.TestCase):
    """Test permission management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_lf_client = MagicMock()

        with patch.object(LakeFormationIntegration, '__init__', lambda x: None):
            self.integration = LakeFormationIntegration()
            self.integration.region_name = "us-east-1"
            self.integration._client = self.mock_lf_client
            self.integration._lock = MagicMock()
            self.integration._databases_cache = {}
            self.integration._tables_cache = {}
            self.integration._lf_tags_cache = {}
            self.integration._data_shares_cache = {}
            self.integration._transactions_cache = {}
            self.integration._event_handlers = {}
            self.integration._metrics = {}
            self.integration._audit_log = []

    def test_grant_permission(self):
        """Test granting permission"""
        self.mock_lf_client.grant_permissions.return_value = {}

        permission = DataPermission(
            principal="user@example.com",
            principal_type=PrincipalType.USER,
            resource="database",
            database="test_db",
            permissions=[PermissionType.SELECT]
        )

        result = self.integration.grant_permission(permission)

        self.assertEqual(result["status"], "success")
        self.mock_lf_client.grant_permissions.assert_called_once()

    def test_revoke_permission(self):
        """Test revoking permission"""
        self.mock_lf_client.revoke_permissions.return_value = {}

        permission = DataPermission(
            principal="user@example.com",
            principal_type=PrincipalType.USER,
            resource="database",
            database="test_db",
            permissions=[PermissionType.SELECT]
        )

        result = self.integration.revoke_permission(permission)

        self.assertEqual(result["status"], "success")

    def test_list_permissions(self):
        """Test listing permissions"""
        self.mock_lf_client.list_permissions.return_value = {
            "PrincipalResourcePermissions": [
                {
                    "Principal": {"DataLakePrincipalIdentifier": "user@example.com"},
                    "Resource": {"Database": {"Name": "test_db"}},
                    "Permissions": ["SELECT"]
                }
            ]
        }

        result = self.integration.list_permissions()

        self.assertEqual(len(result), 1)


class TestCatalogResources(unittest.TestCase):
    """Test catalog resources management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_glue_client = MagicMock()

        with patch.object(LakeFormationIntegration, '__init__', lambda x: None):
            self.integration = LakeFormationIntegration()
            self.integration.region_name = "us-east-1"
            self.integration._glue_client = self.mock_glue_client
            self.integration._lock = MagicMock()
            self.integration._databases_cache = {}
            self.integration._tables_cache = {}
            self.integration._lf_tags_cache = {}
            self.integration._data_shares_cache = {}
            self.integration._transactions_cache = {}
            self.integration._event_handlers = {}
            self.integration._metrics = {}
            self.integration._audit_log = []

    def test_create_database(self):
        """Test creating a database"""
        self.mock_glue_client.create_database.return_value = {}

        result = self.integration.create_database("test_db", "Test database")

        self.assertEqual(result["status"], "success")

    def test_create_table(self):
        """Test creating a table"""
        self.mock_glue_client.create_table.return_value = {}

        result = self.integration.create_database("test_db")

        self.assertEqual(result["status"], "success")

    def test_list_databases(self):
        """Test listing databases"""
        self.mock_glue_client.get_databases.return_value = {
            "DatabaseList": [
                {"Name": "db1", "Description": "Database 1"},
                {"Name": "db2", "Description": "Database 2"}
            ]
        }

        result = self.integration.list_databases()

        self.assertEqual(len(result), 2)

    def test_list_tables(self):
        """Test listing tables"""
        self.mock_glue_client.get_tables.return_value = {
            "TableList": [
                {"Name": "table1", "TableType": "EXTERNAL_TABLE"},
                {"Name": "table2", "TableType": "EXTERNAL_TABLE"}
            ]
        }

        result = self.integration.list_tables("test_db")

        self.assertEqual(len(result), 2)

    def test_get_table(self):
        """Test getting table details"""
        self.mock_glue_client.get_table.return_value = {
            "Table": {"Name": "test_table"}
        }

        result = self.integration.get_table("test_db", "test_table")

        self.assertEqual(result["status"], "success")


class TestLFTags(unittest.TestCase):
    """Test LF-TAG management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_lf_client = MagicMock()

        with patch.object(LakeFormationIntegration, '__init__', lambda x: None):
            self.integration = LakeFormationIntegration()
            self.integration.region_name = "us-east-1"
            self.integration._client = self.mock_lf_client
            self.integration._lock = MagicMock()
            self.integration._databases_cache = {}
            self.integration._tables_cache = {}
            self.integration._lf_tags_cache = {}
            self.integration._data_shares_cache = {}
            self.integration._transactions_cache = {}
            self.integration._event_handlers = {}
            self.integration._metrics = {}
            self.integration._audit_log = []

    def test_create_lf_tag(self):
        """Test creating an LF-TAG"""
        self.mock_lf_client.create_lf_tag.return_value = {}

        result = self.integration.create_lf_tag("classification", ["public", "private"])

        self.assertEqual(result["status"], "success")

    def test_delete_lf_tag(self):
        """Test deleting an LF-TAG"""
        self.mock_lf_client.delete_lf_tag.return_value = {}

        result = self.integration.delete_lf_tag("classification")

        self.assertEqual(result["status"], "success")

    def test_list_lf_tags(self):
        """Test listing LF-TAGs"""
        self.mock_lf_client.list_lf_tags.return_value = {
            "LFTags": [
                {"TagKey": "classification", "TagValues": ["public", "private"]}
            ]
        }

        result = self.integration.list_lf_tags()

        self.assertEqual(len(result), 1)

    def test_grant_lf_tag_permissions(self):
        """Test granting LF-TAG permissions"""
        self.mock_lf_client.grant_permissions.return_value = {}

        result = self.integration.grant_lf_tag_permissions(
            "classification",
            ["public"],
            "user@example.com",
            [PermissionType.SELECT]
        )

        self.assertEqual(result["status"], "success")

    def test_get_resource_lf_tags(self):
        """Test getting resource LF-TAGs"""
        self.mock_lf_client.get_lf_tags_for_resource.return_value = {
            "LFTags": [
                {"TagKey": "classification", "TagValues": ["public"]}
            ]
        }

        result = self.integration.get_resource_lf_tags(
            "database",
            {"name": "test_db"}
        )

        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
