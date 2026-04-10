"""
Tests for workflow_aws_qldb module
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

# Create mock boto3 module before importing workflow_aws_qldb
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

# Import the module
import src.workflow_aws_qldb as _qldb_module

# Extract classes
QLDBIntegration = _qldb_module.QLDBIntegration
LedgerStatus = _qldb_module.LedgerStatus
TableStatus = _qldb_module.TableStatus
IndexStatus = _qldb_module.IndexStatus
ExportStatus = _qldb_module.ExportStatus
BackupStatus = _qldb_module.BackupStatus
PermissionMode = _qldb_module.PermissionMode
LedgerConfig = _qldb_module.LedgerConfig
TableConfig = _qldb_module.TableConfig
DocumentOperation = _qldb_module.DocumentOperation
ExportConfig = _qldb_module.ExportConfig
BackupConfig = _qldb_module.BackupConfig


class TestEnums(unittest.TestCase):
    """Test enum classes"""

    def test_ledger_status_values(self):
        self.assertEqual(LedgerStatus.CREATING.value, "CREATING")
        self.assertEqual(LedgerStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(LedgerStatus.DELETED.value, "DELETED")

    def test_export_status_values(self):
        self.assertEqual(ExportStatus.IN_PROGRESS.value, "IN_PROGRESS")
        self.assertEqual(ExportStatus.COMPLETED.value, "COMPLETED")

    def test_backup_status_values(self):
        self.assertEqual(BackupStatus.CREATING.value, "CREATING")
        self.assertEqual(BackupStatus.ACTIVE.value, "ACTIVE")

    def test_permission_mode_values(self):
        self.assertEqual(PermissionMode.ALLOW_ALL.value, "ALLOW_ALL")
        self.assertEqual(PermissionMode.STANDARD.value, "STANDARD")


class TestLedgerConfig(unittest.TestCase):
    """Test LedgerConfig dataclass"""

    def test_default_config(self):
        config = LedgerConfig(ledger_name="test-ledger")
        self.assertEqual(config.ledger_name, "test-ledger")
        self.assertEqual(config.permissions_mode, "STANDARD")
        self.assertEqual(config.deletion_protection, True)

    def test_custom_config(self):
        config = LedgerConfig(
            ledger_name="my-ledger",
            permissions_mode="ALLOW_ALL",
            kms_key_id="kms-key-123",
            deletion_protection=False
        )
        self.assertEqual(config.ledger_name, "my-ledger")
        self.assertEqual(config.permissions_mode, "ALLOW_ALL")
        self.assertEqual(config.deletion_protection, False)


class TestTableConfig(unittest.TestCase):
    """Test TableConfig dataclass"""

    def test_table_config_creation(self):
        config = TableConfig(
            table_name="test-table",
            ledger_name="test-ledger",
            indexes=[{"Name": "idx-1", "Status": "ACTIVE"}]
        )
        self.assertEqual(config.table_name, "test-table")
        self.assertEqual(config.ledger_name, "test-ledger")


class TestDocumentOperation(unittest.TestCase):
    """Test DocumentOperation dataclass"""

    def test_insert_operation(self):
        op = DocumentOperation(
            operation_type="insert",
            table_name="test-table",
            document={"name": "test", "value": 123}
        )
        self.assertEqual(op.operation_type, "insert")
        self.assertEqual(op.table_name, "test-table")

    def test_update_operation(self):
        op = DocumentOperation(
            operation_type="update",
            table_name="test-table",
            document_id="doc-123",
            condition="id = 'doc-123'"
        )
        self.assertEqual(op.operation_type, "update")
        self.assertEqual(op.document_id, "doc-123")


class TestExportConfig(unittest.TestCase):
    """Test ExportConfig dataclass"""

    def test_export_config_creation(self):
        config = ExportConfig(
            export_id="export-123",
            ledger_name="test-ledger",
            s3_bucket="my-bucket",
            s3_prefix="exports/",
            export_time=datetime.now()
        )
        self.assertEqual(config.export_id, "export-123")
        self.assertEqual(config.s3_bucket, "my-bucket")


class TestQLDBIntegration(unittest.TestCase):
    """Test QLDBIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()
        self.mock_qldb_session = MagicMock()
        self.mock_s3 = MagicMock()
        self.mock_cloudwatch = MagicMock()
        self.mock_iam = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            def client_side_effect(service, **kwargs):
                if service == 'qldb':
                    return self.mock_qldb
                elif service == 'qldb-session':
                    return self.mock_qldb_session
                elif service == 's3':
                    return self.mock_s3
                elif service == 'cloudwatch':
                    return self.mock_cloudwatch
                elif service == 'iam':
                    return self.mock_iam
                return MagicMock()

            mock_session.return_value.client.side_effect = client_side_effect
            self.qldb = QLDBIntegration(region_name="us-east-1", ledger_name="test-ledger")
            self.qldb._qldb = self.mock_qldb
            self.qldb._qldb_session = self.mock_qldb_session
            self.qldb._s3 = self.mock_s3
            self.qldb._cloudwatch = self.mock_cloudwatch
            self.qldb._iam = self.mock_iam

    def test_init(self):
        """Test QLDBIntegration initialization"""
        qldb = QLDBIntegration(region_name="us-west-2", ledger_name="my-ledger")
        self.assertEqual(qldb.region_name, "us-west-2")
        self.assertEqual(qldb.default_ledger_name, "my-ledger")

    def test_is_available(self):
        """Test is_available property"""
        self.assertTrue(self.qldb.is_available)


class TestLedgerManagement(unittest.TestCase):
    """Test ledger management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()
        self.mock_qldb_session = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb if s == 'qldb' else self.mock_qldb_session
            self.qldb = QLDBIntegration(region_name="us-east-1")
            self.qldb._qldb = self.mock_qldb
            self.qldb._qldb_session = self.mock_qldb_session

    def test_create_ledger(self):
        """Test creating a ledger"""
        self.mock_qldb.create_ledger.return_value = {
            "Name": "test-ledger",
            "Arn": "arn:aws:qldb:us-east-1:123456789012:ledger/test-ledger",
            "Status": "ACTIVE"
        }
        self.mock_qldb.describe_ledger.return_value = {
            "Name": "test-ledger",
            "Status": "ACTIVE"
        }

        config = LedgerConfig(ledger_name="test-ledger")
        result = self.qldb.create_ledger(config, wait_for_active=False)
        self.assertEqual(result["Name"], "test-ledger")

    def test_create_ledger_with_tags(self):
        """Test creating a ledger with tags"""
        self.mock_qldb.create_ledger.return_value = {
            "Name": "test-ledger",
            "Tags": [{"Key": "env", "Value": "test"}]
        }

        config = LedgerConfig(ledger_name="test-ledger", tags={"env": "test"})
        result = self.qldb.create_ledger(config, wait_for_active=False)
        self.assertIn("Tags", result)

    def test_describe_ledger(self):
        """Test describing a ledger"""
        self.mock_qldb.describe_ledger.return_value = {
            "Name": "test-ledger",
            "Status": "ACTIVE",
            "DeletionProtection": True
        }

        result = self.qldb.describe_ledger("test-ledger")
        self.assertEqual(result["Name"], "test-ledger")
        self.assertEqual(result["Status"], "ACTIVE")

    def test_list_ledgers(self):
        """Test listing ledgers"""
        self.mock_qldb.get_paginator.return_value.paginate.return_value = [
            {"Ledgers": [
                {"Name": "ledger-1", "Status": "ACTIVE"},
                {"Name": "ledger-2", "Status": "ACTIVE"}
            ]}
        ]

        result = self.qldb.list_ledgers()
        self.assertEqual(len(result), 2)

    def test_list_ledgers_with_filter(self):
        """Test listing ledgers with status filter"""
        self.mock_qldb.get_paginator.return_value.paginate.return_value = [
            {"Ledgers": [
                {"Name": "ledger-1", "Status": "ACTIVE"}
            ]}
        ]

        result = self.qldb.list_ledgers(filter_status="ACTIVE")
        self.assertEqual(len(result), 1)

    def test_update_ledger(self):
        """Test updating a ledger"""
        self.mock_qldb.update_ledger.return_value = {
            "Name": "test-ledger",
            "DeletionProtection": False
        }

        result = self.qldb.update_ledger("test-ledger", deletion_protection=False)
        self.assertEqual(result["DeletionProtection"], False)

    def test_delete_ledger(self):
        """Test deleting a ledger"""
        self.mock_qldb.delete_ledger.return_value = {}

        result = self.qldb.delete_ledger("test-ledger", wait_for_deletion=False)
        self.assertIn("ResponseMetadata", result)


class TestTableManagement(unittest.TestCase):
    """Test table management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()
        self.mock_qldb_session = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb if s == 'qldb' else self.mock_qldb_session
            self.qldb = QLDBIntegration(region_name="us-east-1", ledger_name="test-ledger")
            self.qldb._qldb = self.mock_qldb
            self.qldb._qldb_session = self.mock_qldb_session

    def test_create_table(self):
        """Test creating a table"""
        self.mock_qldb_session.execute_statement.return_value = {}

        result = self.qldb.create_table("test-table", "test-ledger")
        self.mock_qldb_session.execute_statement.assert_called_once()

    def test_list_tables(self):
        """Test listing tables"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": [
                    [{"ScalarValue": "table-1"}],
                    [{"ScalarValue": "table-2"}]
                ]
            }
        }

        result = self.qldb.list_tables("test-ledger")
        self.assertEqual(len(result), 2)

    def test_describe_table(self):
        """Test describing a table"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": [["table-info"]]
            }
        }

        result = self.qldb.describe_table("test-table", "test-ledger")
        self.assertIn("FirstPage", result)

    def test_drop_table(self):
        """Test dropping a table"""
        self.mock_qldb_session.execute_statement.return_value = {}

        result = self.qldb.drop_table("test-table", "test-ledger")
        self.mock_qldb_session.execute_statement.assert_called_once()

    def test_create_index(self):
        """Test creating an index"""
        self.mock_qldb_session.execute_statement.return_value = {}

        result = self.qldb.create_index("test-table", "column1", "test-ledger")
        self.mock_qldb_session.execute_statement.assert_called_once()


class TestDocumentOperations(unittest.TestCase):
    """Test document operations methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()
        self.mock_qldb_session = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb if s == 'qldb' else self.mock_qldb_session
            self.qldb = QLDBIntegration(region_name="us-east-1", ledger_name="test-ledger")
            self.qldb._qldb = self.mock_qldb
            self.qldb._qldb_session = self.mock_qldb_session

    def test_insert_document(self):
        """Test inserting a document"""
        self.mock_qldb_session.execute_statement.return_value = {
            "CommitDigest": "abc123"
        }

        result = self.qldb.insert_document(
            "test-table",
            {"name": "test", "value": 123},
            "test-ledger"
        )
        self.assertIn("document_id", result)

    def test_update_document(self):
        """Test updating a document"""
        self.mock_qldb_session.execute_statement.return_value = {}

        result = self.qldb.update_document(
            "test-table",
            "doc-123",
            {"name": "updated"},
            "test-ledger"
        )
        self.mock_qldb_session.execute_statement.assert_called()

    def test_delete_document(self):
        """Test deleting a document"""
        self.mock_qldb_session.execute_statement.return_value = {}

        result = self.qldb.delete_document("test-table", "doc-123", "test-ledger")
        self.mock_qldb_session.execute_statement.assert_called()

    def test_get_document(self):
        """Test getting a document"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": [[{"name": {"ScalarValue": "test"}}]]
            }
        }

        result = self.qldb.get_document("test-table", "doc-123", "test-ledger")
        self.assertIsInstance(result, dict)

    def test_get_revision_history(self):
        """Test getting revision history"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": [
                    [{"id": {"ScalarValue": "doc-123"}}],
                    [{"id": {"ScalarValue": "doc-123"}}]
                ]
            }
        }

        result = self.qldb.get_revision_history("test-table", "doc-123", "test-ledger")
        self.assertIsInstance(result, list)


class TestQueryOperations(unittest.TestCase):
    """Test query operations methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()
        self.mock_qldb_session = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb if s == 'qldb' else self.mock_qldb_session
            self.qldb = QLDBIntegration(region_name="us-east-1", ledger_name="test-ledger")
            self.qldb._qldb = self.mock_qldb
            self.qldb._qldb_session = self.mock_qldb_session

    def test_execute_query(self):
        """Test executing a query"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": [
                    [{"name": {"ScalarValue": "test"}}]
                ]
            }
        }

        result = self.qldb.execute_query("SELECT * FROM test-table", "test-ledger")
        self.assertIn("FirstPage", result)

    def test_query_table(self):
        """Test querying a table"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": []
            }
        }

        result = self.qldb.query_table("test-table", filter_condition="name = 'test'", ledger_name="test-ledger")
        self.assertIsInstance(result, list)

    def test_query_table_with_columns(self):
        """Test querying a table with specific columns"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": [
                    [{"name": {"ScalarValue": "test"}}]
                ]
            }
        }

        result = self.qldb.query_table("test-table", select_columns=["name"], ledger_name="test-ledger")
        self.assertIsInstance(result, list)

    def test_query_table_with_limit(self):
        """Test querying a table with limit"""
        self.mock_qldb_session.execute_statement.return_value = {
            "FirstPage": {
                "Results": []
            }
        }

        result = self.qldb.query_table("test-table", ledger_name="test-ledger", limit=10)
        self.assertIsInstance(result, list)


class TestJournalExports(unittest.TestCase):
    """Test journal export methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()
        self.mock_qldb_session = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb if s == 'qldb' else self.mock_qldb_session
            self.qldb = QLDBIntegration(region_name="us-east-1")
            self.qldb._qldb = self.mock_qldb
            self.qldb._qldb_session = self.mock_qldb_session

    def test_create_journal_export(self):
        """Test creating a journal export"""
        self.mock_qldb.create_journal_export.return_value = {
            "ExportId": "export-123",
            "LedgerName": "test-ledger"
        }

        result = self.qldb.create_journal_export(
            "test-ledger",
            "my-bucket",
            "exports/",
            datetime.now()
        )
        self.assertEqual(result["ExportId"], "export-123")

    def test_create_journal_export_with_included_forms(self):
        """Test creating a journal export with included forms"""
        self.mock_qldb.create_journal_export.return_value = {
            "ExportId": "export-123",
            "LedgerName": "test-ledger"
        }

        result = self.qldb.create_journal_export(
            "test-ledger",
            "my-bucket",
            "exports/",
            datetime.now(),
            included_forms=["JOURNAL"]
        )
        self.assertEqual(result["ExportId"], "export-123")

    def test_describe_journal_export(self):
        """Test describing a journal export"""
        self.mock_qldb.describe_journal_export.return_value = {
            "ExportId": "export-123",
            "Status": "COMPLETED"
        }

        result = self.qldb.describe_journal_export("test-ledger", "export-123")
        self.assertEqual(result["ExportId"], "export-123")

    def test_list_journal_exports(self):
        """Test listing journal exports"""
        self.mock_qldb.list_journal_exports.return_value = {
            "JournalExports": [
                {"ExportId": "export-1"},
                {"ExportId": "export-2"}
            ]
        }

        result = self.qldb.list_journal_exports("test-ledger")
        self.assertEqual(len(result), 2)


class TestBackups(unittest.TestCase):
    """Test backup methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb
            self.qldb = QLDBIntegration(region_name="us-east-1")
            self.qldb._qldb = self.mock_qldb

    def test_create_backup(self):
        """Test creating a backup"""
        self.mock_qldb.create_backup.return_value = {
            "Backup": {
                "BackupId": "backup-123",
                "LedgerName": "test-ledger",
                "Status": "CREATING"
            }
        }

        result = self.qldb.create_backup("test-ledger", "my-bucket", "backups/")
        self.assertEqual(result["Backup"]["BackupId"], "backup-123")

    def test_describe_backup(self):
        """Test describing a backup"""
        self.mock_qldb.describe_backup.return_value = {
            "Backup": {
                "BackupId": "backup-123",
                "Status": "ACTIVE"
            }
        }

        result = self.qldb.describe_backup("backup-123")
        self.assertEqual(result["Backup"]["BackupId"], "backup-123")

    def test_list_backups(self):
        """Test listing backups"""
        self.mock_qldb.list_backups.return_value = {
            "Backups": [
                {"BackupId": "backup-1"},
                {"BackupId": "backup-2"}
            ]
        }

        result = self.qldb.list_backups("test-ledger")
        self.assertEqual(len(result), 2)

    def test_delete_backup(self):
        """Test deleting a backup"""
        self.mock_qldb.delete_backup.return_value = {}

        result = self.qldb.delete_backup("backup-123")
        self.assertIn("ResponseMetadata", result)


class TestPointInTimeRecovery(unittest.TestCase):
    """Test point-in-time recovery methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_qldb = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_qldb
            self.qldb = QLDBIntegration(region_name="us-east-1")
            self.qldb._qldb = self.mock_qldb

    def test_enable_point_in_time_recovery(self):
        """Test enabling point-in-time recovery"""
        self.mock_qldb.update_ledger.return_value = {}

        result = self.qldb.enable_point_in_time_recovery("test-ledger", True)
        self.assertIn("ResponseMetadata", result)

    def test_get_point_in_time_recovery_status(self):
        """Test getting point-in-time recovery status"""
        self.mock_qldb.describe_ledger.return_value = {
            "PointInTimeRecovery": {
                "PointInTimeRecoveryStatus": "ENABLED"
            }
        }

        result = self.qldb.get_point_in_time_recovery_status("test-ledger")
        self.assertEqual(result["PointInTimeRecoveryStatus"], "ENABLED")


class TestPermissions(unittest.TestCase):
    """Test permissions methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_iam = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_iam if s == 'iam' else MagicMock()
            self.qldb = QLDBIntegration(region_name="us-east-1")
            self.qldb._iam = self.mock_iam

    def test_get_permissions(self):
        """Test getting permissions"""
        self.mock_iam.get_policy.return_value = {
            "Policy": {
                "PolicyName": "test-policy"
            }
        }

        result = self.qldb.get_permissions("arn:aws:qldb:us-east-1:123456789012:ledger/test-ledger")
        self.assertIn("Policy", result)


class TestCloudWatchIntegration(unittest.TestCase):
    """Test CloudWatch integration methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudwatch = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.side_effect = lambda s, **k: self.mock_cloudwatch if s == 'cloudwatch' else MagicMock()
            self.qldb = QLDBIntegration(region_name="us-east-1")
            self.qldb._cloudwatch = self.mock_cloudwatch

    def test_get_storage_metrics(self):
        """Test getting storage metrics"""
        self.mock_cloudwatch.get_metric_statistics.return_value = {
            "Datapoints": [
                {"Timestamp": datetime.now(), "Sum": 1000}
            ]
        }

        result = self.qldb.get_storage_metrics("test-ledger")
        self.assertIn("Datapoints", result)

    def test_get_io_metrics(self):
        """Test getting I/O metrics"""
        self.mock_cloudwatch.get_metric_statistics.return_value = {
            "Datapoints": []
        }

        result = self.qldb.get_io_metrics("test-ledger")
        self.assertIn("Datapoints", result)


if __name__ == '__main__':
    unittest.main()
