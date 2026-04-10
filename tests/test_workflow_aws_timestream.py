"""
Tests for workflow_aws_timestream module
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

# Create mock boto3 module before importing workflow_aws_timestream
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
import src.workflow_aws_timestream as _timestream_module

# Extract classes
TimestreamIntegration = _timestream_module.TimestreamIntegration
TableState = _timestream_module.TableState
MagneticStoreRejectedException = _timestream_module.MagneticStoreRejectedException
ScheduledQueryState = _timestream_module.ScheduledQueryState
ScheduledQueryErrorReportFormat = _timestream_module.ScheduledQueryErrorReportFormat
DatabaseInfo = _timestream_module.DatabaseInfo
TableInfo = _timestream_module.TableInfo
Record = _timestream_module.Record
WriteResult = _timestream_module.WriteResult
QueryResult = _timestream_module.QueryResult
ScheduledQueryInfo = _timestream_module.ScheduledQueryInfo
ReservedCapacityInfo = _timestream_module.ReservedCapacityInfo


class TestEnums(unittest.TestCase):
    """Test enum classes"""

    def test_table_state_values(self):
        self.assertEqual(TableState.ACTIVE.value, "ACTIVE")
        self.assertEqual(TableState.DELETING.value, "DELETING")

    def test_scheduled_query_state_values(self):
        self.assertEqual(ScheduledQueryState.ENABLED.value, "ENABLED")
        self.assertEqual(ScheduledQueryState.DISABLED.value, "DISABLED")


class TestDatabaseInfo(unittest.TestCase):
    """Test DatabaseInfo dataclass"""

    def test_database_info_creation(self):
        db = DatabaseInfo(
            database_name="test-db",
            arn="arn:aws:timestream:us-east-1:123456789012:database/test-db",
            table_count=5
        )
        self.assertEqual(db.database_name, "test-db")
        self.assertEqual(db.table_count, 5)


class TestTableInfo(unittest.TestCase):
    """Test TableInfo dataclass"""

    def test_table_info_creation(self):
        table = TableInfo(
            database_name="test-db",
            table_name="test-table",
            state=TableState.ACTIVE
        )
        self.assertEqual(table.table_name, "test-table")
        self.assertEqual(table.state, TableState.ACTIVE)


class TestRecord(unittest.TestCase):
    """Test Record dataclass"""

    def test_record_creation(self):
        record = Record(
            measure_name="temperature",
            measure_value="25.5",
            measure_value_type="DOUBLE",
            time="1234567890",
            time_unit="MILLISECONDS"
        )
        self.assertEqual(record.measure_name, "temperature")
        self.assertEqual(record.measure_value, "25.5")

    def test_record_with_dimensions(self):
        record = Record(
            measure_name="temperature",
            measure_value="25.5",
            dimensions={"sensor": "sensor-1", "location": "room-1"}
        )
        self.assertEqual(record.dimensions["sensor"], "sensor-1")


class TestTimestreamIntegration(unittest.TestCase):
    """Test TimestreamIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_write_client = MagicMock()
        self.mock_query_client = MagicMock()
        self.mock_cw_client = MagicMock()

        with patch.object(mock_boto3, 'Session') as mock_session:
            def session_client_side_effect(service, **kwargs):
                if service == 'timestream-write':
                    return self.mock_write_client
                elif service == 'timestream-query':
                    return self.mock_query_client
                elif service == 'cloudwatch':
                    return self.mock_cw_client
                return MagicMock()

            mock_session.return_value.client.side_effect = session_client_side_effect
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream = self.mock_write_client
            self.ts.timestream_query = self.mock_query_client
            self.ts.cloudwatch = self.mock_cw_client

    def test_init(self):
        """Test TimestreamIntegration initialization"""
        ts = TimestreamIntegration(region_name="us-west-2")
        self.assertEqual(ts.region_name, "us-west-2")


class TestDatabaseManagement(unittest.TestCase):
    """Test database management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_write_client = MagicMock()
        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.return_value = self.mock_write_client
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream = self.mock_write_client

    def test_create_database(self):
        """Test creating a database"""
        self.mock_write_client.create_database.return_value = {
            "Database": {
                "DatabaseName": "test-db",
                "Arn": "arn:aws:timestream:us-east-1:123456789012:database/test-db"
            }
        }

        result = self.ts.create_database("test-db")
        self.assertIn("Database", result)

    def test_create_database_with_kms(self):
        """Test creating a database with KMS key"""
        self.mock_write_client.create_database.return_value = {
            "Database": {
                "DatabaseName": "test-db",
                "KmsKeyId": "kms-key-123"
            }
        }

        result = self.ts.create_database("test-db", kms_key_id="kms-key-123")
        self.assertIn("Database", result)

    def test_describe_database(self):
        """Test describing a database"""
        self.mock_write_client.describe_database.return_value = {
            "Database": {
                "DatabaseName": "test-db",
                "Arn": "arn:aws:timestream:us-east-1:123456789012:database/test-db",
                "TableCount": 5
            }
        }

        result = self.ts.describe_database("test-db")
        self.assertEqual(result.database_name, "test-db")
        self.assertEqual(result.table_count, 5)

    def test_list_databases(self):
        """Test listing databases"""
        self.mock_write_client.get_paginator.return_value.paginate.return_value = [
            {"Databases": [
                {"DatabaseName": "db-1"},
                {"DatabaseName": "db-2"}
            ]}
        ]

        result = self.ts.list_databases()
        self.assertEqual(len(result), 2)

    def test_update_database(self):
        """Test updating a database"""
        self.mock_write_client.update_database.return_value = {
            "Database": {
                "DatabaseName": "test-db",
                "KmsKeyId": "new-kms-key"
            }
        }

        result = self.ts.update_database("test-db", "new-kms-key")
        self.assertIn("Database", result)

    def test_delete_database(self):
        """Test deleting a database"""
        self.mock_write_client.delete_database.return_value = {}

        result = self.ts.delete_database("test-db")
        self.assertIn("ResponseMetadata", result)


class TestTableManagement(unittest.TestCase):
    """Test table management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_write_client = MagicMock()
        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.return_value = self.mock_write_client
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream = self.mock_write_client

    def test_create_table(self):
        """Test creating a table"""
        self.mock_write_client.create_table.return_value = {
            "Table": {
                "DatabaseName": "test-db",
                "TableName": "test-table"
            }
        }

        result = self.ts.create_table("test-db", "test-table")
        self.assertIn("Table", result)

    def test_create_table_with_retention(self):
        """Test creating a table with retention properties"""
        self.mock_write_client.create_table.return_value = {
            "Table": {
                "DatabaseName": "test-db",
                "TableName": "test-table",
                "RetentionProperties": {
                    "MemoryStoreRetentionPeriodInHours": "3600",
                    "MagneticStoreRetentionPeriodInDays": "365"
                }
            }
        }

        retention = {
            "MemoryStoreRetentionPeriodInHours": "3600",
            "MagneticStoreRetentionPeriodInDays": "365"
        }

        result = self.ts.create_table("test-db", "test-table", retention_properties=retention)
        self.assertIn("Table", result)

    def test_describe_table(self):
        """Test describing a table"""
        self.mock_write_client.describe_table.return_value = {
            "Table": {
                "DatabaseName": "test-db",
                "TableName": "test-table",
                "TableStatus": "ACTIVE"
            }
        }

        result = self.ts.describe_table("test-db", "test-table")
        self.assertEqual(result.table_name, "test-table")
        self.assertEqual(result.state, TableState.ACTIVE)

    def test_list_tables(self):
        """Test listing tables"""
        self.mock_write_client.get_paginator.return_value.paginate.return_value = [
            {"Tables": [
                {"TableName": "table-1"},
                {"TableName": "table-2"}
            ]}
        ]

        result = self.ts.list_tables("test-db")
        self.assertEqual(len(result), 2)

    def test_update_table(self):
        """Test updating a table"""
        self.mock_write_client.update_table.return_value = {
            "Table": {
                "DatabaseName": "test-db",
                "TableName": "test-table",
                "RetentionProperties": {
                    "MemoryStoreRetentionPeriodInHours": "7200"
                }
            }
        }

        retention = {"MemoryStoreRetentionPeriodInHours": "7200"}
        result = self.ts.update_table("test-db", "test-table", retention_properties=retention)
        self.assertIn("Table", result)

    def test_delete_table(self):
        """Test deleting a table"""
        self.mock_write_client.delete_table.return_value = {}

        result = self.ts.delete_table("test-db", "test-table")
        self.assertIn("ResponseMetadata", result)


class TestWriteRecords(unittest.TestCase):
    """Test write records methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_write_client = MagicMock()
        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.return_value = self.mock_write_client
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream = self.mock_write_client

    def test_write_records(self):
        """Test writing records"""
        self.mock_write_client.write_records.return_value = {
            "RecordsIngested": {"TotalRecords": 1}
        }

        records = [
            Record(
                measure_name="temperature",
                measure_value="25.5",
                time="1234567890",
                time_unit="MILLISECONDS"
            )
        ]

        result = self.ts.write_records("test-db", "test-table", records)
        self.assertIn("RecordsIngested", result)

    def test_write_records_with_dimensions(self):
        """Test writing records with dimensions"""
        self.mock_write_client.write_records.return_value = {
            "RecordsIngested": {"TotalRecords": 1}
        }

        records = [
            Record(
                measure_name="temperature",
                measure_value="25.5",
                dimensions={"sensor": "sensor-1"},
                time="1234567890",
                time_unit="MILLISECONDS"
            )
        ]

        result = self.ts.write_records("test-db", "test-table", records)
        self.assertIn("RecordsIngested", result)


class TestQueryData(unittest.TestCase):
    """Test query data methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_query_client = MagicMock()
        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.return_value = self.mock_query_client
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream_query = self.mock_query_client

    def test_query(self):
        """Test querying data"""
        self.mock_query_client.query.return_value = {
            "QueryId": "query-123",
            "Rows": [
                {"Data": [{"ScalarValue": "25.5"}]}
            ],
            "ColumnInfo": [
                {"Name": "temperature", "Type": {"ScalarType": "DOUBLE"}}
            ]
        }

        result = self.ts.query("SELECT * FROM test-db.test-table")
        self.assertEqual(result.query_id, "query-123")

    def test_query_with_params(self):
        """Test querying with parameters"""
        self.mock_query_client.query.return_value = {
            "QueryId": "query-123",
            "Rows": [],
            "ColumnInfo": []
        }

        result = self.ts.query_with_params(
            "SELECT * FROM test-db.test-table WHERE time > ?",
            [{"ScalarValue": "1234567890"}]
        )
        self.assertEqual(result.query_id, "query-123")


class TestScheduledQueries(unittest.TestCase):
    """Test scheduled query methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_write_client = MagicMock()
        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.return_value = self.mock_write_client
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream = self.mock_write_client

    def test_create_scheduled_query(self):
        """Test creating a scheduled query"""
        self.mock_write_client.create_scheduled_query.return_value = {
            "ScheduledQueryArn": "arn:aws:timestream:us-east-1:123456789012:scheduled-query/sq-123"
        }

        result = self.ts.create_scheduled_query(
            name="test-query",
            query_string="SELECT * FROM test-db.test-table",
            target_configuration={"TargetDestination": {"Timestream": {"DatabaseName": "test-db", "TableName": "test-table"}}},
            schedule_configuration={"ScheduleExpression": "rate(1h)"}
        )
        self.assertIn("ScheduledQueryArn", result)

    def test_describe_scheduled_query(self):
        """Test describing a scheduled query"""
        self.mock_write_client.describe_scheduled_query.return_value = {
            "ScheduledQuery": {
                "Name": "test-query",
                "State": "ENABLED"
            }
        }

        result = self.ts.describe_scheduled_query("arn:aws:timestream:us-east-1:123456789012:scheduled-query/sq-123")
        self.assertIn("ScheduledQuery", result)

    def test_list_scheduled_queries(self):
        """Test listing scheduled queries"""
        self.mock_write_client.list_scheduled_queries.return_value = {
            "ScheduledQueries": [
                {"Name": "query-1"},
                {"Name": "query-2"}
            ]
        }

        result = self.ts.list_scheduled_queries()
        self.assertEqual(len(result), 2)

    def test_update_scheduled_query(self):
        """Test updating a scheduled query"""
        self.mock_write_client.update_scheduled_query.return_value = {}

        result = self.ts.update_scheduled_query(
            "arn:aws:timestream:us-east-1:123456789012:scheduled-query/sq-123",
            state=ScheduledQueryState.DISABLED
        )
        self.assertIn("ResponseMetadata", result)

    def test_delete_scheduled_query(self):
        """Test deleting a scheduled query"""
        self.mock_write_client.delete_scheduled_query.return_value = {}

        result = self.ts.delete_scheduled_query("arn:aws:timestream:us-east-1:123456789012:scheduled-query/sq-123")
        self.assertIn("ResponseMetadata", result)


class TestReservedCapacity(unittest.TestCase):
    """Test reserved capacity methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_write_client = MagicMock()
        with patch.object(mock_boto3, 'Session') as mock_session:
            mock_session.return_value.client.return_value = self.mock_write_client
            self.ts = TimestreamIntegration(region_name="us-east-1")
            self.ts.timestream = self.mock_write_client

    def test_list_reserved_offerings(self):
        """Test listing reserved offerings"""
        self.mock_write_client.list_reserved_offerings.return_value = {
            "ReservedOfferings": [
                {"ReservedOfferingId": "offer-1"},
                {"ReservedOfferingId": "offer-2"}
            ]
        }

        result = self.ts.list_reserved_offerings()
        self.assertIsInstance(result, list)

    def test_purchase_reserved_offering(self):
        """Test purchasing a reserved offering"""
        self.mock_write_client.purchase_reserved_offering.return_value = {
            "ReservedOffering": {
                "ReservedOfferingId": "offer-1",
                "CapacityId": "cap-1"
            }
        }

        result = self.ts.purchase_reserved_offering("offer-1", 5)
        self.assertIn("ReservedOffering", result)


class TestQueryResult(unittest.TestCase):
    """Test QueryResult dataclass"""

    def test_query_result_creation(self):
        result = QueryResult(
            query_id="query-123",
            query_string="SELECT * FROM test",
            rows=[],
            column_info=[]
        )
        self.assertEqual(result.query_id, "query-123")
        self.assertEqual(result.rows_scanned, 0)


if __name__ == '__main__':
    unittest.main()
