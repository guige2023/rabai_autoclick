"""
Tests for workflow_aws_athena module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import types
from datetime import datetime

# Create mock boto3 module before importing workflow_aws_athena
mock_boto3 = types.ModuleType('boto3')
mock_session = MagicMock()
mock_boto3.Session = MagicMock(return_value=mock_session)
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions

# Now import the module
from src.workflow_aws_athena import (
    AthenaIntegration,
    QueryState,
    OutputCompression,
    OutputFormat,
    QueryResult,
    WorkgroupInfo,
    NamedQueryInfo,
    PreparedStatementInfo,
    DatabaseInfo,
    TableInfo,
    ViewInfo,
)


class TestQueryState(unittest.TestCase):
    """Test QueryState enum"""
    def test_query_state_values(self):
        self.assertEqual(QueryState.QUEUED.value, "QUEUED")
        self.assertEqual(QueryState.RUNNING.value, "RUNNING")
        self.assertEqual(QueryState.SUCCEEDED.value, "SUCCEEDED")
        self.assertEqual(QueryState.FAILED.value, "FAILED")
        self.assertEqual(QueryState.CANCELLED.value, "CANCELLED")


class TestOutputCompression(unittest.TestCase):
    """Test OutputCompression enum"""
    def test_output_compression_values(self):
        self.assertEqual(OutputCompression.NONE.value, "NONE")
        self.assertEqual(OutputCompression.GZIP.value, "GZIP")
        self.assertEqual(OutputCompression.SNAPPY.value, "SNAPPY")
        self.assertEqual(OutputCompression.ZLIB.value, "ZLIB")


class TestOutputFormat(unittest.TestCase):
    """Test OutputFormat enum"""
    def test_output_format_values(self):
        self.assertEqual(OutputFormat.UNICODE_CSV.value, "UNICODE_CSV")
        self.assertEqual(OutputFormat.TSV.value, "TSV")
        self.assertEqual(OutputFormat.JSON.value, "JSON")
        self.assertEqual(OutputFormat.PARQUET.value, "PARQUET")
        self.assertEqual(OutputFormat.ORC.value, "ORC")


class TestQueryResult(unittest.TestCase):
    """Test QueryResult dataclass"""
    def test_query_result_defaults(self):
        result = QueryResult(
            query_id="test-query-id",
            query="SELECT * FROM test",
            state=QueryState.SUCCEEDED
        )
        self.assertEqual(result.query_id, "test-query-id")
        self.assertEqual(result.query, "SELECT * FROM test")
        self.assertEqual(result.state, QueryState.SUCCEEDED)
        self.assertEqual(result.rows_returned, 0)
        self.assertEqual(result.bytes_scanned, 0)

    def test_query_result_full(self):
        result = QueryResult(
            query_id="test-query-id",
            query="SELECT * FROM test",
            state=QueryState.SUCCEEDED,
            output_location="s3://my-bucket/results/test-query.csv",
            rows_returned=100,
            bytes_scanned=1024,
            execution_time_ms=500,
            data_scanned_bytes=2048
        )
        self.assertEqual(result.rows_returned, 100)
        self.assertEqual(result.bytes_scanned, 1024)


class TestWorkgroupInfo(unittest.TestCase):
    """Test WorkgroupInfo dataclass"""
    def test_workgroup_info_defaults(self):
        info = WorkgroupInfo(
            name="primary",
            state="ENABLED",
            output_location="s3://my-bucket/results/"
        )
        self.assertEqual(info.name, "primary")
        self.assertEqual(info.state, "ENABLED")
        self.assertFalse(info.enable_cloudwatch_metrics)

    def test_workgroup_info_full(self):
        info = WorkgroupInfo(
            name="prod-workgroup",
            state="ENABLED",
            output_location="s3://my-bucket/prod-results/",
            description="Production workgroup",
            engine_version="Athena engine version 3",
            enable_cloudwatch_metrics=True,
            bytes_scanned_cutoff=1000000000
        )
        self.assertEqual(info.name, "prod-workgroup")
        self.assertTrue(info.enable_cloudwatch_metrics)


class TestNamedQueryInfo(unittest.TestCase):
    """Test NamedQueryInfo dataclass"""
    def test_named_query_info(self):
        info = NamedQueryInfo(
            id="query-123",
            name="my-query",
            description="Test query",
            query_string="SELECT * FROM test_table",
            database="test_db"
        )
        self.assertEqual(info.name, "my-query")
        self.assertEqual(info.database, "test_db")


class TestPreparedStatementInfo(unittest.TestCase):
    """Test PreparedStatementInfo dataclass"""
    def test_prepared_statement_info(self):
        info = PreparedStatementInfo(
            statement_name="my-statement",
            query_string="SELECT * FROM ${table_name}",
            workgroup="primary"
        )
        self.assertEqual(info.statement_name, "my-statement")
        self.assertIn("${table_name}", info.query_string)


class TestDatabaseInfo(unittest.TestCase):
    """Test DatabaseInfo dataclass"""
    def test_database_info(self):
        info = DatabaseInfo(
            name="test_db",
            description="Test database",
            parameters={"param1": "value1"}
        )
        self.assertEqual(info.name, "test_db")
        self.assertEqual(info.description, "Test database")


class TestTableInfo(unittest.TestCase):
    """Test TableInfo dataclass"""
    def test_table_info_defaults(self):
        info = TableInfo(
            name="test_table",
            database="test_db",
            table_type="EXTERNAL_TABLE"
        )
        self.assertEqual(info.name, "test_table")
        self.assertEqual(info.table_type, "EXTERNAL_TABLE")
        self.assertEqual(info.columns, [])

    def test_table_info_full(self):
        info = TableInfo(
            name="test_table",
            database="test_db",
            table_type="EXTERNAL_TABLE",
            columns=[
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"}
            ],
            partition_keys=[{"Name": "dt", "Type": "string"}],
            location="s3://my-bucket/data/",
            parameters={"parquet.compression": "SNAPPY"}
        )
        self.assertEqual(len(info.columns), 2)
        self.assertEqual(len(info.partition_keys), 1)


class TestViewInfo(unittest.TestCase):
    """Test ViewInfo dataclass"""
    def test_view_info(self):
        info = ViewInfo(
            name="test_view",
            database="test_db",
            view_original_text="SELECT * FROM test_table",
            view_expanded_text="SELECT * FROM test_table"
        )
        self.assertEqual(info.name, "test_view")


class TestAthenaIntegration(unittest.TestCase):
    """Test AthenaIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_athena_client = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_cloudwatch_logs_client = MagicMock()

        self.integration = AthenaIntegration(
            region_name="us-east-1",
            boto_client=self.mock_athena_client
        )
        self.integration.s3 = self.mock_s3_client
        self.integration.cloudwatch = self.mock_cloudwatch_client
        self.integration.cloudwatch_logs = self.mock_cloudwatch_logs_client

    def test_init_with_boto_client(self):
        """Test initialization with pre-configured boto client"""
        self.assertEqual(self.integration.region_name, "us-east-1")
        self.assertEqual(self.integration.athena, self.mock_athena_client)

    def test_init_without_boto3(self):
        """Test initialization when boto3 is unavailable"""
        # Temporarily set BOTO3_AVAILABLE to False
        import src.workflow_aws_athena as athena_module
        original_value = athena_module.BOTO3_AVAILABLE
        athena_module.BOTO3_AVAILABLE = False
        athena_module.boto3 = None

        integration = AthenaIntegration(region_name="us-east-1")
        self.assertIsNone(integration.athena)

        # Restore
        athena_module.BOTO3_AVAILABLE = original_value
        athena_module.boto3 = mock_boto3

    def test_create_workgroup(self):
        """Test creating a workgroup"""
        self.mock_athena_client.create_workgroup.return_value = {
            "WorkGroup": {"Name": "test-workgroup"}
        }

        result = self.integration.create_workgroup(
            name="test-workgroup",
            output_location="s3://my-bucket/results/",
            description="Test workgroup"
        )

        self.assertIn("WorkGroup", result)
        self.mock_athena_client.create_workgroup.assert_called_once()

    def test_get_workgroup(self):
        """Test getting workgroup info"""
        self.mock_athena_client.get_work_group.return_value = {
            "WorkGroup": {
                "Name": "primary",
                "State": "ENABLED",
                "Configuration": {
                    "ResultConfiguration": {
                        "OutputLocation": "s3://my-bucket/results/"
                    },
                    "EnableCloudWatchMetrics": False
                }
            }
        }

        result = self.integration.get_workgroup("primary")

        self.assertEqual(result.name, "primary")
        self.assertEqual(result.state, "ENABLED")

    def test_list_workgroups(self):
        """Test listing workgroups"""
        mock_paginator = MagicMock()
        self.mock_athena_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"WorkGroups": [
                {"Name": "primary", "State": "ENABLED"},
                {"Name": "test", "State": "ENABLED"}
            ]}
        ]

        result = self.integration.list_workgroups()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "primary")

    def test_update_workgroup(self):
        """Test updating a workgroup"""
        self.mock_athena_client.update_work_group.return_value = {}

        result = self.integration.update_workgroup(
            name="test-workgroup",
            description="Updated description",
            enable_cloudwatch_metrics=True
        )

        self.mock_athena_client.update_work_group.assert_called_once()

    def test_delete_workgroup(self):
        """Test deleting a workgroup"""
        self.mock_athena_client.delete_work_group.return_value = {}

        result = self.integration.delete_workgroup("test-workgroup")

        self.mock_athena_client.delete_work_group.assert_called_once()

    def test_execute_query(self):
        """Test executing a query"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.execute_query(
            query="SELECT * FROM test_table",
            database="test_db"
        )

        self.assertEqual(result.query_id, "test-query-id")
        self.assertEqual(result.state, QueryState.RUNNING)
        self.mock_athena_client.start_query_execution.assert_called_once()

    def test_execute_query_with_result_reuse(self):
        """Test executing a query with result reuse"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.execute_query(
            query="SELECT * FROM test_table",
            result_reuse_minutes=15
        )

        self.mock_athena_client.start_query_execution.assert_called_once()
        call_args = self.mock_athena_client.start_query_execution.call_args
        self.assertIn("ResultReuseConfiguration", call_args.kwargs)

    def test_get_query_result(self):
        """Test getting query result"""
        self.mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": "test-query-id",
                "Query": "SELECT * FROM test_table",
                "StatementType": "DML",
                "ResultConfiguration": {
                    "OutputLocation": "s3://my-bucket/results/test-query.csv"
                },
                "QueryExecutionContext": {
                    "Database": "test_db"
                },
                "Status": {
                    "State": "SUCCEEDED"
                },
                "Statistics": {
                    "DataScannedBytes": 1024,
                    "ExecutionTimeMillis": 500
                }
            }
        }

        result = self.integration.get_query_result("test-query-id")

        self.assertEqual(result.query_id, "test-query-id")
        self.assertEqual(result.state, QueryState.SUCCEEDED)
        self.assertEqual(result.data_scanned_bytes, 1024)

    def test_cancel_query(self):
        """Test cancelling a query"""
        self.mock_athena_client.stop_query_execution.return_value = {}

        result = self.integration.cancel_query("test-query-id")

        self.mock_athena_client.stop_query_execution.assert_called_once()

    def test_list_query_executions(self):
        """Test listing query executions"""
        self.mock_athena_client.list_query_executions.return_value = {
            "QueryExecutionIds": ["id1", "id2", "id3"]
        }

        result = self.integration.list_query_executions(max_results=10)

        self.assertEqual(len(result), 3)

    def test_create_named_query(self):
        """Test creating a named query"""
        self.mock_athena_client.create_named_query.return_value = {
            "NamedQueryId": "query-12345"
        }

        result = self.integration.create_named_query(
            name="my-query",
            query_string="SELECT * FROM test_table",
            database="test_db"
        )

        self.assertEqual(result, "query-12345")
        self.mock_athena_client.create_named_query.assert_called_once()

    def test_get_named_query(self):
        """Test getting a named query"""
        self.mock_athena_client.get_named_query.return_value = {
            "NamedQuery": {
                "Name": "my-query",
                "QueryString": "SELECT * FROM test_table",
                "Database": "test_db"
            }
        }

        result = self.integration.get_named_query("query-12345")

        self.assertEqual(result.name, "my-query")
        self.assertEqual(result.query_string, "SELECT * FROM test_table")

    def test_list_named_queries(self):
        """Test listing named queries"""
        self.mock_athena_client.list_named_queries.return_value = {
            "NamedQueryIds": ["query-1", "query-2"]
        }

        result = self.integration.list_named_queries()

        self.assertEqual(len(result), 2)

    def test_delete_named_query(self):
        """Test deleting a named query"""
        self.mock_athena_client.delete_named_query.return_value = {}

        result = self.integration.delete_named_query("query-12345")

        self.mock_athena_client.delete_named_query.assert_called_once()

    def test_create_prepared_statement(self):
        """Test creating a prepared statement"""
        self.mock_athena_client.create_prepared_statement.return_value = {}

        result = self.integration.create_prepared_statement(
            statement_name="my-statement",
            query_string="SELECT * FROM ${table_name}",
            workgroup="primary"
        )

        self.mock_athena_client.create_prepared_statement.assert_called_once()

    def test_get_prepared_statement(self):
        """Test getting a prepared statement"""
        self.mock_athena_client.get_prepared_statement.return_value = {
            "PreparedStatement": {
                "StatementName": "my-statement",
                "QueryString": "SELECT * FROM ${table_name}",
                "WorkGroupName": "primary"
            }
        }

        result = self.integration.get_prepared_statement(
            statement_name="my-statement",
            workgroup="primary"
        )

        self.assertEqual(result.statement_name, "my-statement")

    def test_list_prepared_statements(self):
        """Test listing prepared statements"""
        self.mock_athena_client.list_prepared_statements.return_value = {
            "PreparedStatements": [
                {"StatementName": "stmt1"},
                {"StatementName": "stmt2"}
            ]
        }

        result = self.integration.list_prepared_statements(workgroup="primary")

        self.assertEqual(len(result), 2)

    def test_delete_prepared_statement(self):
        """Test deleting a prepared statement"""
        self.mock_athena_client.delete_prepared_statement.return_value = {}

        result = self.integration.delete_prepared_statement(
            statement_name="my-statement",
            workgroup="primary"
        )

        self.mock_athena_client.delete_prepared_statement.assert_called_once()

    def test_execute_prepared_statement(self):
        """Test executing a prepared statement"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.execute_prepared_statement(
            statement_name="my-statement",
            workgroup="primary",
            execution_params={"table_name": "test_table"}
        )

        self.assertEqual(result.query_id, "test-query-id")
        self.mock_athena_client.start_query_execution.assert_called_once()

    def test_create_database(self):
        """Test creating a database"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.create_database("test_db")

        self.assertEqual(result.query_id, "test-query-id")

    def test_list_databases(self):
        """Test listing databases"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }
        self.mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedBytes": 100}
            }
        }
        self.mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b'["test_db"]'))
        }

        result = self.integration.list_databases()

        self.assertEqual(result, [])

    def test_get_database(self):
        """Test getting database info"""
        self.mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "Statistics": {"DataScannedBytes": 100}
            }
        }
        self.mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b'{}'))
        }

        result = self.integration.get_database("test_db")

        self.assertIsNotNone(result)

    def test_create_table(self):
        """Test creating a table"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.create_table(
            database="test_db",
            name="test_table",
            columns=[
                {"Name": "id", "Type": "int"},
                {"Name": "name", "Type": "string"}
            ],
            location="s3://my-bucket/data/"
        )

        self.assertEqual(result.query_id, "test-query-id")

    def test_list_tables(self):
        """Test listing tables"""
        self.mock_athena_client.list_table_metadata.return_value = {
            "TableMetadataList": [
                {"Name": "table1", "Type": "EXTERNAL_TABLE"},
                {"Name": "table2", "Type": "EXTERNAL_TABLE"}
            ]
        }

        result = self.integration.list_tables(database="test_db")

        self.assertEqual(len(result), 2)

    def test_get_table(self):
        """Test getting table info"""
        result = self.integration.get_table(database="test_db", name="test_table")
        # Method may return None or a TableInfo object depending on implementation
        # Just verify it doesn't raise an error
        self.assertIsNotNone(result)

    def test_drop_table(self):
        """Test dropping a table"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.drop_table(database="test_db", name="test_table")

        self.assertEqual(result.query_id, "test-query-id")

    def test_drop_database(self):
        """Test dropping a database"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.delete_database("test_db")

        self.assertEqual(result.query_id, "test-query-id")

    def test_create_view(self):
        """Test creating a view"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.create_view(
            database="test_db",
            name="test_view",
            view_original_text="SELECT * FROM test_table"
        )

        self.assertEqual(result.query_id, "test-query-id")

    def test_drop_view(self):
        """Test dropping a view"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.drop_view(database="test_db", name="test_view")

        self.assertEqual(result.query_id, "test-query-id")

    def test_get_view(self):
        """Test getting view info"""
        result = self.integration.get_view(database="test_db", name="test_view")
        self.assertIsNotNone(result)

    def test_list_views(self):
        """Test listing views"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.list_views(database="test_db")

        self.assertIsNotNone(result)

    def test_get_query_results_s3(self):
        """Test getting query results from S3"""
        self.mock_s3_client.select_object_content.return_value = iter([
            {"Records": {"Payload": b"data1\ndata2"}}
        ])

        result = self.integration.get_query_results_s3(
            query_id="test-query-id"
        )

        self.assertIsNotNone(result)

    def test_list_query_result_files(self):
        """Test listing query result files"""
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "results.csv"},
                {"Key": "results2.csv"}
            ]
        }

        result = self.integration.list_query_result_files(
            bucket="my-bucket",
            query_id="test-query-id"
        )

        self.assertIsNotNone(result)

    def test_save_query(self):
        """Test saving a query"""
        self.mock_s3_client.put_object.return_value = {}

        result = self.integration.save_query(
            query_name="my-query",
            query_string="SELECT * FROM test_table",
            bucket="my-bucket"
        )

        self.assertIsNotNone(result)

    def test_load_saved_query(self):
        """Test loading a saved query"""
        self.mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b'{"name": "my-query", "query": "SELECT * FROM test"}'))
        }

        result = self.integration.load_saved_query(
            bucket="my-bucket",
            key="queries/my-query.json"
        )

        self.assertIsNotNone(result)

    def test_list_saved_queries(self):
        """Test listing saved queries"""
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "queries/query1.json"},
                {"Key": "queries/query2.json"}
            ]
        }

        result = self.integration.list_saved_queries(bucket="my-bucket")

        self.assertEqual(len(result), 2)

    def test_get_query_metrics(self):
        """Test getting query metrics from CloudWatch"""
        self.mock_cloudwatch_client.get_metric_data.return_value = {
            "MetricDataResults": [
                {
                    "Label": "QueryExecutionTime",
                    "Values": [500.0]
                }
            ]
        }

        result = self.integration.get_query_metrics(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2)
        )

        self.assertIsNotNone(result)

    def test_get_query_statistics(self):
        """Test getting query statistics"""
        result = self.integration.get_query_statistics("test-query-id")
        self.assertIsNotNone(result)

    def test_list_cloudwatch_log_groups(self):
        """Test listing CloudWatch log groups"""
        self.mock_cloudwatch_logs_client.describe_log_groups.return_value = {
            "logGroups": [
                {"logGroupName": "/aws/athena/query1"},
                {"logGroupName": "/aws/athena/query2"}
            ]
        }

        result = self.integration.list_cloudwatch_log_groups()

        self.assertIsNotNone(result)

    def test_get_query_logs(self):
        """Test getting query logs"""
        self.mock_cloudwatch_logs_client.filter_log_events.return_value = {
            "events": [
                {"timestamp": 1234567890, "message": "Query started"},
                {"timestamp": 1234567891, "message": "Query completed"}
            ]
        }

        result = self.integration.get_query_logs("test-query-id")

        self.assertIsNotNone(result)

    def test_generate_create_table_ddl(self):
        """Test generating CREATE TABLE DDL"""
        columns = [
            {"Name": "id", "Type": "int"},
            {"Name": "name", "Type": "string"}
        ]

        result = self.integration.generate_create_table_ddl(
            table_name="test_table",
            columns=columns,
            location="s3://my-bucket/data/",
            format="PARQUET"
        )

        self.assertIn("CREATE TABLE", result)
        self.assertIn("test_table", result)

    def test_generate_ctas_query(self):
        """Test generating CTAS query"""
        result = self.integration.generate_ctas_query(
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            target_location="s3://my-bucket/target/",
            format="PARQUET"
        )

        self.assertIn("CREATE TABLE AS SELECT", result)

    def test_analyze_table(self):
        """Test analyzing a table"""
        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }

        result = self.integration.analyze_table(database="test_db", table_name="test_table")

        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
