"""
Tests for workflow_aws_cur module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import types
from datetime import datetime

# Create mock boto3 module before importing workflow_aws_cur
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
from src.workflow_aws_cur import (
    CURIntegration,
    TimeGranularity,
    CompressionFormat,
    SchemaType,
    ReportVersioning,
    AthenaTableFormat,
    CURReport,
    ReportDefinition,
    AthenaIntegrationConfig,
    GlueCatalogConfig,
    RetentionPolicy,
    DeliveryMetric,
)


class TestTimeGranularity(unittest.TestCase):
    """Test TimeGranularity enum"""
    def test_time_granularity_values(self):
        self.assertEqual(TimeGranularity.HOURLY.value, "HOURLY")
        self.assertEqual(TimeGranularity.DAILY.value, "DAILY")
        self.assertEqual(TimeGranularity.MONTHLY.value, "MONTHLY")


class TestCompressionFormat(unittest.TestCase):
    """Test CompressionFormat enum"""
    def test_compression_format_values(self):
        self.assertEqual(CompressionFormat.ZIP.value, "ZIP")
        self.assertEqual(CompressionFormat.GZIP.value, "GZIP")


class TestSchemaType(unittest.TestCase):
    """Test SchemaType enum"""
    def test_schema_type_values(self):
        self.assertEqual(SchemaType.RESOURCES.value, "RESOURCES")
        self.assertEqual(SchemaType.LINE_ITEM.value, "LINE_ITEM")


class TestReportVersioning(unittest.TestCase):
    """Test ReportVersioning enum"""
    def test_report_versioning_values(self):
        self.assertEqual(ReportVersioning.CREATE_NEW_REPORT.value, "CREATE_NEW_REPORT")
        self.assertEqual(ReportVersioning.OVERWRITE_EXISTING_REPORT.value, "OVERWRITE_EXISTING_REPORT")


class TestAthenaTableFormat(unittest.TestCase):
    """Test AthenaTableFormat enum"""
    def test_athena_table_format_values(self):
        self.assertEqual(AthenaTableFormat.PARQUET.value, "parquet")
        self.assertEqual(AthenaTableFormat.ORC.value, "orc")
        self.assertEqual(AthenaTableFormat.JSON.value, "json")
        self.assertEqual(AthenaTableFormat.CSV.value, "csv")
        self.assertEqual(AthenaTableFormat.TSV.value, "tsv")


class TestCURReport(unittest.TestCase):
    """Test CURReport dataclass"""
    def test_cur_report_defaults(self):
        report = CURReport(
            report_name="test-report",
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_region="us-east-1",
            time_unit=TimeGranularity.DAILY,
            format="textORcsv",
            compression=CompressionFormat.GZIP,
            schema_type=SchemaType.LINE_ITEM,
            versioning=ReportVersioning.CREATE_NEW_REPORT
        )
        self.assertEqual(report.report_name, "test-report")
        self.assertEqual(report.s3_bucket, "test-bucket")
        self.assertEqual(report.report_status, "")


class TestReportDefinition(unittest.TestCase):
    """Test ReportDefinition dataclass"""
    def test_report_definition_defaults(self):
        definition = ReportDefinition(
            report_name="test-report",
            time_unit="DAILY",
            format="textORcsv",
            compression="GZIP",
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_region="us-east-1",
            schema_elements=["LINE_ITEM"],
            additional_schema_elements=[],
            versioning="CREATE_NEW_REPORT",
            report_versioning="CREATE_NEW_REPORT"
        )
        self.assertEqual(definition.report_name, "test-report")
        self.assertEqual(definition.time_unit, "DAILY")


class TestAthenaIntegrationConfig(unittest.TestCase):
    """Test AthenaIntegrationConfig dataclass"""
    def test_athena_integration_config_defaults(self):
        config = AthenaIntegrationConfig()
        self.assertEqual(config.database_name, "cost_optimization")
        self.assertEqual(config.table_name, "cur_data")
        self.assertEqual(config.output_format, AthenaTableFormat.PARQUET)

    def test_athena_integration_config_full(self):
        config = AthenaIntegrationConfig(
            database_name="test_db",
            table_name="test_table",
            cur_bucket="test-bucket",
            cur_prefix="test-prefix",
            output_location="s3://test-results/",
            output_format=AthenaTableFormat.ORC
        )
        self.assertEqual(config.database_name, "test_db")
        self.assertEqual(config.output_format, AthenaTableFormat.ORC)


class TestGlueCatalogConfig(unittest.TestCase):
    """Test GlueCatalogConfig dataclass"""
    def test_glue_catalog_config_defaults(self):
        config = GlueCatalogConfig()
        self.assertEqual(config.database_name, "cost_optimization")
        self.assertEqual(config.region, "us-east-1")
        self.assertTrue(config.partition_update_enabled)

    def test_glue_catalog_config_full(self):
        config = GlueCatalogConfig(
            database_name="test_glue_db",
            table_name="test_glue_table",
            cur_bucket="test-bucket",
            region="us-west-2"
        )
        self.assertEqual(config.database_name, "test_glue_db")
        self.assertEqual(config.region, "us-west-2")


class TestRetentionPolicy(unittest.TestCase):
    """Test RetentionPolicy dataclass"""
    def test_retention_policy_defaults(self):
        policy = RetentionPolicy()
        self.assertTrue(policy.enabled)
        self.assertEqual(policy.retention_days, 90)
        self.assertFalse(policy.archive_before_delete)

    def test_retention_policy_full(self):
        policy = RetentionPolicy(
            enabled=True,
            retention_days=30,
            archive_before_delete=True,
            archive_location="s3://archive-bucket/"
        )
        self.assertEqual(policy.retention_days, 30)
        self.assertTrue(policy.archive_before_delete)


class TestDeliveryMetric(unittest.TestCase):
    """Test DeliveryMetric dataclass"""
    def test_delivery_metric_defaults(self):
        metric = DeliveryMetric(
            metric_name="ReportDelivery",
            value=1.0,
            unit="Count",
            timestamp=datetime.utcnow()
        )
        self.assertEqual(metric.metric_name, "ReportDelivery")
        self.assertEqual(metric.value, 1.0)


class TestCURIntegration(unittest.TestCase):
    """Test CURIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cur_client = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_athena_client = MagicMock()
        self.mock_glue_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

    def test_initialization_with_boto_clients(self):
        """Test initialization with pre-configured boto clients"""
        integration = CURIntegration(
            region_name="us-west-2",
            boto_cur_client=self.mock_cur_client,
            boto_s3_client=self.mock_s3_client,
            boto_athena_client=self.mock_athena_client,
            boto_glue_client=self.mock_glue_client,
            boto_cloudwatch_client=self.mock_cloudwatch_client
        )
        self.assertEqual(integration.region_name, "us-west-2")
        self.assertEqual(integration.cur, self.mock_cur_client)

    def test_create_report(self):
        """Test create_report method"""
        with patch('src.workflow_aws_cur.boto3.Session') as mock_session_class:
            mock_session_instance = MagicMock()
            mock_session_class.return_value = mock_session_instance
            mock_session_instance.client.side_effect = lambda service, **kwargs: {
                "costandusageReport": self.mock_cur_client,
                "cur": self.mock_cur_client,
            }.get(service, MagicMock())

            self.mock_cur_client.put_report_definition.return_value = {}

            integration = CURIntegration(
                region_name="us-east-1",
                boto_cur_client=self.mock_cur_client
            )
            integration.cur_service = self.mock_cur_client

            result = integration.create_report(
                report_name="test-report",
                s3_bucket="test-bucket",
                s3_prefix="test-prefix",
                s3_region="us-east-1"
            )

            self.assertEqual(result, {})
            self.mock_cur_client.put_report_definition.assert_called_once()

    def test_create_report_raises_when_not_available(self):
        """Test create_report raises error when boto3 not available"""
        integration = CURIntegration(boto_cur_client=None)
        integration.cur_service = None

        with self.assertRaises(RuntimeError) as context:
            integration.create_report(
                report_name="test-report",
                s3_bucket="test-bucket",
                s3_prefix="test-prefix",
                s3_region="us-east-1"
            )

        self.assertIn("Boto3 not available", str(context.exception))

    def test_get_report(self):
        """Test get_report method"""
        with patch('src.workflow_aws_cur.boto3.Session') as mock_session_class:
            mock_session_instance = MagicMock()
            mock_session_class.return_value = mock_session_instance
            mock_session_instance.client.side_effect = lambda service, **kwargs: {
                "costandusageReport": self.mock_cur_client,
            }.get(service, MagicMock())

            self.mock_cur_client.get_report_definition.return_value = {
                "ReportDefinition": {
                    "ReportName": "test-report",
                    "S3Bucket": "test-bucket",
                    "S3Prefix": "test-prefix",
                    "S3Region": "us-east-1",
                    "TimeUnit": "DAILY",
                    "Format": "textORcsv",
                    "Compression": "GZIP",
                    "SchemaElements": ["LINE_ITEM"],
                    "AdditionalSchemaElements": []
                }
            }

            integration = CURIntegration(boto_cur_client=self.mock_cur_client)
            integration.cur_service = self.mock_cur_client

            result = integration.get_report("test-report")

            self.assertEqual(result.report_name, "test-report")
            self.assertEqual(result.s3_bucket, "test-bucket")

    def test_list_reports(self):
        """Test list_reports method"""
        with patch('src.workflow_aws_cur.boto3.Session') as mock_session_class:
            mock_session_instance = MagicMock()
            mock_session_class.return_value = mock_session_instance
            mock_session_instance.client.side_effect = lambda service, **kwargs: {
                "costandusageReport": self.mock_cur_client,
            }.get(service, MagicMock())

            self.mock_cur_client.list_report_definitions.return_value = {
                "ReportDefinitions": [
                    {
                        "ReportName": "report-1",
                        "S3Bucket": "bucket-1",
                        "S3Prefix": "prefix-1",
                        "S3Region": "us-east-1",
                        "TimeUnit": "DAILY",
                        "Format": "textORcsv",
                        "Compression": "GZIP",
                        "SchemaElements": ["LINE_ITEM"],
                        "AdditionalSchemaElements": []
                    },
                    {
                        "ReportName": "report-2",
                        "S3Bucket": "bucket-2",
                        "S3Prefix": "prefix-2",
                        "S3Region": "us-west-2",
                        "TimeUnit": "MONTHLY",
                        "Format": "Parquet",
                        "Compression": "ZIP",
                        "SchemaElements": ["RESOURCES"],
                        "AdditionalSchemaElements": ["TAGS"]
                    }
                ]
            }

            integration = CURIntegration(boto_cur_client=self.mock_cur_client)
            integration.cur_service = self.mock_cur_client

            result = integration.list_reports()

            self.assertEqual(len(result), 2)
            self.assertEqual(result[0].report_name, "report-1")
            self.assertEqual(result[1].report_name, "report-2")

    def test_delete_report(self):
        """Test delete_report method"""
        with patch('src.workflow_aws_cur.boto3.Session') as mock_session_class:
            mock_session_instance = MagicMock()
            mock_session_class.return_value = mock_session_instance
            mock_session_instance.client.side_effect = lambda service, **kwargs: {
                "costandusageReport": self.mock_cur_client,
            }.get(service, MagicMock())

            self.mock_cur_client.delete_report_definition.return_value = {}

            integration = CURIntegration(boto_cur_client=self.mock_cur_client)
            integration.cur_service = self.mock_cur_client
            integration._reports = {"test-report": MagicMock()}

            result = integration.delete_report("test-report")

            self.assertEqual(result, {})
            self.mock_cur_client.delete_report_definition.assert_called_once()

    def test_update_report(self):
        """Test update_report method"""
        with patch('src.workflow_aws_cur.boto3.Session') as mock_session_class:
            mock_session_instance = MagicMock()
            mock_session_class.return_value = mock_session_instance
            mock_session_instance.client.side_effect = lambda service, **kwargs: {
                "costandusageReport": self.mock_cur_client,
            }.get(service, MagicMock())

            self.mock_cur_client.get_report_definition.return_value = {
                "ReportDefinition": {
                    "ReportName": "test-report",
                    "S3Bucket": "test-bucket",
                    "S3Prefix": "test-prefix",
                    "S3Region": "us-east-1",
                    "TimeUnit": "DAILY",
                    "Format": "textORcsv",
                    "Compression": "GZIP",
                    "SchemaElements": ["LINE_ITEM"],
                    "AdditionalSchemaElements": []
                }
            }
            self.mock_cur_client.put_report_definition.return_value = {}

            integration = CURIntegration(boto_cur_client=self.mock_cur_client)
            integration.cur_service = self.mock_cur_client

            result = integration.update_report(
                report_name="test-report",
                time_unit=TimeGranularity.MONTHLY
            )

            self.assertEqual(result, {})
            self.mock_cur_client.put_report_definition.assert_called()

    def test_configure_s3_delivery(self):
        """Test configure_s3_delivery method"""
        integration = CURIntegration(boto_s3_client=self.mock_s3_client)
        integration.s3 = self.mock_s3_client

        self.mock_s3_client.head_bucket.return_value = {}

        result = integration.configure_s3_delivery(
            report_name="test-report",
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_region="us-east-1"
        )

        self.assertTrue(result["configured"])
        self.assertTrue(result["bucket_exists"])

    def test_configure_s3_delivery_creates_bucket(self):
        """Test configure_s3_delivery creates bucket when not exists"""
        from botocore.exceptions import ClientError

        integration = CURIntegration(boto_s3_client=self.mock_s3_client)
        integration.s3 = self.mock_s3_client

        error_response = {"Error": {"Code": "404"}}
        
        class MockClientError(Exception):
            def __init__(self, response, operation_name):
                super().__init__(str(response))
                self.response = response
        
        self.mock_s3_client.head_bucket.side_effect = MockClientError(error_response, "HeadBucket")
        self.mock_s3_client.create_bucket.return_value = {}

        result = integration.configure_s3_delivery(
            report_name="test-report",
            s3_bucket="new-bucket",
            s3_prefix="test-prefix",
            s3_region="us-east-1",
            create_bucket=True
        )

        self.assertTrue(result["bucket_created"])
        self.mock_s3_client.create_bucket.assert_called()

    def test_list_s3_report_files(self):
        """Test list_s3_report_files method"""
        integration = CURIntegration(boto_s3_client=self.mock_s3_client)
        integration.s3 = self.mock_s3_client

        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {
                    "Key": "prefix/report-2024-01.csv",
                    "Size": 1024,
                    "LastModified": "2024-01-01T00:00:00Z",
                    "ETag": "abc123"
                }
            ]
        }

        result = integration.list_s3_report_files(
            s3_bucket="test-bucket",
            s3_prefix="test-prefix"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["key"], "prefix/report-2024-01.csv")
        self.assertEqual(result[0]["size"], 1024)

    def test_customize_report_granularity(self):
        """Test customize_report_granularity method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.customize_report_granularity(
            report_name="test-report",
            time_unit=TimeGranularity.HOURLY
        )

        self.assertEqual(result, {})

    def test_customize_report_units(self):
        """Test customize_report_units method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.customize_report_units(
            report_name="test-report",
            include_tags=True,
            include_linked_accounts=True,
            include_credits=True
        )

        self.assertEqual(result, {})
        self.mock_cur_client.put_report_definition.assert_called()

    def test_set_compression(self):
        """Test set_compression method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.set_compression(
            report_name="test-report",
            compression=CompressionFormat.ZIP
        )

        self.assertEqual(result, {})

    def test_add_linked_account_schema(self):
        """Test add_linked_account_schema method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.add_linked_account_schema("test-report")

        self.assertEqual(result, {})

    def test_add_tags_schema(self):
        """Test add_tags_schema method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.add_tags_schema("test-report")

        self.assertEqual(result, {})

    def test_add_resources_schema(self):
        """Test add_resources_schema method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.add_resources_schema("test-report")

        self.assertEqual(result, {})

    def test_enable_report_versioning(self):
        """Test enable_report_versioning method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client
        integration._reports = {}

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": []
            }
        }
        self.mock_cur_client.put_report_definition.return_value = {}

        result = integration.enable_report_versioning(
            report_name="test-report",
            versioning_type=ReportVersioning.OVERWRITE_EXISTING_REPORT
        )

        self.assertEqual(result, {})

    def test_list_report_versions(self):
        """Test list_report_versions method"""
        integration = CURIntegration(boto_s3_client=self.mock_s3_client)
        integration.s3 = self.mock_s3_client

        self.mock_s3_client.list_object_versions.return_value = {
            "Versions": [
                {
                    "VersionId": "v1",
                    "Key": "prefix/report.csv",
                    "LastModified": "2024-01-01T00:00:00Z",
                    "Size": 1024,
                    "IsLatest": True
                },
                {
                    "VersionId": "v2",
                    "Key": "prefix/report.csv",
                    "LastModified": "2024-01-02T00:00:00Z",
                    "Size": 2048,
                    "IsLatest": False
                }
            ]
        }

        result = integration.list_report_versions(
            s3_bucket="test-bucket",
            s3_prefix="test-prefix"
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["version_id"], "v1")
        self.assertTrue(result[0]["is_latest"])

    def test_setup_athena_integration(self):
        """Test setup_athena_integration method"""
        integration = CURIntegration(
            boto_athena_client=self.mock_athena_client,
            boto_s3_client=self.mock_s3_client
        )
        integration.athena = self.mock_athena_client
        integration.s3 = self.mock_s3_client

        self.mock_athena_client.start_query_execution.return_value = {}

        config = AthenaIntegrationConfig(
            database_name="test_db",
            table_name="test_table",
            cur_bucket="test-bucket",
            cur_prefix="test-prefix",
            output_location="s3://athena-results/"
        )

        result = integration.setup_athena_integration(config)

        self.assertEqual(result["database"], "test_db")
        self.assertEqual(result["table"], "test_table")
        self.assertTrue(result["database_created"])

    def test_setup_athena_integration_raises_when_not_available(self):
        """Test setup_athena_integration raises when Athena not available"""
        integration = CURIntegration()
        integration.athena = None

        config = AthenaIntegrationConfig()

        with self.assertRaises(RuntimeError) as context:
            integration.setup_athena_integration(config)

        self.assertIn("Athena client not available", str(context.exception))

    def test_query_cur_data(self):
        """Test query_cur_data method"""
        integration = CURIntegration(boto_athena_client=self.mock_athena_client)
        integration.athena = self.mock_athena_client

        self.mock_athena_client.start_query_execution.return_value = {
            "QueryExecutionId": "test-query-id"
        }
        self.mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"}
            }
        }
        self.mock_athena_client.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [],
                "ResultSetMetadata": {"ColumnInfo": []}
            }
        }

        result = integration.query_cur_data("SELECT * FROM test")

        self.assertEqual(result["query_execution_id"], "test-query-id")
        self.assertEqual(result["state"], "SUCCEEDED")

    def test_query_cur_data_raises_when_not_available(self):
        """Test query_cur_data raises when Athena not available"""
        integration = CURIntegration()
        integration.athena = None

        with self.assertRaises(RuntimeError) as context:
            integration.query_cur_data("SELECT * FROM test")

        self.assertIn("Athena client not available", str(context.exception))

    def test_generate_cost_query(self):
        """Test generate_cost_query method"""
        integration = CURIntegration()

        query = integration.generate_cost_query(
            database="test_db",
            table="test_table",
            start_date="2024-01-01",
            end_date="2024-01-31",
            group_by=["product_product_name"]
        )

        self.assertIn("SELECT", query)
        self.assertIn("test_db.test_table", query)
        self.assertIn("line_item_usage_start_date", query)
        self.assertIn("GROUP BY", query)

    def test_setup_glue_integration(self):
        """Test setup_glue_integration method"""
        integration = CURIntegration(boto_glue_client=self.mock_glue_client)
        integration.glue = self.mock_glue_client

        self.mock_glue_client.create_database.return_value = {}
        self.mock_glue_client.create_table.return_value = {}

        config = GlueCatalogConfig(
            database_name="test_glue_db",
            table_name="test_glue_table",
            cur_bucket="test-bucket",
            cur_prefix="test-prefix"
        )

        result = integration.setup_glue_integration(config)

        self.assertEqual(result["database"], "test_glue_db")
        self.assertEqual(result["table"], "test_glue_table")

    def test_setup_glue_integration_raises_when_not_available(self):
        """Test setup_glue_integration raises when Glue not available"""
        integration = CURIntegration()
        integration.glue = None

        config = GlueCatalogConfig()

        with self.assertRaises(RuntimeError) as context:
            integration.setup_glue_integration(config)

        self.assertIn("Glue client not available", str(context.exception))

    def test_update_glue_partitions(self):
        """Test update_glue_partitions method"""
        integration = CURIntegration(
            boto_glue_client=self.mock_glue_client,
            boto_s3_client=self.mock_s3_client
        )
        integration.glue = self.mock_glue_client
        integration.s3 = self.mock_s3_client

        # The implementation looks for "year" and "month" as exact path segments
        # followed by their values (e.g., "year", "2024", "month", "01")
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "prefix/year/2024/month/01/data.csv"}
            ]
        }
        self.mock_glue_client.batch_create_partition.return_value = {}

        result = integration.update_glue_partitions(
            database="test_db",
            table="test_table",
            s3_bucket="test-bucket",
            s3_prefix="prefix"
        )

        self.assertEqual(result["partitions_created"], 1)

    def test_update_glue_partitions_raises_when_not_available(self):
        """Test update_glue_partitions raises when Glue not available"""
        integration = CURIntegration(boto_glue_client=self.mock_glue_client)
        integration.glue = None

        with self.assertRaises(RuntimeError) as context:
            integration.update_glue_partitions(
                database="test_db",
                table="test_table",
                s3_bucket="test-bucket",
                s3_prefix="prefix"
            )

        self.assertIn("Glue client not available", str(context.exception))

    def test_set_retention_policy(self):
        """Test set_retention_policy method"""
        integration = CURIntegration(boto_cloudwatch_client=self.mock_cloudwatch_client)
        integration.cloudwatch = self.mock_cloudwatch_client

        result = integration.set_retention_policy(
            report_name="test-report",
            retention_days=30,
            archive_before_delete=True,
            archive_location="s3://archive-bucket/"
        )

        self.assertTrue(result.enabled)
        self.assertEqual(result.retention_days, 30)
        self.assertTrue(result.archive_before_delete)

    def test_get_retention_policy(self):
        """Test get_retention_policy method"""
        integration = CURIntegration()

        integration._retention_policies["test-report"] = RetentionPolicy(
            enabled=True,
            retention_days=60
        )

        result = integration.get_retention_policy("test-report")

        self.assertIsNotNone(result)
        self.assertEqual(result.retention_days, 60)

    def test_get_retention_policy_returns_none_when_not_found(self):
        """Test get_retention_policy returns None when not found"""
        integration = CURIntegration()

        result = integration.get_retention_policy("non-existent")

        self.assertIsNone(result)

    def test_apply_retention_policy(self):
        """Test apply_retention_policy method"""
        from datetime import datetime, timedelta

        integration = CURIntegration(
            boto_s3_client=self.mock_s3_client,
            boto_cloudwatch_client=self.mock_cloudwatch_client
        )
        integration.s3 = self.mock_s3_client
        integration.cloudwatch = self.mock_cloudwatch_client

        integration._retention_policies["test-report"] = RetentionPolicy(
            enabled=True,
            retention_days=0,
            archive_before_delete=False
        )

        old_date = datetime.utcnow() - timedelta(days=1)
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "prefix/old-file.csv", "LastModified": old_date}
            ]
        }
        self.mock_s3_client.delete_objects.return_value = {}

        result = integration.apply_retention_policy(
            report_name="test-report",
            s3_bucket="test-bucket",
            s3_prefix="prefix"
        )

        self.assertEqual(result["deleted_count"], 1)

    def test_apply_retention_policy_returns_error_when_no_policy(self):
        """Test apply_retention_policy returns error when no policy"""
        integration = CURIntegration(boto_s3_client=self.mock_s3_client)
        integration.s3 = self.mock_s3_client

        result = integration.apply_retention_policy(
            report_name="non-existent",
            s3_bucket="test-bucket",
            s3_prefix="prefix"
        )

        self.assertIn("error", result)

    def test_get_delivery_metrics(self):
        """Test get_delivery_metrics method"""
        integration = CURIntegration(boto_cloudwatch_client=self.mock_cloudwatch_client)
        integration.cloudwatch = self.mock_cloudwatch_client

        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            "Datapoints": [
                {
                    "Average": 1.0,
                    "Unit": "Count",
                    "Timestamp": datetime.utcnow()
                }
            ]
        }

        result = integration.get_delivery_metrics("test-report")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].metric_name, "ReportDeliverySuccess")

    def test_get_delivery_metrics_raises_when_not_available(self):
        """Test get_delivery_metrics raises when CloudWatch not available"""
        integration = CURIntegration()
        integration.cloudwatch = None

        with self.assertRaises(RuntimeError) as context:
            integration.get_delivery_metrics("test-report")

        self.assertIn("CloudWatch client not available", str(context.exception))

    def test_setup_cloudwatch_alarms(self):
        """Test setup_cloudwatch_alarms method"""
        integration = CURIntegration(boto_cloudwatch_client=self.mock_cloudwatch_client)
        integration.cloudwatch = self.mock_cloudwatch_client

        self.mock_cloudwatch_client.put_metric_alarm.return_value = {}

        result = integration.setup_cloudwatch_alarms(
            report_name="test-report",
            s3_bucket="test-bucket",
            alarm_topic_arn="arn:aws:sns:us-east-1:123456789012:alarm-topic"
        )

        self.assertTrue(result["alarm_created"])
        self.assertEqual(result["alarm_name"], "test-report-delivery-failure")

    def test_setup_cloudwatch_alarms_raises_when_not_available(self):
        """Test setup_cloudwatch_alarms raises when CloudWatch not available"""
        integration = CURIntegration()
        integration.cloudwatch = None

        with self.assertRaises(RuntimeError) as context:
            integration.setup_cloudwatch_alarms(
                report_name="test-report",
                s3_bucket="test-bucket"
            )

        self.assertIn("CloudWatch client not available", str(context.exception))

    def test_validate_report_configuration_valid(self):
        """Test validate_report_configuration with valid config"""
        integration = CURIntegration()

        result = integration.validate_report_configuration(
            report_name="valid-report",
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_region="us-east-1"
        )

        self.assertTrue(result["valid"])
        self.assertEqual(len(result["errors"]), 0)

    def test_validate_report_configuration_invalid_name(self):
        """Test validate_report_configuration with invalid name"""
        integration = CURIntegration()

        result = integration.validate_report_configuration(
            report_name="",
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_region="us-east-1"
        )

        self.assertFalse(result["valid"])
        self.assertGreater(len(result["errors"]), 0)

    def test_validate_report_configuration_missing_bucket(self):
        """Test validate_report_configuration with missing bucket"""
        integration = CURIntegration()

        result = integration.validate_report_configuration(
            report_name="test-report",
            s3_bucket="",
            s3_prefix="test-prefix",
            s3_region="us-east-1"
        )

        self.assertFalse(result["valid"])

    def test_get_report_status(self):
        """Test get_report_status method"""
        integration = CURIntegration(
            boto_cur_client=self.mock_cur_client,
            boto_s3_client=self.mock_s3_client
        )
        integration.cur_service = self.mock_cur_client
        integration.s3 = self.mock_s3_client

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": [],
                "ReportVersioning": "CREATE_NEW_REPORT"
            }
        }
        self.mock_s3_client.list_objects_v2.return_value = {"Contents": []}

        result = integration.get_report_status("test-report")

        self.assertEqual(result["report_name"], "test-report")
        self.assertEqual(result["status"], "ACTIVE")

    def test_get_report_status_raises_when_not_available(self):
        """Test get_report_status raises when CUR service not available"""
        integration = CURIntegration()
        integration.cur_service = None

        with self.assertRaises(RuntimeError) as context:
            integration.get_report_status("test-report")

        self.assertIn("Boto3 not available", str(context.exception))

    def test_export_report_config(self):
        """Test export_report_config method"""
        integration = CURIntegration(boto_cur_client=self.mock_cur_client)
        integration.cur_service = self.mock_cur_client

        self.mock_cur_client.get_report_definition.return_value = {
            "ReportDefinition": {
                "ReportName": "test-report",
                "S3Bucket": "test-bucket",
                "S3Prefix": "test-prefix",
                "S3Region": "us-east-1",
                "TimeUnit": "DAILY",
                "Format": "textORcsv",
                "Compression": "GZIP",
                "SchemaElements": ["LINE_ITEM"],
                "AdditionalSchemaElements": ["TAGS"],
                "ReportVersioning": "CREATE_NEW_REPORT"
            }
        }

        integration._retention_policies["test-report"] = RetentionPolicy(
            enabled=True,
            retention_days=90
        )

        result = integration.export_report_config("test-report")

        self.assertEqual(result["report_name"], "test-report")
        self.assertEqual(result["s3_bucket"], "test-bucket")
        self.assertIn("retention_policy", result)


if __name__ == '__main__':
    unittest.main()
