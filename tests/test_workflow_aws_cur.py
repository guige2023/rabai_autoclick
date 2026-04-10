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

        # Create a proper mock exception with response attribute
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


if __name__ == '__main__':
    unittest.main()
