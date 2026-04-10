"""
Tests for workflow_aws_glue module
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import os
import types

# Create mock boto3 module before importing workflow_aws_glue
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
from src.workflow_aws_glue import (
    GlueIntegration,
    GlueDatabaseStatus,
    TableStatus,
    CrawlerStatus,
    JobRunStatus,
    TriggerStatus,
    DevEndpointStatus,
    DataQualityRuleType,
    DataFormat,
    TableType,
    DatabaseInfo,
    TableInfo,
    ColumnInfo,
    CrawlerInfo,
    JobInfo,
    JobRunInfo,
    TriggerInfo,
    DevEndpointInfo,
    SchemaInfo,
    DataQualityRule,
    DataQualityResult,
    DataQualityProfile,
)


class TestGlueDatabaseStatus(unittest.TestCase):
    """Test GlueDatabaseStatus enum"""
    def test_glue_database_status_values(self):
        self.assertEqual(GlueDatabaseStatus.READY.value, "READY")
        self.assertEqual(GlueDatabaseStatus.CREATING.value, "CREATING")
        self.assertEqual(GlueDatabaseStatus.DELETING.value, "DELETING")


class TestTableStatus(unittest.TestCase):
    """Test TableStatus enum"""
    def test_table_status_values(self):
        self.assertEqual(TableStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(TableStatus.CREATING.value, "CREATING")
        self.assertEqual(TableStatus.DELETING.value, "DELETING")


class TestCrawlerStatus(unittest.TestCase):
    """Test CrawlerStatus enum"""
    def test_crawler_status_values(self):
        self.assertEqual(CrawlerStatus.READY.value, "READY")
        self.assertEqual(CrawlerStatus.RUNNING.value, "RUNNING")
        self.assertEqual(CrawlerStatus.CANCELLED.value, "CANCELLED")


class TestJobRunStatus(unittest.TestCase):
    """Test JobRunStatus enum"""
    def test_job_run_status_values(self):
        self.assertEqual(JobRunStatus.RUNNING.value, "RUNNING")
        self.assertEqual(JobRunStatus.SUCCEEDED.value, "SUCCEEDED")
        self.assertEqual(JobRunStatus.FAILED.value, "FAILED")
        self.assertEqual(JobRunStatus.TIMEOUT.value, "TIMEOUT")
        self.assertEqual(JobRunStatus.STOPPED.value, "STOPPED")
        self.assertEqual(JobRunStatus.ERROR.value, "ERROR")


class TestTriggerStatus(unittest.TestCase):
    """Test TriggerStatus enum"""
    def test_trigger_status_values(self):
        self.assertEqual(TriggerStatus.CREATED.value, "CREATED")
        self.assertEqual(TriggerStatus.ACTIVATED.value, "ACTIVATED")
        self.assertEqual(TriggerStatus.DEACTIVATED.value, "DEACTIVATED")
        self.assertEqual(TriggerStatus.DELETE.value, "DELETE")


class TestDevEndpointStatus(unittest.TestCase):
    """Test DevEndpointStatus enum"""
    def test_dev_endpoint_status_values(self):
        self.assertEqual(DevEndpointStatus.READY.value, "READY")
        self.assertEqual(DevEndpointStatus.CREATING.value, "CREATING")
        self.assertEqual(DevEndpointStatus.FAILED.value, "FAILED")
        self.assertEqual(DevEndpointStatus.STOPPING.value, "STOPPING")


class TestDataQualityRuleType(unittest.TestCase):
    """Test DataQualityRuleType enum"""
    def test_data_quality_rule_type_values(self):
        self.assertEqual(DataQualityRuleType.IS_NULL.value, "IS_NULL")
        self.assertEqual(DataQualityRuleType.IS_NOT_NULL.value, "IS_NOT_NULL")
        self.assertEqual(DataQualityRuleType.IS_UNIQUE.value, "IS_UNIQUE")
        self.assertEqual(DataQualityRuleType.IS_COMPLETE.value, "IS_COMPLETE")
        self.assertEqual(DataQualityRuleType.MATCHES_PATTERN.value, "MATCHES_PATTERN")
        self.assertEqual(DataQualityRuleType.IN_RANGE.value, "IN_RANGE")
        self.assertEqual(DataQualityRuleType.CONTAINS.value, "CONTAINS")


class TestDataFormat(unittest.TestCase):
    """Test DataFormat enum"""
    def test_data_format_values(self):
        self.assertEqual(DataFormat.JSON.value, "json")
        self.assertEqual(DataFormat.CSV.value, "csv")
        self.assertEqual(DataFormat.PARQUET.value, "parquet")
        self.assertEqual(DataFormat.ORC.value, "orc")
        self.assertEqual(DataFormat.AVRO.value, "avro")


class TestTableType(unittest.TestCase):
    """Test TableType enum"""
    def test_table_type_values(self):
        self.assertEqual(TableType.EXTERNAL_TABLE.value, "EXTERNAL_TABLE")
        self.assertEqual(TableType.MANAGED_TABLE.value, "MANAGED_TABLE")
        self.assertEqual(TableType.VIRTUAL_VIEW.value, "VIRTUAL_VIEW")
        self.assertEqual(TableType.MATERIALIZED_VIEW.value, "MATERIALIZED_VIEW")


class TestDatabaseInfo(unittest.TestCase):
    """Test DatabaseInfo dataclass"""
    def test_database_info_defaults(self):
        info = DatabaseInfo(name="test-db")
        self.assertEqual(info.name, "test-db")
        self.assertEqual(info.status, GlueDatabaseStatus.READY)
        self.assertEqual(info.tags, {})

    def test_database_info_full(self):
        info = DatabaseInfo(
            name="prod-db",
            description="Production database",
            location_uri="s3://my-bucket/data/",
            parameters={"param1": "value1"},
            status=GlueDatabaseStatus.READY,
            tags={"env": "prod"}
        )
        self.assertEqual(info.name, "prod-db")
        self.assertEqual(info.description, "Production database")
        self.assertEqual(info.location_uri, "s3://my-bucket/data/")


class TestTableInfo(unittest.TestCase):
    """Test TableInfo dataclass"""
    def test_table_info_defaults(self):
        info = TableInfo(name="test-table", database_name="test-db")
        self.assertEqual(info.name, "test-table")
        self.assertEqual(info.database_name, "test-db")
        self.assertEqual(info.table_type, TableType.EXTERNAL_TABLE)
        self.assertEqual(info.status, TableStatus.ACTIVE)

    def test_table_info_full(self):
        info = TableInfo(
            name="prod-table",
            database_name="prod-db",
            description="Production table",
            table_type=TableType.MANAGED_TABLE,
            storage_descriptor={"Columns": []},
            partition_keys=[{"Name": "year", "Type": "string"}],
            parameters={"parquet.compression": "SNAPPY"},
            tags={"env": "prod"}
        )
        self.assertEqual(info.name, "prod-table")
        self.assertEqual(info.table_type, TableType.MANAGED_TABLE)
        self.assertEqual(len(info.partition_keys), 1)


class TestColumnInfo(unittest.TestCase):
    """Test ColumnInfo dataclass"""
    def test_column_info(self):
        col = ColumnInfo(name="id", type="int", comment="Primary key", partition_key=False)
        self.assertEqual(col.name, "id")
        self.assertEqual(col.type, "int")
        self.assertFalse(col.partition_key)

    def test_column_info_partition_key(self):
        col = ColumnInfo(name="dt", type="string", partition_key=True)
        self.assertTrue(col.partition_key)


class TestCrawlerInfo(unittest.TestCase):
    """Test CrawlerInfo dataclass"""
    def test_crawler_info_defaults(self):
        info = CrawlerInfo(name="test-crawler")
        self.assertEqual(info.name, "test-crawler")
        self.assertEqual(info.state, CrawlerStatus.READY)

    def test_crawler_info_full(self):
        info = CrawlerInfo(
            name="prod-crawler",
            description="Production crawler",
            database_name="prod-db",
            table_prefix="prod_",
            role="arn:aws:iam::123456789:role/my-role",
            targets={"S3Targets": [{"Path": "s3://my-bucket/data/"}]},
            schedule="cron(0 0 * * ? *)",
            state=CrawlerStatus.RUNNING
        )
        self.assertEqual(info.name, "prod-crawler")
        self.assertEqual(info.state, CrawlerStatus.RUNNING)


class TestJobInfo(unittest.TestCase):
    """Test JobInfo dataclass"""
    def test_job_info_defaults(self):
        info = JobInfo(name="test-job")
        self.assertEqual(info.name, "test-job")
        self.assertEqual(info.arguments, {})
        self.assertEqual(info.default_arguments, {})

    def test_job_info_full(self):
        info = JobInfo(
            name="prod-job",
            description="Production ETL job",
            role="arn:aws:iam::123456789:role/my-role",
            command={"Name": "glueetl", "ScriptLocation": "s3://my-bucket/scripts/job.py"},
            script_location="s3://my-bucket/scripts/job.py",
            python_version="3",
            glue_version="3.0",
            worker_type="G.1X",
            number_of_workers=10,
            timeout=60,
            max_retries=3
        )
        self.assertEqual(info.name, "prod-job")
        self.assertEqual(info.worker_type, "G.1X")
        self.assertEqual(info.number_of_workers, 10)


class TestJobRunInfo(unittest.TestCase):
    """Test JobRunInfo dataclass"""
    def test_job_run_info(self):
        info = JobRunInfo(
            job_name="test-job",
            run_id="run-123",
            status=JobRunStatus.RUNNING
        )
        self.assertEqual(info.job_name, "test-job")
        self.assertEqual(info.run_id, "run-123")
        self.assertEqual(info.status, JobRunStatus.RUNNING)


class TestTriggerInfo(unittest.TestCase):
    """Test TriggerInfo dataclass"""
    def test_trigger_info_defaults(self):
        info = TriggerInfo(name="test-trigger", trigger_type="SCHEDULED")
        self.assertEqual(info.name, "test-trigger")
        self.assertEqual(info.trigger_status, TriggerStatus.CREATED)

    def test_trigger_info_full(self):
        info = TriggerInfo(
            name="prod-trigger",
            trigger_type="SCHEDULED",
            trigger_status=TriggerStatus.ACTIVATED,
            schedule="cron(0 0 * * ? *)",
            predicate={"Conditions": []},
            actions=[{"JobName": "my-job"}],
            description="Production trigger"
        )
        self.assertEqual(info.name, "prod-trigger")
        self.assertEqual(info.trigger_status, TriggerStatus.ACTIVATED)


class TestDevEndpointInfo(unittest.TestCase):
    """Test DevEndpointInfo dataclass"""
    def test_dev_endpoint_info(self):
        info = DevEndpointInfo(
            name="test-endpoint",
            role_arn="arn:aws:iam::123456789:role/my-role",
            security_group_ids=["sg-12345"],
            subnet_id="subnet-12345",
            status=DevEndpointStatus.READY
        )
        self.assertEqual(info.name, "test-endpoint")
        self.assertEqual(info.status, DevEndpointStatus.READY)


class TestSchemaInfo(unittest.TestCase):
    """Test SchemaInfo dataclass"""
    def test_schema_info(self):
        info = SchemaInfo(
            registry_name="my-registry",
            schema_name="my-schema",
            schema_arn="arn:aws:glue:us-east-1:123456789:schema/my-registry/my-schema",
            data_format="JSON",
            compatibility="BACKWARD"
        )
        self.assertEqual(info.registry_name, "my-registry")
        self.assertEqual(info.data_format, "JSON")


class TestDataQualityRule(unittest.TestCase):
    """Test DataQualityRule dataclass"""
    def test_data_quality_rule(self):
        rule = DataQualityRule(
            rule_type=DataQualityRuleType.IS_NOT_NULL,
            column="id",
            description="ID should not be null"
        )
        self.assertEqual(rule.rule_type, DataQualityRuleType.IS_NOT_NULL)
        self.assertEqual(rule.column, "id")

    def test_data_quality_rule_disabled(self):
        rule = DataQualityRule(
            rule_type=DataQualityRuleType.IS_UNIQUE,
            column="email",
            disabled=True
        )
        self.assertTrue(rule.disabled)


class TestDataQualityResult(unittest.TestCase):
    """Test DataQualityResult dataclass"""
    def test_data_quality_result_passed(self):
        result = DataQualityResult(
            rule="IS_NOT_NULL",
            column="id",
            passed=True,
            total_count=1000
        )
        self.assertTrue(result.passed)
        self.assertEqual(result.total_count, 1000)

    def test_data_quality_result_failed(self):
        result = DataQualityResult(
            rule="IS_NOT_NULL",
            column="id",
            passed=False,
            failed_count=50,
            total_count=1000,
            error_message="50 null values found"
        )
        self.assertFalse(result.passed)
        self.assertEqual(result.failed_count, 50)


class TestDataQualityProfile(unittest.TestCase):
    """Test DataQualityProfile dataclass"""
    def test_data_quality_profile(self):
        profile = DataQualityProfile(
            name="test-profile",
            database_name="test-db",
            table_name="test-table",
            rules=[
                DataQualityRule(rule_type=DataQualityRuleType.IS_NOT_NULL, column="id"),
                DataQualityRule(rule_type=DataQualityRuleType.IS_COMPLETE, column="email")
            ]
        )
        self.assertEqual(len(profile.rules), 2)


class TestGlueIntegration(unittest.TestCase):
    """Test GlueIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_glue_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_iam_client = MagicMock()

        self.integration = GlueIntegration(
            region_name="us-east-1",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret"
        )
        self.integration._clients = {
            "glue": self.mock_glue_client,
            "cloudwatch": self.mock_cloudwatch_client,
            "iam": self.mock_iam_client
        }

    def test_init(self):
        """Test initialization"""
        integration = GlueIntegration(region_name="us-west-2")
        self.assertEqual(integration.region_name, "us-west-2")

    def test_glue_client_property(self):
        """Test glue_client property"""
        client = self.integration.glue_client
        self.assertEqual(client, self.mock_glue_client)

    def test_cloudwatch_client_property(self):
        """Test cloudwatch_client property"""
        client = self.integration.cloudwatch_client
        self.assertEqual(client, self.mock_cloudwatch_client)

    def test_create_database(self):
        """Test creating a database"""
        self.mock_glue_client.create_database.return_value = {
            "Database": {
                "Name": "test-db",
                "Arn": "arn:aws:glue:us-east-1:123456789:database/test-db",
                "Description": "Test database"
            }
        }

        result = self.integration.create_database(
            name="test-db",
            description="Test database"
        )

        self.assertEqual(result.name, "test-db")
        self.mock_glue_client.create_database.assert_called_once()

    def test_get_database(self):
        """Test getting database info"""
        self.mock_glue_client.get_database.return_value = {
            "Database": {
                "Name": "test-db",
                "Arn": "arn:aws:glue:us-east-1:123456789:database/test-db",
                "Description": "Test database"
            }
        }

        result = self.integration.get_database(name="test-db")

        self.assertEqual(result.name, "test-db")
        self.mock_glue_client.get_database.assert_called_once()

    def test_list_databases(self):
        """Test listing databases"""
        self.mock_glue_client.get_paginator.return_value.paginate.return_value = [
            {"DatabaseList": [{"Name": "db1"}, {"Name": "db2"}]}
        ]

        result = self.integration.list_databases()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "db1")

    def test_delete_database(self):
        """Test deleting a database"""
        self.mock_glue_client.delete_database.return_value = {}

        result = self.integration.delete_database(name="test-db")

        self.mock_glue_client.delete_database.assert_called_once_with(Name="test-db")

    def test_create_table(self):
        """Test creating a table"""
        self.mock_glue_client.create_table.return_value = {
            "Table": {
                "Name": "test-table",
                "DatabaseName": "test-db",
                "TableType": "EXTERNAL_TABLE"
            }
        }

        result = self.integration.create_table(
            database_name="test-db",
            name="test-table",
            table_type=TableType.EXTERNAL_TABLE
        )

        self.assertEqual(result.name, "test-table")
        self.mock_glue_client.create_table.assert_called_once()

    def test_get_table(self):
        """Test getting table info"""
        self.mock_glue_client.get_table.return_value = {
            "Table": {
                "Name": "test-table",
                "DatabaseName": "test-db",
                "TableType": "EXTERNAL_TABLE"
            }
        }

        result = self.integration.get_table(database_name="test-db", name="test-table")

        self.assertEqual(result.name, "test-table")
        self.mock_glue_client.get_table.assert_called_once()

    def test_update_table(self):
        """Test updating a table"""
        self.mock_glue_client.update_table.return_value = {
            "Table": {
                "Name": "test-table",
                "DatabaseName": "test-db"
            }
        }

        result = self.integration.update_table(
            database_name="test-db",
            name="test-table",
            table_input={"Name": "test-table", "TableType": "EXTERNAL_TABLE"}
        )

        self.mock_glue_client.update_table.assert_called_once()

    def test_delete_table(self):
        """Test deleting a table"""
        self.mock_glue_client.delete_table.return_value = {}

        result = self.integration.delete_table(database_name="test-db", name="test-table")

        self.mock_glue_client.delete_table.assert_called_once()

    def test_get_table_versions(self):
        """Test getting table versions"""
        self.mock_glue_client.get_table_versions.return_value = {
            "TableVersions": [
                {"VersionId": "1", "Table": {"Name": "test-table"}},
                {"VersionId": "2", "Table": {"Name": "test-table"}}
            ]
        }

        result = self.integration.get_table_versions(database_name="test-db", name="test-table")

        self.assertEqual(len(result), 2)

    def test_create_crawler(self):
        """Test creating a crawler"""
        self.mock_glue_client.create_crawler.return_value = {}

        result = self.integration.create_crawler(
            name="test-crawler",
            role="arn:aws:iam::123456789:role/my-role",
            database_name="test-db",
            targets={"S3Targets": [{"Path": "s3://my-bucket/data/"}]}
        )

        self.mock_glue_client.create_crawler.assert_called_once()

    def test_start_crawler(self):
        """Test starting a crawler"""
        self.mock_glue_client.start_crawler.return_value = {}

        result = self.integration.start_crawler(name="test-crawler")

        self.mock_glue_client.start_crawler.assert_called_once_with(Name="test-crawler")

    def test_stop_crawler(self):
        """Test stopping a crawler"""
        self.mock_glue_client.stop_crawler.return_value = {}

        result = self.integration.stop_crawler(name="test-crawler")

        self.mock_glue_client.stop_crawler.assert_called_once()

    def test_get_crawler(self):
        """Test getting crawler info"""
        self.mock_glue_client.get_crawler.return_value = {
            "Crawler": {
                "Name": "test-crawler",
                "State": "READY",
                "DatabaseName": "test-db"
            }
        }

        result = self.integration.get_crawler(name="test-crawler")

        self.assertEqual(result.name, "test-crawler")
        self.assertEqual(result.state, CrawlerStatus.READY)

    def test_delete_crawler(self):
        """Test deleting a crawler"""
        self.mock_glue_client.delete_crawler.return_value = {}

        result = self.integration.delete_crawler(name="test-crawler")

        self.mock_glue_client.delete_crawler.assert_called_once()

    def test_create_job(self):
        """Test creating a job"""
        self.mock_glue_client.create_job.return_value = {
            "Name": "test-job",
            "Arn": "arn:aws:glue:us-east-1:123456789:job/test-job"
        }

        result = self.integration.create_job(
            name="test-job",
            role="arn:aws:iam::123456789:role/my-role",
            command={"Name": "glueetl", "ScriptLocation": "s3://my-bucket/scripts/job.py"}
        )

        self.assertEqual(result.name, "test-job")
        self.mock_glue_client.create_job.assert_called_once()

    def test_start_job_run(self):
        """Test starting a job run"""
        self.mock_glue_client.start_job_run.return_value = {
            "JobRunId": "run-12345"
        }

        result = self.integration.start_job_run(job_name="test-job")

        self.assertEqual(result, "run-12345")
        self.mock_glue_client.start_job_run.assert_called_once()

    def test_get_job_run(self):
        """Test getting job run info"""
        self.mock_glue_client.get_job_run.return_value = {
            "JobRun": {
                "Id": "run-12345",
                "JobName": "test-job",
                "Status": "SUCCEEDED",
                "ExecutionTime": 120.0
            }
        }

        result = self.integration.get_job_run(job_name="test-job", run_id="run-12345")

        self.assertEqual(result.run_id, "run-12345")
        self.assertEqual(result.status, JobRunStatus.SUCCEEDED)

    def test_list_job_runs(self):
        """Test listing job runs"""
        self.mock_glue_client.get_paginator.return_value.paginate.return_value = [
            {"JobRuns": [
                {"Id": "run-1", "Status": "SUCCEEDED"},
                {"Id": "run-2", "Status": "FAILED"}
            ]}
        ]

        result = self.integration.list_job_runs(job_name="test-job")

        self.assertEqual(len(result), 2)

    def test_create_trigger(self):
        """Test creating a trigger"""
        self.mock_glue_client.create_trigger.return_value = {
            "Name": "test-trigger"
        }

        result = self.integration.create_trigger(
            name="test-trigger",
            trigger_type="SCHEDULED",
            schedule="cron(0 0 * * ? *)",
            actions=[{"JobName": "test-job"}]
        )

        self.assertEqual(result.name, "test-trigger")
        self.mock_glue_client.create_trigger.assert_called_once()

    def test_start_trigger(self):
        """Test starting a trigger"""
        self.mock_glue_client.start_trigger.return_value = {}

        result = self.integration.start_trigger(name="test-trigger")

        self.mock_glue_client.start_trigger.assert_called_once()

    def test_stop_trigger(self):
        """Test stopping a trigger"""
        self.mock_glue_client.stop_trigger.return_value = {}

        result = self.integration.stop_trigger(name="test-trigger")

        self.mock_glue_client.stop_trigger.assert_called_once()

    def test_delete_trigger(self):
        """Test deleting a trigger"""
        self.mock_glue_client.delete_trigger.return_value = {}

        result = self.integration.delete_trigger(name="test-trigger")

        self.mock_glue_client.delete_trigger.assert_called_once()

    def test_create_dev_endpoint(self):
        """Test creating a dev endpoint"""
        self.mock_glue_client.create_dev_endpoint.return_value = {
            "DevEndpoint": {
                "EndpointName": "test-endpoint",
                "Status": "PROVISIONING"
            }
        }

        result = self.integration.create_dev_endpoint(
            name="test-endpoint",
            role_arn="arn:aws:iam::123456789:role/my-role",
            security_group_ids=["sg-12345"],
            subnet_id="subnet-12345"
        )

        self.assertEqual(result.name, "test-endpoint")
        self.mock_glue_client.create_dev_endpoint.assert_called_once()

    def test_get_dev_endpoint(self):
        """Test getting dev endpoint info"""
        self.mock_glue_client.get_dev_endpoint.return_value = {
            "DevEndpoint": {
                "EndpointName": "test-endpoint",
                "Status": "READY"
            }
        }

        result = self.integration.get_dev_endpoint(name="test-endpoint")

        self.assertEqual(result.name, "test-endpoint")
        self.assertEqual(result.status, DevEndpointStatus.READY)

    def test_delete_dev_endpoint(self):
        """Test deleting a dev endpoint"""
        self.mock_glue_client.delete_dev_endpoint.return_value = {}

        result = self.integration.delete_dev_endpoint(name="test-endpoint")

        self.mock_glue_client.delete_dev_endpoint.assert_called_once()

    def test_create_schema(self):
        """Test creating a schema"""
        self.mock_glue_client.create_schema.return_value = {
            "SchemaArn": "arn:aws:glue:us-east-1:123456789:schema/my-registry/my-schema",
            "SchemaName": "my-schema"
        }

        result = self.integration.create_schema(
            registry_name="my-registry",
            schema_name="my-schema",
            data_format="JSON",
            compatibility="BACKWARD"
        )

        self.assertIn("SchemaArn", result)
        self.mock_glue_client.create_schema.assert_called_once()

    def test_get_schema(self):
        """Test getting schema info"""
        self.mock_glue_client.get_schema.return_value = {
            "Schema": {
                "RegistryName": "my-registry",
                "SchemaName": "my-schema",
                "SchemaArn": "arn:aws:glue:us-east-1:123456789:schema/my-registry/my-schema"
            }
        }

        result = self.integration.get_schema(registry_name="my-registry", schema_name="my-schema")

        self.assertEqual(result.schema_name, "my-schema")
        self.mock_glue_client.get_schema.assert_called_once()

    def test_register_schema_version(self):
        """Test registering a schema version"""
        self.mock_glue_client.register_schema_version.return_value = {
            "VersionId": "1",
            "SchemaArn": "arn:aws:glue:us-east-1:123456789:schema/my-registry/my-schema"
        }

        result = self.integration.register_schema_version(
            registry_name="my-registry",
            schema_name="my-schema",
            schema_definition='{"type": "record", "name": "Test", "fields": []}'
        )

        self.assertEqual(result["VersionId"], "1")

    def test_get_schema_version(self):
        """Test getting schema version"""
        self.mock_glue_client.get_schema_version.return_value = {
            "SchemaDefinition": '{"type": "record"}',
            "VersionId": "1"
        }

        result = self.integration.get_schema_version(
            registry_name="my-registry",
            schema_name="my-schema"
        )

        self.assertIn("SchemaDefinition", result)

    def test_check_schema_version_compatibility(self):
        """Test checking schema version compatibility"""
        self.mock_glue_client.check_schema_version.return_value = {
            "Compatibility": "BACKWARD",
            "LatestSchemaVersion": "1",
            "EarliestSchemaVersion": "1"
        }

        result = self.integration.check_schema_version_compatibility(
            registry_name="my-registry",
            schema_name="my-schema",
            schema_definition='{"type": "record"}'
        )

        self.assertEqual(result["Compatibility"], "BACKWARD")

    def test_create_data_quality_profile(self):
        """Test creating a data quality profile"""
        profile = self.integration.create_data_quality_profile(
            name="test-profile",
            database_name="test-db",
            table_name="test-table",
            rules=[
                DataQualityRule(rule_type=DataQualityRuleType.IS_NOT_NULL, column="id"),
                DataQualityRule(rule_type=DataQualityRuleType.IS_COMPLETE, column="email")
            ]
        )

        self.assertEqual(profile.name, "test-profile")
        self.assertEqual(len(profile.rules), 2)

    def test_evaluate_data_quality(self):
        """Test evaluating data quality"""
        self.mock_glue_client.get_table.return_value = {
            "Table": {
                "Name": "test-table",
                "DatabaseName": "test-db",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "id", "Type": "int"},
                        {"Name": "email", "Type": "string"}
                    ]
                }
            }
        }

        profile = DataQualityProfile(
            name="test-profile",
            database_name="test-db",
            table_name="test-table",
            rules=[
                DataQualityRule(rule_type=DataQualityRuleType.IS_NOT_NULL, column="id"),
                DataQualityRule(rule_type=DataQualityRuleType.IS_COMPLETE, column="email")
            ]
        )

        result = self.integration.evaluate_data_quality(profile)

        self.assertEqual(len(result.results), 2)

    def test_get_data_quality_results(self):
        """Test getting data quality results"""
        self.mock_glue_client.get_data_quality_results.return_value = {
            "Results": [
                {"Rule": "IS_NOT_NULL", "Passed": True},
                {"Rule": "IS_COMPLETE", "Passed": False}
            ]
        }

        result = self.integration.get_data_quality_results(profile_arn="arn:aws:glue:us-east-1:123456789:dataQuality/profile/test")

        self.assertIn("Results", result)

    def test_get_cloudwatch_metrics(self):
        """Test getting CloudWatch metrics"""
        self.mock_cloudwatch_client.get_metric_data.return_value = {
            "MetricDataResults": [
                {
                    "Label": " glue.aws.amazon.com/job/run/duration ",
                    "Values": [120.0]
                }
            ]
        }

        result = self.integration.get_cloudwatch_metrics(
            job_name="test-job",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2)
        )

        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
