"""
Tests for workflow_aws_iot module

Tests actual implementation in src/workflow_aws_iot.py
"""
import sys
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import types

# Create mock boto3 module before importing workflow_aws_iot
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()
mock_boto3.resource = MagicMock()

# Create mock botocore exceptions
mock_botocore = types.ModuleType('botocore')
mock_botocore_exceptions = types.ModuleType('botocore.exceptions')
mock_botocore_exceptions.ClientError = Exception
mock_botocore_exceptions.BotoCoreError = Exception

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = mock_botocore
sys.modules['botocore.exceptions'] = mock_botocore_exceptions

from src.workflow_aws_iot import (
    IoTIntegration,
    ThingAttributeType,
    ThingTypeStatus,
    CertificateStatus,
    PolicyType,
    JobStatus,
    JobExecutionStatus,
    TunnelStatus,
    IoTConfig,
    ThingConfig,
    ThingInfo,
    ThingTypeConfig,
    ThingTypeInfo,
    ThingGroupConfig,
    ThingGroupInfo,
    CertificateConfig,
    CertificateInfo,
    PolicyConfig,
    PolicyInfo,
    RuleConfig,
    RuleInfo,
    JobConfig,
    JobInfo,
    TunnelConfig,
    TunnelInfo,
    FleetIndexingConfig,
    FleetIndexingInfo,
)


class TestThingAttributeType(unittest.TestCase):
    """Test ThingAttributeType enum"""
    def test_string_value(self):
        self.assertEqual(ThingAttributeType.STRING.value, "string")

    def test_string_list_value(self):
        self.assertEqual(ThingAttributeType.STRING_LIST.value, "string-list")

    def test_string_map_value(self):
        self.assertEqual(ThingAttributeType.STRING_MAP.value, "string-map")


class TestThingTypeStatus(unittest.TestCase):
    """Test ThingTypeStatus enum"""
    def test_active_value(self):
        self.assertEqual(ThingTypeStatus.ACTIVE.value, "ACTIVE")

    def test_inactive_value(self):
        self.assertEqual(ThingTypeStatus.INACTIVE.value, "INACTIVE")

    def test_deprecated_value(self):
        self.assertEqual(ThingTypeStatus.DEPRECATED.value, "DEPRECATED")


class TestCertificateStatus(unittest.TestCase):
    """Test CertificateStatus enum"""
    def test_active_value(self):
        self.assertEqual(CertificateStatus.ACTIVE.value, "ACTIVE")

    def test_inactive_value(self):
        self.assertEqual(CertificateStatus.INACTIVE.value, "INACTIVE")

    def test_revoked_value(self):
        self.assertEqual(CertificateStatus.REVOKED.value, "REVOKED")


class TestJobStatus(unittest.TestCase):
    """Test JobStatus enum"""
    def test_in_progress_value(self):
        self.assertEqual(JobStatus.IN_PROGRESS.value, "IN_PROGRESS")

    def test_queued_value(self):
        self.assertEqual(JobStatus.QUEUED.value, "QUEUED")

    def test_succeeded_value(self):
        self.assertEqual(JobStatus.SUCCEEDED.value, "SUCCEEDED")

    def test_failed_value(self):
        self.assertEqual(JobStatus.FAILED.value, "FAILED")


class TestTunnelStatus(unittest.TestCase):
    """Test TunnelStatus enum"""
    def test_open_value(self):
        self.assertEqual(TunnelStatus.OPEN.value, "Open")

    def test_closed_value(self):
        self.assertEqual(TunnelStatus.CLOSED.value, "Closed")


class TestIoTConfig(unittest.TestCase):
    """Test IoTConfig dataclass"""
    def test_default_values(self):
        config = IoTConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertIsNone(config.endpoint_url)
        self.assertTrue(config.verify_ssl)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)

    def test_custom_values(self):
        config = IoTConfig(
            region_name="us-west-2",
            aws_access_key_id="AKIAIO...MPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            endpoint_url="https://iot.us-west-2.amazonaws.com",
            verify_ssl=False,
            timeout=60,
            max_retries=5
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "AKIAIO...MPLE")
        self.assertFalse(config.verify_ssl)
        self.assertEqual(config.timeout, 60)


class TestThingConfig(unittest.TestCase):
    """Test ThingConfig dataclass"""
    def test_thing_name_required(self):
        config = ThingConfig(thing_name="TestThing")
        self.assertEqual(config.thing_name, "TestThing")
        self.assertIsNone(config.thing_type_name)
        self.assertEqual(config.thing_groups, [])
        self.assertIsNone(config.billing_group_name)

    def test_with_optional_params(self):
        config = ThingConfig(
            thing_name="TestThing",
            thing_type_name="SensorType",
            attribute_payload={"attributes": {"location": "factory"}},
            thing_groups=["Group1", "Group2"],
            billing_group_name="BillingGroup1"
        )
        self.assertEqual(config.thing_name, "TestThing")
        self.assertEqual(config.thing_type_name, "SensorType")
        self.assertEqual(config.thing_groups, ["Group1", "Group2"])


class TestThingInfo(unittest.TestCase):
    """Test ThingInfo dataclass"""
    def test_creation(self):
        thing_info = ThingInfo(
            thing_name="TestThing",
            thing_arn="arn:aws:iot:us-east-1:123456789012:thing/TestThing",
            thing_type_name="SensorType",
            thing_id="uuid-1234",
            attributes={"location": "factory"}
        )
        self.assertEqual(thing_info.thing_name, "TestThing")
        self.assertEqual(thing_info.thing_arn, "arn:aws:iot:us-east-1:123456789012:thing/TestThing")
        self.assertFalse(thing_info.dynamic)


class TestThingTypeConfig(unittest.TestCase):
    """Test ThingTypeConfig dataclass"""
    def test_creation(self):
        config = ThingTypeConfig(
            thing_type_name="SensorType",
            thing_type_properties={"description": "A sensor device"},
            tags={"env": "production"}
        )
        self.assertEqual(config.thing_type_name, "SensorType")
        self.assertEqual(config.tags["env"], "production")


class TestThingGroupConfig(unittest.TestCase):
    """Test ThingGroupConfig dataclass"""
    def test_creation(self):
        config = ThingGroupConfig(
            group_name="TestGroup",
            parent_group_name="ParentGroup",
            thing_group_properties={"description": "Test group"}
        )
        self.assertEqual(config.group_name, "TestGroup")
        self.assertEqual(config.parent_group_name, "ParentGroup")


class TestCertificateConfig(unittest.TestCase):
    """Test CertificateConfig dataclass"""
    def test_creation(self):
        config = CertificateConfig(
            certificate_pem="-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
            certificate_id="cert-id-123",
            certificate_arn="arn:aws:iot:us-east-1:123456789012:cert/cert-id-123"
        )
        self.assertIn("BEGIN CERTIFICATE", config.certificate_pem)


class TestPolicyConfig(unittest.TestCase):
    """Test PolicyConfig dataclass"""
    def test_creation(self):
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["iot:Publish"],
                    "Resource": ["arn:aws:iot:us-east-1:123456789012:topic/test"]
                }
            ]
        }
        config = PolicyConfig(
            policy_name="TestPolicy",
            policy_document=policy_doc,
            policy_description="Test policy description"
        )
        self.assertEqual(config.policy_name, "TestPolicy")
        self.assertEqual(config.policy_document["Version"], "2012-10-17")


class TestRuleConfig(unittest.TestCase):
    """Test RuleConfig dataclass"""
    def test_creation(self):
        actions = [{"iot": {"topic": "my/topic"}}]
        config = RuleConfig(
            rule_name="TestRule",
            sql="SELECT * FROM 'topic/'",
            actions=actions,
            description="Test rule"
        )
        self.assertEqual(config.rule_name, "TestRule")
        self.assertEqual(config.sql, "SELECT * FROM 'topic/'")
        self.assertEqual(config.aws_iot_sql_version, "2016-03-23")
        self.assertFalse(config.rule_disabled)


class TestJobConfig(unittest.TestCase):
    """Test JobConfig dataclass"""
    def test_creation(self):
        config = JobConfig(
            job_id="job-001",
            targets=["thing-1", "thing-2"],
            document={"instruction": "update firmware"},
            description="Firmware update job",
            job_execution_timeout_minutes=120
        )
        self.assertEqual(config.job_id, "job-001")
        self.assertEqual(len(config.targets), 2)
        self.assertEqual(config.job_execution_timeout_minutes, 120)


class TestTunnelConfig(unittest.TestCase):
    """Test TunnelConfig dataclass"""
    def test_creation(self):
        config = TunnelConfig(
            thing_name="TestThing",
            description="Test tunnel",
            timeout=7200
        )
        self.assertEqual(config.thing_name, "TestThing")
        self.assertEqual(config.timeout, 7200)


class TestFleetIndexingConfig(unittest.TestCase):
    """Test FleetIndexingConfig dataclass"""
    def test_default_values(self):
        config = FleetIndexingConfig()
        self.assertEqual(config.thing_indexing_mode, "REGISTRY_AND_SHADOW")
        self.assertEqual(config.thing_connectivity_indexing_mode, "STATUS")

    def test_custom_values(self):
        config = FleetIndexingConfig(
            thing_indexing_mode="REGISTRY_ONLY",
            thing_connectivity_indexing_mode="OFF",
            managed_fields=["thingName", "thingTypeName"]
        )
        self.assertEqual(config.thing_indexing_mode, "REGISTRY_ONLY")
        self.assertEqual(config.thing_connectivity_indexing_mode, "OFF")


class TestIoTIntegration(unittest.TestCase):
    """Test IoTIntegration class"""

    def test_initialization_with_mock_clients(self):
        """Test IoTIntegration initialization with pre-configured clients"""
        mock_iot_client = MagicMock()
        mock_iot_data_client = MagicMock()
        mock_cloudwatch_client = MagicMock()

        integration = IoTIntegration(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region_name="us-west-2",
            endpoint_url="https://iot.us-west-2.amazonaws.com",
            iot_client=mock_iot_client,
            iot_data_client=mock_iot_data_client,
            cloudwatch_client=mock_cloudwatch_client
        )

        self.assertEqual(integration.region_name, "us-west-2")
        self.assertEqual(integration.endpoint_url, "https://iot.us-west-2.amazonaws.com")
        self.assertIs(integration.iot_client, mock_iot_client)
        self.assertIs(integration.iot_data_client, mock_iot_data_client)
        self.assertIs(integration.cloudwatch_client, mock_cloudwatch_client)

    def test_thing_cache_initialization(self):
        """Test thing cache is initialized"""
        mock_iot_client = MagicMock()
        integration = IoTIntegration(iot_client=mock_iot_client)
        self.assertEqual(integration._thing_cache, {})
        self.assertEqual(integration._thing_type_cache, {})
        self.assertEqual(integration._thing_group_cache, {})

    def test_create_thing_success(self):
        """Test successful thing creation"""
        mock_iot_client = MagicMock()
        mock_iot_client.create_thing.return_value = {
            "thingName": "TestThing",
            "thingArn": "arn:aws:iot:us-east-1:123456789012:thing/TestThing",
            "thingId": "uuid-1234"
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        config = ThingConfig(
            thing_name="TestThing",
            thing_type_name="SensorType",
            attribute_payload={"attributes": {"location": "factory"}}
        )

        result = integration.create_thing(config)

        self.assertEqual(result.thing_name, "TestThing")
        self.assertEqual(result.thing_arn, "arn:aws:iot:us-east-1:123456789012:thing/TestThing")
        self.assertEqual(result.thing_type_name, "SensorType")
        mock_iot_client.create_thing.assert_called_once()

    def test_get_thing_from_cache(self):
        """Test getting thing from cache"""
        mock_iot_client = MagicMock()
        integration = IoTIntegration(iot_client=mock_iot_client)

        cached_thing = ThingInfo(
            thing_name="CachedThing",
            thing_arn="arn:aws:iot:us-east-1:123456789012:thing/CachedThing"
        )
        integration._thing_cache["CachedThing"] = cached_thing

        result = integration.get_thing("CachedThing")

        self.assertEqual(result.thing_name, "CachedThing")
        mock_iot_client.describe_thing.assert_not_called()

    def test_get_thing_from_aws(self):
        """Test getting thing from AWS when not in cache"""
        mock_iot_client = MagicMock()
        mock_iot_client.describe_thing.return_value = {
            "thingName": "RemoteThing",
            "thingArn": "arn:aws:iot:us-east-1:123456789012:thing/RemoteThing",
            "thingTypeName": "SensorType",
            "thingId": "uuid-5678",
            "version": 1,
            "attributes": {"location": "warehouse"}
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        result = integration.get_thing("RemoteThing")

        self.assertEqual(result.thing_name, "RemoteThing")
        self.assertEqual(result.thing_type_name, "SensorType")
        self.assertEqual(result.attributes["location"], "warehouse")
        mock_iot_client.describe_thing.assert_called_once_with(thingName="RemoteThing")

    def test_update_thing_success(self):
        """Test successful thing update"""
        mock_iot_client = MagicMock()
        mock_iot_client.update_thing.return_value = {}

        integration = IoTIntegration(iot_client=mock_iot_client)
        integration._thing_cache["TestThing"] = ThingInfo(
            thing_name="TestThing",
            thing_arn="arn:aws:iot:us-east-1:123456789012:thing/TestThing"
        )

        result = integration.update_thing(
            thing_name="TestThing",
            thing_type_name="NewSensorType",
            attribute_payload={"attributes": {"location": "new_location"}}
        )

        self.assertTrue(result)
        mock_iot_client.update_thing.assert_called_once()

    def test_delete_thing_success(self):
        """Test successful thing deletion"""
        mock_iot_client = MagicMock()
        mock_iot_client.delete_thing.return_value = {}

        integration = IoTIntegration(iot_client=mock_iot_client)
        integration._thing_cache["TestThing"] = ThingInfo(
            thing_name="TestThing",
            thing_arn="arn:aws:iot:us-east-1:123456789012:thing/TestThing"
        )

        result = integration.delete_thing("TestThing")

        self.assertTrue(result)
        mock_iot_client.delete_thing.assert_called_once_with(thingName="TestThing")
        self.assertNotIn("TestThing", integration._thing_cache)

    def test_create_thing_type(self):
        """Test thing type creation"""
        mock_iot_client = MagicMock()
        mock_iot_client.create_thing_type.return_value = {
            "thingTypeName": "SensorType",
            "thingTypeArn": "arn:aws:iot:us-east-1:123456789012:thingtype/SensorType",
            "thingTypeId": "type-uuid-1234"
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        config = ThingTypeConfig(
            thing_type_name="SensorType",
            thing_type_properties={"description": "A sensor device"}
        )

        result = integration.create_thing_type(config)

        self.assertEqual(result.thing_type_name, "SensorType")
        self.assertEqual(result.status, ThingTypeStatus.ACTIVE)
        mock_iot_client.create_thing_type.assert_called_once()

    def test_create_thing_group(self):
        """Test thing group creation"""
        mock_iot_client = MagicMock()
        mock_iot_client.create_thing_group.return_value = {
            "groupName": "TestGroup",
            "groupArn": "arn:aws:iot:us-east-1:123456789012:thinggroup/TestGroup",
            "groupId": "group-uuid-1234"
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        config = ThingGroupConfig(
            group_name="TestGroup",
            parent_group_name="ParentGroup"
        )

        result = integration.create_thing_group(config)

        self.assertEqual(result.group_name, "TestGroup")
        mock_iot_client.create_thing_group.assert_called_once()

    def test_list_things(self):
        """Test listing things with pagination"""
        mock_iot_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "things": [
                    {"thingName": "Thing1", "thingArn": "arn:aws:iot:us-east-1:123456789012:thing/Thing1"},
                    {"thingName": "Thing2", "thingArn": "arn:aws:iot:us-east-1:123456789012:thing/Thing2"}
                ]
            }
        ]
        mock_iot_client.get_paginator.return_value = mock_paginator

        integration = IoTIntegration(iot_client=mock_iot_client)
        result = integration.list_things(max_results=10)

        self.assertEqual(len(result), 2)
        mock_iot_client.get_paginator.assert_called_once_with("list_things")

    def test_list_thing_types(self):
        """Test listing thing types with pagination"""
        mock_iot_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "thingTypes": [
                    {"thingTypeName": "SensorType", "thingTypeArn": "arn:aws:iot:.../SensorType"},
                    {"thingTypeName": "ActuatorType", "thingTypeArn": "arn:aws:iot:.../ActuatorType"}
                ]
            }
        ]
        mock_iot_client.get_paginator.return_value = mock_paginator

        integration = IoTIntegration(iot_client=mock_iot_client)
        result = integration.list_thing_types()

        self.assertEqual(len(result), 2)
        mock_iot_client.get_paginator.assert_called_once_with("list_thing_types")

    def test_create_policy(self):
        """Test policy creation"""
        mock_iot_client = MagicMock()
        mock_iam_client = MagicMock()

        mock_iot_client.create_policy.return_value = {
            "policyName": "TestPolicy",
            "policyArn": "arn:aws:iot:us-east-1:123456789012:policy/TestPolicy",
            "policyVersionId": "1"
        }

        integration = IoTIntegration(iot_client=mock_iot_client, iam_client=mock_iam_client)
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": ["iot:Publish"], "Resource": ["*"]}]
        }
        config = PolicyConfig(policy_name="TestPolicy", policy_document=policy_doc)

        result = integration.create_policy(config)

        self.assertEqual(result.policy_name, "TestPolicy")
        mock_iot_client.create_policy.assert_called_once()

    def test_attach_thing_principal(self):
        """Test attaching thing principal"""
        mock_iot_client = MagicMock()
        mock_iot_client.attach_thing_principal.return_value = {}

        integration = IoTIntegration(iot_client=mock_iot_client)
        result = integration.attach_thing_principal("TestThing", "cert-arn")

        self.assertTrue(result)
        mock_iot_client.attach_thing_principal.assert_called_once_with(
            thingName="TestThing",
            principal="cert-arn"
        )

    def test_add_thing_to_group(self):
        """Test adding thing to group"""
        mock_iot_client = MagicMock()
        mock_iot_client.add_thing_to_thing_group.return_value = {}

        integration = IoTIntegration(iot_client=mock_iot_client)
        # Method signature: add_thing_to_group(thing_group_name, thing_name, ...)
        result = integration.add_thing_to_group("TestGroup", "TestThing")

        self.assertTrue(result)
        mock_iot_client.add_thing_to_thing_group.assert_called_once_with(
            thingGroupName="TestGroup",
            thingName="TestThing"
        )

    def test_remove_thing_from_group(self):
        """Test removing thing from group"""
        mock_iot_client = MagicMock()
        mock_iot_client.remove_thing_from_thing_group.return_value = {}

        integration = IoTIntegration(iot_client=mock_iot_client)
        # Method signature: remove_thing_from_group(thing_group_name, thing_name)
        result = integration.remove_thing_from_group("TestGroup", "TestThing")

        self.assertTrue(result)
        mock_iot_client.remove_thing_from_thing_group.assert_called_once_with(
            thingGroupName="TestGroup",
            thingName="TestThing"
        )

    def test_create_job(self):
        """Test job creation"""
        mock_iot_client = MagicMock()
        mock_iot_client.create_job.return_value = {
            "jobId": "job-001",
            "jobArn": "arn:aws:iot:us-east-1:123456789012:job/job-001",
            "description": "Test job"
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        config = JobConfig(
            job_id="job-001",
            targets=["thing-1"],
            document={"instruction": "test"},
            description="Test job"
        )

        result = integration.create_job(config)

        self.assertEqual(result.job_id, "job-001")
        mock_iot_client.create_job.assert_called_once()

    def test_create_tunnel(self):
        """Test tunnel creation"""
        mock_iot_client = MagicMock()
        mock_iot_client.create_tunnel.return_value = {
            "tunnelId": "tunnel-123",
            "tunnelArn": "arn:aws:iot:us-east-1:123456789012:tunnel/tunnel-123"
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        config = TunnelConfig(thing_name="TestThing", description="Test tunnel")

        result = integration.create_tunnel(config)

        self.assertEqual(result.tunnel_id, "tunnel-123")
        self.assertEqual(result.status, TunnelStatus.OPEN)
        mock_iot_client.create_tunnel.assert_called_once()

    def test_get_fleet_indexing_configuration(self):
        """Test getting fleet indexing configuration"""
        mock_iot_client = MagicMock()
        mock_iot_client.get_indexing_configuration.return_value = {
            "thingIndexingConfiguration": {
                "thingIndexingMode": "REGISTRY_AND_SHADOW",
                "thingConnectivityIndexingMode": "STATUS"
            }
        }

        integration = IoTIntegration(iot_client=mock_iot_client)
        result = integration.get_fleet_indexing_configuration()

        self.assertEqual(result.thing_indexing_mode, "REGISTRY_AND_SHADOW")
        self.assertEqual(result.thing_connectivity_indexing_mode, "STATUS")

    def test_update_fleet_indexing(self):
        """Test updating fleet indexing configuration"""
        mock_iot_client = MagicMock()
        mock_iot_client.update_indexing_configuration.return_value = {}

        integration = IoTIntegration(iot_client=mock_iot_client)
        config = FleetIndexingConfig(
            thing_indexing_mode="REGISTRY_ONLY",
            thing_connectivity_indexing_mode="OFF"
        )

        result = integration.update_fleet_indexing(config)

        self.assertTrue(result)
        mock_iot_client.update_indexing_configuration.assert_called_once()


if __name__ == '__main__':
    unittest.main()
