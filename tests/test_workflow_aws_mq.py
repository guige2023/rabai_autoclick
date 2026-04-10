"""
Tests for workflow_aws_mq module
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

# Create mock boto3 module before importing workflow_aws_mq
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
import src.workflow_aws_mq as _mq_module

# Extract classes
MQIntegration = _mq_module.MQIntegration
BrokerEngine = _mq_module.BrokerEngine
BrokerInstanceType = _mq_module.BrokerInstanceType
DeploymentMode = _mq_module.DeploymentMode
BrokerState = _mq_module.BrokerState
LogType = _mq_module.LogType
EncryptionAlgorithm = _mq_module.EncryptionAlgorithm
MQConfig = _mq_module.MQConfig
BrokerConfig = _mq_module.BrokerConfig
UserConfig = _mq_module.UserConfig
ConfigurationConfig = _mq_module.ConfigurationConfig
TagsConfig = _mq_module.TagsConfig


class TestBrokerEngine(unittest.TestCase):
    """Test BrokerEngine enum"""
    def test_activemq_value(self):
        self.assertEqual(BrokerEngine.ACTIVEMQ.value, "ActiveMQ")

    def test_rabbitmq_value(self):
        self.assertEqual(BrokerEngine.RABBITMQ.value, "RabbitMQ")


class TestBrokerInstanceType(unittest.TestCase):
    """Test BrokerInstanceType enum"""
    def test_t2_micro(self):
        self.assertEqual(BrokerInstanceType.T2_MICRO.value, "mq.t2.micro")

    def test_m5_large(self):
        self.assertEqual(BrokerInstanceType.M5_LARGE.value, "mq.m5.large")


class TestDeploymentMode(unittest.TestCase):
    """Test DeploymentMode enum"""
    def test_single_instance(self):
        self.assertEqual(DeploymentMode.SINGLE_INSTANCE.value, "SINGLE_INSTANCE")

    def test_active_standby_multi_az(self):
        self.assertEqual(DeploymentMode.ACTIVE_STANDBY_MULTI_AZ.value, "ACTIVE_STANDBY_MULTI_AZ")

    def test_cluster(self):
        self.assertEqual(DeploymentMode.CLUSTER.value, "CLUSTER")


class TestBrokerState(unittest.TestCase):
    """Test BrokerState enum"""
    def test_creation_in_progress(self):
        self.assertEqual(BrokerState.CREATION_IN_PROGRESS.value, "CREATION_IN_PROGRESS")

    def test_running(self):
        self.assertEqual(BrokerState.RUNNING.value, "RUNNING")


class TestEncryptionAlgorithm(unittest.TestCase):
    """Test EncryptionAlgorithm enum"""
    def test_aes_128(self):
        self.assertEqual(EncryptionAlgorithm.AES_128.value, "AES_128")

    def test_aes_256(self):
        self.assertEqual(EncryptionAlgorithm.AES_256.value, "AES_256")


class TestMQConfig(unittest.TestCase):
    """Test MQConfig dataclass"""
    def test_default_config(self):
        config = MQConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)

    def test_custom_config(self):
        config = MQConfig(
            region_name="us-west-2",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret"
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "test_key")


class TestBrokerConfig(unittest.TestCase):
    """Test BrokerConfig dataclass"""
    def test_default_broker_config(self):
        config = BrokerConfig(broker_name="test-broker")
        self.assertEqual(config.broker_name, "test-broker")
        self.assertEqual(config.engine_type, BrokerEngine.ACTIVEMQ)
        self.assertEqual(config.engine_version, "5.17.6")

    def test_custom_broker_config(self):
        config = BrokerConfig(
            broker_name="my-broker",
            engine_type=BrokerEngine.RABBITMQ,
            engine_version="3.9.1",
            instance_type=BrokerInstanceType.M5_LARGE,
            deployment_mode=DeploymentMode.CLUSTER,
            publicly_accessible=True
        )
        self.assertEqual(config.broker_name, "my-broker")
        self.assertEqual(config.engine_type, BrokerEngine.RABBITMQ)
        self.assertEqual(config.publicly_accessible, True)


class TestUserConfig(unittest.TestCase):
    """Test UserConfig dataclass"""
    def test_user_config_creation(self):
        config = UserConfig(
            broker_name="test-broker",
            username="testuser",
            password="testpass",
            console_access=True,
            groups=["admins"]
        )
        self.assertEqual(config.broker_name, "test-broker")
        self.assertEqual(config.username, "testuser")
        self.assertEqual(config.console_access, True)


class TestMQIntegration(unittest.TestCase):
    """Test MQIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mq_client = MagicMock()
        self.mock_cw_client = MagicMock()
        self.mock_ec2_client = MagicMock()

        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_mq_client
            self.mq = MQIntegration(MQConfig(region_name="us-east-1"))
            self.mq._mq_client = self.mock_mq_client
            self.mq._cw_client = self.mock_cw_client
            self.mq._ec2_client = self.mock_ec2_client

    def test_init_with_config(self):
        """Test MQIntegration initialization with config"""
        config = MQConfig(region_name="us-west-2")
        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_mq_client
            mq = MQIntegration(config)
            self.assertEqual(mq.config.region_name, "us-west-2")

    def test_create_broker(self):
        """Test creating a broker"""
        self.mock_mq_client.create_broker.return_value = {
            "BrokerName": "test-broker",
            "BrokerArn": "arn:aws:mq:us-east-1:123456789012:broker:test-broker",
            "BrokerState": "CREATION_IN_PROGRESS"
        }

        config = BrokerConfig(
            broker_name="test-broker",
            engine_type=BrokerEngine.ACTIVEMQ,
            engine_version="5.17.6",
            instance_type=BrokerInstanceType.T2_MICRO,
            deployment_mode=DeploymentMode.SINGLE_INSTANCE,
            master_username="admin",
            master_password="password"
        )

        result = self.mq.create_broker(config)
        self.assertEqual(result["BrokerName"], "test-broker")
        self.mock_mq_client.create_broker.assert_called_once()

    def test_create_broker_with_vpc(self):
        """Test creating a broker with VPC configuration"""
        self.mock_mq_client.create_broker.return_value = {
            "BrokerName": "vpc-broker",
            "BrokerArn": "arn:aws:mq:us-east-1:123456789012:broker:vpc-broker"
        }

        config = BrokerConfig(
            broker_name="vpc-broker",
            vpc_id="vpc-12345",
            subnet_ids=["subnet-1", "subnet-2"],
            security_groups=["sg-123"]
        )

        result = self.mq.create_broker(config)
        self.assertEqual(result["BrokerName"], "vpc-broker")

    def test_describe_broker(self):
        """Test describing a broker"""
        self.mock_mq_client.describe_broker.return_value = {
            "BrokerName": "test-broker",
            "BrokerState": "RUNNING",
            "EngineType": "ActiveMQ",
            "EngineVersion": "5.17.6"
        }

        result = self.mq.describe_broker("test-broker")
        self.assertEqual(result["BrokerName"], "test-broker")
        self.assertEqual(result["BrokerState"], "RUNNING")

    def test_describe_broker_with_cache(self):
        """Test describing a broker uses cache"""
        cache_key = "broker:test-broker"
        self.mq._cache[cache_key] = {
            "value": {"BrokerName": "test-broker", "BrokerState": "RUNNING"},
            "timestamp": datetime.now()
        }

        result = self.mq.describe_broker("test-broker")
        self.assertEqual(result["BrokerName"], "test-broker")
        self.mock_mq_client.describe_broker.assert_not_called()

    def test_describe_broker_refresh(self):
        """Test describing a broker with refresh"""
        self.mock_mq_client.describe_broker.return_value = {
            "BrokerName": "test-broker",
            "BrokerState": "RUNNING"
        }

        cache_key = "broker:test-broker"
        self.mq._cache[cache_key] = {
            "value": {"BrokerName": "test-broker", "BrokerState": "CREATION_IN_PROGRESS"},
            "timestamp": datetime.now()
        }

        result = self.mq.describe_broker("test-broker", refresh=True)
        self.mock_mq_client.describe_broker.assert_called_once()

    def test_list_brokers(self):
        """Test listing brokers"""
        self.mock_mq_client.get_paginator.return_value.paginate.return_value = [
            {"BrokerSummaries": [{"BrokerName": "broker-1"}, {"BrokerName": "broker-2"}]}
        ]

        result = self.mq.list_brokers()
        self.assertEqual(len(result), 2)

    def test_delete_broker(self):
        """Test deleting a broker"""
        self.mock_mq_client.delete_broker.return_value = {
            "BrokerName": "test-broker"
        }

        result = self.mq.delete_broker("test-broker")
        self.assertEqual(result["BrokerName"], "test-broker")

    def test_get_broker_state(self):
        """Test getting broker state"""
        self.mock_mq_client.describe_broker.return_value = {
            "BrokerName": "test-broker",
            "BrokerState": "RUNNING"
        }

        state = self.mq.get_broker_state("test-broker")
        self.assertEqual(state, BrokerState.RUNNING)

    def test_reboot_broker(self):
        """Test rebooting a broker"""
        self.mock_mq_client.reboot_broker.return_value = {}

        result = self.mq.reboot_broker("test-broker", reboot_standby=True)
        self.mock_mq_client.reboot_broker.assert_called_once_with(
            BrokerName="test-broker",
            RebootRebootStandbyBrokers=True
        )


class TestConfigurationManagement(unittest.TestCase):
    """Test configuration management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mq_client = MagicMock()
        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_mq_client
            self.mq = MQIntegration(MQConfig(region_name="us-east-1"))
            self.mq._mq_client = self.mock_mq_client

    def test_create_configuration(self):
        """Test creating a configuration"""
        self.mock_mq_client.create_configuration.return_value = {
            "ConfigurationId": "config-123",
            "Name": "test-config",
            "EngineType": "ActiveMQ",
            "EngineVersion": "5.17.6"
        }

        result = self.mq.create_configuration(
            name="test-config",
            engine_type=BrokerEngine.ACTIVEMQ,
            engine_version="5.17.6"
        )
        self.assertEqual(result["ConfigurationId"], "config-123")

    def test_describe_configuration(self):
        """Test describing a configuration"""
        self.mock_mq_client.describe_configuration.return_value = {
            "ConfigurationId": "config-123",
            "Name": "test-config",
            "Revision": 1
        }

        result = self.mq.describe_configuration("config-123")
        self.assertEqual(result["ConfigurationId"], "config-123")

    def test_list_configurations(self):
        """Test listing configurations"""
        self.mock_mq_client.list_configurations.return_value = {
            "Configurations": [
                {"ConfigurationId": "config-1", "Name": "config-1"},
                {"ConfigurationId": "config-2", "Name": "config-2"}
            ]
        }

        result = self.mq.list_configurations()
        self.assertEqual(len(result), 2)

    def test_update_configuration(self):
        """Test updating a configuration"""
        self.mock_mq_client.update_configuration.return_value = {
            "ConfigurationId": "config-123",
            "Revision": 2
        }

        result = self.mq.update_configuration("config-123", "<xml>config data</xml>")
        self.assertEqual(result["Revision"], 2)

    def test_delete_configuration(self):
        """Test deleting a configuration"""
        self.mock_mq_client.delete_configuration.return_value = {}

        result = self.mq.delete_configuration("config-123")
        self.mock_mq_client.delete_configuration.assert_called_once()


class TestUserManagement(unittest.TestCase):
    """Test user management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mq_client = MagicMock()
        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_mq_client
            self.mq = MQIntegration(MQConfig(region_name="us-east-1"))
            self.mq._mq_client = self.mock_mq_client

    def test_create_user(self):
        """Test creating a user"""
        self.mock_mq_client.create_user.return_value = {}

        result = self.mq.create_user(
            broker_name="test-broker",
            username="testuser",
            password="password",
            console_access=True,
            groups=["admins"]
        )
        self.mock_mq_client.create_user.assert_called_once()

    def test_describe_user(self):
        """Test describing a user"""
        self.mock_mq_client.describe_user.return_value = {
            "Username": "testuser",
            "ConsoleAccess": True,
            "Groups": ["admins"]
        }

        result = self.mq.describe_user("test-broker", "testuser")
        self.assertEqual(result["Username"], "testuser")

    def test_list_users(self):
        """Test listing users"""
        self.mock_mq_client.list_users.return_value = {
            "Users": [
                {"Username": "user1"},
                {"Username": "user2"}
            ]
        }

        result = self.mq.list_users("test-broker")
        self.assertEqual(len(result), 2)

    def test_update_user(self):
        """Test updating a user"""
        self.mock_mq_client.update_user.return_value = {}

        result = self.mq.update_user(
            broker_name="test-broker",
            username="testuser",
            password="newpassword"
        )
        self.mock_mq_client.update_user.assert_called_once()

    def test_delete_user(self):
        """Test deleting a user"""
        self.mock_mq_client.delete_user.return_value = {}

        result = self.mq.delete_user("test-broker", "testuser")
        self.mock_mq_client.delete_user.assert_called_once()


class TestTagsManagement(unittest.TestCase):
    """Test tags management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mq_client = MagicMock()
        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_mq_client
            self.mq = MQIntegration(MQConfig(region_name="us-east-1"))
            self.mq._mq_client = self.mock_mq_client

    def test_list_tags(self):
        """Test listing tags for broker"""
        self.mock_mq_client.list_tags.return_value = {
            "Tags": [{"Key": "env", "Value": "test"}]
        }

        result = self.mq.list_tags("broker", "test-broker")
        self.assertEqual(result, {"env": "test"})

    def test_add_tags(self):
        """Test adding tags"""
        self.mock_mq_client.tag_resource.return_value = {}

        self.mq.add_tags("broker", "test-broker", {"env": "prod"})
        self.mock_mq_client.tag_resource.assert_called_once()

    def test_remove_tags(self):
        """Test removing tags"""
        self.mock_mq_client.untag_resource.return_value = {}

        self.mq.remove_tags("broker", "test-broker", ["env"])
        self.mock_mq_client.untag_resource.assert_called_once()


class TestSecurityGroups(unittest.TestCase):
    """Test security groups methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_mq_client = MagicMock()
        self.mock_ec2_client = MagicMock()
        with patch.object(mock_boto3, 'client') as mock_client:
            mock_client.return_value = self.mock_ec2_client
            self.mq = MQIntegration(MQConfig(region_name="us-east-1"))
            self.mq._mq_client = self.mock_mq_client
            self.mq._ec2_client = self.mock_ec2_client

    def test_describe_security_groups(self):
        """Test describing security groups"""
        self.mock_ec2_client.describe_security_groups.return_value = {
            "SecurityGroups": [
                {"GroupId": "sg-123", "GroupName": "test-sg"}
            ]
        }

        result = self.mq.describe_security_groups()
        self.assertEqual(len(result), 1)

    def test_create_security_group(self):
        """Test creating a security group"""
        self.mock_ec2_client.create_security_group.return_value = {
            "GroupId": "sg-new"
        }
        self.mock_ec2_client.authorize_security_group_ingress.return_value = {}

        result = self.mq.create_security_group("mq-sg", "MQ Security Group", "vpc-123")
        self.assertEqual(result["GroupId"], "sg-new")


class TestCache(unittest.TestCase):
    """Test cache functionality"""

    def setUp(self):
        """Set up test fixtures"""
        with patch.object(mock_boto3, 'client'):
            self.mq = MQIntegration(MQConfig(region_name="us-east-1"))

    def test_get_cache_key(self):
        """Test cache key generation"""
        key = self.mq._get_cache_key("broker", "test-broker")
        self.assertEqual(key, "broker:test-broker")

    def test_is_cache_valid(self):
        """Test cache validity"""
        cache_key = "test:key"
        self.mq._cache[cache_key] = {
            "value": {"test": "data"},
            "timestamp": datetime.now()
        }
        self.assertTrue(self.mq._is_cache_valid(cache_key))

    def test_is_cache_expired(self):
        """Test cache expiration"""
        cache_key = "test:key"
        self.mq._cache[cache_key] = {
            "value": {"test": "data"},
            "timestamp": datetime.now() - timedelta(seconds=120)
        }
        self.mq._cache_ttl = 60
        self.assertFalse(self.mq._is_cache_valid(cache_key))

    def test_set_cache(self):
        """Test setting cache"""
        self.mq._set_cache("test:key", {"data": "value"})
        self.assertIn("test:key", self.mq._cache)

    def test_invalidate_cache(self):
        """Test cache invalidation"""
        self.mq._cache["broker:test"] = {"value": {}, "timestamp": datetime.now()}
        self.mq._cache["config:test"] = {"value": {}, "timestamp": datetime.now()}
        self.mq._invalidate_cache("broker")
        self.assertNotIn("broker:test", self.mq._cache)
        self.assertIn("config:test", self.mq._cache)


if __name__ == '__main__':
    unittest.main()
