"""
Tests for workflow_aws_directory module
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

# Create mock boto3 module before importing workflow_aws_directory
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
import src.workflow_aws_directory as directory_module

DirectoryServiceIntegration = directory_module.DirectoryServiceIntegration
DirectoryType = directory_module.DirectoryType
DirectoryState = directory_module.DirectoryState
DirectorySize = directory_module.DirectorySize
TrustDirection = directory_module.TrustDirection
TrustType = directory_module.TrustType
TrustState = directory_module.TrustState
ComputerState = directory_module.ComputerState
DomainJoinState = directory_module.DomainJoinState
DirectoryConfig = directory_module.DirectoryConfig
TrustRelationshipConfig = directory_module.TrustRelationshipConfig
DNSConfig = directory_module.DNSConfig
ComputerConfig = directory_module.ComputerConfig
DomainJoinConfig = directory_module.DomainJoinConfig


class TestDirectoryEnums(unittest.TestCase):
    """Test Directory Service enums"""

    def test_directory_type_values(self):
        self.assertEqual(DirectoryType.SIMPLE_AD.value, "SimpleAD")
        self.assertEqual(DirectoryType.MICROSOFT_AD.value, "MicrosoftAD")
        self.assertEqual(DirectoryType.AD_CONNECTOR.value, "ADConnector")

    def test_directory_state_values(self):
        self.assertEqual(DirectoryState.REQUESTED.value, "Requested")
        self.assertEqual(DirectoryState.CREATING.value, "Creating")
        self.assertEqual(DirectoryState.CREATED.value, "Created")
        self.assertEqual(DirectoryState.ACTIVE.value, "Active")
        self.assertEqual(DirectoryState.INOPERATIVE.value, "Inoperative")
        self.assertEqual(DirectoryState.DELETING.value, "Deleting")
        self.assertEqual(DirectoryState.FAILED.value, "Failed")

    def test_directory_size_values(self):
        self.assertEqual(DirectorySize.SMALL.value, "Small")
        self.assertEqual(DirectorySize.LARGE.value, "Large")

    def test_trust_direction_values(self):
        self.assertEqual(TrustDirection.ONE_WAY_INBOUND.value, "One-Way: Inbound")
        self.assertEqual(TrustDirection.ONE_WAY_OUTBOUND.value, "One-Way: Outbound")
        self.assertEqual(TrustDirection.TWO_WAY.value, "Two-Way")

    def test_trust_type_values(self):
        self.assertEqual(TrustType.FOREST.value, "Forest")
        self.assertEqual(TrustType.DOMAIN.value, "Domain")

    def test_trust_state_values(self):
        self.assertEqual(TrustState.CONNECTED.value, "Connected")
        self.assertEqual(TrustState.DISCONNECTED.value, "Disconnected")
        self.assertEqual(TrustState.VERIFYING.value, "Verifying")
        self.assertEqual(TrustState.VERIFY_FAILED.value, "Verify Failed")

    def test_computer_state_values(self):
        self.assertEqual(ComputerState.ONLINE.value, "Online")
        self.assertEqual(ComputerState.OFFLINE.value, "Offline")
        self.assertEqual(ComputerState.CREATING.value, "Creating")
        self.assertEqual(ComputerState.DELETING.value, "Deleting")

    def test_domain_join_state_values(self):
        self.assertEqual(DomainJoinState.SUCCESS.value, "Success")
        self.assertEqual(DomainJoinState.FAILED.value, "Failed")
        self.assertEqual(DomainJoinState.PENDING.value, "Pending")


class TestDirectoryDataclasses(unittest.TestCase):
    """Test Directory Service dataclasses"""

    def test_directory_config_defaults(self):
        config = DirectoryConfig(
            name="test.example.com",
            directory_type=DirectoryType.SIMPLE_AD
        )
        self.assertEqual(config.name, "test.example.com")
        self.assertEqual(config.directory_type, DirectoryType.SIMPLE_AD)
        self.assertEqual(config.size, DirectorySize.SMALL)
        self.assertEqual(config.edition, "Standard")
        self.assertFalse(config.enable_sso)

    def test_directory_config_custom(self):
        config = DirectoryConfig(
            name="test.example.com",
            directory_type=DirectoryType.MICROSOFT_AD,
            size=DirectorySize.LARGE,
            description="Test directory",
            edition="Enterprise",
            enable_sso=True
        )
        self.assertEqual(config.size, DirectorySize.LARGE)
        self.assertEqual(config.description, "Test directory")
        self.assertEqual(config.edition, "Enterprise")
        self.assertTrue(config.enable_sso)

    def test_trust_relationship_config(self):
        config = TrustRelationshipConfig(
            trusted_domain="trust.example.com",
            trust_direction=TrustDirection.TWO_WAY,
            trust_type=TrustType.FOREST,
            trust_password="password123"
        )
        self.assertEqual(config.trusted_domain, "trust.example.com")
        self.assertEqual(config.trust_direction, TrustDirection.TWO_WAY)
        self.assertEqual(config.trust_type, TrustType.FOREST)

    def test_dns_config_defaults(self):
        config = DNSConfig()
        self.assertEqual(len(config.dns_servers), 0)
        self.assertTrue(config.dns_reverse_lookup)

    def test_dns_config_custom(self):
        config = DNSConfig(
            dns_servers=["192.168.1.1", "192.168.1.2"],
            dns_zone_name="test.example.com",
            dns_reverse_lookup=True
        )
        self.assertEqual(len(config.dns_servers), 2)
        self.assertEqual(config.dns_zone_name, "test.example.com")

    def test_computer_config(self):
        config = ComputerConfig(
            computer_name="test-computer",
            organizational_unit="OU=Computers,DC=test,DC=example,DC=com",
            ip_address="192.168.1.100"
        )
        self.assertEqual(config.computer_name, "test-computer")
        self.assertIn("Computers", config.organizational_unit)

    def test_domain_join_config(self):
        config = DomainJoinConfig(
            instance_id="i-1234567890abcdef0",
            directory_id="d-1234567890abcdef0",
            computer_name="test-computer"
        )
        self.assertEqual(config.instance_id, "i-1234567890abcdef0")
        self.assertEqual(config.directory_id, "d-1234567890abcdef0")


class TestDirectoryServiceIntegration(unittest.TestCase):
    """Test DirectoryServiceIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()
        self.mock_ssm_client = MagicMock()
        self.mock_ec2_client = MagicMock()
        self.mock_iam_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_sts_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.profile_name = None
            self.integration._directory_cache = {}
            self.integration._trust_cache = {}
            self.integration.ds_client = self.mock_ds_client
            self.integration.ssm_client = self.mock_ssm_client
            self.integration.ec2_client = self.mock_ec2_client
            self.integration.iam_client = self.mock_iam_client
            self.integration.cloudwatch_client = self.mock_cloudwatch_client
            self.integration.sts_client = self.mock_sts_client

    def test_generate_id(self):
        """Test ID generation"""
        id1 = self.integration._generate_id()
        id2 = self.integration._generate_id()
        self.assertNotEqual(id1, id2)
        self.assertEqual(len(id1), 8)


class TestSimpleADOperations(unittest.TestCase):
    """Test Simple AD operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}
            self.integration._trust_cache = {}

    def test_create_simple_ad(self):
        """Test creating a Simple AD directory"""
        mock_response = {'DirectoryId': 'd-1234567890abcdef0'}
        self.mock_ds_client.create_directory.return_value = mock_response

        result = self.integration.create_simple_ad(
            name="test.example.com",
            size=DirectorySize.SMALL,
            vpc_id="vpc-12345678",
            subnet_ids=["subnet-12345678", "subnet-87654321"]
        )

        self.assertEqual(result['directory_id'], 'd-1234567890abcdef0')
        self.assertEqual(result['directory_type'], 'SimpleAD')
        self.assertEqual(result['status'], 'creating')

    def test_create_simple_ad_missing_subnets(self):
        """Test creating Simple AD without required subnets"""
        result = self.integration.create_simple_ad(
            name="test.example.com",
            vpc_id="vpc-12345678"
        )

        self.assertEqual(result['status'], 'error')
        self.assertIn('subnet_ids required', result['message'])

    def test_get_simple_ad(self):
        """Test getting Simple AD details"""
        mock_response = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'Name': 'test.example.com',
                'Type': 'SimpleAD',
                'Stage': 'Active'
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_simple_ad('d-1234567890abcdef0')

        self.assertEqual(result['DirectoryId'], 'd-1234567890abcdef0')

    def test_get_simple_ad_not_found(self):
        """Test getting non-existent Simple AD"""
        mock_response = {'DirectoryDescriptions': []}
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_simple_ad('d-nonexistent')

        self.assertEqual(result['status'], 'not_found')

    def test_list_simple_ad(self):
        """Test listing Simple AD directories"""
        mock_response = {
            'DirectoryDescriptions': [
                {'DirectoryId': 'd-1', 'Type': 'SimpleAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-2', 'Type': 'MicrosoftAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-3', 'Type': 'SimpleAD', 'Stage': 'Active'}
            ]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.list_simple_ad()

        self.assertEqual(len(result), 2)  # Only SimpleAD type

    def test_list_simple_ad_with_filters(self):
        """Test listing Simple AD with filters"""
        mock_response = {
            'DirectoryDescriptions': [
                {'DirectoryId': 'd-1', 'Type': 'SimpleAD', 'Stage': 'Active', 'VpcSettings': {'VpcId': 'vpc-123'}}
            ]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.list_simple_ad(filters={'vpc_id': 'vpc-123', 'stage': 'Active'})

        self.assertEqual(len(result), 1)

    def test_delete_simple_ad(self):
        """Test deleting Simple AD"""
        self.mock_ds_client.delete_directory.return_value = {}

        result = self.integration.delete_simple_ad('d-1234567890abcdef0')

        self.assertEqual(result['status'], 'deleting')
        self.assertEqual(result['directory_id'], 'd-1234567890abcdef0')


class TestMicrosoftADOperations(unittest.TestCase):
    """Test Microsoft AD operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}
            self.integration._trust_cache = {}

    def test_create_microsoft_ad(self):
        """Test creating Microsoft AD directory"""
        mock_response = {'DirectoryId': 'd-1234567890abcdef0'}
        self.mock_ds_client.create_directory.return_value = mock_response

        result = self.integration.create_microsoft_ad(
            name="test.example.com",
            vpc_id="vpc-12345678",
            subnet_ids=["subnet-12345678", "subnet-87654321"],
            edition="Enterprise"
        )

        self.assertEqual(result['directory_id'], 'd-1234567890abcdef0')
        self.assertEqual(result['directory_type'], 'MicrosoftAD')
        self.assertEqual(result['edition'], 'Enterprise')

    def test_create_microsoft_ad_missing_subnets(self):
        """Test creating Microsoft AD without required subnets"""
        result = self.integration.create_microsoft_ad(
            name="test.example.com",
            vpc_id="vpc-12345678"
        )

        self.assertEqual(result['status'], 'error')

    def test_get_microsoft_ad(self):
        """Test getting Microsoft AD details"""
        mock_response = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'Name': 'test.example.com',
                'Type': 'MicrosoftAD',
                'Stage': 'Active',
                'Edition': 'Enterprise'
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_microsoft_ad('d-1234567890abcdef0')

        self.assertEqual(result['DirectoryId'], 'd-1234567890abcdef0')
        self.assertEqual(result['Edition'], 'Enterprise')


class TestADConnectorOperations(unittest.TestCase):
    """Test AD Connector operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}
            self.integration._trust_cache = {}

    def test_create_ad_connector(self):
        """Test creating AD Connector"""
        mock_response = {'DirectoryId': 'd-1234567890abcdef0'}
        self.mock_ds_client.create_directory.return_value = mock_response

        result = self.integration.create_ad_connector(
            name="connector.example.com",
            vpc_id="vpc-12345678",
            subnet_ids=["subnet-12345678", "subnet-87654321"],
            dns_addresses=["192.168.1.1", "192.168.1.2"],
            customer_username="admin"
        )

        self.assertEqual(result['directory_id'], 'd-1234567890abcdef0')
        self.assertEqual(result['directory_type'], 'ADConnector')

    def test_list_ad_connector(self):
        """Test listing AD Connectors (singular method name)"""
        mock_response = {
            'DirectoryDescriptions': [
                {'DirectoryId': 'd-1', 'Type': 'ADConnector', 'Stage': 'Active'},
                {'DirectoryId': 'd-2', 'Type': 'SimpleAD', 'Stage': 'Active'}
            ]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.list_ad_connector()

        self.assertEqual(len(result), 1)

    def test_get_ad_connector(self):
        """Test getting AD Connector details"""
        mock_response = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'Name': 'connector.example.com',
                'Type': 'ADConnector',
                'Stage': 'Active'
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_ad_connector('d-1234567890abcdef0')

        self.assertEqual(result['DirectoryId'], 'd-1234567890abcdef0')


class TestTrustRelationships(unittest.TestCase):
    """Test trust relationship operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}
            self.integration._trust_cache = {}

    def test_create_trust_relationship(self):
        """Test creating a trust relationship"""
        self.mock_ds_client.create_trust.return_value = {'TrustId': 't-12345678'}

        config = TrustRelationshipConfig(
            trusted_domain="trust.example.com",
            trust_direction=TrustDirection.TWO_WAY,
            trust_type=TrustType.FOREST,
            trust_password="trustpassword123"
        )

        result = self.integration.create_trust_relationship('d-1234567890abcdef0', config)

        self.assertEqual(result['status'], 'creating')
        self.assertEqual(result['directory_id'], 'd-1234567890abcdef0')
        self.assertEqual(result['trusted_domain'], 'trust.example.com')

    def test_get_trust_relationship(self):
        """Test getting trust relationship details"""
        mock_response = {
            'TrustRelationships': [{
                'TrustId': 't-12345678',
                'TrustDirection': 'Two-Way',
                'TrustedDomainName': 'trust.example.com',
                'TrustState': 'Connected',
                'TrustType': 'Forest'
            }]
        }
        self.mock_ds_client.describe_trust_relationships.return_value = mock_response

        # Method signature is (directory_id, trust_id)
        result = self.integration.get_trust_relationship('d-1234567890abcdef0', 't-12345678')

        self.assertEqual(result['TrustId'], 't-12345678')
        self.assertEqual(result['TrustState'], 'Connected')

    def test_list_trust_relationships(self):
        """Test listing trust relationships"""
        mock_response = {
            'TrustRelationships': [
                {'TrustId': 't-1', 'TrustDirection': 'Two-Way', 'TrustedDomainName': 'trust1.example.com'},
                {'TrustId': 't-2', 'TrustDirection': 'One-Way: Outbound', 'TrustedDomainName': 'trust2.example.com'}
            ]
        }
        self.mock_ds_client.describe_trust_relationships.return_value = mock_response

        result = self.integration.list_trust_relationships('d-1234567890abcdef0')

        self.assertEqual(len(result), 2)

    def test_delete_trust_relationship(self):
        """Test deleting a trust relationship"""
        self.mock_ds_client.delete_trust.return_value = {}

        # Method signature is (directory_id, trust_id)
        result = self.integration.delete_trust_relationship('d-1234567890abcdef0', 't-12345678')

        self.assertEqual(result['status'], 'deleted')
        self.assertEqual(result['trust_id'], 't-12345678')

    def test_verify_trust_relationship(self):
        """Test verifying a trust relationship"""
        mock_response = {
            'TrustVerificationId': 'tv-12345678'
        }
        self.mock_ds_client.verify_trust.return_value = mock_response

        # Method signature is (directory_id, trust_id)
        result = self.integration.verify_trust_relationship('d-1234567890abcdef0', 't-12345678')

        self.assertEqual(result['status'], 'verifying')
        self.assertEqual(result['trust_id'], 't-12345678')
        self.assertEqual(result['verification_id'], 'tv-12345678')


class TestDNSManagement(unittest.TestCase):
    """Test DNS management operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()
        self.mock_ec2_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration.ec2_client = self.mock_ec2_client
            self.integration._directory_cache = {}

    def test_get_dns_config(self):
        """Test getting DNS configuration"""
        mock_response = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'DnsIpAddrs': ['192.168.1.1', '192.168.1.2'],
                'Name': 'test.example.com'
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_dns_config('d-1234567890abcdef0')

        self.assertEqual(len(result.dns_servers), 2)
        self.assertEqual(result.dns_zone_name, 'test.example.com')

    def test_add_dns_forwarder(self):
        """Test adding DNS forwarder"""
        result = self.integration.add_dns_forwarder(
            'd-1234567890abcdef0',
            'example.com',
            ['192.168.1.1', '192.168.1.2']
        )

        self.assertEqual(result['status'], 'configured')
        self.assertEqual(result['domain_name'], 'example.com')

    def test_update_dns_config(self):
        """Test updating DNS configuration"""
        mock_describe_response = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'VpcSettings': {'VpcId': 'vpc-12345678'}
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_describe_response
        self.mock_ec2_client.modify_vpc_attribute.return_value = {}

        dns_config = DNSConfig(
            dns_servers=['192.168.1.1', '192.168.1.2'],
            dns_zone_name='test.example.com'
        )

        result = self.integration.update_dns_config('d-1234567890abcdef0', dns_config)

        self.assertEqual(result['status'], 'updated')


class TestComputerManagement(unittest.TestCase):
    """Test computer management operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}

    def test_register_computer(self):
        """Test registering a computer"""
        self.mock_ds_client.register_event_topic.return_value = {}

        config = ComputerConfig(
            computer_name="test-computer",
            organizational_unit="OU=Computers,DC=test,DC=example,DC=com"
        )

        result = self.integration.register_computer('d-1234567890abcdef0', config)

        self.assertEqual(result['status'], 'registered')
        self.assertEqual(result['computer_name'], 'test-computer')

    def test_list_computers(self):
        """Test listing directory computers"""
        mock_response = {
            'Computers': [
                {'ComputerId': 'comp-1', 'ComputerName': 'computer1'},
                {'ComputerId': 'comp-2', 'ComputerName': 'computer2'}
            ]
        }
        self.mock_ds_client.describe_computers.return_value = mock_response

        result = self.integration.list_computers('d-1234567890abcdef0')

        self.assertEqual(len(result), 2)

    def test_get_computer(self):
        """Test getting computer details"""
        mock_response = {
            'Computers': [
                {'ComputerId': 'comp-1', 'ComputerName': 'computer1'},
                {'ComputerId': 'comp-2', 'ComputerName': 'computer2'}
            ]
        }
        self.mock_ds_client.describe_computers.return_value = mock_response

        result = self.integration.get_computer('d-1234567890abcdef0', 'computer1')

        self.assertEqual(result['ComputerName'], 'computer1')

    def test_get_computer_not_found(self):
        """Test getting non-existent computer"""
        mock_response = {
            'Computers': [
                {'ComputerId': 'comp-1', 'ComputerName': 'computer1'}
            ]
        }
        self.mock_ds_client.describe_computers.return_value = mock_response

        result = self.integration.get_computer('d-1234567890abcdef0', 'nonexistent')

        self.assertEqual(result['status'], 'not_found')

    def test_delete_computer(self):
        """Test deleting a computer (delete_computer, not deregister_computer)"""
        self.mock_ds_client.remove_computer_from_directory.return_value = {}

        result = self.integration.delete_computer('d-1234567890abcdef0', 'test-computer')

        self.assertEqual(result['status'], 'deleted')
        self.assertEqual(result['computer_name'], 'test-computer')


class TestDomainJoinOperations(unittest.TestCase):
    """Test domain join operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()
        self.mock_ds_client = MagicMock()
        self.mock_ec2_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ssm_client = self.mock_ssm_client
            self.integration.ds_client = self.mock_ds_client
            self.integration.ec2_client = self.mock_ec2_client
            self.integration._directory_cache = {}

    def test_domain_join_instance(self):
        """Test joining an instance to a domain (domain_join_instance, not join_directory)"""
        mock_describe = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'Name': 'test.example.com'
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_describe
        self.mock_ssm_client.send_command.return_value = {
            'Command': {'CommandId': 'c-1234567890abcdef0'}
        }

        config = DomainJoinConfig(
            instance_id="i-1234567890abcdef0",
            directory_id="d-1234567890abcdef0"
        )

        result = self.integration.domain_join_instance(config)

        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['command_id'], 'c-1234567890abcdef0')

    def test_domain_leave_instance(self):
        """Test removing an instance from a domain (domain_leave_instance, not leave_directory)"""
        self.mock_ssm_client.send_command.return_value = {
            'Command': {'CommandId': 'c-1234567890abcdef0'}
        }

        result = self.integration.domain_leave_instance("i-1234567890abcdef0", "d-1234567890abcdef0")

        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['command_id'], 'c-1234567890abcdef0')

    def test_domain_join_instance_simple(self):
        """Test simple domain join"""
        self.mock_ssm_client.send_command.return_value = {
            'Command': {'CommandId': 'c-1234567890abcdef0'}
        }

        result = self.integration.domain_join_instance_simple(
            instance_id="i-1234567890abcdef0",
            directory_id="d-1234567890abcdef0",
            directory_name="test.example.com",
            dns_ip="192.168.1.1"
        )

        self.assertEqual(result['status'], 'pending')

    def test_get_domain_join_status(self):
        """Test getting domain join status"""
        mock_response = {
            'Commands': [{
                'CommandId': 'c-1234567890abcdef0',
                'Status': 'Success',
                'InstanceIds': ['i-1234567890abcdef0']
            }]
        }
        self.mock_ssm_client.list_commands.return_value = mock_response

        result = self.integration.get_domain_join_status('c-1234567890abcdef0')

        self.assertEqual(result['status'], 'Success')
        self.assertEqual(result['command_id'], 'c-1234567890abcdef0')


class TestMultiRegionOperations(unittest.TestCase):
    """Test multi-region operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}

    def test_enable_multi_region_replication(self):
        """Test enabling multi-region replication"""
        mock_response = {'DirectoryId': 'd-replica-123'}
        self.mock_ds_client.create_microsoft_ad.return_value = mock_response

        vpc_config = {
            'us-west-2': {
                'vpc_id': 'vpc-123',
                'subnet_ids': ['subnet-1', 'subnet-2']
            }
        }

        result = self.integration.enable_multi_region_replication(
            'd-1234567890abcdef0',
            ['us-west-2'],
            vpc_config
        )

        self.assertEqual(result['status'], 'configured')
        self.assertEqual(len(result['replicas']), 1)

    def test_list_directory_replicas(self):
        """Test listing directory replicas"""
        mock_response = {
            'replicas': [
                {'Region': 'us-west-2', 'Status': 'Active'},
                {'Region': 'eu-west-1', 'Status': 'Creating'}
            ]
        }
        self.mock_ds_client.describe_directory_replicas.return_value = mock_response

        result = self.integration.list_directory_replicas('d-1234567890abcdef0')

        self.assertEqual(len(result), 2)

    def test_remove_directory_replica(self):
        """Test removing directory replica"""
        self.mock_ds_client.remove_directory.return_value = {}

        result = self.integration.remove_directory_replica('d-1234567890abcdef0', 'us-west-2')

        self.assertEqual(result['status'], 'removed')
        self.assertEqual(result['region'], 'us-west-2')


class TestIAMRoleManagement(unittest.TestCase):
    """Test IAM role management operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_iam_client = MagicMock()
        self.mock_sts_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.iam_client = self.mock_iam_client
            self.integration.sts_client = self.mock_sts_client

    def test_create_directory_service_role(self):
        """Test creating directory service role"""
        # First call raises to simulate role doesn't exist
        self.mock_iam_client.get_role.side_effect = Exception("Role not found")
        self.mock_iam_client.create_role.return_value = {
            'Role': {'Arn': 'arn:aws:iam::123456789012:role/AWSDirectoryServiceRole'}
        }

        result = self.integration.create_directory_service_role()

        self.assertEqual(result['status'], 'created')
        self.assertEqual(result['role_name'], 'AWSDirectoryServiceRole')

    def test_create_directory_service_role_exists(self):
        """Test creating directory service role when it already exists"""
        self.mock_iam_client.get_role.return_value = {
            'Role': {'RoleName': 'AWSDirectoryServiceRole'}
        }

        result = self.integration.create_directory_service_role()

        self.assertEqual(result['status'], 'exists')

    def test_get_directory_service_role(self):
        """Test getting directory service role"""
        self.mock_iam_client.get_role.return_value = {
            'Role': {'RoleName': 'AWSDirectoryServiceRole', 'Arn': 'arn:aws:iam::123456789012:role/AWSDirectoryServiceRole'}
        }

        result = self.integration.get_directory_service_role()

        self.assertEqual(result['RoleName'], 'AWSDirectoryServiceRole')

    def test_assume_directory_service_role(self):
        """Test assuming directory service role"""
        self.mock_sts_client.assume_role.return_value = {
            'Credentials': {
                'AccessKeyId': 'AKIAIOSFODNN7EXAMPLE',
                'SecretAccessKey': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY',
                'SessionToken': 'FwoGZXIvYXdzEBYaDKsD'
            }
        }

        result = self.integration.assume_directory_service_role('d-1234567890abcdef0')

        self.assertIn('access_key', result)
        self.assertIn('secret_key', result)
        self.assertIn('token', result)


class TestCloudWatchIntegration(unittest.TestCase):
    """Test CloudWatch integration operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_cloudwatch_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.cloudwatch_client = self.mock_cloudwatch_client

    def test_get_directory_metrics(self):
        """Test getting directory metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [
                {'Average': 10.0, 'Maximum': 20.0, 'Minimum': 5.0, 'Timestamp': '2024-01-01T00:00:00Z'}
            ]
        }

        result = self.integration.get_directory_metrics('d-1234567890abcdef0', 'DirectoryConnections')

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Average'], 10.0)

    def test_list_directory_metrics(self):
        """Test listing available metrics"""
        result = self.integration.list_directory_metrics('d-1234567890abcdef0')

        self.assertIn('DirectorySSOEvents', result)
        self.assertIn('DirectoryCacheHits', result)

    def test_enable_directory_monitoring(self):
        """Test enabling directory monitoring"""
        self.mock_ds_client = MagicMock()
        self.integration.ds_client = self.mock_ds_client

        result = self.integration.enable_directory_monitoring('d-1234567890abcdef0', 'standard')

        self.assertEqual(result['status'], 'enabled')
        self.assertEqual(result['monitoring_type'], 'standard')

    def test_create_directory_alarm(self):
        """Test creating directory alarm"""
        self.mock_cloudwatch_client.put_metric_alarm.return_value = {}

        result = self.integration.create_directory_alarm(
            'd-1234567890abcdef0',
            'test-alarm',
            'DirectoryConnections',
            100.0
        )

        self.assertEqual(result['status'], 'created')
        self.assertEqual(result['alarm_name'], 'test-alarm')


class TestUtilityMethods(unittest.TestCase):
    """Test utility methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ds_client = MagicMock()

        with patch.object(DirectoryServiceIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = DirectoryServiceIntegration()
            self.integration.region = "us-east-1"
            self.integration.ds_client = self.mock_ds_client
            self.integration._directory_cache = {}

    def test_get_directory(self):
        """Test getting directory details regardless of type"""
        mock_response = {
            'DirectoryDescriptions': [{
                'DirectoryId': 'd-1234567890abcdef0',
                'Name': 'test.example.com',
                'Type': 'SimpleAD',
                'Stage': 'Active'
            }]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_directory('d-1234567890abcdef0')

        self.assertEqual(result['DirectoryId'], 'd-1234567890abcdef0')

    def test_get_directory_not_found(self):
        """Test getting non-existent directory"""
        mock_response = {'DirectoryDescriptions': []}
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_directory('d-nonexistent')

        self.assertEqual(result['status'], 'not_found')

    def test_list_directories(self):
        """Test listing all directories"""
        mock_response = {
            'DirectoryDescriptions': [
                {'DirectoryId': 'd-1', 'Type': 'SimpleAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-2', 'Type': 'MicrosoftAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-3', 'Type': 'ADConnector', 'Stage': 'Active'}
            ]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.list_directories()

        self.assertEqual(len(result), 3)

    def test_list_directories_filtered_by_type(self):
        """Test listing directories filtered by type"""
        mock_response = {
            'DirectoryDescriptions': [
                {'DirectoryId': 'd-1', 'Type': 'SimpleAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-2', 'Type': 'MicrosoftAD', 'Stage': 'Active'}
            ]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.list_directories(directory_type=DirectoryType.SIMPLE_AD)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Type'], 'SimpleAD')

    def test_delete_directory(self):
        """Test deleting a directory"""
        self.mock_ds_client.delete_directory.return_value = {}

        result = self.integration.delete_directory('d-1234567890abcdef0')

        self.assertEqual(result['status'], 'deleting')
        self.assertEqual(result['directory_id'], 'd-1234567890abcdef0')

    def test_get_directory_status_summary(self):
        """Test getting directory status summary"""
        mock_response = {
            'DirectoryDescriptions': [
                {'DirectoryId': 'd-1', 'Type': 'SimpleAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-2', 'Type': 'MicrosoftAD', 'Stage': 'Active'},
                {'DirectoryId': 'd-3', 'Type': 'SimpleAD', 'Stage': 'Creating'}
            ]
        }
        self.mock_ds_client.describe_directories.return_value = mock_response

        result = self.integration.get_directory_status_summary()

        self.assertEqual(result['total'], 3)
        self.assertEqual(result['by_type']['SimpleAD'], 2)
        self.assertEqual(result['by_type']['MicrosoftAD'], 1)


if __name__ == '__main__':
    unittest.main()
