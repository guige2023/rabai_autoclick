"""
Tests for workflow_aws_connect module
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

# Create mock boto3 module before importing workflow_aws_connect
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
import src.workflow_aws_connect as connect_module

ConnectIntegration = connect_module.ConnectIntegration
InstanceAttributeType = connect_module.InstanceAttributeType
ContactStateType = connect_module.ContactStateType
ContactInitiationMethod = connect_module.ContactInitiationMethod
QueueType = connect_module.QueueType
RoutingProfileEventType = connect_module.RoutingProfileEventType
QuickConnectType = connect_module.QuickConnectType
HoursOfOperationType = connect_module.HoursOfOperationType
HoursOfOperationTimeShiftType = connect_module.HoursOfOperationTimeShiftType
AgentStatusType = connect_module.AgentStatusType
ContactFlowType = connect_module.ContactFlowType
SecurityProfileType = connect_module.SecurityProfileType
ConnectConfig = connect_module.ConnectConfig
InstanceConfig = connect_module.InstanceConfig
InstanceInfo = connect_module.InstanceInfo
ContactFlowConfig = connect_module.ContactFlowConfig
ContactFlowInfo = connect_module.ContactFlowInfo
QuickConnectConfig = connect_module.QuickConnectConfig
QuickConnectInfo = connect_module.QuickConnectInfo
UserConfig = connect_module.UserConfig
UserInfo = connect_module.UserInfo
RoutingProfileConfig = connect_module.RoutingProfileConfig
RoutingProfileInfo = connect_module.RoutingProfileInfo
QueueConfig = connect_module.QueueConfig
QueueInfo = connect_module.QueueInfo
HoursOfOperationConfig = connect_module.HoursOfOperationConfig
HoursOfOperationInfo = connect_module.HoursOfOperationInfo
PromptConfig = connect_module.PromptConfig
PromptInfo = connect_module.PromptInfo
ContactSearchResult = connect_module.ContactSearchResult
ContactMetrics = connect_module.ContactMetrics


class TestConnectEnums(unittest.TestCase):
    """Test Connect enums"""

    def test_instance_attribute_type_values(self):
        self.assertEqual(InstanceAttributeType.INBOUND_CALL.value, "INBOUND_CALL")
        self.assertEqual(InstanceAttributeType.OUTBOUND_CALL.value, "OUTBOUND_CALL")
        self.assertEqual(InstanceAttributeType.CONTACT_LENS.value, "CONTACT_LENS")

    def test_contact_state_type_values(self):
        self.assertEqual(ContactStateType.INCOMING.value, "INCOMING")
        self.assertEqual(ContactStateType.CONNECTED.value, "CONNECTED")
        self.assertEqual(ContactStateType.ENDED.value, "ENDED")

    def test_contact_initiation_method_values(self):
        self.assertEqual(ContactInitiationMethod.INBOUND.value, "INBOUND")
        self.assertEqual(ContactInitiationMethod.OUTBOUND.value, "OUTBOUND")
        self.assertEqual(ContactInitiationMethod.CALLBACK.value, "CALLBACK")

    def test_queue_type_values(self):
        self.assertEqual(QueueType.STANDARD.value, "STANDARD")
        self.assertEqual(QueueType.AGENT.value, "AGENT")

    def test_quick_connect_type_values(self):
        self.assertEqual(QuickConnectType.PHONE_NUMBER.value, "PHONE_NUMBER")
        self.assertEqual(QuickConnectType.QUEUE.value, "QUEUE")
        self.assertEqual(QuickConnectType.USER.value, "USER")

    def test_agent_status_type_values(self):
        self.assertEqual(AgentStatusType.ONLINE.value, "ONLINE")
        self.assertEqual(AgentStatusType.OFFLINE.value, "OFFLINE")
        self.assertEqual(AgentStatusType.AWAY.value, "AWAY")

    def test_contact_flow_type_values(self):
        self.assertEqual(ContactFlowType.CONTACT_FLOW.value, "CONTACT_FLOW")
        self.assertEqual(ContactFlowType.CUSTOMER_QUEUE.value, "CUSTOMER_QUEUE")

    def test_security_profile_type_values(self):
        self.assertEqual(SecurityProfileType.AGENT.value, "AGENT")
        self.assertEqual(SecurityProfileType.ADMIN.value, "ADMIN")


class TestConnectDataclasses(unittest.TestCase):
    """Test Connect dataclasses"""

    def test_connect_config_defaults(self):
        config = ConnectConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)
        self.assertTrue(config.verify_ssl)

    def test_connect_config_custom(self):
        config = ConnectConfig(
            region_name="us-west-2",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            timeout=60
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.timeout, 60)

    def test_instance_config(self):
        config = InstanceConfig(
            identity_management_type="SAML",
            instance_alias="test-instance",
            inbound_calls_enabled=True,
            outbound_calls_enabled=False
        )
        self.assertEqual(config.identity_management_type, "SAML")
        self.assertEqual(config.instance_alias, "test-instance")
        self.assertTrue(config.inbound_calls_enabled)
        self.assertFalse(config.outbound_calls_enabled)

    def test_instance_info(self):
        info = InstanceInfo(
            instance_id="test-id",
            instance_arn="arn:aws:connect:us-east-1:123456789012:instance/test-id",
            identity_management_type="SAML",
            instance_alias="test",
            created_time="2024-01-01T00:00:00Z"
        )
        self.assertEqual(info.instance_id, "test-id")
        self.assertEqual(info.identity_management_type, "SAML")

    def test_contact_flow_config(self):
        config = ContactFlowConfig(
            name="Test Flow",
            description="Test description",
            type="CONTACT_FLOW"
        )
        self.assertEqual(config.name, "Test Flow")
        self.assertEqual(config.type, "CONTACT_FLOW")

    def test_quick_connect_config(self):
        config = QuickConnectConfig(
            name="Test QC",
            quick_connect_type=QuickConnectType.QUEUE,
            destination={"queueId": "queue-123"}
        )
        self.assertEqual(config.name, "Test QC")
        self.assertEqual(config.quick_connect_type, QuickConnectType.QUEUE)

    def test_user_config(self):
        config = UserConfig(
            username="testuser",
            password="password123",
            identity_info={"email": "test@example.com"},
            phone_config={"phoneType": "SOFT_PHONE"},
            routing_profile_id="rp-123",
            security_profile_ids=["sp-1", "sp-2"]
        )
        self.assertEqual(config.username, "testuser")
        self.assertEqual(len(config.security_profile_ids), 2)

    def test_routing_profile_config(self):
        config = RoutingProfileConfig(
            name="Test RP",
            description="Test routing profile",
            instance_id="instance-123",
            default_outbound_queue_id="queue-123",
            media_concurrencies=[{"channel": "VOICE", "concurrency": 1}]
        )
        self.assertEqual(config.name, "Test RP")
        self.assertEqual(len(config.media_concurrencies), 1)

    def test_queue_config(self):
        config = QueueConfig(
            name="Test Queue",
            queue_type=QueueType.STANDARD,
            max_contacts=10
        )
        self.assertEqual(config.name, "Test Queue")
        self.assertEqual(config.max_contacts, 10)

    def test_hours_of_operation_config(self):
        config = HoursOfOperationConfig(
            name="Test Hours",
            time_zone="UTC",
            hours_of_operation_config=[{"day": "MONDAY", "startTime": {"hours": 9, "minutes": 0}, "endTime": {"hours": 17, "minutes": 0}}]
        )
        self.assertEqual(config.name, "Test Hours")
        self.assertEqual(config.time_zone, "UTC")

    def test_prompt_config(self):
        config = PromptConfig(
            name="Test Prompt",
            description="Test prompt description",
            s3_uri="s3://bucket/prompts/test.mp3"
        )
        self.assertEqual(config.name, "Test Prompt")
        self.assertEqual(config.s3_uri, "s3://bucket/prompts/test.mp3")

    def test_contact_search_result(self):
        result = ContactSearchResult(
            contact_id="contact-123",
            contact_arn="arn:aws:connect:us-east-1:123456789012:contact/contact-123",
            state="CONNECTED"
        )
        self.assertEqual(result.contact_id, "contact-123")
        self.assertEqual(result.state, "CONNECTED")

    def test_contact_metrics(self):
        metrics = ContactMetrics(
            metrics={"ActiveContacts": 5, "QueueDepth": 2},
            timestamp="2024-01-01T00:00:00Z",
            period=300
        )
        self.assertEqual(metrics.metrics["ActiveContacts"], 5)
        self.assertEqual(metrics.period, 300)


class TestConnectIntegration(unittest.TestCase):
    """Test ConnectIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration.region_name = "us-east-1"
            self.integration.endpoint_url = None
            self.integration._clients = {'connect': self.mock_connect_client, 'cloudwatch': self.mock_cloudwatch_client}
            self.integration._session = MagicMock()
            self.integration._config = None

    def test_init_with_boto3(self):
        """Test initialization with boto3 session"""
        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            integration = ConnectIntegration()
            integration.region_name = "us-east-1"
            integration.endpoint_url = None
            integration._clients = {}
            integration._session = MagicMock()
            integration._config = None

    def test_connect_client_property(self):
        """Test connect client property"""
        result = self.integration.connect
        self.assertEqual(result, self.mock_connect_client)

    def test_cloudwatch_client_property(self):
        """Test cloudwatch client property"""
        result = self.integration.cloudwatch
        self.assertEqual(result, self.mock_cloudwatch_client)


class TestConnectInstanceManagement(unittest.TestCase):
    """Test Connect instance management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration.region_name = "us-east-1"
            self.integration.endpoint_url = None
            self.integration._clients = {'connect': self.mock_connect_client}
            self.integration._session = MagicMock()

    def test_create_instance(self):
        """Test creating a Connect instance"""
        mock_response = {
            'InstanceId': 'test-instance-id',
            'InstanceArn': 'arn:aws:connect:us-east-1:123456789012:instance/test-instance-id',
            'CreatedTime': '2024-01-01T00:00:00Z',
            'ServiceRole': 'arn:aws:iam::123456789012:role/service-role',
            'Status': 'ACTIVE'
        }
        self.mock_connect_client.create_instance.return_value = mock_response

        config = InstanceConfig(
            identity_management_type="SAML",
            instance_alias="test-instance"
        )

        result = self.integration.create_instance(config)

        self.assertEqual(result.instance_id, 'test-instance-id')
        self.assertEqual(result.instance_alias, 'test-instance')
        self.mock_connect_client.create_instance.assert_called_once()

    def test_create_instance_with_attributes(self):
        """Test creating a Connect instance with custom attributes"""
        mock_response = {
            'InstanceId': 'test-instance-id',
            'InstanceArn': 'arn:aws:connect:us-east-1:123456789012:instance/test-instance-id',
            'CreatedTime': '2024-01-01T00:00:00Z',
            'ServiceRole': 'arn:aws:iam::123456789012:role/service-role'
        }
        self.mock_connect_client.create_instance.return_value = mock_response

        config = InstanceConfig(
            identity_management_type="SAML",
            attributes={"attr1": "value1"}
        )

        result = self.integration.create_instance(config)

        self.assertEqual(result.instance_id, 'test-instance-id')
        call_args = self.mock_connect_client.create_instance.call_args
        self.assertEqual(call_args[1]['Attributes'], {"attr1": "value1"})


class TestConnectContactFlows(unittest.TestCase):
    """Test Connect contact flow methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_contact_flow(self):
        """Test creating a contact flow"""
        mock_response = {
            'ContactFlowId': 'cf-123',
            'ContactFlowArn': 'arn:aws:connect:us-east-1:123456789012:contact-flow/cf-123'
        }
        self.mock_connect_client.create_contact_flow.return_value = mock_response

        config = ContactFlowConfig(
            name="Test Flow",
            description="Test contact flow"
        )

        result = self.integration.create_contact_flow("instance-id", config)

        self.assertEqual(result.contact_flow_id, 'cf-123')

    def test_describe_contact_flow(self):
        """Test describing a contact flow"""
        mock_response = {
            'ContactFlow': {
                'Id': 'cf-123',
                'Arn': 'arn:aws:connect:us-east-1:123456789012:contact-flow/cf-123',
                'Name': 'Test Flow',
                'Type': 'CONTACT_FLOW',
                'State': 'ACTIVE'
            }
        }
        self.mock_connect_client.describe_contact_flow.return_value = mock_response

        result = self.integration.describe_contact_flow("instance-id", "cf-123")

        self.assertEqual(result['ContactFlow']['Id'], 'cf-123')


class TestConnectQueues(unittest.TestCase):
    """Test Connect queue methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_queue(self):
        """Test creating a queue"""
        mock_response = {
            'QueueId': 'queue-123',
            'QueueArn': 'arn:aws:connect:us-east-1:123456789012:queue/queue-123'
        }
        self.mock_connect_client.create_queue.return_value = mock_response

        config = QueueConfig(
            name="Test Queue",
            description="Test queue"
        )

        result = self.integration.create_queue("instance-id", config)

        self.assertEqual(result.queue_id, 'queue-123')

    def test_list_queues(self):
        """Test listing queues"""
        mock_response = {
            'QueueSummaryList': [
                {'Id': 'queue-1', 'Name': 'Queue 1'},
                {'Id': 'queue-2', 'Name': 'Queue 2'}
            ]
        }
        self.mock_connect_client.list_queues.return_value = mock_response

        result = self.integration.list_queues("instance-id")

        self.assertEqual(len(result), 2)


class TestConnectUsers(unittest.TestCase):
    """Test Connect user management methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_user(self):
        """Test creating a user"""
        mock_response = {
            'UserId': 'user-123',
            'UserArn': 'arn:aws:connect:us-east-1:123456789012:user/user-123'
        }
        self.mock_connect_client.create_user.return_value = mock_response

        config = UserConfig(
            username="testuser",
            password="password123",
            identity_info={"FirstName": "Test", "LastName": "User", "Email": "test@example.com"},
            phone_config={"PhoneType": "SOFT_PHONE"},
            routing_profile_id="rp-123",
            security_profile_ids=["sp-123"]
        )

        result = self.integration.create_user("instance-id", config)

        self.assertEqual(result.id, 'user-123')

    def test_describe_user(self):
        """Test describing a user"""
        mock_response = {
            'User': {
                'Id': 'user-123',
                'Arn': 'arn:aws:connect:us-east-1:123456789012:user/user-123',
                'Username': 'testuser'
            }
        }
        self.mock_connect_client.describe_user.return_value = mock_response

        result = self.integration.describe_user("instance-id", "user-123")

        self.assertEqual(result['User']['Id'], 'user-123')


class TestConnectHoursOfOperation(unittest.TestCase):
    """Test Connect hours of operation methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_hours_of_operation(self):
        """Test creating hours of operation"""
        mock_response = {
            'HoursOfOperationId': 'hours-123',
            'HoursOfOperationArn': 'arn:aws:connect:us-east-1:123456789012:hours-of-operation/hours-123'
        }
        self.mock_connect_client.create_hours_of_operation.return_value = mock_response

        config = HoursOfOperationConfig(
            name="Test Hours",
            time_zone="UTC",
            hours_of_operation_config=[{
                "Day": "MONDAY",
                "StartTime": {"Hours": 9, "Minutes": 0},
                "EndTime": {"Hours": 17, "Minutes": 0}
            }]
        )

        result = self.integration.create_hours_of_operation("instance-id", config)

        self.assertEqual(result.hours_of_operation_id, 'hours-123')


class TestConnectQuickConnects(unittest.TestCase):
    """Test Connect quick connect methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_quick_connect(self):
        """Test creating a quick connect"""
        mock_response = {
            'QuickConnectId': 'qc-123',
            'QuickConnectArn': 'arn:aws:connect:us-east-1:123456789012:quick-connect/qc-123'
        }
        self.mock_connect_client.create_quick_connect.return_value = mock_response

        config = QuickConnectConfig(
            name="Test QC",
            quick_connect_type=QuickConnectType.QUEUE,
            destination={"queueId": "queue-123"}
        )

        result = self.integration.create_quick_connect("instance-id", config)

        self.assertEqual(result.quick_connect_id, 'qc-123')


class TestConnectRoutingProfiles(unittest.TestCase):
    """Test Connect routing profile methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_routing_profile(self):
        """Test creating a routing profile"""
        mock_response = {
            'RoutingProfileId': 'rp-123',
            'RoutingProfileArn': 'arn:aws:connect:us-east-1:123456789012:routing-profile/rp-123'
        }
        self.mock_connect_client.create_routing_profile.return_value = mock_response

        config = RoutingProfileConfig(
            name="Test RP",
            description="Test routing profile",
            instance_id="instance-123",
            default_outbound_queue_id="queue-123",
            media_concurrencies=[{"Channel": "VOICE", "Concurrency": 1}]
        )

        result = self.integration.create_routing_profile("instance-id", config)

        self.assertEqual(result.routing_profile_id, 'rp-123')


class TestConnectPrompts(unittest.TestCase):
    """Test Connect prompt methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_create_prompt(self):
        """Test creating a prompt"""
        mock_response = {
            'PromptId': 'prompt-123',
            'PromptARN': 'arn:aws:connect:us-east-1:123456789012:prompt/prompt-123'
        }
        self.mock_connect_client.create_prompt.return_value = mock_response

        config = PromptConfig(
            name="Test Prompt",
            description="Test prompt"
        )

        result = self.integration.create_prompt("instance-id", config)

        self.assertEqual(result.prompt_id, 'prompt-123')


class TestConnectContactSearch(unittest.TestCase):
    """Test Connect contact search methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {'connect': self.mock_connect_client}

    def test_search_contacts(self):
        """Test searching contacts"""
        mock_response = {
            'Contacts': [
                {
                    'ContactId': 'contact-123',
                    'Arn': 'arn:aws:connect:us-east-1:123456789012:contact/contact-123',
                    'State': 'CONNECTED'
                }
            ],
            'TotalCount': 1
        }
        self.mock_connect_client.search_contacts.return_value = mock_response

        result = self.integration.search_contacts("instance-id", start_time=datetime.now())

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].contact_id, 'contact-123')


class TestConnectCloudWatchMetrics(unittest.TestCase):
    """Test Connect CloudWatch metrics methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_connect_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

        with patch.object(ConnectIntegration, '__init__', lambda x, **kwargs: None):
            self.integration = ConnectIntegration()
            self.integration._clients = {
                'connect': self.mock_connect_client,
                'cloudwatch': self.mock_cloudwatch_client
            }

    def test_get_metrics(self):
        """Test getting contact center metrics"""
        self.mock_cloudwatch_client.get_metric_statistics.return_value = {
            'Datapoints': [
                {'Average': 5.0, 'Maximum': 10.0, 'Minimum': 1.0, 'Timestamp': '2024-01-01T00:00:00Z'}
            ]
        }

        result = self.integration.get_metrics("instance-id", metric_name="ActiveContacts")

        self.assertIsNotNone(result)
        self.mock_cloudwatch_client.get_metric_statistics.assert_called()


if __name__ == '__main__':
    unittest.main()
