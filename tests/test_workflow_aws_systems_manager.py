"""
Tests for workflow_aws_systems_manager module
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

# Create mock boto3 module before importing workflow_aws_systems_manager
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

# Now we can import the module
from src.workflow_aws_systems_manager import (
    SSMIntegration,
    ParameterType,
    ParameterTier,
    DocumentType,
    DocumentFormat,
    CommandStatus,
    SessionStatus,
    AssociationStatus,
    PatchComplianceLevel,
    OpsItemStatus,
    Parameter,
    SSMDocument,
    CommandExecution,
    SessionInfo,
    Association,
    MaintenanceWindow,
    OpsItem,
    InventoryEntry,
    PatchState,
)


class TestParameterType(unittest.TestCase):
    """Test ParameterType enum"""

    def test_parameter_type_values(self):
        self.assertEqual(ParameterType.STRING.value, "String")
        self.assertEqual(ParameterType.STRING_LIST.value, "StringList")
        self.assertEqual(ParameterType.SECURE_STRING.value, "SecureString")

    def test_parameter_type_count(self):
        self.assertEqual(len(ParameterType), 3)


class TestParameterTier(unittest.TestCase):
    """Test ParameterTier enum"""

    def test_parameter_tier_values(self):
        self.assertEqual(ParameterTier.STANDARD.value, "Standard")
        self.assertEqual(ParameterTier.ADVANCED.value, "Advanced")
        self.assertEqual(ParameterTier.INTELLIGENT_TIERING.value, "Intelligent-Tiering")

    def test_parameter_tier_count(self):
        self.assertEqual(len(ParameterTier), 3)


class TestDocumentType(unittest.TestCase):
    """Test DocumentType enum"""

    def test_document_type_values(self):
        self.assertEqual(DocumentType.COMMAND.value, "Command")
        self.assertEqual(DocumentType.POLICY.value, "Policy")
        self.assertEqual(DocumentType.AUTOMATION.value, "Automation")
        self.assertEqual(DocumentType.SESSION.value, "Session")


class TestDocumentFormat(unittest.TestCase):
    """Test DocumentFormat enum"""

    def test_document_format_values(self):
        self.assertEqual(DocumentFormat.YAML.value, "YAML")
        self.assertEqual(DocumentFormat.JSON.value, "JSON")


class TestCommandStatus(unittest.TestCase):
    """Test CommandStatus enum"""

    def test_command_status_values(self):
        self.assertEqual(CommandStatus.PENDING.value, "Pending")
        self.assertEqual(CommandStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(CommandStatus.SUCCESS.value, "Success")
        self.assertEqual(CommandStatus.FAILED.value, "Failed")
        self.assertEqual(CommandStatus.TIMED_OUT.value, "TimedOut")
        self.assertEqual(CommandStatus.CANCELLED.value, "Cancelled")


class TestSessionStatus(unittest.TestCase):
    """Test SessionStatus enum"""

    def test_session_status_values(self):
        self.assertEqual(SessionStatus.ACTIVE.value, "Active")
        self.assertEqual(SessionStatus.IDLE.value, "Idle")
        self.assertEqual(SessionStatus.DISCONNECTED.value, "Disconnected")
        self.assertEqual(SessionStatus.TERMINATED.value, "Terminated")


class TestAssociationStatus(unittest.TestCase):
    """Test AssociationStatus enum"""

    def test_association_status_values(self):
        self.assertEqual(AssociationStatus.ASSOCIATED.value, "Associated")
        self.assertEqual(AssociationStatus.PENDING.value, "Pending")
        self.assertEqual(AssociationStatus.FAILED.value, "Failed")


class TestPatchComplianceLevel(unittest.TestCase):
    """Test PatchComplianceLevel enum"""

    def test_patch_compliance_level_values(self):
        self.assertEqual(PatchComplianceLevel.CRITICAL.value, "Critical")
        self.assertEqual(PatchComplianceLevel.HIGH.value, "High")
        self.assertEqual(PatchComplianceLevel.MEDIUM.value, "Medium")
        self.assertEqual(PatchComplianceLevel.LOW.value, "Low")


class TestOpsItemStatus(unittest.TestCase):
    """Test OpsItemStatus enum"""

    def test_ops_item_status_values(self):
        self.assertEqual(OpsItemStatus.OPEN.value, "Open")
        self.assertEqual(OpsItemStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(OpsItemStatus.RESOLVED.value, "Resolved")
        self.assertEqual(OpsItemStatus.PENDING.value, "Pending")


class TestParameter(unittest.TestCase):
    """Test Parameter dataclass"""

    def test_parameter_creation(self):
        param = Parameter(
            name="/test/parameter",
            value="test-value",
            param_type=ParameterType.STRING
        )
        self.assertEqual(param.name, "/test/parameter")
        self.assertEqual(param.value, "test-value")
        self.assertEqual(param.param_type, ParameterType.STRING)
        self.assertEqual(param.version, 1)
        self.assertEqual(param.tier, ParameterTier.STANDARD)

    def test_parameter_with_tags(self):
        param = Parameter(
            name="/test/parameter",
            value="test-value",
            tags={"Environment": "Production", "Application": "Web"}
        )
        self.assertEqual(param.tags["Environment"], "Production")
        self.assertEqual(len(param.tags), 2)

    def test_parameter_secure_string(self):
        param = Parameter(
            name="/test/secret",
            value="secret-value",
            param_type=ParameterType.SECURE_STRING,
            key_id="kms-key-123"
        )
        self.assertEqual(param.param_type, ParameterType.SECURE_STRING)
        self.assertEqual(param.key_id, "kms-key-123")


class TestSSMDocument(unittest.TestCase):
    """Test SSMDocument dataclass"""

    def test_ssm_document_creation(self):
        doc = SSMDocument(
            name="test-document",
            content={"commands": ["echo hello"]},
            doc_type=DocumentType.COMMAND
        )
        self.assertEqual(doc.name, "test-document")
        self.assertEqual(doc.doc_type, DocumentType.COMMAND)
        self.assertEqual(doc.format, DocumentFormat.JSON)

    def test_ssm_document_with_tags(self):
        doc = SSMDocument(
            name="test-doc",
            content={},
            tags={"Version": "1.0", "Author": "Test"}
        )
        self.assertEqual(doc.tags["Version"], "1.0")


class TestCommandExecution(unittest.TestCase):
    """Test CommandExecution dataclass"""

    def test_command_execution_creation(self):
        execution = CommandExecution(
            command_id="cmd-123",
            status=CommandStatus.SUCCESS,
            requested_date=datetime.now(),
            instance_ids=["i-123", "i-456"]
        )
        self.assertEqual(execution.command_id, "cmd-123")
        self.assertEqual(execution.status, CommandStatus.SUCCESS)
        self.assertEqual(len(execution.instance_ids), 2)
        self.assertEqual(execution.success_count, 0)

    def test_command_execution_with_counts(self):
        execution = CommandExecution(
            command_id="cmd-123",
            status=CommandStatus.SUCCESS,
            requested_date=datetime.now(),
            target_count=3,
            success_count=2,
            failed_count=1
        )
        self.assertEqual(execution.target_count, 3)
        self.assertEqual(execution.success_count, 2)
        self.assertEqual(execution.failed_count, 1)


class TestSessionInfo(unittest.TestCase):
    """Test SessionInfo dataclass"""

    def test_session_info_creation(self):
        session = SessionInfo(
            session_id="session-123",
            target="i-12345678",
            status=SessionStatus.ACTIVE,
            start_date=datetime.now()
        )
        self.assertEqual(session.session_id, "session-123")
        self.assertEqual(session.target, "i-12345678")
        self.assertEqual(session.status, SessionStatus.ACTIVE)


class TestAssociation(unittest.TestCase):
    """Test Association dataclass"""

    def test_association_creation(self):
        assoc = Association(
            association_id="assoc-123",
            name="SSM-Document",
            instance_id="i-12345678",
            status=AssociationStatus.ASSOCIATED
        )
        self.assertEqual(assoc.association_id, "assoc-123")
        self.assertEqual(assoc.status, AssociationStatus.ASSOCIATED)


class TestMaintenanceWindow(unittest.TestCase):
    """Test MaintenanceWindow dataclass"""

    def test_maintenance_window_creation(self):
        mw = MaintenanceWindow(
            window_id="mw-123",
            name="test-window",
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(hours=2),
            schedule="cron(0 0 * * ? *)",
            duration=2,
            cutoff=1
        )
        self.assertEqual(mw.window_id, "mw-123")
        self.assertEqual(mw.enabled, True)
        self.assertEqual(mw.target_count, 0)


class TestOpsItem(unittest.TestCase):
    """Test OpsItem dataclass"""

    def test_ops_item_creation(self):
        item = OpsItem(
            ops_item_id="oi-123",
            title="Test Issue",
            status=OpsItemStatus.OPEN,
            priority=2,
            severity="High"
        )
        self.assertEqual(item.ops_item_id, "oi-123")
        self.assertEqual(item.title, "Test Issue")
        self.assertEqual(item.priority, 2)

    def test_ops_item_with_operational_data(self):
        item = OpsItem(
            ops_item_id="oi-123",
            title="Test",
            status=OpsItemStatus.OPEN,
            operational_data={"key": "value"}
        )
        self.assertEqual(item.operational_data["key"], "value")


class TestInventoryEntry(unittest.TestCase):
    """Test InventoryEntry dataclass"""

    def test_inventory_entry_creation(self):
        entry = InventoryEntry(
            instance_id="i-12345678",
            capture_time=datetime.now(),
            entries={"Application": "TestApp", "Version": "1.0"}
        )
        self.assertEqual(entry.instance_id, "i-12345678")
        self.assertEqual(entry.entries["Application"], "TestApp")


class TestPatchState(unittest.TestCase):
    """Test PatchState dataclass"""

    def test_patch_state_creation(self):
        state = PatchState(
            instance_id="i-12345678",
            patch_group="prod",
            baseline_id="pb-123",
            critical_count=5,
            high_count=10
        )
        self.assertEqual(state.instance_id, "i-12345678")
        self.assertEqual(state.critical_count, 5)
        self.assertEqual(state.high_count, 10)


class TestSSMIntegration(unittest.TestCase):
    """Test SSMIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_logs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

    def test_integration_initialization(self):
        """Test SSMIntegration initialization"""
        integration = SSMIntegration(
            region="us-east-1",
            profile="test-profile"
        )
        self.assertEqual(integration.region, "us-east-1")
        self.assertEqual(integration.profile, "test-profile")

    def test_integration_with_custom_clients(self):
        """Test SSMIntegration with custom clients"""
        integration = SSMIntegration(
            ssm_client=self.mock_ssm_client,
            s3_client=self.mock_s3_client,
            logs_client=self.mock_logs_client,
            cloudwatch_client=self.mock_cloudwatch_client
        )
        self.assertIsNotNone(integration.ssm)
        self.assertIsNotNone(integration.s3)

    def test_ssm_property(self):
        """Test SSM property getter"""
        integration = SSMIntegration()
        # Access the property - should create client if not exists
        integration.ssm  # This will fail without boto3 but that's expected in test env

    def test_s3_property(self):
        """Test S3 property getter"""
        integration = SSMIntegration()
        integration.s3  # Access property

    def test_logs_property(self):
        """Test Logs property getter"""
        integration = SSMIntegration()
        integration.logs  # Access property

    def test_cloudwatch_property(self):
        """Test CloudWatch property getter"""
        integration = SSMIntegration()
        integration.cloudwatch  # Access property


class TestParameterStoreOperations(unittest.TestCase):
    """Test Parameter Store operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_put_parameter_string(self):
        """Test putting a string parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.put_parameter.return_value = {
            "Version": 1,
            "Tier": "Standard"
        }

        result = integration.put_parameter(
            name="/test/parameter",
            value="test-value",
            param_type=ParameterType.STRING
        )

        self.assertEqual(result.name, "/test/parameter")
        self.assertEqual(result.value, "test-value")
        self.mock_ssm_client.put_parameter.assert_called_once()

    def test_put_parameter_secure_string(self):
        """Test putting a secure string parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.put_parameter.return_value = {
            "Version": 1,
            "Tier": "Standard"
        }

        result = integration.put_parameter(
            name="/test/secret",
            value="secret-value",
            param_type=ParameterType.SECURE_STRING,
            key_id="kms-key-123"
        )

        call_kwargs = self.mock_ssm_client.put_parameter.call_args[1]
        self.assertEqual(call_kwargs["Type"], "SecureString")
        self.assertEqual(call_kwargs["KeyId"], "kms-key-123")

    def test_put_parameter_with_tags(self):
        """Test putting a parameter with tags"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.put_parameter.return_value = {
            "Version": 1,
            "Tier": "Standard"
        }

        result = integration.put_parameter(
            name="/test/parameter",
            value="test-value",
            tags={"Environment": "Production"}
        )

        call_kwargs = self.mock_ssm_client.put_parameter.call_args[1]
        self.assertEqual(len(call_kwargs["Tags"]), 1)

    def test_put_parameter_overwrite(self):
        """Test overwriting a parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.put_parameter.return_value = {
            "Version": 2,
            "Tier": "Standard"
        }

        result = integration.put_parameter(
            name="/test/parameter",
            value="new-value",
            overwrite=True
        )

        call_kwargs = self.mock_ssm_client.put_parameter.call_args[1]
        self.assertTrue(call_kwargs["Overwrite"])

    def test_get_parameter_string(self):
        """Test getting a string parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_parameter.return_value = {
            "Parameter": {
                "Name": "/test/parameter",
                "Value": "test-value",
                "Type": "String",
                "Version": 1,
                "ARN": "arn:aws:ssm:us-east-1:123456789:parameter/test/parameter",
                "LastModifiedDate": "2024-01-01T00:00:00.000Z",
                "Tags": []
            }
        }

        result = integration.get_parameter("/test/parameter")

        self.assertEqual(result.name, "/test/parameter")
        self.assertEqual(result.value, "test-value")
        self.assertEqual(result.param_type, ParameterType.STRING)

    def test_get_parameter_secure_string(self):
        """Test getting a secure string parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_parameter.return_value = {
            "Parameter": {
                "Name": "/test/secret",
                "Value": "decrypted-secret",
                "Type": "SecureString",
                "Version": 1,
                "ARN": "arn:aws:ssm:us-east-1:123456789:parameter/test/secret"
            }
        }

        result = integration.get_parameter("/test/secret", with_decryption=True)

        self.assertEqual(result.param_type, ParameterType.SECURE_STRING)

    def test_get_parameter_specific_version(self):
        """Test getting a specific version of a parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_parameter.return_value = {
            "Parameter": {
                "Name": "/test/parameter",
                "Value": "versioned-value",
                "Type": "String",
                "Version": 5
            }
        }

        result = integration.get_parameter("/test/parameter", version=5)

        call_kwargs = self.mock_ssm_client.get_parameter.call_args[1]
        self.assertEqual(call_kwargs["Version"], 5)

    def test_get_parameters_multiple(self):
        """Test getting multiple parameters"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_parameters.return_value = {
            "Parameters": [
                {
                    "Name": "/test/param1",
                    "Value": "value1",
                    "Type": "String",
                    "Version": 1
                },
                {
                    "Name": "/test/param2",
                    "Value": "value2",
                    "Type": "String",
                    "Version": 1
                }
            ]
        }

        results = integration.get_parameters(["/test/param1", "/test/param2"])

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].name, "/test/param1")
        self.assertEqual(results[1].name, "/test/param2")

    def test_delete_parameter(self):
        """Test deleting a parameter"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.delete_parameter.return_value = {}

        result = integration.delete_parameter("/test/parameter")

        self.assertTrue(result)
        self.mock_ssm_client.delete_parameter.assert_called_once_with(
            Name="/test/parameter"
        )

    def test_list_parameters(self):
        """Test listing parameters"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_parameters.return_value = {
            "Parameters": [
                {
                    "Name": "/test/param1",
                    "Type": "String",
                    "Version": 1
                },
                {
                    "Name": "/test/param2",
                    "Type": "SecureString",
                    "Version": 2
                }
            ]
        }

        results = integration.list_parameters(path="/test/")

        self.assertEqual(len(results), 2)

    def test_list_parameters_with_pagination(self):
        """Test listing parameters with pagination"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_parameters.return_value = {
            "Parameters": [
                {"Name": "/test/param1", "Type": "String", "Version": 1}
            ],
            "NextToken": "next-page-token"
        }

        results = integration.list_parameters(path="/test/", max_results=1)

        call_kwargs = self.mock_ssm_client.describe_parameters.call_args[1]
        self.assertEqual(call_kwargs["MaxResults"], 1)


class TestDocumentOperations(unittest.TestCase):
    """Test SSM Document operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_create_document(self):
        """Test creating a document"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.create_document.return_value = {
            "DocumentDescription": {
                "Name": "test-document",
                "DocumentType": "Command",
                "Status": "Creating"
            }
        }

        content = json.dumps({"schemaVersion": "2.0", "mainStep": [{"action": "aws:runShellScript", "inputs": {"commands": ["echo hello"]}}]})
        result = integration.create_document(
            name="test-document",
            content=content,
            doc_type=DocumentType.COMMAND
        )

        self.assertEqual(result.name, "test-document")
        self.mock_ssm_client.create_document.assert_called_once()

    def test_create_document_with_tags(self):
        """Test creating a document with tags"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.create_document.return_value = {
            "DocumentDescription": {
                "Name": "test-doc",
                "DocumentType": "Command",
                "Tags": [{"Key": "Version", "Value": "1.0"}]
            }
        }

        result = integration.create_document(
            name="test-doc",
            content="{}",
            tags={"Version": "1.0"}
        )

    def test_get_document(self):
        """Test getting a document"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_document.return_value = {
            "Name": "test-document",
            "Content": '{"schemaVersion":"2.0"}',
            "DocumentType": "Command",
            "Status": "Active"
        }

        result = integration.get_document("test-document")

        self.assertEqual(result.name, "test-document")
        self.assertIn("schemaVersion", result.content)

    def test_delete_document(self):
        """Test deleting a document"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.delete_document.return_value = {}

        integration.delete_document("test-document")

        self.mock_ssm_client.delete_document.assert_called_once()

    def test_list_documents(self):
        """Test listing documents"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.list_documents.return_value = {
            "DocumentIdentifiers": [
                {"Name": "doc-1", "DocumentType": "Command"},
                {"Name": "doc-2", "DocumentType": "Automation"}
            ]
        }

        results = integration.list_documents()

        self.assertEqual(len(results), 2)

    def test_update_document(self):
        """Test updating a document"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.update_document.return_value = {
            "DocumentDescription": {
                "Name": "test-document",
                "DocumentVersion": "2"
            }
        }

        result = integration.update_document(
            name="test-document",
            content="{}"
        )

        self.mock_ssm_client.update_document.assert_called_once()


class TestCommandExecutionOperations(unittest.TestCase):
    """Test Run Command execution operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_send_command(self):
        """Test sending a command"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.send_command.return_value = {
            "Command": {
                "CommandId": "cmd-12345678",
                "Status": "Pending",
                "DocumentName": "AWS-RunShellScript",
                "InstanceIds": ["i-12345678"]
            }
        }

        result = integration.send_command(
            instance_ids=["i-12345678"],
            document_name="AWS-RunShellScript",
            parameters={"commands": ["echo hello"]}
        )

        self.assertEqual(result.command_id, "cmd-12345678")
        self.assertEqual(result.status, CommandStatus.PENDING)

    def test_send_command_with_s3_output(self):
        """Test sending a command with S3 output"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.send_command.return_value = {
            "Command": {
                "CommandId": "cmd-123",
                "Status": "Pending"
            }
        }

        result = integration.send_command(
            instance_ids=["i-123"],
            document_name="AWS-RunShellScript",
            parameters={"commands": ["echo hello"]},
            output_s3_bucket="my-bucket",
            output_s3_key_prefix="ssm-output"
        )

        call_kwargs = self.mock_ssm_client.send_command.call_args[1]
        self.assertEqual(call_kwargs["OutputS3BucketName"], "my-bucket")

    def test_list_commands(self):
        """Test listing commands"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.list_commands.return_value = {
            "Commands": [
                {
                    "CommandId": "cmd-1",
                    "Status": "Success",
                    "DocumentName": "AWS-RunShellScript"
                },
                {
                    "CommandId": "cmd-2",
                    "Status": "Failed",
                    "DocumentName": "AWS-RunShellScript"
                }
            ]
        }

        results = integration.list_commands()

        self.assertEqual(len(results), 2)

    def test_get_command_invocation(self):
        """Test getting command invocation"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_command_invocation.return_value = {
            "CommandId": "cmd-123",
            "InstanceId": "i-12345678",
            "Status": "Success",
            "StandardOutputContent": "Hello World"
        }

        result = integration.get_command_invocation("cmd-123", "i-12345678")

        self.assertEqual(result["Status"], "Success")
        self.assertEqual(result["StandardOutputContent"], "Hello World")

    def test_cancel_command(self):
        """Test cancelling a command"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.cancel_command.return_value = {}

        integration.cancel_command("cmd-12345678")

        self.mock_ssm_client.cancel_command.assert_called_once()


class TestSessionManagerOperations(unittest.TestCase):
    """Test Session Manager operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_start_session(self):
        """Test starting a session"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.start_session.return_value = {
            "SessionId": "session-12345678",
            "TokenValue": "token-value",
            "StreamUrl": "wss://ssm.amazonaws.com"
        }

        result = integration.start_session(target="i-12345678")

        self.assertEqual(result.session_id, "session-12345678")
        self.assertEqual(result.target, "i-12345678")
        self.assertEqual(result.status, SessionStatus.ACTIVE)

    def test_terminate_session(self):
        """Test terminating a session"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.terminate_session.return_value = {
            "SessionId": "session-123"
        }

        result = integration.terminate_session("session-12345678")

        self.assertTrue(result)

    def test_list_sessions(self):
        """Test listing sessions"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_sessions.return_value = {
            "Sessions": [
                {
                    "SessionId": "session-1",
                    "Target": "i-123",
                    "Status": "Active"
                },
                {
                    "SessionId": "session-2",
                    "Target": "i-456",
                    "Status": "Terminated"
                }
            ]
        }

        results = integration.list_sessions(status=SessionStatus.ACTIVE)

        self.assertEqual(len(results), 2)


class TestStateManagerOperations(unittest.TestCase):
    """Test State Manager operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_create_association(self):
        """Test creating an association"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.create_association.return_value = {
            "AssociationDescription": {
                "AssociationId": "assoc-12345678",
                "Name": "SSM-Document",
                "InstanceId": "i-12345678",
                "Status": "Associated"
            }
        }

        result = integration.create_association(
            name="SSM-Document",
            instance_id="i-12345678"
        )

        self.assertEqual(result.association_id, "assoc-12345678")

    def test_list_associations(self):
        """Test listing associations"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_associations.return_value = {
            "Associations": [
                {
                    "AssociationId": "assoc-1",
                    "Name": "SSM-Document-1",
                    "InstanceId": "i-123",
                    "Status": "Associated"
                }
            ]
        }

        results = integration.list_associations()

        self.assertEqual(len(results), 1)

    def test_get_association_status(self):
        """Test getting association status"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_association.return_value = {
            "AssociationDescription": {
                "AssociationId": "assoc-123",
                "Name": "SSM-Document",
                "InstanceId": "i-123",
                "Status": {
                    "Date": datetime.now(),
                    "Name": "Associated"
                }
            }
        }

        result = integration.get_association_status("assoc-123")

        self.assertEqual(result["AssociationId"], "assoc-123")

    def test_delete_association(self):
        """Test deleting an association"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.delete_association.return_value = {}

        integration.delete_association("assoc-12345678")

        self.mock_ssm_client.delete_association.assert_called_once()


class TestMaintenanceWindowOperations(unittest.TestCase):
    """Test Maintenance Window operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_create_maintenance_window(self):
        """Test creating a maintenance window"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.create_maintenance_window.return_value = {
            "WindowId": "mw-12345678"
        }

        result = integration.create_maintenance_window(
            name="test-window",
            schedule="cron(0 0 * * ? *)",
            duration=2,
            cutoff=1
        )

        self.assertEqual(result.window_id, "mw-12345678")

    def test_list_maintenance_windows(self):
        """Test listing maintenance windows"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_maintenance_windows.return_value = {
            "WindowIdentities": [
                {"WindowId": "mw-1", "Name": "window-1"},
                {"WindowId": "mw-2", "Name": "window-2"}
            ]
        }

        results = integration.list_maintenance_windows()

        self.assertEqual(len(results), 2)

    def test_delete_maintenance_window(self):
        """Test deleting a maintenance window"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.delete_maintenance_window.return_value = {}

        integration.delete_maintenance_window("mw-12345678")

        self.mock_ssm_client.delete_maintenance_window.assert_called_once()


class TestOpsCenterOperations(unittest.TestCase):
    """Test OpsCenter operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_create_ops_item(self):
        """Test creating an OpsItem"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.ops_item.return_value = {
            "OpsItemId": "oi-12345678"
        }

        result = integration.create_ops_item(
            title="Test Issue",
            description="Something went wrong",
            priority=2,
            category="Availability",
            severity="High"
        )

        self.assertEqual(result.ops_item_id, "oi-12345678")

    def test_get_ops_item(self):
        """Test getting an OpsItem"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_ops_item.return_value = {
            "OpsItem": {
                "OpsItemId": "oi-123",
                "Title": "Test Issue",
                "Status": "Open",
                "Priority": 2
            }
        }

        result = integration.get_ops_item("oi-12345678")

        self.assertEqual(result["Title"], "Test Issue")

    def test_update_ops_item(self):
        """Test updating an OpsItem"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.update_ops_item.return_value = {}

        integration.update_ops_item(
            ops_item_id="oi-123",
            status=OpsItemStatus.RESOLVED
        )

        self.mock_ssm_client.update_ops_item.assert_called_once()

    def test_list_ops_items(self):
        """Test listing OpsItems"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_ops_items.return_value = {
            "OpsItemSummaries": [
                {
                    "OpsItemId": "oi-1",
                    "Title": "Issue 1",
                    "Status": "Open"
                },
                {
                    "OpsItemId": "oi-2",
                    "Title": "Issue 2",
                    "Status": "Resolved"
                }
            ]
        }

        results = integration.list_ops_items(status=OpsItemStatus.OPEN)

        self.assertEqual(len(results), 2)

    def test_close_ops_item(self):
        """Test closing an OpsItem"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.update_ops_item.return_value = {}

        integration.close_ops_item("oi-12345678")

        call_kwargs = self.mock_ssm_client.update_ops_item.call_args[1]
        self.assertEqual(call_kwargs["Status"], "Resolved")


class TestInventoryOperations(unittest.TestCase):
    """Test Inventory operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_list_inventory_entries(self):
        """Test listing inventory entries"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_inventory.return_value = {
            "Entries": [
                {"InstanceId": "i-123", "Application": "TestApp", "Version": "1.0"},
                {"InstanceId": "i-456", "Application": "TestApp2", "Version": "2.0"}
            ]
        }

        results = integration.list_inventory_entries(instance_id="i-12345678")

        self.assertEqual(len(results), 2)

    def test_put_inventory(self):
        """Test putting inventory"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.put_inventory.return_value = {
            "Message": "Successfully posted 1 inventory entries"
        }

        result = integration.put_inventory(
            instance_id="i-12345678",
            entries=[{"Application": "TestApp", "Version": "1.0"}]
        )

        self.assertTrue(result)

    def test_delete_inventory(self):
        """Test deleting inventory"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.delete_inventory.return_value = {
            "DeletionSummary": {"TotalCount": 1}
        }

        result = integration.delete_inventory(
            instance_id="i-12345678",
            type_name="Custom:AppInfo"
        )

        self.assertTrue(result)


class TestPatchManagerOperations(unittest.TestCase):
    """Test Patch Manager operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_describe_patch_baselines(self):
        """Test describing patch baselines"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_patch_baselines.return_value = {
            "BaselineIdentities": [
                {"BaselineId": "pb-1", "Name": "AWS-DefaultPatchBaseline"},
                {"BaselineId": "pb-2", "Name": "Custom-Baseline"}
            ]
        }

        results = integration.describe_patch_baselines()

        self.assertEqual(len(results), 2)

    def test_create_patch_baseline(self):
        """Test creating a patch baseline"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.create_patch_baseline.return_value = {
            "BaselineId": "pb-12345678"
        }

        result = integration.create_patch_baseline(
            name="Custom-Baseline",
            operating_system="AMAZON_LINUX_2"
        )

        self.assertEqual(result.baseline_id, "pb-12345678")

    def test_get_patch_baseline(self):
        """Test getting a patch baseline"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_patch_baseline.return_value = {
            "BaselineId": "pb-123",
            "Name": "AWS-DefaultPatchBaseline",
            "OperatingSystem": "AMAZON_LINUX_2"
        }

        result = integration.get_patch_baseline("pb-12345678")

        self.assertEqual(result["BaselineId"], "pb-123")

    def test_describe_patch_groups(self):
        """Test describing patch groups"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_patch_groups.return_value = {
            "PatchGroups": [
                {"PatchGroup": "prod", "BaselineId": "pb-1"},
                {"PatchGroup": "dev", "BaselineId": "pb-2"}
            ]
        }

        results = integration.describe_patch_groups()

        self.assertEqual(len(results), 2)

    def test_register_patch_baseline_for_patch_group(self):
        """Test registering a patch baseline for a patch group"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.register_patch_baseline_for_patch_group.return_value = {
            "PatchGroup": "prod",
            "BaselineId": "pb-12345678"
        }

        result = integration.register_patch_baseline_for_patch_group(
            patch_group="prod",
            baseline_id="pb-12345678"
        )

        self.assertEqual(result["PatchGroup"], "prod")

    def test_describe_instance_patch_states(self):
        """Test describing instance patch states"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_instance_patch_states.return_value = {
            "InstancePatchStates": [
                {
                    "InstanceId": "i-123",
                    "PatchGroup": "prod",
                    "CriticalCount": 5,
                    "HighCount": 10,
                    "Overall": "CRITICAL"
                }
            ]
        }

        results = integration.describe_instance_patch_states(["i-12345678"])

        self.assertEqual(len(results), 1)


class TestAutomationOperations(unittest.TestCase):
    """Test Automation operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ssm_client = MagicMock()

    def test_start_automation_execution(self):
        """Test starting an automation execution"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.start_automation_execution.return_value = {
            "AutomationExecutionId": "exec-12345678"
        }

        result = integration.start_automation_execution(
            document_name="AWS-RestartEC2Instance",
            parameters={"InstanceId": "i-12345678"}
        )

        self.assertEqual(result.execution_id, "exec-12345678")

    def test_get_automation_execution(self):
        """Test getting an automation execution"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.get_automation_execution.return_value = {
            "AutomationExecution": {
                "AutomationExecutionId": "exec-123",
                "DocumentName": "AWS-RestartEC2Instance",
                "ExecutionStatus": "Success"
            }
        }

        result = integration.get_automation_execution("exec-12345678")

        self.assertEqual(result["ExecutionStatus"], "Success")

    def test_list_automation_executions(self):
        """Test listing automation executions"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.describe_automation_executions.return_value = {
            "AutomationExecutionMetadataList": [
                {
                    "AutomationExecutionId": "exec-1",
                    "DocumentName": "AWS-StopEC2Instance",
                    "ExecutionStatus": "Complete"
                }
            ]
        }

        results = integration.list_automation_executions()

        self.assertEqual(len(results), 1)

    def test_stop_automation_execution(self):
        """Test stopping an automation execution"""
        integration = SSMIntegration(ssm_client=self.mock_ssm_client)

        self.mock_ssm_client.stop_automation_execution.return_value = {}

        integration.stop_automation_execution("exec-12345678")

        self.mock_ssm_client.stop_automation_execution.assert_called_once()


class TestCloudWatchIntegration(unittest.TestCase):
    """Test CloudWatch integration"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_logs_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()

    def test_create_log_group(self):
        """Test creating a log group"""
        integration = SSMIntegration(
            logs_client=self.mock_logs_client,
            cloudwatch_client=self.mock_cloudwatch_client
        )

        self.mock_logs_client.create_log_group.return_value = {}

        result = integration.create_log_group("/ssm/test-logs")

        self.assertTrue(result)
        self.mock_logs_client.create_log_group.assert_called_once()

    def test_put_log_events(self):
        """Test putting log events"""
        integration = SSMIntegration(
            logs_client=self.mock_logs_client
        )

        self.mock_logs_client.put_log_events.return_value = {
            "nextSequenceToken": "token-123"
        }

        result = integration.put_log_events(
            log_group_name="/ssm/test",
            log_stream_name="test-stream",
            log_events=[{"timestamp": 1234567890, "message": "test message"}]
        )

        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
