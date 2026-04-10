"""
Tests for workflow_aws_cloudformation module
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

# Create mock boto3 module before importing workflow_aws_cloudformation
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
from src.workflow_aws_cloudformation import (
    CloudFormationIntegration,
    StackStatus,
    DriftDetectionStatus,
    ResourceSignalStatus,
    ChangeSetType,
    ChangeSetStatus,
    StackSetOperationStatus,
    PermissionModel,
    Capability,
    OnFailure,
    CloudFormationConfig,
    StackConfig,
    StackInfo,
    ChangeSetInfo,
    StackResourceInfo,
    StackEventInfo,
    DriftDetectionResult,
    StackSetConfig,
    StackSetInfo,
    StackPolicyConfig,
    TemplateEstimate,
    CustomResourceConfig,
    ExportInfo,
    ImportInfo,
)


class TestStackStatus(unittest.TestCase):
    """Test StackStatus enum"""

    def test_stack_status_values(self):
        self.assertEqual(StackStatus.CREATE_IN_PROGRESS.value, "CREATE_IN_PROGRESS")
        self.assertEqual(StackStatus.CREATE_COMPLETE.value, "CREATE_COMPLETE")
        self.assertEqual(StackStatus.CREATE_FAILED.value, "CREATE_FAILED")
        self.assertEqual(StackStatus.DELETE_IN_PROGRESS.value, "DELETE_IN_PROGRESS")
        self.assertEqual(StackStatus.DELETE_COMPLETE.value, "DELETE_COMPLETE")
        self.assertEqual(StackStatus.UPDATE_IN_PROGRESS.value, "UPDATE_IN_PROGRESS")
        self.assertEqual(StackStatus.UPDATE_COMPLETE.value, "UPDATE_COMPLETE")

    def test_stack_status_count(self):
        """Test that all expected status values exist"""
        self.assertEqual(len(StackStatus), 20)


class TestDriftDetectionStatus(unittest.TestCase):
    """Test DriftDetectionStatus enum"""

    def test_drift_detection_status_values(self):
        self.assertEqual(DriftDetectionStatus.DETECTION_IN_PROGRESS.value, "DETECTION_IN_PROGRESS")
        self.assertEqual(DriftDetectionStatus.DETECTION_COMPLETE.value, "DETECTION_COMPLETE")
        self.assertEqual(DriftDetectionStatus.DRIFTED.value, "DRIFTED")
        self.assertEqual(DriftDetectionStatus.IN_SYNC.value, "IN_SYNC")


class TestChangeSetType(unittest.TestCase):
    """Test ChangeSetType enum"""

    def test_change_set_type_values(self):
        self.assertEqual(ChangeSetType.CREATE.value, "CREATE")
        self.assertEqual(ChangeSetType.UPDATE.value, "UPDATE")
        self.assertEqual(ChangeSetType.IMPORT.value, "IMPORT")


class TestChangeSetStatus(unittest.TestCase):
    """Test ChangeSetStatus enum"""

    def test_change_set_status_values(self):
        self.assertEqual(ChangeSetStatus.CREATE_PENDING.value, "CREATE_PENDING")
        self.assertEqual(ChangeSetStatus.CREATE_IN_PROGRESS.value, "CREATE_IN_PROGRESS")
        self.assertEqual(ChangeSetStatus.CREATE_COMPLETE.value, "CREATE_COMPLETE")
        self.assertEqual(ChangeSetStatus.DELETE_COMPLETE.value, "DELETE_COMPLETE")


class TestStackSetOperationStatus(unittest.TestCase):
    """Test StackSetOperationStatus enum"""

    def test_stack_set_operation_status_values(self):
        self.assertEqual(StackSetOperationStatus.RUNNING.value, "RUNNING")
        self.assertEqual(StackSetOperationStatus.SUCCEEDED.value, "SUCCEEDED")
        self.assertEqual(StackSetOperationStatus.FAILED.value, "FAILED")


class TestPermissionModel(unittest.TestCase):
    """Test PermissionModel enum"""

    def test_permission_model_values(self):
        self.assertEqual(PermissionModel.SERVICE_MANAGED.value, "SERVICE_MANAGED")
        self.assertEqual(PermissionModel.SELF_MANAGED.value, "SELF_MANAGED")


class TestCapability(unittest.TestCase):
    """Test Capability enum"""

    def test_capability_values(self):
        self.assertEqual(Capability.CAPABILITY_IAM.value, "CAPABILITY_IAM")
        self.assertEqual(Capability.CAPABILITY_NAMED_IAM.value, "CAPABILITY_NAMED_IAM")
        self.assertEqual(Capability.CAPABILITY_AUTO_EXPAND.value, "CAPABILITY_AUTO_EXPAND")


class TestOnFailure(unittest.TestCase):
    """Test OnFailure enum"""

    def test_on_failure_values(self):
        self.assertEqual(OnFailure.ROLLBACK.value, "ROLLBACK")
        self.assertEqual(OnFailure.DELETE.value, "DELETE")
        self.assertEqual(OnFailure.DO_NOTHING.value, "DO_NOTHING")


class TestCloudFormationConfig(unittest.TestCase):
    """Test CloudFormationConfig dataclass"""

    def test_config_defaults(self):
        config = CloudFormationConfig()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertTrue(config.verify_ssl)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)

    def test_config_custom(self):
        config = CloudFormationConfig(
            region_name="us-west-2",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            profile_name="test-profile"
        )
        self.assertEqual(config.region_name, "us-west-2")
        self.assertEqual(config.aws_access_key_id, "test-key")
        self.assertEqual(config.profile_name, "test-profile")


class TestStackConfig(unittest.TestCase):
    """Test StackConfig dataclass"""

    def test_stack_config_creation(self):
        config = StackConfig(
            stack_name="test-stack",
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}',
            parameters={"Param1": "Value1"},
            tags={"Environment": "Production"}
        )
        self.assertEqual(config.stack_name, "test-stack")
        self.assertEqual(config.parameters["Param1"], "Value1")
        self.assertEqual(config.tags["Environment"], "Production")

    def test_stack_config_with_capabilities(self):
        config = StackConfig(
            stack_name="test-stack",
            capabilities=[Capability.CAPABILITY_IAM, Capability.CAPABILITY_NAMED_IAM]
        )
        self.assertEqual(len(config.capabilities), 2)

    def test_stack_config_with_on_failure(self):
        config = StackConfig(
            stack_name="test-stack",
            on_failure=OnFailure.ROLLBACK
        )
        self.assertEqual(config.on_failure, OnFailure.ROLLBACK)


class TestStackInfo(unittest.TestCase):
    """Test StackInfo dataclass"""

    def test_stack_info_creation(self):
        stack = StackInfo(
            stack_id="arn:aws:cloudformation:us-east-1:123456789:stack/test-stack/id",
            stack_name="test-stack",
            stack_status="CREATE_COMPLETE"
        )
        self.assertEqual(stack.stack_name, "test-stack")
        self.assertEqual(stack.stack_status, "CREATE_COMPLETE")
        self.assertFalse(stack.enable_termination_protection)

    def test_stack_info_with_outputs(self):
        stack = StackInfo(
            stack_id="test-id",
            stack_name="test-stack",
            stack_status="CREATE_COMPLETE",
            outputs=[
                {"OutputKey": "Key1", "OutputValue": "Value1"},
                {"OutputKey": "Key2", "OutputValue": "Value2"}
            ]
        )
        self.assertEqual(len(stack.outputs), 2)


class TestChangeSetInfo(unittest.TestCase):
    """Test ChangeSetInfo dataclass"""

    def test_change_set_info_creation(self):
        cs = ChangeSetInfo(
            change_set_id="arn:aws:cloudformation:us-east-1:123456789:changeset/test-cs/id",
            change_set_name="test-cs",
            stack_id="stack-id",
            stack_name="test-stack",
            change_set_type="CREATE",
            status="CREATE_COMPLETE"
        )
        self.assertEqual(cs.change_set_name, "test-cs")
        self.assertEqual(cs.status, "CREATE_COMPLETE")


class TestStackResourceInfo(unittest.TestCase):
    """Test StackResourceInfo dataclass"""

    def test_stack_resource_info_creation(self):
        resource = StackResourceInfo(
            logical_resource_id="MyFunction",
            physical_resource_id="function-123",
            resource_type="AWS::Lambda::Function",
            resource_status="CREATE_COMPLETE"
        )
        self.assertEqual(resource.logical_resource_id, "MyFunction")
        self.assertEqual(resource.resource_type, "AWS::Lambda::Function")


class TestStackEventInfo(unittest.TestCase):
    """Test StackEventInfo dataclass"""

    def test_stack_event_info_creation(self):
        event = StackEventInfo(
            event_id="event-123",
            stack_name="test-stack",
            stack_id="stack-123",
            logical_resource_id="MyFunction",
            resource_type="AWS::Lambda::Function",
            resource_status="CREATE_COMPLETE"
        )
        self.assertEqual(event.logical_resource_id, "MyFunction")


class TestDriftDetectionResult(unittest.TestCase):
    """Test DriftDetectionResult dataclass"""

    def test_drift_detection_result_creation(self):
        result = DriftDetectionResult(
            stack_drift_status="DRIFTED",
            total_checks=10,
            drifted_stacks=2,
            in_sync_stacks=8
        )
        self.assertEqual(result.stack_drift_status, "DRIFTED")
        self.assertEqual(result.total_checks, 10)


class TestStackSetConfig(unittest.TestCase):
    """Test StackSetConfig dataclass"""

    def test_stack_set_config_creation(self):
        config = StackSetConfig(
            stack_set_name="test-stackset",
            description="Test stack set"
        )
        self.assertEqual(config.stack_set_name, "test-stackset")
        self.assertEqual(config.permission_model, PermissionModel.SELF_MANAGED)


class TestStackSetInfo(unittest.TestCase):
    """Test StackSetInfo dataclass"""

    def test_stack_set_info_creation(self):
        info = StackSetInfo(
            stack_set_id="stackset-id",
            stack_set_name="test-stackset",
            status="ACTIVE"
        )
        self.assertEqual(info.stack_set_name, "test-stackset")
        self.assertEqual(info.status, "ACTIVE")


class TestStackPolicyConfig(unittest.TestCase):
    """Test StackPolicyConfig dataclass"""

    def test_stack_policy_config_creation(self):
        config = StackPolicyConfig(
            stack_name="test-stack",
            stack_policy_body='{"Statement":[{"Effect":"Deny","Principal":"*","Action":"Update:Delete","Resource":"*"}]}'
        )
        self.assertEqual(config.stack_name, "test-stack")


class TestTemplateEstimate(unittest.TestCase):
    """Test TemplateEstimate dataclass"""

    def test_template_estimate_creation(self):
        estimate = TemplateEstimate(
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}',
            resources=[{"Type": "AWS::S3::Bucket"}],
            total_estimated_cost="$0.50 per month"
        )
        self.assertEqual(len(estimate.resources), 1)


class TestCustomResourceConfig(unittest.TestCase):
    """Test CustomResourceConfig dataclass"""

    def test_custom_resource_config_creation(self):
        config = CustomResourceConfig(
            type_name="Custom::MyResource",
            schema={"type": "object"}
        )
        self.assertEqual(config.type_name, "Custom::MyResource")


class TestExportInfo(unittest.TestCase):
    """Test ExportInfo dataclass"""

    def test_export_info_creation(self):
        export = ExportInfo(
            exporting_stack_id="stack-123",
            exporting_stack_name="export-stack",
            name="MyExport",
            value="export-value"
        )
        self.assertEqual(export.name, "MyExport")
        self.assertEqual(export.value, "export-value")


class TestImportInfo(unittest.TestCase):
    """Test ImportInfo dataclass"""

    def test_import_info_creation(self):
        imp = ImportInfo(
            import_id="import-123",
            import_status="COMPLETE",
            parent_stacks=["stack-1", "stack-2"],
            imported_resources=5
        )
        self.assertEqual(imp.import_status, "COMPLETE")
        self.assertEqual(len(imp.parent_stacks), 2)


class TestCloudFormationIntegration(unittest.TestCase):
    """Test CloudFormationIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_session = MagicMock()
        self.mock_cf_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_logs_client = MagicMock()
        self.mock_events_client = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_iam_client = MagicMock()

        mock_boto3.Session.return_value = self.mock_session
        self.mock_session.client.side_effect = [
            self.mock_cf_client,
            self.mock_cloudwatch_client,
            self.mock_logs_client,
            self.mock_events_client,
            self.mock_s3_client,
            self.mock_iam_client,
        ]

    def test_integration_initialization(self):
        """Test CloudFormationIntegration initialization"""
        integration = CloudFormationIntegration(
            region_name="us-east-1",
            profile_name="test-profile"
        )
        self.assertEqual(integration.region_name, "us-east-1")
        self.assertEqual(integration.profile_name, "test-profile")

    def test_cloudformation_client_property(self):
        """Test cloudformation client property"""
        integration = CloudFormationIntegration()
        # Client should be initialized after first access
        integration.cloudformation  # Access property

    def test_cloudwatch_client_property(self):
        """Test cloudwatch client property"""
        integration = CloudFormationIntegration()
        integration.cloudwatch  # Access property

    def test_logs_client_property(self):
        """Test logs client property"""
        integration = CloudFormationIntegration()
        integration.logs  # Access property

    def test_events_client_property(self):
        """Test events client property"""
        integration = CloudFormationIntegration()
        integration.events  # Access property

    def test_create_stack_success(self):
        """Test successful stack creation"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789:stack/test-stack/id"
        }

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "arn:aws:cloudformation:us-east-1:123456789:stack/test-stack/id",
                "StackName": "test-stack",
                "StackStatus": "CREATE_COMPLETE"
            }]
        }

        config = StackConfig(
            stack_name="test-stack",
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}'
        )

        stack = integration.create_stack(config)
        self.assertIsNotNone(stack)
        self.mock_cf_client.create_stack.assert_called_once()

    def test_create_stack_with_parameters(self):
        """Test stack creation with parameters"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:us-east-1:123456789:stack/test-stack/id"
        }

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "stack-id",
                "StackName": "test-stack",
                "StackStatus": "CREATE_COMPLETE",
                "Parameters": [
                    {"ParameterKey": "Param1", "ParameterValue": "Value1"}
                ]
            }]
        }

        config = StackConfig(
            stack_name="test-stack",
            parameters={"Param1": "Value1", "Param2": "Value2"}
        )

        stack = integration.create_stack(config)
        call_kwargs = self.mock_cf_client.create_stack.call_args[1]
        self.assertEqual(len(call_kwargs["Parameters"]), 2)

    def test_create_stack_with_tags(self):
        """Test stack creation with tags"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_stack.return_value = {
            "StackId": "stack-id"
        }

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "stack-id",
                "StackName": "test-stack",
                "StackStatus": "CREATE_COMPLETE"
            }]
        }

        config = StackConfig(
            stack_name="test-stack",
            tags={"Environment": "Production", "Application": "Test"}
        )

        integration.create_stack(config)
        call_kwargs = self.mock_cf_client.create_stack.call_args[1]
        self.assertEqual(len(call_kwargs["Tags"]), 2)

    def test_create_stack_with_capabilities(self):
        """Test stack creation with IAM capabilities"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_stack.return_value = {
            "StackId": "stack-id"
        }

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "stack-id",
                "StackName": "test-stack",
                "StackStatus": "CREATE_COMPLETE"
            }]
        }

        config = StackConfig(
            stack_name="test-stack",
            capabilities=[Capability.CAPABILITY_IAM, Capability.CAPABILITY_NAMED_IAM]
        )

        integration.create_stack(config)
        call_kwargs = self.mock_cf_client.create_stack.call_args[1]
        self.assertIn("CAPABILITY_IAM", call_kwargs["Capabilities"])

    def test_create_stack_with_on_failure(self):
        """Test stack creation with on_failure option"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_stack.return_value = {
            "StackId": "stack-id"
        }

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "stack-id",
                "StackName": "test-stack",
                "StackStatus": "CREATE_COMPLETE"
            }]
        }

        config = StackConfig(
            stack_name="test-stack",
            on_failure=OnFailure.DELETE
        )

        integration.create_stack(config)
        call_kwargs = self.mock_cf_client.create_stack.call_args[1]
        self.assertEqual(call_kwargs["OnFailure"], "DELETE")

    def test_delete_stack(self):
        """Test stack deletion"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.delete_stack.return_value = {}

        result = integration.delete_stack("test-stack")
        self.mock_cf_client.delete_stack.assert_called_once_with(StackName="test-stack")

    def test_describe_stacks(self):
        """Test describing stacks"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "stack-id",
                "StackName": "test-stack",
                "StackStatus": "CREATE_COMPLETE",
                "Outputs": [
                    {"OutputKey": "Key", "OutputValue": "Value"}
                ]
            }]
        }

        stacks = integration.describe_stacks("test-stack")
        self.assertEqual(len(stacks), 1)

    def test_list_stacks(self):
        """Test listing stacks"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [
                {"StackName": "stack-1", "StackStatus": "CREATE_COMPLETE"},
                {"StackName": "stack-2", "StackStatus": "UPDATE_COMPLETE"}
            ]
        }

        stacks = integration.list_stacks()
        self.assertEqual(len(stacks), 2)

    def test_update_stack(self):
        """Test stack update"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.update_stack.return_value = {
            "StackId": "stack-id"
        }

        self.mock_cf_client.describe_stacks.return_value = {
            "Stacks": [{
                "StackId": "stack-id",
                "StackName": "test-stack",
                "StackStatus": "UPDATE_COMPLETE"
            }]
        }

        config = StackConfig(
            stack_name="test-stack",
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}'
        )

        result = integration.update_stack(config)
        self.mock_cf_client.update_stack.assert_called_once()

    def test_get_stack_template(self):
        """Test getting stack template"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.get_template.return_value = {
            "TemplateBody": '{"AWSTemplateFormatVersion":"2010-09-09"}'
        }

        template = integration.get_stack_template("test-stack")
        self.assertIn("AWSTemplateFormatVersion", template)

    def test_validate_template(self):
        """Test template validation"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.validate_template.return_value = {
            "Parameters": [],
            "Description": "Valid template"
        }

        result = integration.validate_template(
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}'
        )
        self.assertIn("Description", result)

    def test_estimate_template_cost(self):
        """Test template cost estimation"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.estimate_template_cost.return_value = {
            "Url": "https://calculator.aws/costEstimate"
        }

        result = integration.estimate_template_cost(
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}'
        )
        self.assertIn("Url", result)

    def test_create_change_set(self):
        """Test change set creation"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_change_set.return_value = {
            "ChangeSetId": "cs-id",
            "StackId": "stack-id"
        }

        self.mock_cf_client.describe_change_sets.return_value = {
            "ChangeSets": [{
                "ChangeSetId": "cs-id",
                "ChangeSetName": "test-changeset",
                "Status": "CREATE_COMPLETE"
            }]
        }

        result = integration.create_change_set(
            stack_name="test-stack",
            change_set_name="test-changeset",
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}'
        )
        self.assertIsNotNone(result)

    def test_describe_change_sets(self):
        """Test describing change sets"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.list_change_sets.return_value = {
            "Summaries": [
                {"ChangeSetName": "cs-1", "Status": "CREATE_COMPLETE"},
                {"ChangeSetName": "cs-2", "Status": "FAILED"}
            ]
        }

        change_sets = integration.describe_change_sets("test-stack")
        self.assertEqual(len(change_sets), 2)

    def test_execute_change_set(self):
        """Test executing change set"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.execute_change_set.return_value = {}

        integration.execute_change_set(
            stack_name="test-stack",
            change_set_name="test-changeset"
        )
        self.mock_cf_client.execute_change_set.assert_called_once()

    def test_describe_stack_resources(self):
        """Test describing stack resources"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.list_stack_resources.return_value = {
            "StackResourceSummaries": [
                {
                    "LogicalResourceId": "MyFunction",
                    "PhysicalResourceId": "function-123",
                    "ResourceType": "AWS::Lambda::Function",
                    "ResourceStatus": "CREATE_COMPLETE"
                }
            ]
        }

        resources = integration.describe_stack_resources("test-stack")
        self.assertEqual(len(resources), 1)

    def test_get_stack_events(self):
        """Test getting stack events"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.describe_stack_events.return_value = {
            "StackEvents": [
                {
                    "EventId": "event-1",
                    "StackName": "test-stack",
                    "LogicalResourceId": "MyFunction",
                    "ResourceStatus": "CREATE_COMPLETE"
                }
            ]
        }

        events = integration.get_stack_events("test-stack")
        self.assertEqual(len(events), 1)

    def test_detect_stack_drift(self):
        """Test drift detection"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.detect_stack_drift.return_value = {
            "StackDriftDetectionId": "detection-id"
        }

        self.mock_cf_client.describe_stack_drift_detection_status.return_value = {
            "StackId": "stack-id",
            "StackDriftStatus": "DRIFTED"
        }

        result = integration.detect_stack_drift("test-stack")
        self.assertIsNotNone(result)

    def test_estimate_template_cost_with_params(self):
        """Test template cost estimation with parameters"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.estimate_template_cost.return_value = {
            "Url": "https://calculator.aws/costEstimate"
        }

        result = integration.estimate_template_cost(
            template_body='{"AWSTemplateFormatVersion":"2010-09-09"}',
            parameters=[{"ParameterKey": "Param1", "ParameterValue": "Value1"}]
        )
        self.assertIn("Url", result)

    def test_set_stack_policy(self):
        """Test setting stack policy"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.set_stack_policy.return_value = {}

        policy_body = '{"Statement":[{"Effect":"Deny","Principal":"*","Action":"Update:Delete","Resource":"*"}]}'
        integration.set_stack_policy("test-stack", stack_policy_body=policy_body)
        self.mock_cf_client.set_stack_policy.assert_called_once()

    def test_get_stack_policy(self):
        """Test getting stack policy"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.get_stack_policy.return_value = {
            "StackPolicyBody": '{"Statement":[]}'
        }

        policy = integration.get_stack_policy("test-stack")
        self.assertIsNotNone(policy)

    def test_list_exports(self):
        """Test listing exports"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.list_exports.return_value = {
            "Exports": [
                {
                    "ExportingStackId": "stack-1",
                    "ExportingStackName": "export-stack",
                    "Name": "MyExport",
                    "Value": "export-value"
                }
            ]
        }

        exports = integration.list_exports()
        self.assertEqual(len(exports), 1)
        self.assertEqual(exports[0].name, "MyExport")

    def test_list_imports(self):
        """Test listing imports"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.list_imports.return_value = {
            "Imports": ["importing-stack-1", "importing-stack-2"],
            "NextToken": None
        }

        imports = integration.list_imports("MyExport")
        self.assertEqual(len(imports), 2)

    def test_create_stack_set(self):
        """Test stack set creation"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.create_stack_set.return_value = {
            "StackSetId": "stackset-id"
        }

        self.mock_cf_client.describe_stack_set.return_value = {
            "StackSet": {
                "StackSetName": "test-stackset",
                "StackSetId": "stackset-id",
                "Status": "ACTIVE"
            }
        }

        config = StackSetConfig(
            stack_set_name="test-stackset",
            description="Test stack set"
        )

        result = integration.create_stack_set(config)
        self.assertIsNotNone(result)

    def test_delete_stack_set(self):
        """Test stack set deletion"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.delete_stack_set.return_value = {}

        integration.delete_stack_set("test-stackset")
        self.mock_cf_client.delete_stack_set.assert_called_once()

    def test_list_stack_sets(self):
        """Test listing stack sets"""
        integration = CloudFormationIntegration()

        self.mock_cf_client.list_stack_sets.return_value = {
            "Summaries": [
                {"StackSetName": "stackset-1", "Status": "ACTIVE"},
                {"StackSetName": "stackset-2", "Status": "DELETED"}
            ]
        }

        stack_sets = integration.list_stack_sets()
        self.assertEqual(len(stack_sets), 2)


class TestCloudFormationIntegrationNoBoto3(unittest.TestCase):
    """Test CloudFormationIntegration without boto3 available"""

    def test_initialization_without_boto3(self):
        """Test initialization when boto3 is not available"""
        # Create a fresh import without boto3
        import importlib
        import sys

        # Save original modules
        original_boto3 = sys.modules.get('boto3')
        original_botocore = sys.modules.get('botocore')
        original_exceptions = sys.modules.get('botocore.exceptions')

        try:
            # Remove boto3 to simulate not available
            if 'boto3' in sys.modules:
                del sys.modules['boto3']
            if 'botocore' in sys.modules:
                del sys.modules['botocore']
            if 'botocore.exceptions' in sys.modules:
                del sys.modules['botocore.exceptions']

            # Should handle gracefully
            integration = CloudFormationIntegration()
            # Client operations should fail appropriately
        except Exception:
            pass  # Expected - boto3 not available
        finally:
            # Restore modules
            if original_boto3:
                sys.modules['boto3'] = original_boto3
            if original_botocore:
                sys.modules['botocore'] = original_botocore
            if original_exceptions:
                sys.modules['botocore.exceptions'] = original_exceptions


if __name__ == '__main__':
    unittest.main()
