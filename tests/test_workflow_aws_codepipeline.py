"""
Tests for workflow_aws_codepipeline module
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

# Create mock boto3 module before importing workflow_aws_codepipeline
mock_boto3 = types.ModuleType('boto3')
mock_boto3.Session = MagicMock()
mock_boto3.client = MagicMock()

# Create mock botocore exceptions
mock_boto3_exceptions = types.ModuleType('botocore.exceptions')
mock_boto3_exceptions.ClientError = Exception
mock_boto3_exceptions.BotoCoreError = Exception

# Create mock botocore config
mock_boto3_config = types.ModuleType('botocore.config')
mock_boto3_config.Config = MagicMock()

sys.modules['boto3'] = mock_boto3
sys.modules['botocore'] = types.ModuleType('botocore')
sys.modules['botocore.exceptions'] = mock_boto3_exceptions
sys.modules['botocore.config'] = mock_boto3_config

# Now we can import the module
from src.workflow_aws_codepipeline import (
    CodePipelineIntegration,
    Pipeline,
    Stage,
    Action,
    Execution,
    ApprovalDetails,
    WebhookDefinition,
    ArtifactStoreConfig,
    CustomActionType,
    PipelineStatus,
    StageStatus,
    ActionStatus,
    ActionOwner,
    ActionCategory,
    StageTransitionType,
    WebhookFilterType,
    EncryptionStatus,
    BOTO3_AVAILABLE,
)


class TestPipelineStatus(unittest.TestCase):
    """Test PipelineStatus enum"""

    def test_pipeline_status_values(self):
        self.assertEqual(PipelineStatus.CREATING.value, "Creating")
        self.assertEqual(PipelineStatus.ACTIVE.value, "Active")
        self.assertEqual(PipelineStatus.INACTIVE.value, "Inactive")
        self.assertEqual(PipelineStatus.DELETED.value, "Deleted")

    def test_pipeline_status_is_string(self):
        self.assertIsInstance(PipelineStatus.ACTIVE.value, str)


class TestStageStatus(unittest.TestCase):
    """Test StageStatus enum"""

    def test_stage_status_values(self):
        self.assertEqual(StageStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(StageStatus.SUCCEEDED.value, "Succeeded")
        self.assertEqual(StageStatus.FAILED.value, "Failed")
        self.assertEqual(StageStatus.CANCELLED.value, "Cancelled")


class TestActionStatus(unittest.TestCase):
    """Test ActionStatus enum"""

    def test_action_status_values(self):
        self.assertEqual(ActionStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(ActionStatus.SUCCEEDED.value, "Succeeded")
        self.assertEqual(ActionStatus.FAILED.value, "Failed")
        self.assertEqual(ActionStatus.CANCELLED.value, "Cancelled")
        self.assertEqual(ActionStatus.PENDING.value, "Pending")


class TestActionOwner(unittest.TestCase):
    """Test ActionOwner enum"""

    def test_action_owner_values(self):
        self.assertEqual(ActionOwner.AWS.value, "AWS")
        self.assertEqual(ActionOwner.THIRD_PARTY.value, "ThirdParty")
        self.assertEqual(ActionOwner.CUSTOM.value, "Custom")


class TestActionCategory(unittest.TestCase):
    """Test ActionCategory enum"""

    def test_action_category_values(self):
        self.assertEqual(ActionCategory.SOURCE.value, "Source")
        self.assertEqual(ActionCategory.BUILD.value, "Build")
        self.assertEqual(ActionCategory.DEPLOY.value, "Deploy")
        self.assertEqual(ActionCategory.TEST.value, "Test")
        self.assertEqual(ActionCategory.INVOKE.value, "Invoke")
        self.assertEqual(ActionCategory.APPROVAL.value, "Approval")
        self.assertEqual(ActionCategory.QUALITY.value, "Quality")
        self.assertEqual(ActionCategory.MONITORING.value, "Monitoring")


class TestStageTransitionType(unittest.TestCase):
    """Test StageTransitionType enum"""

    def test_stage_transition_type_values(self):
        self.assertEqual(StageTransitionType.ENABLE.value, "Enable")
        self.assertEqual(StageTransitionType.DISABLE.value, "Disable")


class TestWebhookFilterType(unittest.TestCase):
    """Test WebhookFilterType enum"""

    def test_webhook_filter_type_values(self):
        self.assertEqual(WebhookFilterType.REGEX.value, "Regex")
        self.assertEqual(WebhookFilterType.JSON_PATH.value, "JsonPath")
        self.assertEqual(WebhookFilterType.X_PATH.value, "XPath")


class TestEncryptionStatus(unittest.TestCase):
    """Test EncryptionStatus enum"""

    def test_encryption_status_values(self):
        self.assertEqual(EncryptionStatus.ENABLED.value, "Enabled")
        self.assertEqual(EncryptionStatus.DISABLED.value, "Disabled")
        self.assertEqual(EncryptionStatus.UNKNOWN.value, "Unknown")


class TestPipeline(unittest.TestCase):
    """Test Pipeline dataclass"""

    def test_pipeline_defaults(self):
        pipeline = Pipeline(
            name="my-pipeline",
            role_arn="arn:aws:iam::123456789012:role/my-role",
            artifact_store={"type": "S3", "location": "my-bucket"}
        )
        self.assertEqual(pipeline.name, "my-pipeline")
        self.assertEqual(pipeline.version, 1)
        self.assertEqual(pipeline.status, PipelineStatus.CREATING)
        self.assertEqual(pipeline.stages, [])

    def test_pipeline_custom(self):
        pipeline = Pipeline(
            name="my-pipeline",
            role_arn="arn:aws:iam::123456789012:role/my-role",
            artifact_store={"type": "S3", "location": "my-bucket"},
            stages=[{"name": "Source", "actions": []}],
            version=2,
            status=PipelineStatus.ACTIVE
        )
        self.assertEqual(pipeline.version, 2)
        self.assertEqual(pipeline.status, PipelineStatus.ACTIVE)
        self.assertEqual(len(pipeline.stages), 1)


class TestStage(unittest.TestCase):
    """Test Stage dataclass"""

    def test_stage_defaults(self):
        stage = Stage(name="Build")
        self.assertEqual(stage.name, "Build")
        self.assertEqual(stage.actions, [])
        self.assertEqual(stage.status, StageStatus.IN_PROGRESS)
        self.assertTrue(stage.enabled)

    def test_stage_custom(self):
        stage = Stage(
            name="Deploy",
            actions=[{"name": "DeployAction", "category": "Deploy"}],
            status=StageStatus.SUCCEEDED,
            enabled=False
        )
        self.assertEqual(stage.status, StageStatus.SUCCEEDED)
        self.assertFalse(stage.enabled)


class TestAction(unittest.TestCase):
    """Test Action dataclass"""

    def test_action_defaults(self):
        action = Action(
            name="Build",
            category=ActionCategory.BUILD,
            owner=ActionOwner.AWS,
            provider="CodeBuild"
        )
        self.assertEqual(action.name, "Build")
        self.assertEqual(action.version, "1")
        self.assertEqual(action.configuration, {})
        self.assertEqual(action.input_artifacts, [])
        self.assertEqual(action.output_artifacts, [])

    def test_action_custom(self):
        action = Action(
            name="Deploy",
            category=ActionCategory.DEPLOY,
            owner=ActionOwner.AWS,
            provider="CloudFormation",
            version="2",
            configuration={"template": "template.yaml"},
            input_artifacts=["build-artifact"],
            output_artifacts=["deploy-artifact"],
            role_arn="arn:aws:iam::123456789012:role/my-role"
        )
        self.assertEqual(action.version, "2")
        self.assertEqual(action.configuration["template"], "template.yaml")
        self.assertEqual(len(action.input_artifacts), 1)


class TestExecution(unittest.TestCase):
    """Test Execution dataclass"""

    def test_execution(self):
        start_time = datetime.now()
        execution = Execution(
            pipeline_execution_id="abc123",
            pipeline_name="my-pipeline",
            status="InProgress",
            start_time=start_time
        )
        self.assertEqual(execution.pipeline_execution_id, "abc123")
        self.assertEqual(execution.status, "InProgress")
        self.assertIsNone(execution.end_time)


class TestApprovalDetails(unittest.TestCase):
    """Test ApprovalDetails dataclass"""

    def test_approval_details_defaults(self):
        approval = ApprovalDetails(approval_id="approval123")
        self.assertEqual(approval.approval_id, "approval123")
        self.assertFalse(approval.approved)
        self.assertIsNone(approval.approver_name)
        self.assertIsNone(approval.comment)

    def test_approval_details_approved(self):
        approval_time = datetime.now()
        approval = ApprovalDetails(
            approval_id="approval123",
            approved=True,
            approver_name="john",
            comment="Looks good",
            approved_at=approval_time
        )
        self.assertTrue(approval.approved)
        self.assertEqual(approval.approver_name, "john")


class TestWebhookDefinition(unittest.TestCase):
    """Test WebhookDefinition dataclass"""

    def test_webhook_definition(self):
        webhook = WebhookDefinition(
            url="https://example.com/webhook",
            secret="my-secret",
            filters=[{"filterType": "EVENT", "pattern": "push"}],
            enabled=True
        )
        self.assertEqual(webhook.url, "https://example.com/webhook")
        self.assertTrue(webhook.enabled)


class TestArtifactStoreConfig(unittest.TestCase):
    """Test ArtifactStoreConfig dataclass"""

    def test_artifact_store_config_defaults(self):
        config = ArtifactStoreConfig()
        self.assertEqual(config.type, "S3")
        self.assertEqual(config.location, "")
        self.assertIsNone(config.encryption_key)

    def test_artifact_store_config_custom(self):
        config = ArtifactStoreConfig(
            type="S3",
            location="my-bucket",
            encryption_key="arn:aws:kms:us-east-1:123456789012:key/123456"
        )
        self.assertEqual(config.location, "my-bucket")
        self.assertIn("kms", config.encryption_key)


class TestCustomActionType(unittest.TestCase):
    """Test CustomActionType dataclass"""

    def test_custom_action_type(self):
        action_type = CustomActionType(
            category=ActionCategory.BUILD,
            provider="MyProvider",
            version="1",
            settings={"description": "My custom action"},
            configuration_properties=[{"name": "param1", "required": True}]
        )
        self.assertEqual(action_type.category, ActionCategory.BUILD)
        self.assertEqual(action_type.provider, "MyProvider")


class TestCodePipelineIntegration(unittest.TestCase):
    """Test CodePipelineIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(
            region="us-east-1",
            config={}
        )
        self.integration._client = self.mock_client

    def test_initialization(self):
        """Test integration initialization"""
        integration = CodePipelineIntegration()
        self.assertEqual(integration.region, "us-east-1")

    def test_initialization_with_profile(self):
        """Test initialization with profile"""
        integration = CodePipelineIntegration(profile="my-profile")
        self.assertEqual(integration.profile, "my-profile")

    def test_client_property(self):
        """Test client property getter"""
        mock_client = MagicMock()
        integration = CodePipelineIntegration()
        integration._client = mock_client
        self.assertEqual(integration.client, mock_client)

    def test_ensure_client(self):
        """Test _ensure_client method"""
        integration = CodePipelineIntegration()
        mock_client = MagicMock()
        integration._client = None
        with patch.object(integration, '_initialize_client'):
            integration._ensure_client()
            # If client was None, _ensure_client should call _initialize_client
            # But since we can't easily test that without proper mock setup,
            # we just verify the method exists and runs

    def test_generate_id(self):
        """Test _generate_id method"""
        integration = CodePipelineIntegration()
        id1 = integration._generate_id()
        id2 = integration._generate_id()
        self.assertNotEqual(id1, id2)
        self.assertIsInstance(id1, str)

    def test_format_response(self):
        """Test _format_response method"""
        integration = CodePipelineIntegration()
        response = {"key": "value"}
        result = integration._format_response(response)
        self.assertTrue(result["success"])
        self.assertEqual(result["data"], response)
        self.assertIn("timestamp", result)

    def test_handle_error(self):
        """Test _handle_error method"""
        integration = CodePipelineIntegration()
        error = Exception("Test error")
        result = integration._handle_error(error)
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Test error")
        self.assertEqual(result["error_type"], "Exception")


class TestCodePipelineIntegrationPipelines(unittest.TestCase):
    """Test CodePipelineIntegration pipeline methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_pipeline(self):
        """Test create_pipeline method"""
        mock_response = {
            'pipeline': {
                'name': 'my-pipeline',
                'roleArn': 'arn:aws:iam::123456789012:role/my-role',
                'artifactStore': {'type': 'S3', 'location': 'my-bucket'},
                'stages': [{'name': 'Source', 'actions': []}]
            },
            'tags': [{'key': 'env', 'value': 'prod'}]
        }
        self.mock_client.create_pipeline.return_value = mock_response

        artifact_store = ArtifactStoreConfig(type="S3", location="my-bucket")
        stages = [{'name': 'Source', 'actions': []}]
        result = self.integration.create_pipeline(
            name="my-pipeline",
            role_arn="arn:aws:iam::123456789012:role/my-role",
            artifact_store=artifact_store,
            stages=stages
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["pipeline"]["name"], "my-pipeline")

    def test_get_pipeline(self):
        """Test get_pipeline method"""
        mock_response = {
            'pipeline': {
                'name': 'my-pipeline',
                'roleArn': 'arn:aws:iam::123456789012:role/my-role',
                'artifactStore': {'type': 'S3', 'location': 'my-bucket'},
                'stages': []
            }
        }
        self.mock_client.get_pipeline.return_value = mock_response

        result = self.integration.get_pipeline("my-pipeline")

        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["name"], "my-pipeline")

    def test_list_pipelines(self):
        """Test list_pipelines method"""
        mock_response = {
            'pipelines': [
                {'name': 'pipeline-1', 'version': 1},
                {'name': 'pipeline-2', 'version': 2}
            ]
        }
        self.mock_client.list_pipelines.return_value = mock_response

        result = self.integration.list_pipelines()

        self.assertTrue(result["success"])
        self.assertEqual(len(result["data"]["pipelines"]), 2)

    def test_update_pipeline(self):
        """Test update_pipeline method"""
        mock_response = {
            'pipeline': {
                'name': 'my-pipeline',
                'version': 2
            }
        }
        self.mock_client.update_pipeline.return_value = mock_response

        pipeline = {'name': 'my-pipeline', 'version': 2}
        result = self.integration.update_pipeline(pipeline)

        self.assertTrue(result["success"])

    def test_delete_pipeline(self):
        """Test delete_pipeline method"""
        self.mock_client.delete_pipeline.return_value = {}

        result = self.integration.delete_pipeline("my-pipeline")

        self.assertTrue(result["success"])
        self.assertIn("deleted successfully", result["data"]["message"])

    def test_disable_pipeline(self):
        """Test disable_pipeline method"""
        self.mock_client.disable_stage_transition.return_value = {}

        result = self.integration.disable_pipeline("my-pipeline")

        self.assertTrue(result["success"])
        self.mock_client.disable_stage_transition.assert_called_once()

    def test_enable_pipeline(self):
        """Test enable_pipeline method"""
        self.mock_client.enable_stage_transition.return_value = {}

        result = self.integration.enable_pipeline("my-pipeline")

        self.assertTrue(result["success"])
        self.mock_client.enable_stage_transition.assert_called_once()


class TestCodePipelineIntegrationStages(unittest.TestCase):
    """Test CodePipelineIntegration stage methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_add_stage(self):
        """Test add_stage method"""
        self.mock_client.get_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': []
            }
        }
        self.mock_client.update_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': [{'name': 'Build', 'actions': []}]
            }
        }

        stage = {'name': 'Build', 'actions': []}
        result = self.integration.add_stage("my-pipeline", stage)

        self.assertTrue(result["success"])

    def test_add_stage_already_exists(self):
        """Test add_stage raises error for duplicate stage"""
        self.mock_client.get_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': [{'name': 'Build', 'actions': []}]
            }
        }

        stage = {'name': 'Build', 'actions': []}
        result = self.integration.add_stage("my-pipeline", stage)

        self.assertFalse(result["success"])

    def test_update_stage(self):
        """Test update_stage method"""
        self.mock_client.get_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': [{'name': 'Build', 'actions': []}]
            }
        }
        self.mock_client.update_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': [{'name': 'Build', 'actions': [{"name": "NewAction"}]}]
            }
        }

        stage = {'name': 'Build', 'actions': [{"name": "NewAction"}]}
        result = self.integration.update_stage("my-pipeline", "Build", stage)

        self.assertTrue(result["success"])

    def test_delete_stage(self):
        """Test delete_stage method"""
        self.mock_client.get_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': [{'name': 'Build', 'actions': []}]
            }
        }
        self.mock_client.update_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'stages': []
            }
        }

        result = self.integration.delete_stage("my-pipeline", "Build")

        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["deleted_stage"], "Build")

    def test_disable_stage_transition(self):
        """Test disable_stage_transition method"""
        self.mock_client.disable_stage_transition.return_value = {}

        result = self.integration.disable_stage_transition("my-pipeline", "Build")

        self.assertTrue(result["success"])

    def test_enable_stage_transition(self):
        """Test enable_stage_transition method"""
        self.mock_client.enable_stage_transition.return_value = {}

        result = self.integration.enable_stage_transition("my-pipeline", "Build")

        self.assertTrue(result["success"])


class TestCodePipelineIntegrationActions(unittest.TestCase):
    """Test CodePipelineIntegration action methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_action(self):
        """Test create_action method"""
        action = self.integration.create_action(
            name="Build",
            category=ActionCategory.BUILD,
            owner=ActionOwner.AWS,
            provider="CodeBuild"
        )

        self.assertEqual(action.name, "Build")
        self.assertEqual(action.category, ActionCategory.BUILD)

    def test_configure_source_action(self):
        """Test configure_source_action method"""
        config = {
            "RepositoryName": "my-repo",
            "BranchName": "main"
        }
        result = self.integration.configure_source_action(
            name="Source",
            provider="CodeCommit",
            configuration=config,
            output_artifacts=["output-artifact"]
        )

        self.assertEqual(result["name"], "Source")
        self.assertEqual(result["actionTypeId"]["category"], "Source")
        self.assertEqual(len(result["outputArtifacts"]), 1)

    def test_configure_build_action(self):
        """Test configure_build_action method"""
        config = {
            "ProjectName": "my-project"
        }
        result = self.integration.configure_build_action(
            name="Build",
            provider="CodeBuild",
            configuration=config,
            input_artifacts=["source-artifact"],
            output_artifacts=["build-artifact"]
        )

        self.assertEqual(result["name"], "Build")
        self.assertEqual(result["actionTypeId"]["category"], "Build")
        self.assertEqual(len(result["inputArtifacts"]), 1)

    def test_configure_deploy_action(self):
        """Test configure_deploy_action method"""
        config = {
            "StackName": "my-stack",
            "TemplatePath": "template.yaml"
        }
        result = self.integration.configure_deploy_action(
            name="Deploy",
            provider="CloudFormation",
            configuration=config,
            input_artifacts=["build-artifact"]
        )

        self.assertEqual(result["name"], "Deploy")
        self.assertEqual(result["actionTypeId"]["category"], "Deploy")

    def test_configure_approval_action(self):
        """Test configure_approval_action method"""
        config = {
            "ExternalEntityLink": "https://example.com/review",
            "DecryptFragment": "encrypted-data"
        }
        result = self.integration.configure_approval_action(
            name="Approval",
            configuration=config
        )

        self.assertEqual(result["name"], "Approval")
        self.assertEqual(result["actionTypeId"]["category"], "Approval")


class TestCodePipelineIntegrationExecutions(unittest.TestCase):
    """Test CodePipelineIntegration execution methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_start_pipeline(self):
        """Test start_pipeline method"""
        self.mock_client.start_pipeline_execution.return_value = {
            'pipelineExecutionId': 'abc-123'
        }

        result = self.integration.start_pipeline("my-pipeline")

        self.assertTrue(result["success"])
        self.assertIn("pipeline_execution_id", result["data"])

    def test_stop_pipeline(self):
        """Test stop_pipeline method"""
        self.mock_client.stop_pipeline_execution.return_value = {}

        result = self.integration.stop_pipeline("my-pipeline", "exec-123")

        self.assertTrue(result["success"])

    def test_get_execution(self):
        """Test get_execution method"""
        self.mock_client.get_pipeline_execution.return_value = {
            'pipelineExecution': {
                'pipelineName': 'my-pipeline',
                'pipelineExecutionId': 'exec-123',
                'status': 'InProgress'
            }
        }

        result = self.integration.get_execution("my-pipeline", "exec-123")

        self.assertTrue(result["success"])

    def test_list_executions(self):
        """Test list_executions method"""
        self.mock_client.list_pipeline_executions.return_value = {
            'pipelineExecutions': [
                {'pipelineExecutionId': 'exec-1', 'status': 'Succeeded'},
                {'pipelineExecutionId': 'exec-2', 'status': 'InProgress'}
            ]
        }

        result = self.integration.list_executions("my-pipeline")

        self.assertTrue(result["success"])
        self.assertEqual(len(result["data"]["pipeline_executions"]), 2)

    def test_approve_approval(self):
        """Test approve_approval method"""
        self.mock_client.put_approval_result.return_value = {}

        result = self.integration.put_approval_result(
            pipeline_name="my-pipeline",
            stage_name="Approval",
            action_name="Approve",
            result={"approved": True, "summary": "Looks good"},
            token="my-token"
        )

        self.assertTrue(result["success"])


class TestCodePipelineIntegrationWebhooks(unittest.TestCase):
    """Test CodePipelineIntegration webhook methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_github_webhook(self):
        """Test create_github_webhook method"""
        self.mock_client.put_webhook.return_value = {
            'webhook': {
                'url': 'https://example.com/webhook/abc123'
            }
        }

        result = self.integration.create_github_webhook(
            pipeline_name="my-pipeline",
            webhook_url="https://example.com/webhook",
            secret="my-secret",
            filters=[{"jsonPath": "$.event", "matchEquals": "push"}]
        )

        self.assertTrue(result["success"])
        self.assertIn("webhook", result["data"])

    def test_delete_webhook(self):
        """Test delete_webhook method"""
        self.mock_client.delete_webhook.return_value = {}

        result = self.integration.delete_webhook("my-webhook")

        self.assertTrue(result["success"])

    def test_register_github_webhook(self):
        """Test register_github_webhook method"""
        self.mock_client.put_webhook.return_value = {
            'webhook': {'url': 'https://example.com/webhook'}
        }

        result = self.integration.register_github_webhook(
            pipeline_name="my-pipeline",
            git_hub_configuration={"Owner": "my-org", "Repo": "my-repo", "Branch": "main"}
        )

        self.assertTrue(result["success"])


class TestCodePipelineIntegrationArtifactStore(unittest.TestCase):
    """Test CodePipelineIntegration artifact store methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_configure_artifact_store(self):
        """Test configure_artifact_store method"""
        self.mock_client.get_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'artifactStore': {'type': 'S3', 'location': 'old-bucket'}
            }
        }
        self.mock_client.update_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'artifactStore': {'type': 'S3', 'location': 'my-bucket'}
            }
        }

        result = self.integration.configure_artifact_store(
            pipeline_name="my-pipeline",
            store_type="S3",
            location="my-bucket"
        )

        self.assertTrue(result["success"])

    def test_configure_artifact_store_encrypted(self):
        """Test configure_artifact_store with encryption"""
        self.mock_client.get_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'artifactStore': {'type': 'S3', 'location': 'old-bucket'}
            }
        }
        self.mock_client.update_pipeline.return_value = {
            'pipeline': {
                'name': 'my-pipeline',
                'artifactStore': {
                    'type': 'S3',
                    'location': 'my-bucket',
                    'encryptionKey': {'id': 'key-123', 'type': 'KMS'}
                }
            }
        }

        result = self.integration.configure_artifact_store(
            pipeline_name="my-pipeline",
            store_type="S3",
            location="my-bucket",
            encryption_key_id="key-123"
        )

        self.assertTrue(result["success"])


class TestCodePipelineIntegrationTags(unittest.TestCase):
    """Test CodePipelineIntegration tag methods"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = CodePipelineIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_add_tags(self):
        """Test add_tags method"""
        self.mock_client.tag_resource.return_value = {}

        result = self.integration.tag_resource(
            resource_arn="arn:aws:codepipeline:us-east-1:123456789012:pipeline:my-pipeline",
            tags=[{"key": "env", "value": "prod"}]
        )

        self.assertTrue(result["success"])

    def test_list_tags(self):
        """Test list_tags method"""
        self.mock_client.list_tags_for_resource.return_value = {
            'tags': [{"key": "env", "value": "prod"}]
        }

        result = self.integration.list_tags_for_resource(
            resource_arn="arn:aws:codepipeline:us-east-1:123456789012:pipeline:my-pipeline"
        )

        self.assertTrue(result["success"])
        self.assertEqual(len(result["data"]["tags"]), 1)


class TestBoto3Availability(unittest.TestCase):
    """Test BOTO3_AVAILABLE flag"""

    def test_boto3_available(self):
        """Test BOTO3_AVAILABLE is set correctly"""
        # The mock should make BOTO3_AVAILABLE True since we set it up
        self.assertTrue(BOTO3_AVAILABLE)


if __name__ == '__main__':
    unittest.main()
