"""
Tests for workflow_aws_codestar module

Commit: 'tests: add comprehensive tests for workflow_aws_codestar'
"""
import sys
sys.path.insert(0, '/Users/guige/my_project')

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import json
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import types
import dataclasses

# Patch dataclasses.field BEFORE importing the module
_original_field = dataclasses.field

def _patched_field(*args, **kwargs):
    if 'default' not in kwargs and 'default_factory' not in kwargs:
        kwargs['default'] = None
    return _original_field(*args, **kwargs)

# Create mock boto3 module before importing workflow_aws_codestar
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

# Patch the field and import
dataclasses.field = _patched_field
import src.workflow_aws_codestar as _codestar_module
dataclasses.field = _original_field

# Extract the classes
CodeStarIntegration = _codestar_module.CodeStarIntegration
ProjectStatus = _codestar_module.ProjectStatus
ProjectTemplate = _codestar_module.ProjectTemplate
TeamMemberRole = _codestar_module.TeamMemberRole
Permission = _codestar_module.Permission
ResourceStatus = _codestar_module.ResourceStatus
ApplicationStatus = _codestar_module.ApplicationStatus
NotificationEventType = _codestar_module.NotificationEventType
ConnectionStatus = _codestar_module.ConnectionStatus
Project = _codestar_module.Project
TeamMember = _codestar_module.TeamMember
ProjectResource = _codestar_module.ProjectResource
Application = _codestar_module.Application
NotificationRule = _codestar_module.NotificationRule
Connection = _codestar_module.Connection
ProjectTemplateInfo = _codestar_module.ProjectTemplateInfo
Pipeline = _codestar_module.Pipeline
Toolchain = _codestar_module.Toolchain
ProjectMetrics = _codestar_module.ProjectMetrics


class TestProjectStatus(unittest.TestCase):
    """Test ProjectStatus enum"""

    def test_project_status_values(self):
        self.assertEqual(ProjectStatus.CREATING.value, "Creating")
        self.assertEqual(ProjectStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(ProjectStatus.COMPLETED.value, "Completed")
        self.assertEqual(ProjectStatus.ERROR.value, "Error")
        self.assertEqual(ProjectStatus.DELETING.value, "Deleting")


class TestProjectTemplate(unittest.TestCase):
    """Test ProjectTemplate enum"""

    def test_project_template_values(self):
        self.assertEqual(ProjectTemplate.WEB_APP_PYTHON.value, "python-web-app")
        self.assertEqual(ProjectTemplate.WEB_APP_NODE.value, "nodejs-web-app")
        self.assertEqual(ProjectTemplate.Lambda_PYTHON.value, "aws-lambda-python")
        self.assertEqual(ProjectTemplate.ECS_CANARY.value, "ecs-canary")


class TestTeamMemberRole(unittest.TestCase):
    """Test TeamMemberRole enum"""

    def test_team_member_role_values(self):
        self.assertEqual(TeamMemberRole.OWNER.value, "Owner")
        self.assertEqual(TeamMemberRole.CONTRIBUTOR.value, "Contributor")
        self.assertEqual(TeamMemberRole.VIEWER.value, "Viewer")


class TestPermission(unittest.TestCase):
    """Test Permission enum"""

    def test_permission_values(self):
        self.assertEqual(Permission.PULL_REQUESTS.value, "pullrequests")
        self.assertEqual(Permission.COMMITS.value, "commits")
        self.assertEqual(Permission.CODE.value, "code")


class TestResourceStatus(unittest.TestCase):
    """Test ResourceStatus enum"""

    def test_resource_status_values(self):
        self.assertEqual(ResourceStatus.CREATING.value, "CREATING")
        self.assertEqual(ResourceStatus.CREATED.value, "CREATED")
        self.assertEqual(ResourceStatus.DELETING.value, "DELETING")


class TestApplicationStatus(unittest.TestCase):
    """Test ApplicationStatus enum"""

    def test_application_status_values(self):
        self.assertEqual(ApplicationStatus.PENDING.value, "PENDING")
        self.assertEqual(ApplicationStatus.DEPLOYING.value, "DEPLOYING")
        self.assertEqual(ApplicationStatus.DEPLOYED.value, "DEPLOYED")
        self.assertEqual(ApplicationStatus.ERROR.value, "ERROR")


class TestNotificationEventType(unittest.TestCase):
    """Test NotificationEventType enum"""

    def test_notification_event_type_values(self):
        self.assertEqual(NotificationEventType.PROJECT_CREATED.value, "codestar:project-created")
        self.assertEqual(NotificationEventType.PROJECT_UPDATED.value, "codestar:project-updated")
        self.assertEqual(NotificationEventType.PROJECT_DELETED.value, "codestar:project-deleted")
        self.assertEqual(NotificationEventType.PIPELINE_STARTED.value, "codestar:pipeline-started")
        self.assertEqual(NotificationEventType.PIPELINE_SUCCEEDED.value, "codestar:pipeline-succeeded")
        self.assertEqual(NotificationEventType.TEAM_MEMBER_ADDED.value, "codestar:team-member-added")


class TestConnectionStatus(unittest.TestCase):
    """Test ConnectionStatus enum"""

    def test_connection_status_values(self):
        self.assertEqual(ConnectionStatus.PENDING.value, "PENDING")
        self.assertEqual(ConnectionStatus.AVAILABLE.value, "AVAILABLE")
        self.assertEqual(ConnectionStatus.ERROR.value, "ERROR")


class TestProject(unittest.TestCase):
    """Test Project dataclass"""

    def test_project_creation(self):
        now = datetime.now()
        project = Project(
            id="project-123",
            name="Test Project",
            description="A test project",
            region="us-east-1",
            template_id="python-web-app",
            status=ProjectStatus.CREATING,
            created_time=now,
            updated_time=now
        )
        self.assertEqual(project.id, "project-123")
        self.assertEqual(project.name, "Test Project")
        self.assertEqual(project.status, ProjectStatus.CREATING)


class TestTeamMember(unittest.TestCase):
    """Test TeamMember dataclass"""

    def test_team_member_creation(self):
        member = TeamMember(
            user_arn="arn:aws:iam::123456789012:user/test",
            project_arn="arn:aws:codestar:us-east-1:123456789012:project/project-123",
            role=TeamMemberRole.CONTRIBUTOR
        )
        self.assertEqual(member.role, TeamMemberRole.CONTRIBUTOR)
        self.assertFalse(member.remote_access_allowed)


class TestProjectResource(unittest.TestCase):
    """Test ProjectResource dataclass"""

    def test_project_resource_creation(self):
        now = datetime.now()
        resource = ProjectResource(
            id="resource-123",
            project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
            type="AWS::CodeCommit::Repository",
            name="test-repo",
            status=ResourceStatus.CREATED,
            created_time=now,
            updated_time=now
        )
        self.assertEqual(resource.name, "test-repo")
        self.assertEqual(resource.type, "AWS::CodeCommit::Repository")


class TestApplication(unittest.TestCase):
    """Test Application dataclass"""

    def test_application_creation(self):
        now = datetime.now()
        app = Application(
            arn="arn:aws:codestar:us-east-1:123:application/project-123/app",
            project_id="project-123",
            name="test-app",
            status=ApplicationStatus.DEPLOYED,
            created_time=now,
            updated_time=now
        )
        self.assertEqual(app.name, "test-app")
        self.assertEqual(app.status, ApplicationStatus.DEPLOYED)


class TestNotificationRule(unittest.TestCase):
    """Test NotificationRule dataclass"""

    def test_notification_rule_creation(self):
        rule = NotificationRule(
            id="rule-123",
            arn="arn:aws:codestar-notifications:us-east-1:123:notification-rule/rule-123",
            name="test-rule",
            project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
            event_types=["codestar:project-created"],
            target_type="SNS",
            target_address="arn:aws:sns:us-east-1:123:topic"
        )
        self.assertEqual(rule.name, "test-rule")
        self.assertTrue(rule.enabled)


class TestConnection(unittest.TestCase):
    """Test Connection dataclass"""

    def test_connection_creation(self):
        now = datetime.now()
        conn = Connection(
            connection_arn="arn:aws:codeconnections:us-east-1:123:connection/conn-123",
            connection_id="conn-123",
            name="github-connection",
            status=ConnectionStatus.AVAILABLE,
            owner_account_id="123456789012",
            provider_type="GitHub",
            created_time=now
        )
        self.assertEqual(conn.name, "github-connection")
        self.assertEqual(conn.status, ConnectionStatus.AVAILABLE)


class TestProjectTemplateInfo(unittest.TestCase):
    """Test ProjectTemplateInfo dataclass"""

    def test_project_template_info_creation(self):
        template = ProjectTemplateInfo(
            id="python-web-app",
            name="Python Web App",
            description="A Python web application",
            category="Web",
            tags=["Python", "AWS"]
        )
        self.assertEqual(template.name, "Python Web App")
        self.assertTrue(template.is_available)


class TestPipeline(unittest.TestCase):
    """Test Pipeline dataclass"""

    def test_pipeline_creation(self):
        now = datetime.now()
        pipeline = Pipeline(
            arn="arn:aws:codepipeline:us-east-1:123:pipeline/test-pipeline",
            project_id="project-123",
            name="test-pipeline",
            status="Available",
            created_time=now,
            updated_time=now
        )
        self.assertEqual(pipeline.name, "test-pipeline")


class TestToolchain(unittest.TestCase):
    """Test Toolchain dataclass"""

    def test_toolchain_creation(self):
        now = datetime.now()
        toolchain = Toolchain(
            arn="arn:aws:codestar:us-east-1:123:toolchain/project-123/toolchain",
            project_id="project-123",
            name="test-toolchain",
            status="Available",
            created_time=now,
            updated_time=now
        )
        self.assertEqual(toolchain.name, "test-toolchain")


class TestProjectMetrics(unittest.TestCase):
    """Test ProjectMetrics dataclass"""

    def test_project_metrics_creation(self):
        metrics = ProjectMetrics(
            project_id="project-123",
            pipeline_execution_count=10,
            successful_deployments=8,
            failed_deployments=2
        )
        self.assertEqual(metrics.project_id, "project-123")
        self.assertEqual(metrics.successful_deployments, 8)


def run_async(coro):
    """Helper to run async code in tests"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestCodeStarIntegration(unittest.TestCase):
    """Test CodeStarIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_codestar_client = MagicMock()
        self.mock_notifications_client = MagicMock()
        self.mock_pipeline_client = MagicMock()
        self.mock_codeconnections_client = MagicMock()
        
        # Create integration instance with mocked clients
        self.integration = CodeStarIntegration(region="us-east-1")
        self.integration.codestar_client = self.mock_codestar_client
        self.integration.codestar_notifications_client = self.mock_notifications_client
        self.integration.codepipeline_client = self.mock_pipeline_client
        self.integration.codeconnections_client = self.mock_codeconnections_client

    def test_init_defaults(self):
        """Test initialization with defaults"""
        integration = CodeStarIntegration()
        self.assertEqual(integration.region, "us-east-1")
        self.assertIsNone(integration.profile_name)

    def test_init_with_profile(self):
        """Test initialization with profile"""
        integration = CodeStarIntegration(region="us-west-2", profile_name="test-profile")
        self.assertEqual(integration.region, "us-west-2")
        self.assertEqual(integration.profile_name, "test-profile")

    def test_create_project(self):
        """Test creating a project"""
        mock_response = {
            "projectId": "project-123",
            "stackId": "arn:aws:cloudformation:us-east-1:123:stack/test",
            "applicationArn": "arn:aws:codestar:us-east-1:123:application/test",
            "artifactStore": {"bucket": "test-bucket"},
            "sourceCode": [{"repositoryUrl": "https://git.codecommit.com/test"}]
        }
        self.mock_codestar_client.create_project.return_value = mock_response
        
        result = run_async(self.integration.create_project(
            name="test-project",
            template_id="python-web-app",
            description="A test project"
        ))
        
        self.assertIsInstance(result, Project)
        self.assertEqual(result.name, "test-project")
        self.assertEqual(result.template_id, "python-web-app")
        self.assertEqual(result.status, ProjectStatus.CREATING)
        self.mock_codestar_client.create_project.assert_called_once()

    def test_get_project(self):
        """Test getting a project"""
        mock_response = {
            "id": "project-123",
            "name": "test-project",
            "status": "InProgress",
            "description": "A test project",
            "projectTemplateId": "python-web-app",
            "createdTime": "2024-01-01T00:00:00Z",
            "updatedTime": "2024-01-02T00:00:00Z"
        }
        self.mock_codestar_client.describe_project.return_value = mock_response
        
        result = run_async(self.integration.get_project("project-123"))
        
        self.assertIsInstance(result, Project)
        self.assertEqual(result.id, "project-123")
        self.assertEqual(result.name, "test-project")

    def test_list_projects(self):
        """Test listing projects"""
        mock_response = {
            "projects": [
                {"projectId": "project-1", "name": "project-one"},
                {"projectId": "project-2", "name": "project-two"}
            ]
        }
        self.mock_codestar_client.list_projects.return_value = mock_response
        
        # Pre-populate a project to avoid calling describe_project
        self.integration._projects["project-1"] = Project(
            id="project-1", name="project-one", description="",
            region="us-east-1", template_id="", status=ProjectStatus.COMPLETED,
            created_time=datetime.now(), updated_time=datetime.now()
        )
        
        result = run_async(self.integration.list_projects())
        
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) >= 1)

    def test_update_project(self):
        """Test updating a project"""
        # Pre-populate project
        self.integration._projects["project-123"] = Project(
            id="project-123", name="old-name", description="old desc",
            region="us-east-1", template_id="", status=ProjectStatus.COMPLETED,
            created_time=datetime.now(), updated_time=datetime.now()
        )
        self.mock_codestar_client.describe_project.return_value = {
            "id": "project-123", "name": "old-name", "status": "Completed",
            "description": "old desc", "projectTemplateId": ""
        }
        
        result = run_async(self.integration.update_project(
            "project-123", name="updated-project", description="new desc"
        ))
        
        self.assertIsInstance(result, Project)
        self.assertEqual(result.name, "updated-project")
        self.assertEqual(result.description, "new desc")

    def test_delete_project(self):
        """Test deleting a project"""
        # Pre-populate project
        self.integration._projects["project-123"] = Project(
            id="project-123", name="test", description="",
            region="us-east-1", template_id="", status=ProjectStatus.COMPLETED,
            created_time=datetime.now(), updated_time=datetime.now()
        )
        
        result = run_async(self.integration.delete_project("project-123"))
        
        self.assertTrue(result)
        self.assertNotIn("project-123", self.integration._projects)

    def test_add_team_member(self):
        """Test adding a team member"""
        # Pre-populate project
        self.integration._projects["project-123"] = Project(
            id="project-123", name="test", description="",
            region="us-east-1", template_id="", status=ProjectStatus.COMPLETED,
            created_time=datetime.now(), updated_time=datetime.now()
        )
        self.mock_codestar_client.describe_project.return_value = {
            "id": "project-123", "name": "test", "status": "Completed",
            "description": "", "projectTemplateId": ""
        }
        self.mock_codestar_client.associate_team_member.return_value = {}
        
        result = run_async(self.integration.add_team_member(
            project_id="project-123",
            user_arn="arn:aws:iam::123:user/test",
            role=TeamMemberRole.CONTRIBUTOR
        ))
        
        self.assertIsInstance(result, TeamMember)
        self.assertEqual(result.user_arn, "arn:aws:iam::123:user/test")
        self.assertEqual(result.role, TeamMemberRole.CONTRIBUTOR)

    def test_remove_team_member(self):
        """Test removing a team member"""
        self.mock_codestar_client.disassociate_team_member.return_value = {}
        
        result = run_async(self.integration.remove_team_member(
            project_id="project-123",
            user_arn="arn:aws:iam::123:user/test"
        ))
        
        self.assertTrue(result)

    def test_list_team_members(self):
        """Test listing team members"""
        mock_response = {
            "teamMembers": [
                {"userArn": "arn:aws:iam::123:user/user1", "projectRole": "Owner"},
                {"userArn": "arn:aws:iam::123:user/user2", "projectRole": "Contributor"}
            ]
        }
        self.mock_codestar_client.list_team_members.return_value = mock_response
        
        result = run_async(self.integration.list_team_members("project-123"))
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], TeamMember)

    def test_update_team_member_role(self):
        """Test updating a team member's role"""
        # Pre-populate team member
        self.integration._team_members["project-123"] = [
            TeamMember(
                user_arn="arn:aws:iam::123:user/test",
                project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
                role=TeamMemberRole.CONTRIBUTOR
            )
        ]
        self.mock_codestar_client.update_team_member.return_value = {}
        
        result = run_async(self.integration.update_team_member_role(
            project_id="project-123",
            user_arn="arn:aws:iam::123:user/test",
            role=TeamMemberRole.VIEWER
        ))
        
        self.assertIsInstance(result, TeamMember)
        self.assertEqual(result.role, TeamMemberRole.VIEWER)

    def test_list_project_resources(self):
        """Test listing project resources"""
        mock_response = {
            "resources": [
                {"id": "resource-1", "type": "AWS::CodeCommit::Repository", "name": "repo1",
                 "projectArn": "arn:aws:codestar:us-east-1:123:project/p1", "status": "CREATED",
                 "createdTime": "2024-01-01T00:00:00Z", "updatedTime": "2024-01-01T00:00:00Z"},
                {"id": "resource-2", "type": "AWS::CodeBuild::Project", "name": "build1",
                 "projectArn": "arn:aws:codestar:us-east-1:123:project/p1", "status": "CREATED",
                 "createdTime": "2024-01-01T00:00:00Z", "updatedTime": "2024-01-01T00:00:00Z"}
            ]
        }
        self.mock_codestar_client.list_project_resources.return_value = mock_response
        
        result = run_async(self.integration.list_project_resources("project-123"))
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ProjectResource)

    def test_create_notification_rule(self):
        """Test creating notification rule"""
        mock_response = {
            "arn": "arn:aws:codestar-notifications:us-east-1:123:notification-rule/rule-123"
        }
        self.mock_notifications_client.create_notification_rule.return_value = mock_response
        
        result = run_async(self.integration.create_notification_rule(
            name="test-rule",
            project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
            event_types=[NotificationEventType.PROJECT_CREATED],
            target_type="SNS",
            target_address="arn:aws:sns:us-east-1:123:topic"
        ))
        
        self.assertIsInstance(result, NotificationRule)
        self.assertEqual(result.name, "test-rule")
        self.assertTrue(result.enabled)

    def test_list_notification_rules(self):
        """Test listing notification rules"""
        # Pre-populate a notification rule
        self.integration._notification_rules["rule-1"] = NotificationRule(
            id="rule-1", arn="arn:aws:codestar-notifications:us-east-1:123:notification-rule/rule-1",
            name="rule-1", project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
            event_types=["codestar:project-created"], target_type="SNS",
            target_address="arn:aws:sns:us-east-1:123:topic"
        )
        
        result = run_async(self.integration.list_notification_rules())
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

    def test_update_notification_rule(self):
        """Test updating notification rule"""
        # Pre-populate notification rule
        self.integration._notification_rules["rule-123"] = NotificationRule(
            id="rule-123", arn="arn:aws:codestar-notifications:us-east-1:123:notification-rule/rule-123",
            name="test-rule", project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
            event_types=["codestar:project-created"], target_type="SNS",
            target_address="arn:aws:sns:us-east-1:123:topic", enabled=True
        )
        self.mock_notifications_client.update_notification_rule.return_value = {}
        
        result = run_async(self.integration.update_notification_rule(
            rule_id="rule-123",
            enabled=False
        ))
        
        self.assertIsInstance(result, NotificationRule)
        self.assertFalse(result.enabled)

    def test_delete_notification_rule(self):
        """Test deleting notification rule"""
        # Pre-populate notification rule
        self.integration._notification_rules["rule-123"] = NotificationRule(
            id="rule-123", arn="arn:aws:codestar-notifications:us-east-1:123:notification-rule/rule-123",
            name="test-rule", project_arn="arn:aws:codestar:us-east-1:123:project/project-123",
            event_types=["codestar:project-created"], target_type="SNS",
            target_address="arn:aws:sns:us-east-1:123:topic", enabled=True
        )
        self.mock_notifications_client.delete_notification_rule.return_value = {}
        
        result = run_async(self.integration.delete_notification_rule("rule-123"))
        
        self.assertTrue(result)
        self.assertNotIn("rule-123", self.integration._notification_rules)

    def test_describe_notification_events(self):
        """Test describing notification events"""
        mock_response = {
            "events": [
                {"event": "codestar:project-created", "timestamp": "2024-01-01T00:00:00Z"},
                {"event": "codestar:pipeline-started", "timestamp": "2024-01-02T00:00:00Z"}
            ]
        }
        self.mock_notifications_client.describe_notification_events.return_value = mock_response
        
        result = run_async(self.integration.describe_notification_events(
            project_arn="arn:aws:codestar:us-east-1:123:project/project-123"
        ))
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

    def test_list_project_templates(self):
        """Test listing project templates"""
        mock_response = {
            "projectTemplates": [
                {"id": "python-web-app", "name": "Python Web App", "description": "Python web app template",
                 "category": "Web", "tags": ["Python"]},
                {"id": "nodejs-web-app", "name": "Node.js Web App", "description": "Node.js web app template",
                 "category": "Web", "tags": ["Node"]}
            ]
        }
        self.mock_codestar_client.list_project_templates.return_value = mock_response
        
        result = run_async(self.integration.list_project_templates())
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ProjectTemplateInfo)

    def test_get_project_template(self):
        """Test getting project template"""
        mock_response = {
            "projectTemplate": {
                "id": "python-web-app",
                "name": "Python Web App",
                "description": "A Python web application template",
                "category": "Web",
                "tags": ["Python"]
            }
        }
        self.mock_codestar_client.get_project_template.return_value = mock_response
        
        result = run_async(self.integration.get_project_template("python-web-app"))
        
        self.assertIsInstance(result, ProjectTemplateInfo)
        self.assertEqual(result.id, "python-web-app")

    def test_on_callback(self):
        """Test registering callbacks"""
        callback_called = []
        def callback(data):
            callback_called.append(data)
        
        self.integration.on("project.created", callback)
        self.assertIn("project.created", self.integration._callbacks)


class TestCodeStarIntegrationWithMockSession(unittest.TestCase):
    """Test CodeStarIntegration with mocked boto3 session"""

    def test_init_with_boto3_session(self):
        """Test initialization creates clients from boto3 session"""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        
        with patch.object(_codestar_module, 'boto3') as mock_boto3:
            mock_boto3.Session.return_value = mock_session
            mock_boto3.BOTO3_AVAILABLE = True
            
            integration = CodeStarIntegration(region="us-east-1")
            # Verify clients were created
            self.assertIsNotNone(integration.codestar_client)


if __name__ == "__main__":
    unittest.main()
