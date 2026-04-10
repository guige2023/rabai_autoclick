"""
Tests for workflow_aws_inspector module
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

# Create mock boto3 module before importing workflow_aws_inspector
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
import src.workflow_aws_inspector as _inspector_module

# Extract classes
InspectorIntegration = _inspector_module.InspectorIntegration
AssessmentTarget = _inspector_module.AssessmentTarget
AssessmentTemplate = _inspector_module.AssessmentTemplate
AssessmentRun = _inspector_module.AssessmentRun
RulesPackage = _inspector_module.RulesPackage
Finding = _inspector_module.Finding
ResourceGroup = _inspector_module.ResourceGroup
SNSConfiguration = _inspector_module.SNSConfiguration
CloudWatchMetrics = _inspector_module.CloudWatchMetrics
AssessmentRunStatus = _inspector_module.AssessmentRunStatus
AssessmentTargetStatus = _inspector_module.AssessmentTargetStatus
RulesPackageStatus = _inspector_module.RulesPackageStatus
FindingSeverity = _inspector_module.FindingSeverity
Inspector2FindingStatus = _inspector_module.Inspector2FindingStatus
Inspector2ResourceType = _inspector_module.Inspector2ResourceType
Inspector2PackageFormat = _inspector_module.Inspector2PackageFormat


class TestAssessmentTarget(unittest.TestCase):
    """Test AssessmentTarget dataclass"""

    def test_assessment_target_creation(self):
        target = AssessmentTarget(
            target_id="target-123",
            name="test-target",
            region="us-east-1"
        )
        self.assertEqual(target.target_id, "target-123")
        self.assertEqual(target.name, "test-target")
        self.assertEqual(target.region, "us-east-1")
        self.assertEqual(target.status, AssessmentTargetStatus.ACTIVE)

    def test_assessment_target_with_resource_groups(self):
        target = AssessmentTarget(
            target_id="target-456",
            name="target-with-resources",
            region="us-west-2",
            resource_groups=["arn1", "arn2"]
        )
        self.assertEqual(len(target.resource_groups), 2)


class TestAssessmentTemplate(unittest.TestCase):
    """Test AssessmentTemplate dataclass"""

    def test_assessment_template_creation(self):
        template = AssessmentTemplate(
            template_id="template-123",
            name="test-template",
            region="us-east-1",
            target_id="target-123",
            rules_package_arns=["arn:aws:inspector:us-east-1:123456789012:rulespackage/0-abc"]
        )
        self.assertEqual(template.template_id, "template-123")
        self.assertEqual(template.duration_seconds, 3600)
        self.assertEqual(template.assessment_run_count, 0)


class TestAssessmentRun(unittest.TestCase):
    """Test AssessmentRun dataclass"""

    def test_assessment_run_creation(self):
        run = AssessmentRun(
            run_id="run-123",
            template_id="template-123",
            region="us-east-1"
        )
        self.assertEqual(run.run_id, "run-123")
        self.assertEqual(run.state, AssessmentRunStatus.CREATED)
        self.assertEqual(run.findings_count, 0)


class TestRulesPackage(unittest.TestCase):
    """Test RulesPackage dataclass"""

    def test_rules_package_creation(self):
        pkg = RulesPackage(
            arn="arn:aws:inspector:us-east-1:123456789012:rulespackage/0-abc",
            name="Common Vulnerabilities and Exposures",
            version="1.0",
            region="us-east-1"
        )
        self.assertEqual(pkg.arn, "arn:aws:inspector:us-east-1:123456789012:rulespackage/0-abc")
        self.assertEqual(pkg.provider, "AWS")


class TestFinding(unittest.TestCase):
    """Test Finding dataclass"""

    def test_finding_creation(self):
        finding = Finding(
            finding_id="finding-123",
            region="us-east-1",
            severity=FindingSeverity.HIGH,
            title="CVE-2021-12345",
            description="A critical vulnerability",
            asset_type="ec2",
            asset_id="i-1234567890abcdef0"
        )
        self.assertEqual(finding.finding_id, "finding-123")
        self.assertEqual(finding.severity, FindingSeverity.HIGH)
        self.assertEqual(finding.status, "ACTIVE")

    def test_finding_with_rules_package(self):
        finding = Finding(
            finding_id="finding-456",
            region="us-east-1",
            severity=FindingSeverity.MEDIUM,
            title="Security Best Practice Violation",
            description="Instance has security group open to all",
            asset_type="ec2",
            asset_id="i-0987654321fedcba0",
            rules_package_arn="arn:aws:inspector:rulespackage/0-abc"
        )
        self.assertEqual(finding.rules_package_arn, "arn:aws:inspector:rulespackage/0-abc")


class TestResourceGroup(unittest.TestCase):
    """Test ResourceGroup dataclass"""

    def test_resource_group_creation(self):
        group = ResourceGroup(
            group_id="group-123",
            name="test-group",
            region="us-east-1",
            resource_arns=["arn:aws:ec2:us-east-1:123456789012:instance/i-123"]
        )
        self.assertEqual(group.group_id, "group-123")
        self.assertEqual(len(group.resource_arns), 1)


class TestSNSConfiguration(unittest.TestCase):
    """Test SNSConfiguration dataclass"""

    def test_sns_configuration_creation(self):
        sns_config = SNSConfiguration(
            topic_arn="arn:aws:sns:us-east-1:123456789012:inspector-topics",
            sns_role_arn="arn:aws:iam::123456789012:role/inspector-sns-role",
            event_types=["ASSESSMENT_RUN_COMPLETED", "FINDING_REPORTED"]
        )
        self.assertEqual(sns_config.topic_arn, "arn:aws:sns:us-east-1:123456789012:inspector-topics")
        self.assertTrue(sns_config.enabled)


class TestCloudWatchMetrics(unittest.TestCase):
    """Test CloudWatchMetrics dataclass"""

    def test_cloudwatch_metrics_default(self):
        metrics = CloudWatchMetrics()
        self.assertTrue(metrics.metrics_enabled)
        self.assertTrue(metrics.assessment_run_metrics)
        self.assertTrue(metrics.finding_metrics)

    def test_cloudwatch_metrics_custom(self):
        metrics = CloudWatchMetrics(
            metrics_enabled=True,
            assessment_run_metrics=False,
            finding_metrics=True,
            custom_namespace="Custom/Inspector"
        )
        self.assertEqual(metrics.custom_namespace, "Custom/Inspector")


class TestInspectorIntegration(unittest.TestCase):
    """Test InspectorIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()
        self.mock_inspector2 = MagicMock()
        self.mock_resource_groups = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [
            self.mock_inspector,
            self.mock_inspector2,
            self.mock_resource_groups
        ]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_initialization(self):
        """Test InspectorIntegration initialization"""
        integration = InspectorIntegration()
        self.assertEqual(integration.region, "us-east-1")
        self.assertIsNone(integration.profile_name)

    def test_initialization_with_params(self):
        """Test initialization with custom parameters"""
        integration = InspectorIntegration(
            region="us-west-2",
            profile_name="my-profile",
            config={"timeout": 60}
        )
        self.assertEqual(integration.region, "us-west-2")
        self.assertEqual(integration.profile_name, "my-profile")
        self.assertEqual(integration.config["timeout"], 60)


class TestAssessmentTargets(unittest.TestCase):
    """Test assessment target operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_assessment_target(self):
        """Test create_assessment_target method"""
        self.mock_inspector.create_assessment_target.return_value = {
            "assessmentTargetArn": "arn:aws:inspector:us-east-1:123456789012:target/123"
        }
        self.mock_inspector.add_tags_to_resource.return_value = {}
        
        integration = InspectorIntegration()
        target = integration.create_assessment_target(
            name="test-target",
            resource_groups=["arn1", "arn2"],
            tags={"env": "test"}
        )
        
        self.assertEqual(target.name, "test-target")
        self.assertEqual(target.status, AssessmentTargetStatus.ACTIVE)

    def test_get_assessment_target(self):
        """Test get_assessment_target method"""
        self.mock_inspector.describe_assessment_targets.return_value = {
            "assessmentTargets": [
                {
                    "name": "my-target",
                    "resourceGroupArns": ["arn1"],
                    "assessmentTargetStatus": "ACTIVE",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        target = integration.get_assessment_target("123")
        
        self.assertIsNotNone(target)
        self.assertEqual(target.name, "my-target")

    def test_list_assessment_targets(self):
        """Test list_assessment_targets method"""
        self.mock_inspector.list_assessment_targets.return_value = {
            "assessmentTargetArns": [
                "arn:aws:inspector:us-east-1:123456789012:target/1",
                "arn:aws:inspector:us-east-1:123456789012:target/2"
            ]
        }
        self.mock_inspector.describe_assessment_targets.return_value = {
            "assessmentTargets": [
                {
                    "name": "target-1",
                    "resourceGroupArns": [],
                    "assessmentTargetStatus": "ACTIVE",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        targets = integration.list_assessment_targets()
        
        # List returns target IDs, get returns objects
        self.assertIsInstance(targets, list)

    def test_update_assessment_target(self):
        """Test update_assessment_target method"""
        self.mock_inspector.update_assessment_target.return_value = {}
        self.mock_inspector.describe_assessment_targets.return_value = {
            "assessmentTargets": [
                {
                    "name": "updated-target",
                    "resourceGroupArns": [],
                    "assessmentTargetStatus": "ACTIVE",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        target = integration.update_assessment_target(
            target_id="123",
            name="updated-target"
        )
        
        self.assertIsNotNone(target)

    def test_delete_assessment_target(self):
        """Test delete_assessment_target method"""
        self.mock_inspector.delete_assessment_target.return_value = {}
        
        integration = InspectorIntegration()
        result = integration.delete_assessment_target("123")
        
        self.assertTrue(result)


class TestAssessmentTemplates(unittest.TestCase):
    """Test assessment template operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_assessment_template(self):
        """Test create_assessment_template method"""
        self.mock_inspector.create_assessment_template.return_value = {
            "assessmentTemplateArn": "arn:aws:inspector:us-east-1:123456789012:template/456"
        }
        self.mock_inspector.add_tags_to_resource.return_value = {}
        
        integration = InspectorIntegration()
        template = integration.create_assessment_template(
            name="test-template",
            target_id="123",
            rules_package_arns=["arn:aws:inspector:rulespackage/0-abc"],
            duration_seconds=1800
        )
        
        self.assertEqual(template.name, "test-template")
        self.assertEqual(template.duration_seconds, 1800)

    def test_get_assessment_template(self):
        """Test get_assessment_template method"""
        self.mock_inspector.describe_assessment_templates.return_value = {
            "assessmentTemplates": [
                {
                    "name": "my-template",
                    "assessmentTargetArn": "arn:aws:inspector:us-east-1:123456789012:target/123",
                    "rulesPackageArns": ["arn:aws:inspector:rulespackage/0-abc"],
                    "durationInSeconds": 3600,
                    "assessmentRunCount": 5,
                    "lastAssessmentRunStartedAt": "2024-01-02T00:00:00Z",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        template = integration.get_assessment_template("456")
        
        self.assertIsNotNone(template)
        self.assertEqual(template.name, "my-template")

    def test_list_assessment_templates(self):
        """Test list_assessment_templates method"""
        self.mock_inspector.list_assessment_templates.return_value = {
            "assessmentTemplateArns": [
                "arn:aws:inspector:us-east-1:123456789012:template/1",
                "arn:aws:inspector:us-east-1:123456789012:template/2"
            ]
        }
        
        integration = InspectorIntegration()
        templates = integration.list_assessment_templates()
        
        self.assertIsInstance(templates, list)

    def test_update_assessment_template(self):
        """Test update_assessment_template method"""
        self.mock_inspector.update_assessment_template.return_value = {}
        self.mock_inspector.describe_assessment_templates.return_value = {
            "assessmentTemplates": [
                {
                    "name": "updated-template",
                    "assessmentTargetArn": "arn:aws:inspector:us-east-1:123456789012:target/123",
                    "rulesPackageArns": ["arn:aws:inspector:rulespackage/0-abc"],
                    "durationInSeconds": 7200,
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        template = integration.update_assessment_template(
            template_id="456",
            name="updated-template",
            duration_seconds=7200
        )
        
        self.assertIsNotNone(template)

    def test_delete_assessment_template(self):
        """Test delete_assessment_template method"""
        self.mock_inspector.delete_assessment_template.return_value = {}
        
        integration = InspectorIntegration()
        result = integration.delete_assessment_template("456")
        
        self.assertTrue(result)


class TestAssessmentRuns(unittest.TestCase):
    """Test assessment run operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_start_assessment_run(self):
        """Test start_assessment_run method"""
        self.mock_inspector.start_assessment_run.return_value = {
            "assessmentRunArn": "arn:aws:inspector:us-east-1:123456789012:run/789"
        }
        self.mock_inspector.add_tags_to_resource.return_value = {}
        
        integration = InspectorIntegration()
        run = integration.start_assessment_run(
            template_id="456",
            name="test-run",
            tags={"env": "test"}
        )
        
        self.assertEqual(run.template_id, "456")
        self.assertEqual(run.state, AssessmentRunStatus.STARTED)

    def test_get_assessment_run(self):
        """Test get_assessment_run method"""
        self.mock_inspector.describe_assessment_runs.return_value = {
            "assessmentRuns": [
                {
                    "assessmentTemplateArn": "arn:aws:inspector:us-east-1:123456789012:template/456",
                    "state": "COMPLETED",
                    "startedAt": "2024-01-01T00:00:00Z",
                    "completedAt": "2024-01-01T01:00:00Z",
                    "findingIdsCount": 10,
                    "rulesPackagesCount": 3
                }
            ]
        }
        
        integration = InspectorIntegration()
        run = integration.get_assessment_run("789")
        
        self.assertIsNotNone(run)
        self.assertEqual(run.state, AssessmentRunStatus.COMPLETED)
        self.assertEqual(run.findings_count, 10)

    def test_list_assessment_runs(self):
        """Test list_assessment_runs method"""
        self.mock_inspector.list_assessment_runs.return_value = {
            "assessmentRunArns": [
                "arn:aws:inspector:us-east-1:123456789012:run/1",
                "arn:aws:inspector:us-east-1:123456789012:run/2"
            ]
        }
        
        integration = InspectorIntegration()
        runs = integration.list_assessment_runs()
        
        self.assertIsInstance(runs, list)

    def test_stop_assessment_run(self):
        """Test stop_assessment_run method"""
        self.mock_inspector.stop_assessment_run.return_value = {}
        
        integration = InspectorIntegration()
        result = integration.stop_assessment_run("789")
        
        self.assertTrue(result)


class TestRulesPackages(unittest.TestCase):
    """Test rules package operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_list_rules_packages(self):
        """Test list_rules_packages method"""
        self.mock_inspector.list_rules_packages.return_value = {
            "rulesPackageArns": [
                "arn:aws:inspector:us-east-1:123456789012:rulespackage/0-abc",
                "arn:aws:inspector:us-east-1:123456789012:rulespackage/0-def"
            ]
        }
        self.mock_inspector.describe_rules_packages.return_value = {
            "rulesPackages": [
                {
                    "name": "Common Vulnerabilities and Exposures",
                    "version": "1.0",
                    "ruleCount": 50,
                    "description": "CVE rules",
                    "provider": "AWS"
                }
            ]
        }
        
        integration = InspectorIntegration()
        packages = integration.list_rules_packages()
        
        self.assertIsInstance(packages, list)

    def test_get_rules_package(self):
        """Test get_rules_package method"""
        self.mock_inspector.describe_rules_packages.return_value = {
            "rulesPackages": [
                {
                    "name": "Security Best Practices",
                    "version": "2.0",
                    "ruleCount": 100,
                    "description": "Security best practice rules",
                    "provider": "AWS"
                }
            ]
        }
        
        integration = InspectorIntegration()
        pkg = integration.get_rules_package("arn:aws:inspector:rulespackage/0-abc")
        
        self.assertIsNotNone(pkg)
        self.assertEqual(pkg.name, "Security Best Practices")

    def test_get_available_rules_packages(self):
        """Test get_available_rules_packages method"""
        self.mock_inspector.list_available_agent_versions.return_value = {
            "agentVersions": [
                {"version": "1.0"},
                {"version": "2.0"}
            ]
        }
        
        integration = InspectorIntegration()
        result = integration.get_available_rules_packages()
        
        self.assertIn("agentVersions", result)


class TestFindings(unittest.TestCase):
    """Test findings operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_list_findings(self):
        """Test list_findings method"""
        self.mock_inspector.list_findings.return_value = {
            "findingArns": [
                "arn:aws:inspector:us-east-1:123456789012:finding/1",
                "arn:aws:inspector:us-east-1:123456789012:finding/2"
            ]
        }
        self.mock_inspector.describe_findings.return_value = {
            "findings": [
                {
                    "id": "1",
                    "severity": "HIGH",
                    "title": "Finding 1",
                    "description": "First finding",
                    "assetType": "ec2",
                    "asset": {"id": "i-123"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        findings = integration.list_findings()
        
        self.assertIsInstance(findings, list)

    def test_get_finding(self):
        """Test get_finding method"""
        self.mock_inspector.describe_findings.return_value = {
            "findings": [
                {
                    "id": "finding-123",
                    "severity": "CRITICAL",
                    "title": "Critical Finding",
                    "description": "A critical vulnerability",
                    "assetType": "ec2",
                    "asset": {"id": "i-1234567890abcdef0"},
                    "rulesPackageArn": "arn:aws:inspector:rulespackage/0-abc",
                    "assessmentRunArn": "arn:aws:inspector:run/123",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        finding = integration.get_finding("arn:aws:inspector:us-east-1:123456789012:finding/1")
        
        self.assertIsNotNone(finding)
        self.assertEqual(finding.severity, FindingSeverity.CRITICAL)

    def test_get_finding_statistics(self):
        """Test get_finding_statistics method"""
        self.mock_inspector.get_assessment_summary.return_value = {
            "assessmentRunCount": 10,
            "totalFindingCount": 25,
            "findingCounts": {
                "high": 5,
                "medium": 10,
                "low": 10
            }
        }
        
        integration = InspectorIntegration()
        stats = integration.get_finding_statistics()
        
        self.assertIn("assessmentRunCount", stats)


class TestResourceGroups(unittest.TestCase):
    """Test resource group operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_resource_groups = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [MagicMock(), MagicMock(), self.mock_resource_groups]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_create_resource_group(self):
        """Test create_resource_group method"""
        self.mock_resource_groups.create_group.return_value = {
            "Group": {
                "GroupArn": "arn:aws:resource-groups:us-east-1:123456789012:group/my-group",
                "name": "my-group"
            }
        }
        
        integration = InspectorIntegration()
        group = integration.create_resource_group(
            name="my-group",
            resource_arns=["arn:aws:ec2:us-east-1:123456789012:instance/i-123"],
            tags={"env": "test"}
        )
        
        self.assertEqual(group.name, "my-group")

    def test_get_resource_group(self):
        """Test get_resource_group method"""
        self.mock_resource_groups.get_group.return_value = {
            "Group": {
                "GroupArn": "arn:aws:resource-groups:us-east-1:123456789012:group/my-group",
                "Name": "my-group",
                "Description": "My resource group"
            }
        }
        
        integration = InspectorIntegration()
        group = integration.get_resource_group("my-group")
        
        self.assertIsNotNone(group)
        self.assertEqual(group.name, "my-group")

    def test_delete_resource_group(self):
        """Test delete_resource_group method"""
        self.mock_resource_groups.delete_group.return_value = {}
        
        integration = InspectorIntegration()
        result = integration.delete_resource_group("my-group")
        
        self.assertTrue(result)


class TestInspectorV2(unittest.TestCase):
    """Test Inspector v2 operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()
        self.mock_inspector2 = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, self.mock_inspector2, MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_listInspector2_findings(self):
        """Test list_findings_v2 method"""
        self.mock_inspector2.list_findings.return_value = {
            "findings": [
                {
                    "findingArn": "arn:aws:inspector2:us-east-1:123456789012:finding/1",
                    "awsAccountId": "123456789012",
                    "resourceType": "AWS_EC2_INSTANCE",
                    "severity": "HIGH",
                    "status": "ACTIVE",
                    "title": "Inspector V2 Finding",
                    "description": "A finding from Inspector V2"
                }
            ]
        }
        
        integration = InspectorIntegration()
        findings = integration.list_findings_v2()
        
        self.assertIsInstance(findings, list)

    def test_get_finding_v2(self):
        """Test get_finding_v2 method"""
        self.mock_inspector2.list_findings.return_value = {
            "findings": [
                {
                    "findingArn": "arn:aws:inspector2:us-east-1:123456789012:finding/1",
                    "awsAccountId": "123456789012",
                    "resourceType": "AWS_EC2_INSTANCE",
                    "severity": "CRITICAL",
                    "status": "ACTIVE",
                    "title": "Critical V2 Finding",
                    "description": "Critical finding from Inspector V2"
                }
            ]
        }
        
        integration = InspectorIntegration()
        finding = integration.get_finding_v2("arn:aws:inspector2:us-east-1:123456789012:finding/1")
        
        self.assertIsNotNone(finding)

    def test_archive_finding_v2(self):
        """Test archive_finding_v2 method"""
        self.mock_inspector2.update_findings.return_value = {}
        
        integration = InspectorIntegration()
        result = integration.archive_finding_v2("arn:aws:inspector2:us-east-1:123456789012:finding/1")
        
        self.assertTrue(result)


class TestSNSTopics(unittest.TestCase):
    """Test SNS topic operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_configure_sns_topic(self):
        """Test configure_sns_topic method"""
        self.mock_inspector.set_tags_for_resource.return_value = {}
        
        integration = InspectorIntegration()
        config = integration.configure_sns_topic(
            topic_arn="arn:aws:sns:us-east-1:123456789012:inspector-topic",
            sns_role_arn="arn:aws:iam::123456789012:role/inspector-sns",
            event_types=["ASSESSMENT_RUN_COMPLETED", "FINDING_REPORTED"]
        )
        
        self.assertEqual(config.topic_arn, "arn:aws:sns:us-east-1:123456789012:inspector-topic")
        self.assertTrue(config.enabled)


class TestCloudWatchIntegration(unittest.TestCase):
    """Test CloudWatch integration"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()
        self.mock_cloudwatch = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, MagicMock(), MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_enable_cloudwatch_metrics(self):
        """Test enable_cloudwatch_metrics method"""
        self.mock_inspector.get_assessment_summary.return_value = {
            "assessmentRunCount": 5,
            "totalFindingCount": 15
        }
        
        integration = InspectorIntegration()
        result = integration.enable_cloudwatch_metrics()
        
        self.assertTrue(result)

    def test_publish_assessment_run_metrics(self):
        """Test publish_assessment_run_metrics method"""
        self.mock_inspector.describe_assessment_runs.return_value = {
            "assessmentRuns": [
                {
                    "assessmentTemplateArn": "arn:aws:inspector:template/123",
                    "state": "COMPLETED",
                    "startedAt": "2024-01-01T00:00:00Z",
                    "completedAt": "2024-01-01T01:00:00Z",
                    "findingIdsCount": 10,
                    "rulesPackagesCount": 3
                }
            ]
        }
        
        integration = InspectorIntegration()
        result = integration.publish_assessment_run_metrics("run-123")
        
        self.assertTrue(result)


class TestInspectorWorkflows(unittest.TestCase):
    """Test Inspector workflow operations"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_inspector = MagicMock()
        self.mock_inspector2 = MagicMock()

        self.patcher = patch('src.workflow_aws_inspector.boto3')
        self.mock_boto3 = self.patcher.start()
        
        mock_session = MagicMock()
        self.mock_boto3.Session.return_value = mock_session
        mock_session.client.side_effect = [self.mock_inspector, self.mock_inspector2, MagicMock()]

    def tearDown(self):
        """Tear down test fixtures"""
        self.patcher.stop()

    def test_run_assessment_and_wait(self):
        """Test run_assessment_and_wait method"""
        self.mock_inspector.start_assessment_run.return_value = {
            "assessmentRunArn": "arn:aws:inspector:run/789"
        }
        self.mock_inspector.describe_assessment_runs.return_value = {
            "assessmentRuns": [
                {
                    "assessmentTemplateArn": "arn:aws:inspector:template/456",
                    "state": "COMPLETED",
                    "startedAt": "2024-01-01T00:00:00Z",
                    "completedAt": "2024-01-01T01:00:00Z",
                    "findingIdsCount": 5,
                    "rulesPackagesCount": 3
                }
            ]
        }
        
        integration = InspectorIntegration()
        run = integration.run_assessment_and_wait("456", timeout=60)
        
        self.assertIsNotNone(run)

    def test_get_severity_summary(self):
        """Test get_severity_summary method"""
        self.mock_inspector.list_findings.return_value = {
            "findingArns": [
                "arn:aws:inspector:finding/1",
                "arn:aws:inspector:finding/2"
            ]
        }
        self.mock_inspector.describe_findings.return_value = {
            "findings": [
                {
                    "id": "1",
                    "severity": "HIGH",
                    "title": "High Finding",
                    "description": "High severity",
                    "assetType": "ec2",
                    "asset": {"id": "i-123"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:00:00Z"
                },
                {
                    "id": "2",
                    "severity": "LOW",
                    "title": "Low Finding",
                    "description": "Low severity",
                    "assetType": "ec2",
                    "asset": {"id": "i-456"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:00:00Z"
                }
            ]
        }
        
        integration = InspectorIntegration()
        summary = integration.get_severity_summary()
        
        self.assertIn("high", summary)
        self.assertIn("low", summary)


if __name__ == '__main__':
    unittest.main()
