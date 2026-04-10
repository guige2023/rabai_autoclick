"""
Tests for workflow_aws_codedeploy module
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

# Create mock boto3 module before importing workflow_aws_codedeploy
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
from src.workflow_aws_codedeploy import (
    CodeDeployIntegration,
    ComputePlatform,
    DeploymentStatus,
    DeploymentOption,
    DeploymentType,
    TrafficRoutingType,
    DeploymentReadyOption,
    TerminationOption,
    RevisionLocationType,
    InstanceStatus,
    ErrorCode,
    AlarmStatus,
    CodeDeployConfig,
    S3Location,
    GitHubLocation,
    RevisionLocation,
    AlarmConfig,
    AutoRollbackConfig,
    DeploymentStyle,
    BlueGreenDeploymentConfig,
    EC2TagFilter,
    OnPremisesTagFilter,
    EC2TargetGroupInfo,
    LoadBalancerInfo,
    DeploymentGroupConfig,
    DeploymentConfigInfo,
    ApplicationInfo,
    DeploymentGroupInfo,
    DeploymentInfo,
    InstanceInfo,
    LifecycleEventInfo,
    AppSpecContent,
    AppSpecHooks,
    AppSpecResources,
    ECSContainerImage,
    ECSLoadBalancerInfo,
    LambdaFunction,
    CloudWatchEventConfig,
    AppSpecValidator,
    BOTO3_AVAILABLE,
)


class TestComputePlatform(unittest.TestCase):
    """Test ComputePlatform enum"""

    def test_compute_platform_values(self):
        self.assertEqual(ComputePlatform.EC2.value, "EC2")
        self.assertEqual(ComputePlatform.LAMBDA.value, "Lambda")
        self.assertEqual(ComputePlatform.ECS.value, "ECS")

    def test_compute_platform_is_string(self):
        self.assertIsInstance(ComputePlatform.EC2.value, str)


class TestDeploymentStatus(unittest.TestCase):
    """Test DeploymentStatus enum"""

    def test_deployment_status_values(self):
        self.assertEqual(DeploymentStatus.PENDING.value, "PENDING")
        self.assertEqual(DeploymentStatus.IN_PROGRESS.value, "IN_PROGRESS")
        self.assertEqual(DeploymentStatus.SUCCEEDED.value, "SUCCEEDED")
        self.assertEqual(DeploymentStatus.FAILED.value, "FAILED")
        self.assertEqual(DeploymentStatus.STOPPED.value, "STOPPED")
        self.assertEqual(DeploymentStatus.READY.value, "READY")


class TestDeploymentOption(unittest.TestCase):
    """Test DeploymentOption enum"""

    def test_deployment_option_values(self):
        self.assertEqual(DeploymentOption.WITH_TRAFFIC.value, "WITH_TRAFFIC")
        self.assertEqual(DeploymentOption.WITHOUT_TRAFFIC.value, "WITHOUT_TRAFFIC")


class TestDeploymentType(unittest.TestCase):
    """Test DeploymentType enum"""

    def test_deployment_type_values(self):
        self.assertEqual(DeploymentType.IN_PLACE.value, "IN_PLACE")
        self.assertEqual(DeploymentType.BLUE_GREEN.value, "BLUE_GREEN")


class TestTrafficRoutingType(unittest.TestCase):
    """Test TrafficRoutingType enum"""

    def test_traffic_routing_type_values(self):
        self.assertEqual(TrafficRoutingType.ALL_AT_ONCE.value, "AllAtOnce")
        self.assertEqual(TrafficRoutingType.TIME_BASED_CANARY.value, "TimeBasedCanary")
        self.assertEqual(TrafficRoutingType.TIME_BASED_LINEAR.value, "TimeBasedLinear")


class TestDeploymentReadyOption(unittest.TestCase):
    """Test DeploymentReadyOption enum"""

    def test_deployment_ready_option_values(self):
        self.assertEqual(DeploymentReadyOption.NONE.value, "NONE")
        self.assertEqual(DeploymentReadyOption.AUTO_BEFORE_ALARM.value, "AutoBeforeAlarm")


class TestTerminationOption(unittest.TestCase):
    """Test TerminationOption enum"""

    def test_termination_option_values(self):
        self.assertEqual(TerminationOption.TERMINATION.value, "TERMINATION")
        self.assertEqual(TerminationOption.KEEP_ALIVE.value, "KEEP_ALIVE")


class TestRevisionLocationType(unittest.TestCase):
    """Test RevisionLocationType enum"""

    def test_revision_location_type_values(self):
        self.assertEqual(RevisionLocationType.S3.value, "S3")
        self.assertEqual(RevisionLocationType.GITHUB.value, "GitHub")
        self.assertEqual(RevisionLocationType.STRING.value, "String")
        self.assertEqual(RevisionLocationType.APPSPEC_CONTENT.value, "AppSpecContent")


class TestInstanceStatus(unittest.TestCase):
    """Test InstanceStatus enum"""

    def test_instance_status_values(self):
        self.assertEqual(InstanceStatus.PENDING.value, "Pending")
        self.assertEqual(InstanceStatus.IN_PROGRESS.value, "InProgress")
        self.assertEqual(InstanceStatus.SUCCEEDED.value, "Succeeded")
        self.assertEqual(InstanceStatus.FAILED.value, "Failed")
        self.assertEqual(InstanceStatus.SKIPPED.value, "Skipped")
        self.assertEqual(InstanceStatus.UNKNOWN.value, "Unknown")


class TestErrorCode(unittest.TestCase):
    """Test ErrorCode enum"""

    def test_error_code_values(self):
        self.assertEqual(ErrorCode.SUCCESS.value, "Success")
        self.assertEqual(ErrorCode.DEPLOYMENT_MISSING_APP_SPEC.value, "DeploymentMissingAppSpec")
        self.assertEqual(ErrorCode.DEPLOYMENT_GROUP_MISSING.value, "DeploymentGroupMissing")
        self.assertEqual(ErrorCode.DEPLOYMENT_IN_PROGRESS.value, "DeploymentInProgress")


class TestAlarmStatus(unittest.TestCase):
    """Test AlarmStatus enum"""

    def test_alarm_status_values(self):
        self.assertEqual(AlarmStatus.ALARM_OK.value, "AlarmOk")
        self.assertEqual(AlarmStatus.ALARM_INSUFFICIENT_DATA.value, "AlarmInsufficientData")
        self.assertEqual(AlarmStatus.ALARM_CONFIGURATION_ERROR.value, "AlarmConfigurationError")


class TestS3Location(unittest.TestCase):
    """Test S3Location dataclass"""

    def test_s3_location_defaults(self):
        s3_loc = S3Location(bucket="my-bucket", key="my-key")
        self.assertEqual(s3_loc.bucket, "my-bucket")
        self.assertEqual(s3_loc.key, "my-key")
        self.assertEqual(s3_loc.bundle_type, "zip")
        self.assertIsNone(s3_loc.version)
        self.assertIsNone(s3_loc.e_tag)

    def test_s3_location_custom(self):
        s3_loc = S3Location(
            bucket="my-bucket",
            key="my-key",
            bundle_type="tar",
            version="v1",
            e_tag="abc123"
        )
        self.assertEqual(s3_loc.bundle_type, "tar")
        self.assertEqual(s3_loc.version, "v1")
        self.assertEqual(s3_loc.e_tag, "abc123")


class TestGitHubLocation(unittest.TestCase):
    """Test GitHubLocation dataclass"""

    def test_github_location_defaults(self):
        gh_loc = GitHubLocation(repository="my-repo", commit_id="abc123")
        self.assertEqual(gh_loc.repository, "my-repo")
        self.assertEqual(gh_loc.commit_id, "abc123")
        self.assertIsNone(gh_loc.branch)
        self.assertIsNone(gh_loc.git_hub_location)

    def test_github_location_custom(self):
        gh_loc = GitHubLocation(
            repository="my-repo",
            commit_id="abc123",
            branch="main",
            git_hub_location="https://github.com/my-repo"
        )
        self.assertEqual(gh_loc.branch, "main")
        self.assertEqual(gh_loc.git_hub_location, "https://github.com/my-repo")


class TestRevisionLocation(unittest.TestCase):
    """Test RevisionLocation dataclass"""

    def test_revision_location_s3(self):
        s3_loc = S3Location(bucket="my-bucket", key="my-key")
        rev_loc = RevisionLocation(
            revision_type=RevisionLocationType.S3,
            s3_location=s3_loc
        )
        self.assertEqual(rev_loc.revision_type, RevisionLocationType.S3)
        self.assertEqual(rev_loc.s3_location, s3_loc)
        self.assertIsNone(rev_loc.git_hub_location)
        self.assertIsNone(rev_loc.app_spec_content)


class TestAlarmConfig(unittest.TestCase):
    """Test AlarmConfig dataclass"""

    def test_alarm_config_defaults(self):
        alarm_config = AlarmConfig()
        self.assertEqual(alarm_config.alarms, [])
        self.assertTrue(alarm_config.enabled)
        self.assertFalse(alarm_config.ignore_poll_alarm_failure)

    def test_alarm_config_custom(self):
        alarm_config = AlarmConfig(
            alarms=[{"name": "my-alarm"}],
            enabled=False,
            ignore_poll_alarm_failure=True
        )
        self.assertEqual(len(alarm_config.alarms), 1)
        self.assertFalse(alarm_config.enabled)
        self.assertTrue(alarm_config.ignore_poll_alarm_failure)


class TestAutoRollbackConfig(unittest.TestCase):
    """Test AutoRollbackConfig dataclass"""

    def test_auto_rollback_config_defaults(self):
        rollback_config = AutoRollbackConfig()
        self.assertFalse(rollback_config.enabled)
        self.assertEqual(rollback_config.events, ["DEPLOYMENT_FAILURE", "DEPLOYMENT_STOP_ON_REQUEST"])

    def test_auto_rollback_config_custom(self):
        rollback_config = AutoRollbackConfig(
            enabled=True,
            events=["DEPLOYMENT_FAILURE"]
        )
        self.assertTrue(rollback_config.enabled)
        self.assertEqual(len(rollback_config.events), 1)


class TestDeploymentStyle(unittest.TestCase):
    """Test DeploymentStyle dataclass"""

    def test_deployment_style_defaults(self):
        style = DeploymentStyle()
        self.assertEqual(style.deployment_type, DeploymentType.IN_PLACE)
        self.assertEqual(style.deployment_option, DeploymentOption.WITH_TRAFFIC)

    def test_deployment_style_custom(self):
        style = DeploymentStyle(
            deployment_type=DeploymentType.BLUE_GREEN,
            deployment_option=DeploymentOption.WITHOUT_TRAFFIC
        )
        self.assertEqual(style.deployment_type, DeploymentType.BLUE_GREEN)
        self.assertEqual(style.deployment_option, DeploymentOption.WITHOUT_TRAFFIC)


class TestBlueGreenDeploymentConfig(unittest.TestCase):
    """Test BlueGreenDeploymentConfig dataclass"""

    def test_blue_green_deployment_config_defaults(self):
        bg_config = BlueGreenDeploymentConfig()
        self.assertIsNone(bg_config.terminate_blue_instances_on_deployment_success)
        self.assertIsNone(bg_config.deployment_ready_option)
        self.assertIsNone(bg_config.green_fleet_provisioning_option)

    def test_blue_green_deployment_config_custom(self):
        bg_config = BlueGreenDeploymentConfig(
            terminate_blue_instances_on_deployment_success={"action": "terminate"},
            deployment_ready_option=DeploymentReadyOption.AUTO_BEFORE_ALARM,
            green_fleet_provisioning_option={"option": "Instant"}
        )
        self.assertEqual(bg_config.deployment_ready_option, DeploymentReadyOption.AUTO_BEFORE_ALARM)


class TestEC2TagFilter(unittest.TestCase):
    """Test EC2TagFilter dataclass"""

    def test_ec2_tag_filter_defaults(self):
        tag_filter = EC2TagFilter()
        self.assertIsNone(tag_filter.key)
        self.assertIsNone(tag_filter.value)
        self.assertEqual(tag_filter.type, "KEY_AND_VALUE")

    def test_ec2_tag_filter_custom(self):
        tag_filter = EC2TagFilter(key="Name", value="web-server", type="KEY_ONLY")
        self.assertEqual(tag_filter.key, "Name")
        self.assertEqual(tag_filter.value, "web-server")
        self.assertEqual(tag_filter.type, "KEY_ONLY")


class TestOnPremisesTagFilter(unittest.TestCase):
    """Test OnPremisesTagFilter dataclass"""

    def test_on_premises_tag_filter_defaults(self):
        tag_filter = OnPremisesTagFilter()
        self.assertIsNone(tag_filter.key)
        self.assertIsNone(tag_filter.value)
        self.assertEqual(tag_filter.type, "KEY_AND_VALUE")


class TestLoadBalancerInfo(unittest.TestCase):
    """Test LoadBalancerInfo dataclass"""

    def test_load_balancer_info_defaults(self):
        lb_info = LoadBalancerInfo()
        self.assertEqual(lb_info.elb_infos, [])
        self.assertEqual(lb_info.target_group_infos, [])
        self.assertEqual(lb_info.target_groups, [])

    def test_load_balancer_info_custom(self):
        lb_info = LoadBalancerInfo(
            elb_infos=[{"name": "my-elb"}],
            target_group_infos=[{"name": "my-tg"}]
        )
        self.assertEqual(len(lb_info.elb_infos), 1)
        self.assertEqual(len(lb_info.target_group_infos), 1)


class TestDeploymentGroupConfig(unittest.TestCase):
    """Test DeploymentGroupConfig dataclass"""

    def test_deployment_group_config_defaults(self):
        config = DeploymentGroupConfig(
            deployment_group_name="my-dg",
            application_name="my-app"
        )
        self.assertEqual(config.deployment_group_name, "my-dg")
        self.assertEqual(config.application_name, "my-app")
        self.assertIsNone(config.deployment_style)
        self.assertIsNone(config.deployment_config_name)
        self.assertEqual(config.ec2_tag_filters, [])
        self.assertEqual(config.auto_scaling_groups, [])
        self.assertEqual(config.tags, {})


class TestApplicationInfo(unittest.TestCase):
    """Test ApplicationInfo dataclass"""

    def test_application_info(self):
        app_info = ApplicationInfo(
            application_id="app-123",
            application_name="my-app",
            compute_platform="EC2",
            linked_to_github=False,
            created="2024-01-01T00:00:00Z",
            last_modified="2024-01-01T00:00:00Z"
        )
        self.assertEqual(app_info.application_id, "app-123")
        self.assertEqual(app_info.application_name, "my-app")
        self.assertEqual(app_info.compute_platform, "EC2")
        self.assertFalse(app_info.linked_to_github)


class TestDeploymentGroupInfo(unittest.TestCase):
    """Test DeploymentGroupInfo dataclass"""

    def test_deployment_group_info(self):
        dg_info = DeploymentGroupInfo(
            deployment_group_id="dg-123",
            deployment_group_name="my-dg",
            application_name="my-app",
            deployment_config_name="CodeDeployDefault.OneAtATime",
            service_role_arn="arn:aws:iam::123456789012:role/my-role",
            created="2024-01-01T00:00:00Z",
            last_modified="2024-01-01T00:00:00Z"
        )
        self.assertEqual(dg_info.deployment_group_id, "dg-123")
        self.assertEqual(dg_info.deployment_group_name, "my-dg")


class TestDeploymentInfo(unittest.TestCase):
    """Test DeploymentInfo dataclass"""

    def test_deployment_info(self):
        dep_info = DeploymentInfo(
            deployment_id="dep-123",
            application_name="my-app",
            deployment_group_name="my-dg",
            deployment_config_name="CodeDeployDefault.OneAtATime",
            status="Succeeded",
            create_time="2024-01-01T00:00:00Z"
        )
        self.assertEqual(dep_info.deployment_id, "dep-123")
        self.assertEqual(dep_info.status, "Succeeded")


class TestInstanceInfo(unittest.TestCase):
    """Test InstanceInfo dataclass"""

    def test_instance_info(self):
        inst_info = InstanceInfo(
            instance_id="i-123",
            deployment_id="dep-123",
            status="Succeeded"
        )
        self.assertEqual(inst_info.instance_id, "i-123")
        self.assertEqual(inst_info.status, "Succeeded")


class TestLifecycleEventInfo(unittest.TestCase):
    """Test LifecycleEventInfo dataclass"""

    def test_lifecycle_event_info(self):
        event_info = LifecycleEventInfo(
            lifecycle_event_name="BeforeInstall",
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-01T00:01:00Z",
            status="Succeeded"
        )
        self.assertEqual(event_info.lifecycle_event_name, "BeforeInstall")
        self.assertEqual(event_info.status, "Succeeded")


class TestAppSpecContent(unittest.TestCase):
    """Test AppSpecContent dataclass"""

    def test_appspec_content(self):
        content = AppSpecContent(
            content="version: 0.0\nfiles:\n  - source: /\n    destination: /var/www/html",
            content_sha="abc123",
            bucket_name="my-bucket",
            object_key="my-key"
        )
        self.assertIsNotNone(content.content)
        self.assertEqual(content.content_sha, "abc123")


class TestAppSpecHooks(unittest.TestCase):
    """Test AppSpecHooks dataclass"""

    def test_appspec_hooks_defaults(self):
        hooks = AppSpecHooks()
        self.assertEqual(hooks.after_allow_traffic, [])
        self.assertEqual(hooks.before_allow_traffic, [])
        self.assertEqual(hooks.after_block_traffic, [])
        self.assertEqual(hooks.before_block_traffic, [])

    def test_appspec_hooks_custom(self):
        hooks = AppSpecHooks(
            after_allow_traffic=[{"name": "my-hook"}],
            before_allow_traffic=[{"name": "pre-hook"}]
        )
        self.assertEqual(len(hooks.after_allow_traffic), 1)
        self.assertEqual(len(hooks.before_allow_traffic), 1)


class TestECSContainerImage(unittest.TestCase):
    """Test ECSContainerImage dataclass"""

    def test_ecs_container_image(self):
        image = ECSContainerImage(
            image="my-repo/my-image:latest",
            container_name="web"
        )
        self.assertEqual(image.image, "my-repo/my-image:latest")
        self.assertEqual(image.container_name, "web")


class TestLambdaFunction(unittest.TestCase):
    """Test LambdaFunction dataclass"""

    def test_lambda_function_defaults(self):
        func = LambdaFunction(name="my-function")
        self.assertEqual(func.name, "my-function")
        self.assertIsNone(func.alias)
        self.assertIsNone(func.current_version)
        self.assertIsNone(func.target_version)

    def test_lambda_function_custom(self):
        func = LambdaFunction(
            name="my-function",
            alias="prod",
            current_version="1",
            target_version="2"
        )
        self.assertEqual(func.alias, "prod")
        self.assertEqual(func.current_version, "1")
        self.assertEqual(func.target_version, "2")


class TestCloudWatchEventConfig(unittest.TestCase):
    """Test CloudWatchEventConfig dataclass"""

    def test_cloudwatch_event_config(self):
        config = CloudWatchEventConfig(
            event_type="DeploymentInProgress",
            trigger_name="my-trigger",
            target_role_arn="arn:aws:iam::123456789012:role/my-role",
            target_arn="arn:aws:sns:us-east-1:123456789012:my-topic"
        )
        self.assertEqual(config.event_type, "DeploymentInProgress")
        self.assertIsNotNone(config.trigger_name)


class TestAppSpecValidator(unittest.TestCase):
    """Test AppSpecValidator class"""

    def test_validate_ec2_appspec_valid(self):
        appspec = {
            "version": "0.0",
            "os": "linux",
            "files": [{"source": "/", "destination": "/var/www/html"}]
        }
        self.assertTrue(AppSpecValidator.validate_ec2_appspec(appspec))

    def test_validate_ec2_appspec_missing_version(self):
        appspec = {"os": "linux", "files": []}
        with self.assertRaises(ValueError) as context:
            AppSpecValidator.validate_ec2_appspec(appspec)
        self.assertIn("version", str(context.exception))

    def test_validate_ec2_appspec_missing_os(self):
        appspec = {"version": "0.0", "files": []}
        with self.assertRaises(ValueError) as context:
            AppSpecValidator.validate_ec2_appspec(appspec)
        self.assertIn("os", str(context.exception))

    def test_validate_ec2_appspec_missing_files(self):
        appspec = {"version": "0.0", "os": "linux"}
        with self.assertRaises(ValueError) as context:
            AppSpecValidator.validate_ec2_appspec(appspec)
        self.assertIn("files", str(context.exception))

    def test_validate_ecs_appspec_valid(self):
        appspec = {
            "version": "0.0",
            "resources": [{"name": "my-task"}],
            "hooks": {}
        }
        self.assertTrue(AppSpecValidator.validate_ecs_appspec(appspec))

    def test_validate_ecs_appspec_missing_resources(self):
        appspec = {"version": "0.0", "hooks": {}}
        with self.assertRaises(ValueError):
            AppSpecValidator.validate_ecs_appspec(appspec)

    def test_validate_lambda_appspec_valid(self):
        appspec = {
            "version": "0.0",
            "resources": ["my-function"],
            "hooks": {}
        }
        self.assertTrue(AppSpecValidator.validate_lambda_appspec(appspec))

    def test_validate_yaml_valid(self):
        yaml_content = "version: 0.0\nos: linux\nfiles: []"
        result = AppSpecValidator.validate_yaml(yaml_content)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["version"], "0.0")

    def test_validate_yaml_invalid(self):
        yaml_content = "invalid: yaml: content:"
        with self.assertRaises(ValueError):
            AppSpecValidator.validate_yaml(yaml_content)

    def test_validate_json_valid(self):
        json_content = '{"version": "0.0", "os": "linux"}'
        result = AppSpecValidator.validate_json(json_content)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["version"], "0.0")

    def test_validate_json_invalid(self):
        json_content = '{"invalid": json}'
        with self.assertRaises(ValueError):
            AppSpecValidator.validate_json(json_content)


class TestCodeDeployIntegrationInit(unittest.TestCase):
    """Test CodeDeployIntegration initialization"""

    def test_init_with_defaults(self):
        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', False):
            # Re-import to reset module state
            integration = CodeDeployIntegration()
            self.assertEqual(integration.region_name, "us-east-1")
            self.assertIsNone(integration.aws_access_key_id)
            self.assertIsNone(integration.aws_secret_access_key)

    def test_init_with_custom_params(self):
        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', False):
            integration = CodeDeployIntegration(
                aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
                aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                region_name="us-west-2",
                endpoint_url="http://localhost:4566"
            )
            self.assertEqual(integration.region_name, "us-west-2")
            self.assertEqual(integration.aws_access_key_id, "AKIAIOSFODNN7EXAMPLE")
            self.assertEqual(integration.endpoint_url, "http://localhost:4566")

    def test_init_with_preconfigured_clients(self):
        mock_codedeploy = MagicMock()
        mock_cloudwatch = MagicMock()
        mock_events = MagicMock()
        mock_s3 = MagicMock()
        mock_iam = MagicMock()

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', False):
            integration = CodeDeployIntegration(
                codedeploy_client=mock_codedeploy,
                cloudwatch_client=mock_cloudwatch,
                events_client=mock_events,
                s3_client=mock_s3,
                iam_client=mock_iam
            )
            self.assertEqual(integration._codedeploy_client, mock_codedeploy)
            self.assertEqual(integration._cloudwatch_client, mock_cloudwatch)
            self.assertEqual(integration._events_client, mock_events)


class TestCodeDeployIntegrationApplicationManagement(unittest.TestCase):
    """Test CodeDeployIntegration application management"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.create_application.return_value = {
            'applicationId': 'app-123'
        }
        self.mock_codedeploy.get_application.return_value = {
            'application': {
                'applicationId': 'app-123',
                'applicationName': 'my-app',
                'computePlatform': 'EC2',
                'linkedToGitHub': False,
                'created': '2024-01-01T00:00:00Z',
                'lastModified': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_codedeploy.list_applications.return_value = {
            'applications': ['app-1', 'app-2']
        }
        self.mock_codedeploy.delete_application.return_value = {}

        self.mock_cloudwatch = MagicMock()
        self.mock_events = MagicMock()
        self.mock_s3 = MagicMock()
        self.mock_iam = MagicMock()

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy,
                cloudwatch_client=self.mock_cloudwatch,
                events_client=self.mock_events,
                s3_client=self.mock_s3,
                iam_client=self.mock_iam
            )

    def test_create_application(self):
        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            result = self.integration.create_application(
                application_name="my-app",
                compute_platform=ComputePlatform.EC2
            )

        self.assertIsInstance(result, ApplicationInfo)
        self.assertEqual(result.application_name, "my-app")
        self.mock_codedeploy.create_application.assert_called_once()

    def test_create_application_without_boto3(self):
        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', False):
            integration_no_boto3 = CodeDeployIntegration(
                codedeploy_client=None
            )
            with self.assertRaises(RuntimeError) as context:
                integration_no_boto3.create_application(application_name="my-app")
            self.assertIn("boto3 is required", str(context.exception))

    def test_get_application(self):
        result = self.integration.get_application(application_name="my-app")

        self.assertIsInstance(result, ApplicationInfo)
        self.assertEqual(result.application_name, "my-app")
        self.mock_codedeploy.get_application.assert_called_once_with(
            applicationName="my-app"
        )

    def test_get_application_from_cache(self):
        # First call to populate cache
        self.integration._applications_cache["cached-app"] = ApplicationInfo(
            application_id="cached-123",
            application_name="cached-app",
            compute_platform="EC2",
            linked_to_github=False,
            created="2024-01-01T00:00:00Z",
            last_modified="2024-01-01T00:00:00Z"
        )

        result = self.integration.get_application(application_name="cached-app")

        # Should return from cache without calling API
        self.assertEqual(result.application_name, "cached-app")
        self.mock_codedeploy.get_application.assert_not_called()

    def test_list_applications(self):
        result = self.integration.list_applications()

        self.mock_codedeploy.list_applications.assert_called_once()
        self.assertIsInstance(result, list)

    def test_delete_application(self):
        result = self.integration.delete_application(application_name="my-app")

        self.assertTrue(result)
        self.mock_codedeploy.delete_application.assert_called_once_with(
            applicationName="my-app"
        )

    def test_add_tags_to_application(self):
        tags = {"Environment": "Production", "Team": "DevOps"}
        self.mock_codedeploy.tag_resources.return_value = {}

        result = self.integration.add_tags_to_application(
            application_name="my-app",
            tags=tags
        )

        self.assertTrue(result)
        self.mock_codedeploy.tag_resources.assert_called_once()


class TestCodeDeployIntegrationDeploymentGroup(unittest.TestCase):
    """Test CodeDeployIntegration deployment group management"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.create_deployment_group.return_value = {
            'deploymentGroupId': 'dg-123',
            'deploymentGroupName': 'my-dg'
        }
        self.mock_codedeploy.get_deployment_group.return_value = {
            'deploymentGroup': {
                'deploymentGroupId': 'dg-123',
                'deploymentGroupName': 'my-dg',
                'applicationName': 'my-app',
                'deploymentConfigName': 'CodeDeployDefault.OneAtATime',
                'serviceRoleArn': 'arn:aws:iam::123456789012:role/my-role',
                'created': '2024-01-01T00:00:00Z',
                'lastModified': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_codedeploy.list_deployment_groups.return_value = {
            'deploymentGroups': ['my-dg']
        }

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy
            )

    def test_create_deployment_group_basic(self):
        result = self.integration.create_deployment_group(
            application_name="my-app",
            deployment_group_name="my-dg",
            service_role_arn="arn:aws:iam::123456789012:role/my-role"
        )

        self.assertIsInstance(result, DeploymentGroupInfo)
        self.assertEqual(result.deployment_group_name, "my-dg")
        self.mock_codedeploy.create_deployment_group.assert_called_once()

    def test_create_deployment_group_with_ec2_filters(self):
        ec2_filters = [EC2TagFilter(key="Name", value="web-server")]

        result = self.integration.create_deployment_group(
            application_name="my-app",
            deployment_group_name="my-dg",
            ec2_tag_filters=ec2_filters,
            service_role_arn="arn:aws:iam::123456789012:role/my-role"
        )

        self.assertIsInstance(result, DeploymentGroupInfo)
        call_args = self.mock_codedeploy.create_deployment_group.call_args
        self.assertIn('ec2TagSet', call_args.kwargs)

    def test_create_deployment_group_with_auto_rollback(self):
        rollback_config = AutoRollbackConfig(
            enabled=True,
            events=["DEPLOYMENT_FAILURE"]
        )

        result = self.integration.create_deployment_group(
            application_name="my-app",
            deployment_group_name="my-dg",
            auto_rollback_config=rollback_config,
            service_role_arn="arn:aws:iam::123456789012:role/my-role"
        )

        call_args = self.mock_codedeploy.create_deployment_group.call_args
        self.assertIn('autoRollbackConfiguration', call_args.kwargs)

    def test_get_deployment_group(self):
        result = self.integration.get_deployment_group(
            application_name="my-app",
            deployment_group_name="my-dg"
        )

        self.assertIsInstance(result, DeploymentGroupInfo)
        self.assertEqual(result.deployment_group_name, "my-dg")

    def test_list_deployment_groups(self):
        result = self.integration.list_deployment_groups(
            application_name="my-app"
        )

        self.mock_codedeploy.list_deployment_groups.assert_called_once()
        self.assertIsInstance(result, list)


class TestCodeDeployIntegrationDeployment(unittest.TestCase):
    """Test CodeDeployIntegration deployment management"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.create_deployment.return_value = {
            'deploymentId': 'dep-123'
        }
        self.mock_codedeploy.get_deployment.return_value = {
            'deploymentInfo': {
                'deploymentId': 'dep-123',
                'applicationName': 'my-app',
                'deploymentGroupName': 'my-dg',
                'status': 'Succeeded',
                'createTime': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_codedeploy.list_deployments.return_value = {
            'deployments': ['dep-1', 'dep-2']
        }
        self.mock_codedeploy.stop_deployment.return_value = {
            'deploymentId': 'dep-123',
            'status': 'Stopped'
        }

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy
            )

    def test_create_deployment(self):
        s3_location = S3Location(bucket="my-bucket", key="my-key")

        result = self.integration.create_deployment(
            application_name="my-app",
            deployment_group_name="my-dg",
            revision=RevisionLocation(
                revision_type=RevisionLocationType.S3,
                s3_location=s3_location
            )
        )

        self.assertIsInstance(result, DeploymentInfo)
        self.assertEqual(result.deployment_id, "dep-123")
        self.mock_codedeploy.create_deployment.assert_called_once()

    def test_create_deployment_with_description(self):
        result = self.integration.create_deployment(
            application_name="my-app",
            deployment_group_name="my-dg",
            description="My deployment description"
        )

        self.assertIsInstance(result, DeploymentInfo)
        call_args = self.mock_codedeploy.create_deployment.call_args
        self.assertEqual(call_args.kwargs.get('description'), "My deployment description")

    def test_get_deployment(self):
        result = self.integration.get_deployment(deployment_id="dep-123")

        self.assertIsInstance(result, DeploymentInfo)
        self.assertEqual(result.deployment_id, "dep-123")

    def test_list_deployments(self):
        result = self.integration.list_deployments(
            application_name="my-app"
        )

        self.mock_codedeploy.list_deployments.assert_called_once()
        self.assertIsInstance(result, list)

    def test_stop_deployment(self):
        result = self.integration.stop_deployment(
            deployment_id="dep-123"
        )

        self.assertTrue(result)
        self.mock_codedeploy.stop_deployment.assert_called_once()


class TestCodeDeployIntegrationDeploymentConfig(unittest.TestCase):
    """Test CodeDeployIntegration deployment configuration"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.create_deployment_config.return_value = {
            'deploymentConfigId': 'config-123',
            'deploymentConfigName': 'my-config'
        }
        self.mock_codedeploy.get_deployment_config.return_value = {
            'deploymentConfigInfo': {
                'deploymentConfigId': 'config-123',
                'deploymentConfigName': 'my-config',
                'deploymentConfigArn': 'arn:aws:codedeploy:us-east-1:123456789012:deploymentconfig:my-config',
                'created': '2024-01-01T00:00:00Z'
            }
        }
        self.mock_codedeploy.list_deployment_configs.return_value = {
            'deploymentConfigs': ['config-1', 'config-2']
        }

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy
            )

    def test_create_deployment_config(self):
        result = self.integration.create_deployment_config(
            deployment_config_name="my-config",
            traffic_routing_type=TrafficRoutingType.TIME_BASED_CANARY,
            first_bake_time_minutes=15,
            second_bake_time_minutes=30
        )

        self.assertIsInstance(result, DeploymentConfigInfo)
        self.assertEqual(result.deployment_config_name, "my-config")

    def test_get_deployment_config(self):
        result = self.integration.get_deployment_config(
            deployment_config_name="my-config"
        )

        self.assertIsInstance(result, DeploymentConfigInfo)

    def test_list_deployment_configs(self):
        result = self.integration.list_deployment_configs()

        self.assertIsInstance(result, list)


class TestCodeDeployIntegrationInstance(unittest.TestCase):
    """Test CodeDeployIntegration instance management"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.list_deployment_instances.return_value = {
            'instancesList': ['i-123', 'i-456']
        }
        self.mock_codedeploy.get_deployment_instance.return_value = {
            'instanceSummary': {
                'instanceId': 'i-123',
                'deploymentId': 'dep-123',
                'status': 'Succeeded',
                'lastUpdated': '2024-01-01T00:00:00Z'
            }
        }

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy
            )

    def test_list_deployment_instances(self):
        result = self.integration.list_deployment_instances(
            deployment_id="dep-123"
        )

        self.mock_codedeploy.list_deployment_instances.assert_called_once()
        self.assertIsInstance(result, list)

    def test_get_deployment_instance(self):
        result = self.integration.get_deployment_instance(
            deployment_id="dep-123",
            instance_id="i-123"
        )

        self.assertIsInstance(result, InstanceInfo)
        self.assertEqual(result.instance_id, "i-123")


class TestCodeDeployIntegrationAppSpec(unittest.TestCase):
    """Test CodeDeployIntegration AppSpec management"""

    def setUp(self):
        self.mock_s3 = MagicMock()
        self.mock_s3.put_object.return_value = {}

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                s3_client=self.mock_s3
            )

    def test_create_appspec_file_ec2(self):
        appspec = {
            "version": "0.0",
            "os": "linux",
            "files": [
                {
                    "source": "/",
                    "destination": "/var/www/html"
                }
            ]
        }

        result = self.integration.create_appspec_file(
            appspec=appspec,
            bucket_name="my-bucket",
            object_key="appspec.yaml"
        )

        self.mock_s3.put_object.assert_called_once()

    def test_register_appspec_revision(self):
        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration._codedeploy_client = MagicMock()
            self.integration._codedeploy_client.register_application_revision.return_value = {}

            result = self.integration.register_appspec_revision(
                application_name="my-app",
                s3_location=S3Location(bucket="my-bucket", key="appspec.yaml")
            )

            self.integration._codedeploy_client.register_application_revision.assert_called_once()


class TestCodeDeployIntegrationRollback(unittest.TestCase):
    """Test CodeDeployIntegration rollback operations"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.stop_deployment.return_value = {
            'deploymentId': 'dep-123',
            'status': 'Stopped'
        }

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy
            )

    def test_rollback_deployment(self):
        result = self.integration.rollback_deployment(
            deployment_id="dep-123"
        )

        self.assertTrue(result)

    def test_enable_auto_rollback(self):
        result = self.integration.enable_auto_rollback(
            application_name="my-app",
            deployment_group_name="my-dg",
            events=["DEPLOYMENT_FAILURE"]
        )

        self.mock_codedeploy.update_deployment_group.assert_called()

    def test_disable_auto_rollback(self):
        result = self.integration.disable_auto_rollback(
            application_name="my-app",
            deployment_group_name="my-dg"
        )

        self.mock_codedeploy.update_deployment_group.assert_called()


class TestCodeDeployIntegrationCloudWatchEvents(unittest.TestCase):
    """Test CodeDeployIntegration CloudWatch events"""

    def setUp(self):
        self.mock_events = MagicMock()
        self.mock_events.put_rule.return_value = {
            'RuleArn': 'arn:aws:events:us-east-1:123456789012:rule/my-rule'
        }
        self.mock_events.put_targets.return_value = {}

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                events_client=self.mock_events
            )

    def test_create_deployment_trigger(self):
        result = self.integration.create_deployment_trigger(
            trigger_name="my-trigger",
            target_arn="arn:aws:sns:us-east-1:123456789012:my-topic",
            events=["DeploymentInProgress", "DeploymentSucceeded"]
        )

        self.assertTrue(result)
        self.mock_events.put_rule.assert_called()
        self.mock_events.put_targets.assert_called()

    def test_delete_deployment_trigger(self):
        self.mock_events.delete_rule.return_value = {}
        self.mock_events.remove_targets.return_value = {}

        result = self.integration.delete_deployment_trigger(
            trigger_name="my-trigger"
        )

        self.assertTrue(result)


class TestCodeDeployIntegrationRetry(unittest.TestCase):
    """Test CodeDeployIntegration retry operations"""

    def setUp(self):
        self.mock_codedeploy = MagicMock()
        self.mock_codedeploy.create_deployment.return_value = {
            'deploymentId': 'dep-retry-123'
        }

        with patch('src.workflow_aws_codedeploy.BOTO3_AVAILABLE', True):
            self.integration = CodeDeployIntegration(
                codedeploy_client=self.mock_codedeploy
            )

    def test_retry_deployment(self):
        result = self.integration.retry_deployment(
            application_name="my-app",
            deployment_group_name="my-dg",
            original_deployment_id="dep-original"
        )

        self.assertIsInstance(result, DeploymentInfo)
        self.mock_codedeploy.create_deployment.assert_called()


class TestBoto3Availability(unittest.TestCase):
    """Test BOTO3_AVAILABLE flag"""

    def test_boto3_available_flag(self):
        # This tests the module-level flag
        self.assertIn('BOTO3_AVAILABLE', dir())


if __name__ == '__main__':
    unittest.main()
