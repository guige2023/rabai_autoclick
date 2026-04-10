"""
Tests for workflow_aws_appconfig module
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

# Create mock boto3 module before importing workflow_aws_appconfig
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
import src.workflow_aws_appconfig as _appconfig_module

# Extract classes
AppConfigIntegration = _appconfig_module.AppConfigIntegration
ConfigType = _appconfig_module.ConfigType
ValidatorType = _appconfig_module.ValidatorType
ExtensionPoint = _appconfig_module.ExtensionPoint
DeploymentStrategyType = _appconfig_module.DeploymentStrategyType
DeploymentStatus = _appconfig_module.DeploymentStatus
EnvironmentStatus = _appconfig_module.EnvironmentStatus
AppConfigValidator = _appconfig_module.AppConfigValidator
DeploymentStrategy = _appconfig_module.DeploymentStrategy
ExtensionParameter = _appconfig_module.ExtensionParameter
HostedConfigurationVersion = _appconfig_module.HostedConfigurationVersion


class TestConfigType(unittest.TestCase):
    """Test ConfigType enum"""

    def test_config_type_values(self):
        self.assertEqual(ConfigType.FREE_FORM.value, "FreeForm")
        self.assertEqual(ConfigType.FEATURE_FLAGS.value, "FeatureFlags")
        self.assertEqual(ConfigType.AWS_ECS_CONTAINERS.value, "AWS.EC2Containers.Service.Configuration")
        self.assertEqual(ConfigType.AWS_ECS_TASK_DEFINITION.value, "AWS.ECS.TaskDefinition")
        self.assertEqual(ConfigType.AWS_LAMBDA_ALIAS.value, "AWS.Lambda.Function")
        self.assertEqual(ConfigType.AWS_LAMBDA_ARNS.value, "AWS.Lambda.PreviousVersion")
        self.assertEqual(ConfigType.AWS_PARAMETER_STORE.value, "AWS.ParameterStore")


class TestValidatorType(unittest.TestCase):
    """Test ValidatorType enum"""

    def test_validator_type_values(self):
        self.assertEqual(ValidatorType.JSON_SCHEMA.value, "JSON_SCHEMA")
        self.assertEqual(ValidatorType.LAMBDA.value, "LAMBDA")
        self.assertEqual(ValidatorType.EXTERNAL_PARAMETER_STORE.value, "EXTERNAL_PARAMETER_STORE")


class TestExtensionPoint(unittest.TestCase):
    """Test ExtensionPoint enum"""

    def test_extension_point_values(self):
        self.assertEqual(ExtensionPoint.PRE_CREATE_HOSTED_CONFIGURATION_VERSION.value, "PRE_CREATE_HOSTED_CONFIGURATION_VERSION")
        self.assertEqual(ExtensionPoint.PRE_START_DEPLOYMENT.value, "PRE_START_DEPLOYMENT")
        self.assertEqual(ExtensionPoint.PRE_STOP_DEPLOYMENT.value, "PRE_STOP_DEPLOYMENT")
        self.assertEqual(ExtensionPoint.ON_DEPLOYMENT_START.value, "ON_DEPLOYMENT_START")
        self.assertEqual(ExtensionPoint.ON_DEPLOYMENT_COMPLETE.value, "ON_DEPLOYMENT_COMPLETE")
        self.assertEqual(ExtensionPoint.ON_DEPLOYMENT_ROLLED_BACK.value, "ON_DEPLOYMENT_ROLLBACK")
        self.assertEqual(ExtensionPoint.ON_DEPLOYMENT_BAKING.value, "ON_DEPLOYMENT_BAKING")
        self.assertEqual(ExtensionPoint.ON_DEPLOYMENT_TERMINATION_CHECK.value, "ON_DEPLOYMENT_TERMINATION_CHECK")
        self.assertEqual(ExtensionPoint.ON_CREATE_HOSTED_CONFIGURATION_VERSION.value, "ON_CREATE_HOSTED_CONFIGURATION_VERSION")


class TestDeploymentStrategyType(unittest.TestCase):
    """Test DeploymentStrategyType enum"""

    def test_deployment_strategy_type_values(self):
        self.assertEqual(DeploymentStrategyType.LINEAR_50_PERCENT_EVERY_30_SECONDS.value, "AppConfig.Linear.50PercentEvery30Seconds")
        self.assertEqual(DeploymentStrategyType.LINEAR_10_PERCENT_EVERY_10_MINUTES.value, "AppConfig.Linear.10PercentEvery10Minutes")
        self.assertEqual(DeploymentStrategyType.LINEAR_20_PERCENT_EVERY_5_MINUTES.value, "AppConfig.Linear.20PercentEvery5Minutes")
        self.assertEqual(DeploymentStrategyType.EXPONENTIAL_50_PERCENT_EVERY_30_SECONDS.value, "AppConfig.Exponential.50PercentEvery30Seconds")
        self.assertEqual(DeploymentStrategyType.CANARY_10_PERCENT_20_MINUTES.value, "AppConfig.Canary10Percent20Minutes")
        self.assertEqual(DeploymentStrategyType.ALL_AT_ONCE.value, "AppConfig.AllAtOnce")


class TestDeploymentStatus(unittest.TestCase):
    """Test DeploymentStatus enum"""

    def test_deployment_status_values(self):
        self.assertEqual(DeploymentStatus.PENDING.value, "PENDING")
        self.assertEqual(DeploymentStatus.IN_PROGRESS.value, "IN_PROGRESS")
        self.assertEqual(DeploymentStatus.DEPLOYING.value, "DEPLOYING")
        self.assertEqual(DeploymentStatus.COMPLETE.value, "COMPLETE")
        self.assertEqual(DeploymentStatus.BAKING.value, "BAKING")
        self.assertEqual(DeploymentStatus.ROLLED_BACK.value, "ROLLED_BACK")
        self.assertEqual(DeploymentStatus.ABORTED.value, "ABORTED")
        self.assertEqual(DeploymentStatus.INVALID.value, "INVALID")


class TestEnvironmentStatus(unittest.TestCase):
    """Test EnvironmentStatus enum"""

    def test_environment_status_values(self):
        self.assertEqual(EnvironmentStatus.PREPARED.value, "PREPARED")
        self.assertEqual(EnvironmentStatus.DEPLOYING.value, "DEPLOYING")
        self.assertEqual(EnvironmentStatus.ROLLED_BACK.value, "ROLLED_BACK")
        self.assertEqual(EnvironmentStatus.READY_FOR_DEPLOYMENT.value, "READY_FOR_DEPLOYMENT")
        self.assertEqual(EnvironmentStatus.DEPLOYMENT_ABORTED.value, "DEPLOYMENT_ABORTED")


class TestAppConfigValidator(unittest.TestCase):
    """Test AppConfigValidator dataclass"""

    def test_validator_creation(self):
        validator = AppConfigValidator(
            validator_type=ValidatorType.JSON_SCHEMA,
            content='{"type":"object","properties":{}}'
        )
        self.assertEqual(validator.validator_type, ValidatorType.JSON_SCHEMA)
        self.assertIn("type", validator.content)


class TestDeploymentStrategy(unittest.TestCase):
    """Test DeploymentStrategy dataclass"""

    def test_deployment_strategy_creation(self):
        strategy = DeploymentStrategy(
            name="MyStrategy",
            description="Test deployment strategy",
            deployment_duration_in_minutes=10,
            growth_factor=10.0,
            final_percentage=100.0
        )
        self.assertEqual(strategy.name, "MyStrategy")
        self.assertEqual(strategy.deployment_duration_in_minutes, 10)
        self.assertEqual(strategy.growth_factor, 10.0)


class TestExtensionParameter(unittest.TestCase):
    """Test ExtensionParameter dataclass"""

    def test_extension_parameter_creation(self):
        param = ExtensionParameter(
            name="param1",
            required=True,
            description="Test parameter",
            pattern="^[a-z]+$"
        )
        self.assertEqual(param.name, "param1")
        self.assertTrue(param.required)


class TestHostedConfigurationVersion(unittest.TestCase):
    """Test HostedConfigurationVersion dataclass"""

    def test_hosted_configuration_version_creation(self):
        now = datetime.utcnow()
        version = HostedConfigurationVersion(
            version_number=1,
            content='{"key":"value"}',
            content_type="application/json",
            description="Test version",
            created_at=now
        )
        self.assertEqual(version.version_number, 1)
        self.assertEqual(version.content, '{"key":"value"}')


class TestAppConfigIntegration(unittest.TestCase):
    """Test AppConfigIntegration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_init_default(self):
        integration = AppConfigIntegration()
        self.assertEqual(integration.region, "us-east-1")
        self.assertEqual(integration.tags, {})

    def test_init_custom(self):
        integration = AppConfigIntegration(
            region="us-west-2",
            profile_name="my-profile",
            kms_key_id="kms-key-123",
            tags={"Environment": "test"},
            notifications_topic_arn="arn:aws:sns:us-west-2:123456789:topic"
        )
        self.assertEqual(integration.region, "us-west-2")
        self.assertEqual(integration.profile_name, "my-profile")
        self.assertEqual(integration.kms_key_id, "kms-key-123")
        self.assertEqual(integration.tags, {"Environment": "test"})
        self.assertEqual(integration.notifications_topic_arn, "arn:aws:sns:us-west-2:123456789:topic")

    def test_create_application(self):
        self.mock_client.create_application.return_value = {
            "Id": "app123",
            "Name": "my-app",
            "Description": "Test application"
        }

        result = self.integration.create_application(
            name="my-app",
            description="Test application"
        )

        self.assertEqual(result["Id"], "app123")
        self.assertEqual(result["Name"], "my-app")
        self.mock_client.create_application.assert_called_once()

    def test_create_application_with_tags(self):
        self.mock_client.create_application.return_value = {
            "Id": "app123",
            "Name": "my-app"
        }

        self.integration.create_application(
            name="my-app",
            tags={"Team": "dev"}
        )

        call_kwargs = self.mock_client.create_application.call_args[1]
        self.assertEqual(call_kwargs["Tags"], {"Team": "dev"})

    def test_get_application(self):
        self.mock_client.get_application.return_value = {
            "Id": "app123",
            "Name": "my-app"
        }

        result = self.integration.get_application(application_id="app123")

        self.assertEqual(result["Id"], "app123")
        self.mock_client.get_application.assert_called_once_with(ApplicationId="app123")

    def test_list_applications(self):
        self.mock_client.list_applications.return_value = {
            "Items": [
                {"Id": "app1", "Name": "app1"},
                {"Id": "app2", "Name": "app2"}
            ],
            "NextToken": None
        }

        result = self.integration.list_applications()

        self.assertEqual(len(result["Items"]), 2)
        self.mock_client.list_applications.assert_called_once()

    def test_list_applications_with_pagination(self):
        self.mock_client.list_applications.return_value = {
            "Items": [{"Id": "app1"}],
            "NextToken": "token123"
        }

        result = self.integration.list_applications(next_token="token123")

        self.assertEqual(result["NextToken"], "token123")
        self.mock_client.list_applications.assert_called_once_with(
            MaxResults=50,
            NextToken="token123"
        )

    def test_update_application(self):
        self.mock_client.update_application.return_value = {
            "Id": "app123",
            "Name": "my-app-updated",
            "Description": "Updated description"
        }

        result = self.integration.update_application(
            application_id="app123",
            name="my-app-updated"
        )

        self.assertEqual(result["Name"], "my-app-updated")
        self.mock_client.update_application.assert_called_once()

    def test_delete_application(self):
        self.mock_client.delete_application.return_value = {}

        self.integration.delete_application(application_id="app123")

        self.mock_client.delete_application.assert_called_once_with(ApplicationId="app123")


class TestAppConfigIntegrationEnvironment(unittest.TestCase):
    """Test AppConfigIntegration environment management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_environment(self):
        self.mock_client.create_environment.return_value = {
            "Id": "env123",
            "ApplicationId": "app123",
            "Name": "test-env"
        }

        result = self.integration.create_environment(
            application_id="app123",
            name="test-env",
            description="Test environment"
        )

        self.assertEqual(result["Id"], "env123")
        self.assertEqual(result["Name"], "test-env")
        self.mock_client.create_environment.assert_called_once()

    def test_create_environment_with_monitors(self):
        self.mock_client.create_environment.return_value = {
            "Id": "env123",
            "ApplicationId": "app123",
            "Name": "test-env"
        }

        monitors = [
            {"AlarmArn": "arn:aws:cloudwatch:us-east-1:123456789:alarm:my-alarm"}
        ]

        self.integration.create_environment(
            application_id="app123",
            name="test-env",
            monitors=monitors
        )

        call_kwargs = self.mock_client.create_environment.call_args[1]
        self.assertEqual(call_kwargs["Monitors"], monitors)

    def test_get_environment(self):
        self.mock_client.get_environment.return_value = {
            "Id": "env123",
            "ApplicationId": "app123",
            "Name": "test-env",
            "State": "READY_FOR_DEPLOYMENT"
        }

        result = self.integration.get_environment(
            application_id="app123",
            environment_id="env123"
        )

        self.assertEqual(result["Id"], "env123")
        self.assertEqual(result["State"], "READY_FOR_DEPLOYMENT")

    def test_list_environments(self):
        self.mock_client.list_environments.return_value = {
            "Items": [
                {"Id": "env1", "Name": "dev"},
                {"Id": "env2", "Name": "prod"}
            ]
        }

        result = self.integration.list_environments(application_id="app123")

        self.assertEqual(len(result["Items"]), 2)

    def test_update_environment(self):
        self.mock_client.update_environment.return_value = {
            "Id": "env123",
            "Name": "updated-env"
        }

        result = self.integration.update_environment(
            application_id="app123",
            environment_id="env123",
            name="updated-env"
        )

        self.assertEqual(result["Name"], "updated-env")

    def test_delete_environment(self):
        self.mock_client.delete_environment.return_value = {}

        self.integration.delete_environment(
            application_id="app123",
            environment_id="env123"
        )

        self.mock_client.delete_environment.assert_called_once()


class TestAppConfigIntegrationConfigurationProfile(unittest.TestCase):
    """Test AppConfigIntegration configuration profile management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_configuration_profile(self):
        self.mock_client.create_configuration_profile.return_value = {
            "Id": "prof123",
            "ApplicationId": "app123",
            "Name": "my-profile",
            "Type": "FreeForm"
        }

        result = self.integration.create_configuration_profile(
            application_id="app123",
            name="my-profile",
            config_type=ConfigType.FREE_FORM
        )

        self.assertEqual(result["Id"], "prof123")
        self.assertEqual(result["Type"], "FreeForm")

    def test_create_configuration_profile_with_validators(self):
        self.mock_client.create_configuration_profile.return_value = {
            "Id": "prof123",
            "ApplicationId": "app123",
            "Name": "my-profile"
        }

        validators = [
            {"ValidatorType": "JSON_SCHEMA", "Content": '{"type":"object"}'}
        ]

        self.integration.create_configuration_profile(
            application_id="app123",
            name="my-profile",
            validators=validators
        )

        call_kwargs = self.mock_client.create_configuration_profile.call_args[1]
        self.assertEqual(call_kwargs["Validators"], validators)

    def test_create_configuration_profile_with_location_uri(self):
        self.mock_client.create_configuration_profile.return_value = {
            "Id": "prof123",
            "ApplicationId": "app123",
            "Name": "my-profile",
            "LocationUri": "s3://my-bucket/config.json"
        }

        result = self.integration.create_configuration_profile(
            application_id="app123",
            name="my-profile",
            location_uri="s3://my-bucket/config.json"
        )

        self.assertEqual(result["LocationUri"], "s3://my-bucket/config.json")

    def test_get_configuration_profile(self):
        self.mock_client.get_configuration_profile.return_value = {
            "Id": "prof123",
            "ApplicationId": "app123",
            "Name": "my-profile"
        }

        result = self.integration.get_configuration_profile(
            application_id="app123",
            configuration_profile_id="prof123"
        )

        self.assertEqual(result["Id"], "prof123")

    def test_list_configuration_profiles(self):
        self.mock_client.list_configuration_profiles.return_value = {
            "Items": [
                {"Id": "prof1", "Name": "profile1"},
                {"Id": "prof2", "Name": "profile2"}
            ]
        }

        result = self.integration.list_configuration_profiles(application_id="app123")

        self.assertEqual(len(result["Items"]), 2)

    def test_update_configuration_profile(self):
        self.mock_client.update_configuration_profile.return_value = {
            "Id": "prof123",
            "Name": "updated-profile"
        }

        result = self.integration.update_configuration_profile(
            application_id="app123",
            configuration_profile_id="prof123",
            name="updated-profile"
        )

        self.assertEqual(result["Name"], "updated-profile")

    def test_delete_configuration_profile(self):
        self.mock_client.delete_configuration_profile.return_value = {}

        self.integration.delete_configuration_profile(
            application_id="app123",
            configuration_profile_id="prof123"
        )

        self.mock_client.delete_configuration_profile.assert_called_once()


class TestAppConfigIntegrationDeployment(unittest.TestCase):
    """Test AppConfigIntegration deployment management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_deployment(self):
        self.mock_client.start_deployment.return_value = {
            "DeploymentId": "dep123",
            "ConfigurationProfileId": "prof123",
            "EnvironmentId": "env123",
            "State": "DEPLOYING"
        }

        result = self.integration.create_deployment(
            application_id="app123",
            environment_id="env123",
            configuration_profile_id="prof123",
            deployment_strategy_id="ds123"
        )

        self.assertEqual(result["DeploymentId"], "dep123")
        self.assertEqual(result["State"], "DEPLOYING")

    def test_get_deployment(self):
        self.mock_client.get_deployment.return_value = {
            "DeploymentId": "dep123",
            "State": "COMPLETE"
        }

        result = self.integration.get_deployment(
            application_id="app123",
            environment_id="env123",
            deployment_id="dep123"
        )

        self.assertEqual(result["State"], "COMPLETE")

    def test_list_deployments(self):
        self.mock_client.list_deployments.return_value = {
            "Items": [
                {"DeploymentId": "dep1", "State": "COMPLETE"},
                {"DeploymentId": "dep2", "State": "IN_PROGRESS"}
            ]
        }

        result = self.integration.list_deployments(
            application_id="app123",
            environment_id="env123"
        )

        self.assertEqual(len(result["Items"]), 2)

    def test_stop_deployment(self):
        self.mock_client.stop_deployment.return_value = {
            "DeploymentId": "dep123",
            "State": "ABORTED"
        }

        result = self.integration.stop_deployment(
            application_id="app123",
            environment_id="env123",
            deployment_id="dep123"
        )

        self.assertEqual(result["State"], "ABORTED")


class TestAppConfigIntegrationDeploymentStrategy(unittest.TestCase):
    """Test AppConfigIntegration deployment strategy management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_deployment_strategy(self):
        self.mock_client.create_deployment_strategy.return_value = {
            "Id": "ds123",
            "Name": "my-strategy",
            "Type": "LINEAR",
            "DeploymentDurationInMinutes": 10,
            "GrowthFactor": 10.0
        }

        result = self.integration.create_deployment_strategy(
            name="my-strategy",
            deployment_duration_in_minutes=10,
            growth_factor=10.0,
            final_percentage=100.0
        )

        self.assertEqual(result["Id"], "ds123")
        self.assertEqual(result["DeploymentDurationInMinutes"], 10)

    def test_get_deployment_strategy(self):
        self.mock_client.get_deployment_strategy.return_value = {
            "Id": "ds123",
            "Name": "my-strategy"
        }

        result = self.integration.get_deployment_strategy(deployment_strategy_id="ds123")

        self.assertEqual(result["Id"], "ds123")

    def test_list_deployment_strategies(self):
        self.mock_client.list_deployment_strategies.return_value = {
            "Items": [
                {"Id": "ds1", "Name": "strategy1"},
                {"Id": "ds2", "Name": "strategy2"}
            ]
        }

        result = self.integration.list_deployment_strategies()

        self.assertEqual(len(result["Items"]), 2)

    def test_update_deployment_strategy(self):
        self.mock_client.update_deployment_strategy.return_value = {
            "Id": "ds123",
            "Name": "updated-strategy"
        }

        result = self.integration.update_deployment_strategy(
            deployment_strategy_id="ds123",
            name="updated-strategy"
        )

        self.assertEqual(result["Name"], "updated-strategy")

    def test_delete_deployment_strategy(self):
        self.mock_client.delete_deployment_strategy.return_value = {}

        self.integration.delete_deployment_strategy(deployment_strategy_id="ds123")

        self.mock_client.delete_deployment_strategy.assert_called_once()


class TestAppConfigIntegrationHostedConfiguration(unittest.TestCase):
    """Test AppConfigIntegration hosted configuration management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_hosted_configuration_version(self):
        self.mock_client.create_hosted_configuration_version.return_value = {
            "ApplicationId": "app123",
            "ConfigurationProfileId": "prof123",
            "VersionNumber": 1,
            "Content": '{"key":"value"}',
            "ContentType": "application/json"
        }

        result = self.integration.create_hosted_configuration_version(
            application_id="app123",
            configuration_profile_id="prof123",
            content='{"key":"value"}'
        )

        self.assertEqual(result["VersionNumber"], 1)
        self.assertIn("key", result["Content"])

    def test_get_hosted_configuration_version(self):
        self.mock_client.get_hosted_configuration_version.return_value = {
            "ApplicationId": "app123",
            "ConfigurationProfileId": "prof123",
            "VersionNumber": 1,
            "Content": '{"key":"value"}'
        }

        result = self.integration.get_hosted_configuration_version(
            application_id="app123",
            configuration_profile_id="prof123",
            version_number=1
        )

        self.assertEqual(result["VersionNumber"], 1)

    def test_list_hosted_configuration_versions(self):
        self.mock_client.list_hosted_configuration_versions.return_value = {
            "Items": [
                {"VersionNumber": 1},
                {"VersionNumber": 2}
            ]
        }

        result = self.integration.list_hosted_configuration_versions(
            application_id="app123",
            configuration_profile_id="prof123"
        )

        self.assertEqual(len(result["Items"]), 2)


class TestAppConfigIntegrationExtension(unittest.TestCase):
    """Test AppConfigIntegration extension management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_create_extension(self):
        self.mock_client.create_extension.return_value = {
            "Id": "ext123",
            "Name": "my-extension",
            "Arn": "arn:aws:appconfig:us-east-1:123456789:extension/my-extension"
        }

        result = self.integration.create_extension(
            name="my-extension",
            description="Test extension",
            version="1"
        )

        self.assertEqual(result["Id"], "ext123")
        self.assertEqual(result["Name"], "my-extension")

    def test_get_extension(self):
        self.mock_client.get_extension.return_value = {
            "Id": "ext123",
            "Name": "my-extension"
        }

        result = self.integration.get_extension(extension_id="ext123")

        self.assertEqual(result["Id"], "ext123")

    def test_list_extensions(self):
        self.mock_client.list_extensions.return_value = {
            "Items": [
                {"Id": "ext1", "Name": "extension1"},
                {"Id": "ext2", "Name": "extension2"}
            ]
        }

        result = self.integration.list_extensions()

        self.assertEqual(len(result["Items"]), 2)

    def test_delete_extension(self):
        self.mock_client.delete_extension.return_value = {}

        self.integration.delete_extension(extension_id="ext123")

        self.mock_client.delete_extension.assert_called_once()

    def test_create_extension_association(self):
        self.mock_client.create_extension_association.return_value = {
            "Id": "assoc123",
            "ExtensionId": "ext123",
            "ResourceType": "Application",
            "ResourceArn": "arn:aws:appconfig:us-east-1:123456789:application/app123"
        }

        result = self.integration.create_extension_association(
            extension_id="ext123",
            resource_arn="arn:aws:appconfig:us-east-1:123456789:application/app123"
        )

        self.assertEqual(result["Id"], "assoc123")
        self.assertEqual(result["ExtensionId"], "ext123")

    def test_get_extension_association(self):
        self.mock_client.get_extension_association.return_value = {
            "Id": "assoc123",
            "ExtensionId": "ext123"
        }

        result = self.integration.get_extension_association(extension_association_id="assoc123")

        self.assertEqual(result["Id"], "assoc123")

    def test_list_extension_associations(self):
        self.mock_client.list_extension_associations.return_value = {
            "Items": [
                {"Id": "assoc1"},
                {"Id": "assoc2"}
            ]
        }

        result = self.integration.list_extension_associations(extension_id="ext123")

        self.assertEqual(len(result["Items"]), 2)

    def test_delete_extension_association(self):
        self.mock_client.delete_extension_association.return_value = {}

        self.integration.delete_extension_association(extension_association_id="assoc123")

        self.mock_client.delete_extension_association.assert_called_once()


class TestAppConfigIntegrationValidators(unittest.TestCase):
    """Test AppConfigIntegration validators"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_validate_configuration(self):
        self.mock_client.validate_configuration.return_value = {}

        # This would validate a configuration against validators
        result = self.integration.validate_configuration(
            application_id="app123",
            configuration_profile_id="prof123",
            content='{"key":"value"}'
        )

        self.assertIsNone(result)


class TestAppConfigIntegrationResourcePolicy(unittest.TestCase):
    """Test AppConfigIntegration resource policy"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_get_resource_policy(self):
        self.mock_client.get_resource_policy.return_value = {
            "Policy": '{"Version":"2012-10-17"}'
        }

        result = self.integration.get_resource_policy(resource_arn="arn:aws:appconfig:...")

        self.assertEqual(result["Policy"], '{"Version":"2012-10-17"}')

    def test_put_resource_policy(self):
        self.mock_client.put_resource_policy.return_value = {}

        result = self.integration.put_resource_policy(
            resource_arn="arn:aws:appconfig:...",
            policy='{"Version":"2012-10-17"}'
        )

        self.assertTrue(result)


class TestAppConfigIntegrationAccountWide(unittest.TestCase):
    """Test AppConfigIntegration account-wide settings"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1")
        self.integration._client = self.mock_client

    def test_get_account_wide_settings(self):
        """Test getting account-wide settings"""
        result = self.integration.get_account_wide_settings()

        self.assertIsInstance(result, dict)

    def test_update_account_wide_settings(self):
        """Test updating account-wide settings"""
        self.mock_client.update_account_wide_settings.return_value = {}

        result = self.integration.update_account_wide_settings(
            tags={"Enabled": "true"}
        )

        self.assertTrue(result)


class TestAppConfigIntegrationTags(unittest.TestCase):
    """Test AppConfigIntegration tag management"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()
        self.integration = AppConfigIntegration(region="us-east-1", tags={"Default": "tag"})
        self.integration._client = self.mock_client

    def test_tag_resource(self):
        self.mock_client.tag.return_value = {}

        result = self.integration.tag_resource(
            resource_arn="arn:aws:appconfig:...",
            tags={"Team": "dev"}
        )

        self.assertTrue(result)

    def test_untag_resource(self):
        self.mock_client.untag.return_value = {}

        result = self.integration.untag_resource(
            resource_arn="arn:aws:appconfig:...",
            tag_keys=["Team"]
        )

        self.assertTrue(result)

    def test_list_tags_for_resource(self):
        self.mock_client.list_tags_for_resource.return_value = {
            "Tags": {"Team": "dev", "Environment": "test"}
        }

        result = self.integration.list_tags_for_resource(resource_arn="arn:aws:appconfig:...")

        self.assertEqual(result["Tags"], {"Team": "dev", "Environment": "test"})


if __name__ == '__main__':
    unittest.main()
