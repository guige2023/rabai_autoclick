"""
AWS AppConfig Integration Module for Workflow System

Implements an AppConfigIntegration class with:
1. Application management: Create/manage AppConfig applications
2. Environment management: Create/manage environments
3. Configuration profile: Create/manage configuration profiles
4. Deployment: Create/manage deployments
5. Strategy: Configure deployment strategies
6. Hosted config: Hosted configuration content
7. Validators: Configure validators
8. Extensions: AppConfig extensions
9. Lambda: Lambda extensions
10. CloudWatch integration: Monitoring and metrics

Commit: 'feat(aws-appconfig): add AWS AppConfig with application management, environments, configuration profiles, deployments, deployment strategies, hosted config, validators, extensions, Lambda extensions, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import base64
import io
import os
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = None
    BotoCoreError = None


logger = logging.getLogger(__name__)


class ConfigType(Enum):
    """AppConfig configuration profile types."""
    FREE_FORM = "FreeForm"
    FEATURE_FLAGS = "FeatureFlags"
    AWS_ECS_CONTAINERS = "AWS.EC2Containers.Service.Configuration"
    AWS_ECS_TASK_DEFINITION = "AWS.ECS.TaskDefinition"
    AWS_LAMBDA_ALIAS = "AWS.Lambda.Function"
    AWS_LAMBDA_ARNS = "AWS.Lambda.PreviousVersion"
    AWS_PARAMETER_STORE = "AWS.ParameterStore"


class ValidatorType(Enum):
    """Configuration validators."""
    JSON_SCHEMA = "JSON_SCHEMA"
    LAMBDA = "LAMBDA"
    EXTERNAL_PARAMETER_STORE = "EXTERNAL_PARAMETER_STORE"


class ExtensionPoint(Enum):
    """AppConfig extension points."""
    PRE_CREATE_HOSTED_CONFIGURATION_VERSION = "PRE_CREATE_HOSTED_CONFIGURATION_VERSION"
    PRE_START_DEPLOYMENT = "PRE_START_DEPLOYMENT"
    PRE_STOP_DEPLOYMENT = "PRE_STOP_DEPLOYMENT"
    ON_DEPLOYMENT_START = "ON_DEPLOYMENT_START"
    ON_DEPLOYMENT_COMPLETE = "ON_DEPLOYMENT_COMPLETE"
    ON_DEPLOYMENT_ROLLED_BACK = "ON_DEPLOYMENT_ROLLBACK"
    ON_DEPLOYMENT_BAKING = "ON_DEPLOYMENT_BAKING"
    ON_DEPLOYMENT_TERMINATION_CHECK = "ON_DEPLOYMENT_TERMINATION_CHECK"
    ON_CREATE_HOSTED_CONFIGURATION_VERSION = "ON_CREATE_HOSTED_CONFIGURATION_VERSION"


class DeploymentStrategyType(Enum):
    """Predefined deployment strategy types."""
    LINEAR_50_PERCENT_EVERY_30_SECONDS = "AppConfig.Linear.50PercentEvery30Seconds"
    LINEAR_10_PERCENT_EVERY_10_MINUTES = "AppConfig.Linear.10PercentEvery10Minutes"
    LINEAR_20_PERCENT_EVERY_5_MINUTES = "AppConfig.Linear.20PercentEvery5Minutes"
    EXPONENTIAL_50_PERCENT_EVERY_30_SECONDS = "AppConfig.Exponential.50PercentEvery30Seconds"
    CANARY_10_PERCENT_20_MINUTES = "AppConfig.Canary10Percent20Minutes"
    ALL_AT_ONCE = "AppConfig.AllAtOnce"


class DeploymentStatus(Enum):
    """Deployment status values."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DEPLOYING = "DEPLOYING"
    COMPLETE = "COMPLETE"
    BAKING = "BAKING"
    ROLLED_BACK = "ROLLED_BACK"
    ABORTED = "ABORTED"
    INVALID = "INVALID"


class EnvironmentStatus(Enum):
    """Environment status values."""
    PREPARED = "PREPARED"
    DEPLOYING = "DEPLOYING"
    ROLLED_BACK = "ROLLED_BACK"
    READY_FOR_DEPLOYMENT = "READY_FOR_DEPLOYMENT"
    DEPLOYMENT_ABORTED = "DEPLOYMENT_ABORTED"


@dataclass
class AppConfigValidator:
    """Configuration validator definition."""
    validator_type: ValidatorType
    content: str  # JSON schema content or Lambda ARN


@dataclass
class DeploymentStrategy:
    """Deployment strategy configuration."""
    name: str
    description: str
    deployment_duration_in_minutes: int
    growth_factor: float
    final_percentage: float = 100.0
    replicate_to: str = "NONE"
    immediate_rollback: bool = False


@dataclass
class ExtensionParameter:
    """Extension parameter definition."""
    name: str
    required: bool = False
    description: str = ""
    pattern: str = ""


@dataclass
class HostedConfigurationVersion:
    """Hosted configuration version metadata."""
    version_number: int
    content: str
    content_type: str = "application/json"
    description: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)


class AppConfigIntegration:
    """
    AWS AppConfig Integration for configuration management.
    
    Provides comprehensive AWS AppConfig functionality including:
    - Application management
    - Environment management
    - Configuration profiles
    - Deployments
    - Deployment strategies
    - Hosted configuration
    - Validators
    - Extensions
    - Lambda extensions
    - CloudWatch monitoring
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        profile_name: Optional[str] = None,
        kms_key_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        notifications_topic_arn: Optional[str] = None,
    ):
        """
        Initialize AppConfig integration.
        
        Args:
            region: AWS region
            profile_name: AWS profile name
            kms_key_id: KMS key ID for encryption
            tags: Default tags for resources
            notifications_topic_arn: SNS topic ARN for deployment notifications
        """
        self.region = region
        self.profile_name = profile_name
        self.kms_key_id = kms_key_id
        self.tags = tags or {}
        self.notifications_topic_arn = notifications_topic_arn
        self._client = None
        self._resource_policy_cache = {}
        self._lock = threading.Lock()
        
    @property
    def client(self):
        """Get boto3 AppConfig client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    if BOTO3_AVAILABLE:
                        session = boto3.Session(
                            region_name=self.region,
                            profile_name=self.profile_name
                        )
                        config = {}
                        if self.kms_key_id:
                            config['encryption'] = {'KmsKeyId': self.kms_key_id}
                        self._client = session.client('appconfig', **config)
                    else:
                        raise ImportError("boto3 is required for AWS AppConfig integration")
        return self._client
    
    # ==================== Application Management ====================
    
    def create_application(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create an AppConfig application.
        
        Args:
            name: Application name
            description: Application description
            tags: Tags to associate with the application
            
        Returns:
            Application details
        """
        kwargs = {
            'Name': name,
        }
        if description:
            kwargs['Description'] = description
        if tags:
            kwargs['Tags'] = tags
        elif self.tags:
            kwargs['Tags'] = self.tags.copy()
            
        try:
            response = self.client.create_application(**kwargs)
            logger.info(f"Created AppConfig application: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create application: {e}")
            raise
    
    def get_application(self, application_id: str) -> Dict[str, Any]:
        """Get application details."""
        try:
            return self.client.get_application(ApplicationId=application_id)
        except ClientError as e:
            logger.error(f"Failed to get application: {e}")
            raise
    
    def list_applications(
        self,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List all AppConfig applications."""
        kwargs = {'MaxResults': max_results}
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_applications(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list applications: {e}")
            raise
    
    def update_application(
        self,
        application_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update an application."""
        kwargs = {'ApplicationId': application_id}
        if name:
            kwargs['Name'] = name
        if description is not None:
            kwargs['Description'] = description
            
        try:
            return self.client.update_application(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to update application: {e}")
            raise
    
    def delete_application(self, application_id: str) -> None:
        """Delete an application."""
        try:
            self.client.delete_application(ApplicationId=application_id)
            logger.info(f"Deleted application: {application_id}")
        except ClientError as e:
            logger.error(f"Failed to delete application: {e}")
            raise
    
    # ==================== Environment Management ====================
    
    def create_environment(
        self,
        application_id: str,
        name: str,
        description: str = "",
        environment_variables: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        monitors: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Create an environment within an application.
        
        Args:
            application_id: Application ID
            name: Environment name
            description: Environment description
            environment_variables: Environment variables
            tags: Tags for the environment
            monitors: CloudWatch alarms to monitor
            
        Returns:
            Environment details
        """
        kwargs = {
            'ApplicationId': application_id,
            'Name': name,
        }
        if description:
            kwargs['Description'] = description
        if environment_variables:
            kwargs['EnvironmentVariables'] = environment_variables
        if tags:
            kwargs['Tags'] = tags
        elif self.tags:
            kwargs['Tags'] = self.tags.copy()
        if monitors:
            kwargs['Monitors'] = monitors
            
        try:
            response = self.client.create_environment(**kwargs)
            logger.info(f"Created environment: {name} in application: {application_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create environment: {e}")
            raise
    
    def get_environment(
        self,
        application_id: str,
        environment_id: str,
    ) -> Dict[str, Any]:
        """Get environment details."""
        try:
            return self.client.get_environment(
                ApplicationId=application_id,
                EnvironmentId=environment_id
            )
        except ClientError as e:
            logger.error(f"Failed to get environment: {e}")
            raise
    
    def list_environments(
        self,
        application_id: str,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List environments for an application."""
        kwargs = {
            'ApplicationId': application_id,
            'MaxResults': max_results,
        }
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_environments(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list environments: {e}")
            raise
    
    def update_environment(
        self,
        application_id: str,
        environment_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        monitors: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Update an environment."""
        kwargs = {
            'ApplicationId': application_id,
            'EnvironmentId': environment_id,
        }
        if name:
            kwargs['Name'] = name
        if description is not None:
            kwargs['Description'] = description
        if environment_variables is not None:
            kwargs['EnvironmentVariables'] = environment_variables
        if monitors is not None:
            kwargs['Monitors'] = monitors
            
        try:
            return self.client.update_environment(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to update environment: {e}")
            raise
    
    def delete_environment(
        self,
        application_id: str,
        environment_id: str,
    ) -> None:
        """Delete an environment."""
        try:
            self.client.delete_environment(
                ApplicationId=application_id,
                EnvironmentId=environment_id
            )
            logger.info(f"Deleted environment: {environment_id}")
        except ClientError as e:
            logger.error(f"Failed to delete environment: {e}")
            raise
    
    # ==================== Configuration Profile Management ====================
    
    def create_configuration_profile(
        self,
        application_id: str,
        name: str,
        config_type: ConfigType = ConfigType.FREE_FORM,
        description: str = "",
        location_uri: Optional[str] = None,
        validators: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[Dict[str, str]] = None,
        retrieval_word: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a configuration profile.
        
        Args:
            application_id: Application ID
            name: Profile name
            config_type: Configuration type
            description: Profile description
            location_uri: URI to configuration (S3, SSM Parameter, etc.)
            validators: List of validators for the configuration
            tags: Tags for the profile
            retrieval_word: Retrieval word for the profile
            
        Returns:
            Configuration profile details
        """
        kwargs = {
            'ApplicationId': application_id,
            'Name': name,
            'Type': config_type.value,
        }
        if description:
            kwargs['Description'] = description
        if location_uri:
            kwargs['LocationUri'] = location_uri
        if validators:
            kwargs['Validators'] = validators
        if tags:
            kwargs['Tags'] = tags
        elif self.tags:
            kwargs['Tags'] = self.tags.copy()
        if retrieval_word:
            kwargs['RetrievalWord'] = retrieval_word
            
        try:
            response = self.client.create_configuration_profile(**kwargs)
            logger.info(f"Created configuration profile: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create configuration profile: {e}")
            raise
    
    def get_configuration_profile(
        self,
        application_id: str,
        configuration_profile_id: str,
    ) -> Dict[str, Any]:
        """Get configuration profile details."""
        try:
            return self.client.get_configuration_profile(
                ApplicationId=application_id,
                ConfigurationProfileId=configuration_profile_id
            )
        except ClientError as e:
            logger.error(f"Failed to get configuration profile: {e}")
            raise
    
    def list_configuration_profiles(
        self,
        application_id: str,
        environment_id: Optional[str] = None,
        config_type: Optional[str] = None,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List configuration profiles."""
        kwargs = {
            'ApplicationId': application_id,
            'MaxResults': max_results,
        }
        if environment_id:
            kwargs['EnvironmentId'] = environment_id
        if config_type:
            kwargs['Type'] = config_type
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_configuration_profiles(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list configuration profiles: {e}")
            raise
    
    def update_configuration_profile(
        self,
        application_id: str,
        configuration_profile_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        validators: Optional[List[Dict[str, Any]]] = None,
        retrieval_word: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update a configuration profile."""
        kwargs = {
            'ApplicationId': application_id,
            'ConfigurationProfileId': configuration_profile_id,
        }
        if name:
            kwargs['Name'] = name
        if description is not None:
            kwargs['Description'] = description
        if validators is not None:
            kwargs['Validators'] = validators
        if retrieval_word:
            kwargs['RetrievalWord'] = retrieval_word
            
        try:
            return self.client.update_configuration_profile(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to update configuration profile: {e}")
            raise
    
    def delete_configuration_profile(
        self,
        application_id: str,
        configuration_profile_id: str,
    ) -> None:
        """Delete a configuration profile."""
        try:
            self.client.delete_configuration_profile(
                ApplicationId=application_id,
                ConfigurationProfileId=configuration_profile_id
            )
            logger.info(f"Deleted configuration profile: {configuration_profile_id}")
        except ClientError as e:
            logger.error(f"Failed to delete configuration profile: {e}")
            raise
    
    # ==================== Hosted Configuration Content ====================
    
    def create_hosted_configuration_version(
        self,
        application_id: str,
        configuration_profile_id: str,
        content: Union[str, Dict, List],
        content_type: str = "application/json",
        description: str = "",
        validators: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Create a hosted configuration version.
        
        Args:
            application_id: Application ID
            configuration_profile_id: Configuration profile ID
            content: Configuration content (str, dict, or list)
            content_type: Content MIME type
            description: Version description
            validators: Configuration validators
            
        Returns:
            Hosted configuration version details
        """
        if isinstance(content, (dict, list)):
            content_str = json.dumps(content)
        else:
            content_str = content
            
        kwargs = {
            'ApplicationId': application_id,
            'ConfigurationProfileId': configuration_profile_id,
            'Content': content_str,
            'ContentType': content_type,
        }
        if description:
            kwargs['Description'] = description
        if validators:
            kwargs['Validators'] = validators
            
        try:
            response = self.client.create_hosted_configuration_version(**kwargs)
            logger.info(f"Created hosted configuration version for profile: {configuration_profile_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create hosted configuration version: {e}")
            raise
    
    def get_hosted_configuration_version(
        self,
        application_id: str,
        configuration_profile_id: str,
        version_number: str,
    ) -> Dict[str, Any]:
        """Get hosted configuration version details."""
        try:
            return self.client.get_hosted_configuration_version(
                ApplicationId=application_id,
                ConfigurationProfileId=configuration_profile_id,
                VersionNumber=version_number
            )
        except ClientError as e:
            logger.error(f"Failed to get hosted configuration version: {e}")
            raise
    
    def list_hosted_configuration_versions(
        self,
        application_id: str,
        configuration_profile_id: str,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List hosted configuration versions."""
        kwargs = {
            'ApplicationId': application_id,
            'ConfigurationProfileId': configuration_profile_id,
            'MaxResults': max_results,
        }
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_hosted_configuration_versions(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list hosted configuration versions: {e}")
            raise
    
    # ==================== Deployment Strategy Management ====================
    
    def create_deployment_strategy(
        self,
        name: str,
        description: str = "",
        deployment_duration_in_minutes: int = 0,
        growth_factor: float = 0.0,
        final_percentage: float = 100.0,
        replicate_to: str = "NONE",
        immediate_rollback: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a deployment strategy.
        
        Args:
            name: Strategy name
            description: Strategy description
            deployment_duration_in_minutes: Time for full deployment
            growth_factor: Percentage increase per interval (0.0-100.0)
            final_percentage: Final percentage of traffic
            replicate_to: Where to replicate ('NONE', 'SSM_DOCUMENT')
            immediate_rollback: Enable immediate rollback on failure
            tags: Tags for the strategy
            
        Returns:
            Deployment strategy details
        """
        kwargs = {
            'Name': name,
            'DeploymentDurationInMinutes': deployment_duration_in_minutes,
            'GrowthFactor': growth_factor,
            'FinalBakeTimeInMinutes': int((final_percentage / 100.0) * deployment_duration_in_minutes) if deployment_duration_in_minutes > 0 else 0,
        }
        if description:
            kwargs['Description'] = description
        if tags:
            kwargs['Tags'] = tags
        elif self.tags:
            kwargs['Tags'] = self.tags.copy()
        if immediate_rollback:
            kwargs['ImmediateRollback'] = True
            
        try:
            response = self.client.create_deployment_strategy(**kwargs)
            logger.info(f"Created deployment strategy: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create deployment strategy: {e}")
            raise
    
    def get_deployment_strategy(
        self,
        deployment_strategy_id: str,
    ) -> Dict[str, Any]:
        """Get deployment strategy details."""
        try:
            return self.client.get_deployment_strategy(
                DeploymentStrategyId=deployment_strategy_id
            )
        except ClientError as e:
            logger.error(f"Failed to get deployment strategy: {e}")
            raise
    
    def list_deployment_strategies(
        self,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List deployment strategies."""
        kwargs = {'MaxResults': max_results}
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_deployment_strategies(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list deployment strategies: {e}")
            raise
    
    def update_deployment_strategy(
        self,
        deployment_strategy_id: str,
        description: Optional[str] = None,
        deployment_duration_in_minutes: Optional[int] = None,
        growth_factor: Optional[float] = None,
        final_percentage: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Update a deployment strategy."""
        kwargs = {'DeploymentStrategyId': deployment_strategy_id}
        if description is not None:
            kwargs['Description'] = description
        if deployment_duration_in_minutes is not None:
            kwargs['DeploymentDurationInMinutes'] = deployment_duration_in_minutes
        if growth_factor is not None:
            kwargs['GrowthFactor'] = growth_factor
        if final_percentage is not None:
            kwargs['FinalBakeTimeInMinutes'] = int(final_percentage)
            
        try:
            return self.client.update_deployment_strategy(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to update deployment strategy: {e}")
            raise
    
    def delete_deployment_strategy(
        self,
        deployment_strategy_id: str,
    ) -> None:
        """Delete a deployment strategy."""
        try:
            self.client.delete_deployment_strategy(
                DeploymentStrategyId=deployment_strategy_id
            )
            logger.info(f"Deleted deployment strategy: {deployment_strategy_id}")
        except ClientError as e:
            logger.error(f"Failed to delete deployment strategy: {e}")
            raise
    
    # ==================== Deployment Management ====================
    
    def start_deployment(
        self,
        application_id: str,
        environment_id: str,
        configuration_profile_id: str,
        deployment_strategy_id: str,
        description: str = "",
        version_label: Optional[str] = None,
        kms_key_arn: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Start a configuration deployment.
        
        Args:
            application_id: Application ID
            environment_id: Environment ID
            configuration_profile_id: Configuration profile ID
            deployment_strategy_id: Deployment strategy ID
            description: Deployment description
            version_label: Version label for the deployment
            kms_key_arn: KMS key ARN for decryption
            
        Returns:
            Deployment details
        """
        kwargs = {
            'ApplicationId': application_id,
            'EnvironmentId': environment_id,
            'ConfigurationProfileId': configuration_profile_id,
            'DeploymentStrategyId': deployment_strategy_id,
        }
        if description:
            kwargs['Description'] = description
        if version_label:
            kwargs['VersionLabel'] = version_label
        if kms_key_arn:
            kwargs['KmsKeyArn'] = kms_key_arn
            
        try:
            response = self.client.start_deployment(**kwargs)
            logger.info(f"Started deployment to environment: {environment_id}")
            return response
        except ClientError as e:
            logger.error(f"Failed to start deployment: {e}")
            raise
    
    def get_deployment(
        self,
        application_id: str,
        environment_id: str,
        deployment_number: int,
    ) -> Dict[str, Any]:
        """Get deployment details."""
        try:
            return self.client.get_deployment(
                ApplicationId=application_id,
                EnvironmentId=environment_id,
                DeploymentNumber=deployment_number
            )
        except ClientError as e:
            logger.error(f"Failed to get deployment: {e}")
            raise
    
    def list_deployments(
        self,
        application_id: str,
        environment_id: str,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List deployments for an environment."""
        kwargs = {
            'ApplicationId': application_id,
            'EnvironmentId': environment_id,
            'MaxResults': max_results,
        }
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_deployments(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list deployments: {e}")
            raise
    
    def stop_deployment(
        self,
        application_id: str,
        environment_id: str,
        deployment_number: int,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Stop an active deployment.
        
        Args:
            application_id: Application ID
            environment_id: Environment ID
            deployment_number: Deployment number
            force: Force stop even if it causes issues
            
        Returns:
            Updated deployment details
        """
        try:
            return self.client.stop_deployment(
                ApplicationId=application_id,
                EnvironmentId=environment_id,
                DeploymentNumber=deployment_number,
                Force=force
            )
        except ClientError as e:
            logger.error(f"Failed to stop deployment: {e}")
            raise
    
    def wait_for_deployment(
        self,
        application_id: str,
        environment_id: str,
        deployment_number: int,
        timeout: int = 1800,
        poll_interval: int = 10,
    ) -> Dict[str, Any]:
        """
        Wait for deployment to complete.
        
        Args:
            application_id: Application ID
            environment_id: Environment ID
            deployment_number: Deployment number
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            Final deployment status
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            deployment = self.get_deployment(
                application_id, environment_id, deployment_number
            )
            status = deployment.get('State', deployment.get('Status'))
            
            if status in ['COMPLETE', 'SUCCESS']:
                logger.info(f"Deployment {deployment_number} completed successfully")
                return deployment
            elif status in ['FAILED', 'ROLLED_BACK', 'ABORTED', 'INVALID']:
                logger.error(f"Deployment {deployment_number} ended with status: {status}")
                return deployment
                
            logger.debug(f"Deployment status: {status}, waiting...")
            time.sleep(poll_interval)
            
        raise TimeoutError(f"Deployment did not complete within {timeout} seconds")
    
    # ==================== Validators ====================
    
    def create_json_schema_validator(
        self,
        schema: Union[str, Dict],
    ) -> Dict[str, Any]:
        """
        Create a JSON schema validator.
        
        Args:
            schema: JSON schema as string or dict
            
        Returns:
            Validator definition for use in configuration profiles
        """
        if isinstance(schema, dict):
            schema_str = json.dumps(schema)
        else:
            schema_str = schema
            
        return {
            'ValidatorType': ValidatorType.JSON_SCHEMA.value,
            'Content': schema_str,
        }
    
    def create_lambda_validator(
        self,
        lambda_arn: str,
    ) -> Dict[str, Any]:
        """
        Create a Lambda function validator.
        
        Args:
            lambda_arn: Lambda function ARN
            
        Returns:
            Validator definition for use in configuration profiles
        """
        return {
            'ValidatorType': ValidatorType.LAMBDA.value,
            'Content': lambda_arn,
        }
    
    def validate_configuration(
        self,
        configuration: Union[str, Dict, List],
        validators: List[Dict[str, Any]],
    ) -> bool:
        """
        Validate configuration content against validators.
        
        Note: This is a local validation helper. AWS also validates
        when creating hosted configuration versions.
        
        Args:
            configuration: Configuration content
            validators: List of validators
            
        Returns:
            True if valid
            
        Raises:
            ValueError: If validation fails
        """
        if isinstance(configuration, (dict, list)):
            config_str = json.dumps(configuration)
        else:
            config_str = configuration
            
        for validator in validators:
            validator_type = validator.get('ValidatorType')
            content = validator.get('Content', '')
            
            if validator_type == ValidatorType.JSON_SCHEMA.value:
                try:
                    import jsonschema
                    schema = json.loads(content)
                    instance = json.loads(config_str)
                    jsonschema.validate(instance=instance, schema=schema)
                except ImportError:
                    logger.warning("jsonschema not installed, skipping JSON schema validation")
                except jsonschema.ValidationError as e:
                    raise ValueError(f"JSON schema validation failed: {e}")
                    
        return True
    
    # ==================== Extensions ====================
    
    def create_extension(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create an AppConfig extension.
        
        Args:
            name: Extension name
            description: Extension description
            tags: Tags for the extension
            
        Returns:
            Extension details
        """
        kwargs = {'Name': name}
        if description:
            kwargs['Description'] = description
        if tags:
            kwargs['Tags'] = tags
        elif self.tags:
            kwargs['Tags'] = self.tags.copy()
            
        try:
            response = self.client.create_extension(**kwargs)
            logger.info(f"Created extension: {name}")
            return response
        except ClientError as e:
            logger.error(f"Failed to create extension: {e}")
            raise
    
    def get_extension(
        self,
        extension_id: str,
    ) -> Dict[str, Any]:
        """Get extension details."""
        try:
            return self.client.get_extension(ExtensionId=extension_id)
        except ClientError as e:
            logger.error(f"Failed to get extension: {e}")
            raise
    
    def list_extensions(
        self,
        max_results: int = 50,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List AppConfig extensions."""
        kwargs = {'MaxResults': max_results}
        if next_token:
            kwargs['NextToken'] = next_token
            
        try:
            return self.client.list_extensions(**kwargs)
        except ClientError as e:
            logger.error(f"Failed to list extensions: {e}")
            raise
    
    def delete_extension(
        self,
        extension_id: str,
    ) -> None:
        """Delete an extension."""
        try:
            self.client.delete_extension(ExtensionId=extension_id)
            logger.info(f"Deleted extension: {extension_id}")
        except ClientError as e:
            logger.error(f"Failed to delete extension: {e}")
            raise
    
    def associate_extension_to_application(
        self,
        extension_id: str,
        application_id: str,
        parameters: Optional[Dict[str, str]] = None,
    ) -> None:
        """Associate an extension with an application."""
        try:
            self.client.associate_extension_to_application(
                ExtensionId=extension_id,
                ApplicationId=application_id,
                Parameters=parameters or {}
            )
            logger.info(f"Associated extension {extension_id} with application {application_id}")
        except ClientError as e:
            logger.error(f"Failed to associate extension: {e}")
            raise
    
    def disassociate_extension_from_application(
        self,
        extension_id: str,
        application_id: str,
    ) -> None:
        """Disassociate an extension from an application."""
        try:
            self.client.disassociate_extension_from_application(
                ExtensionId=extension_id,
                ApplicationId=application_id
            )
            logger.info(f"Disassociated extension {extension_id} from application {application_id}")
        except ClientError as e:
            logger.error(f"Failed to disassociate extension: {e}")
            raise
    
    # ==================== Lambda Extensions ====================
    
    def create_lambda_extension(
        self,
        name: str,
        lambda_arn: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a Lambda extension for AppConfig.
        
        This creates an extension with Lambda invocation actions.
        
        Args:
            name: Extension name
            lambda_arn: Lambda function ARN to invoke
            description: Extension description
            tags: Tags for the extension
            
        Returns:
            Extension details with Lambda actions
        """
        extension = self.create_extension(
            name=name,
            description=description,
            tags=tags,
        )
        
        extension_id = extension['Id']
        actions = {}
        
        for point in ExtensionPoint:
            actions[point.value] = [{
                'Name': f"{name}_{point.value}",
                'Uri': lambda_arn,
                'InvocationType': 'LAMBDA',
            }]
            
        try:
            self.client.update_extension(
                ExtensionId=extension_id,
                Actions=actions
            )
            logger.info(f"Created Lambda extension: {name}")
            return self.get_extension(extension_id)
        except ClientError as e:
            logger.error(f"Failed to configure Lambda extension: {e}")
            raise
    
    def create_lambda_extension_action(
        self,
        extension_id: str,
        extension_point: ExtensionPoint,
        name: str,
        lambda_arn: str,
        invocation_type: str = "LAMBDA",
    ) -> Dict[str, Any]:
        """
        Add a Lambda action to an extension for a specific extension point.
        
        Args:
            extension_id: Extension ID
            extension_point: Extension point to hook into
            name: Action name
            lambda_arn: Lambda function ARN
            invocation_type: Invocation type (LAMBDA, SSM)
            
        Returns:
            Updated extension details
        """
        action = {
            'Uri': lambda_arn,
            'InvocationType': invocation_type,
        }
        
        try:
            return self.client.update_extension(
                ExtensionId=extension_id,
                Actions={
                    extension_point.value: [action]
                }
            )
        except ClientError as e:
            logger.error(f"Failed to add Lambda action: {e}")
            raise
    
    # ==================== CloudWatch Integration ====================
    
    def create_environment_with_monitoring(
        self,
        application_id: str,
        name: str,
        description: str = "",
        alarms: Optional[List[Dict[str, Any]]] = None,
        alarm_role_arn: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create an environment with CloudWatch monitoring.
        
        Args:
            application_id: Application ID
            name: Environment name
            description: Environment description
            alarms: CloudWatch alarm configurations
            alarm_role_arn: IAM role ARN for alarm actions
            tags: Tags for the environment
            
        Returns:
            Environment details with monitoring
        """
        monitors = None
        if alarms:
            monitors = []
            for alarm in alarms:
                monitor = {
                    'AlarmArn': alarm['alarm_arn'],
                }
                if alarm_role_arn:
                    monitor['AlarmRoleArn'] = alarm_role_arn
                if 'alarm_suffix' in alarm:
                    monitor['AlarmSuffix'] = alarm['alarm_suffix']
                monitors.append(monitor)
                
        return self.create_environment(
            application_id=application_id,
            name=name,
            description=description,
            tags=tags,
            monitors=monitors,
        )
    
    def update_environment_monitors(
        self,
        application_id: str,
        environment_id: str,
        alarms: Optional[List[Dict[str, Any]]] = None,
        alarm_role_arn: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update environment CloudWatch monitors.
        
        Args:
            application_id: Application ID
            environment_id: Environment ID
            alarms: List of CloudWatch alarm configurations
            alarm_role_arn: IAM role ARN for alarm actions
            
        Returns:
            Updated environment details
        """
        monitors = None
        if alarms is not None:
            monitors = []
            for alarm in alarms:
                monitor = {
                    'AlarmArn': alarm['alarm_arn'],
                }
                if alarm_role_arn:
                    monitor['AlarmRoleArn'] = alarm_role_arn
                if 'alarm_suffix' in alarm:
                    monitor['AlarmSuffix'] = alarm['alarm_suffix']
                monitors.append(monitor)
                
        return self.update_environment(
            application_id=application_id,
            environment_id=environment_id,
            monitors=monitors,
        )
    
    def get_environment_monitors(
        self,
        application_id: str,
        environment_id: str,
    ) -> List[Dict[str, Any]]:
        """Get CloudWatch monitors for an environment."""
        env = self.get_environment(application_id, environment_id)
        return env.get('Monitors', [])
    
    def setup_deployment_metrics(
        self,
        application_id: str,
        environment_id: str,
        configuration_profile_id: str,
        version_label: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Set up CloudWatch metrics for monitoring deployments.
        
        Returns metric configuration for use with CloudWatch dashboards.
        
        Returns:
            CloudWatch metrics configuration
        """
        metrics = {
            'Application': application_id,
            'Environment': environment_id,
            'ConfigurationProfile': configuration_profile_id,
            'Metrics': [
                {
                    'MetricName': 'DeploymentDuration',
                    'Unit': 'Seconds',
                    'Description': 'Time taken for deployment to complete',
                },
                {
                    'MetricName': 'DeploymentSuccess',
                    'Unit': 'Count',
                    'Description': 'Successful deployments',
                },
                {
                    'MetricName': 'DeploymentFailure',
                    'Unit': 'Count',
                    'Description': 'Failed deployments',
                },
                {
                    'MetricName': 'ConfigurationChanges',
                    'Unit': 'Count',
                    'Description': 'Configuration version changes',
                },
            ],
            'CloudWatchNamespace': 'AppConfig/Deployments',
            'DimensionName': 'Environment',
        }
        
        if version_label:
            metrics['VersionLabel'] = version_label
            
        return metrics
    
    # ==================== Resource Policies ====================
    
    def put_resource_policy(
        self,
        resource_arn: str,
        policy: Dict[str, Any],
    ) -> None:
        """
        Put a resource policy on an AppConfig resource.
        
        Args:
            resource_arn: ARN of the AppConfig resource
            policy: Resource policy document
        """
        policy_json = json.dumps(policy) if isinstance(policy, dict) else policy
        
        try:
            self.client.put_resource_policy(
                ResourceArn=resource_arn,
                ResourcePolicy=policy_json
            )
            logger.info(f"Put resource policy on: {resource_arn}")
        except ClientError as e:
            logger.error(f"Failed to put resource policy: {e}")
            raise
    
    def get_resource_policy(
        self,
        resource_arn: str,
    ) -> Dict[str, Any]:
        """Get resource policy for an AppConfig resource."""
        try:
            return self.client.get_resource_policy(ResourceArn=resource_arn)
        except ClientError as e:
            logger.error(f"Failed to get resource policy: {e}")
            raise
    
    def delete_resource_policy(
        self,
        resource_arn: str,
    ) -> None:
        """Delete resource policy from an AppConfig resource."""
        try:
            self.client.delete_resource_policy(ResourceArn=resource_arn)
            logger.info(f"Deleted resource policy from: {resource_arn}")
        except ClientError as e:
            logger.error(f"Failed to delete resource policy: {e}")
            raise
    
    # ==================== Tags Management ====================
    
    def list_tags_for_resource(
        self,
        resource_arn: str,
    ) -> Dict[str, str]:
        """List tags for an AppConfig resource."""
        try:
            response = self.client.list_tags_for_resource(ResourceArn=resource_arn)
            tags = response.get('Tags', {})
            return tags if tags else {}
        except ClientError as e:
            logger.error(f"Failed to list tags: {e}")
            raise
    
    def tag_resource(
        self,
        resource_arn: str,
        tags: Dict[str, str],
    ) -> None:
        """Add tags to an AppConfig resource."""
        try:
            self.client.tag_resource(ResourceArn=resource_arn, Tags=tags)
            logger.info(f"Added tags to resource: {resource_arn}")
        except ClientError as e:
            logger.error(f"Failed to tag resource: {e}")
            raise
    
    def untag_resource(
        self,
        resource_arn: str,
        tag_keys: List[str],
    ) -> None:
        """Remove tags from an AppConfig resource."""
        try:
            self.client.untag_resource(ResourceArn=resource_arn, TagKeys=tag_keys)
            logger.info(f"Removed tags from resource: {resource_arn}")
        except ClientError as e:
            logger.error(f"Failed to untag resource: {e}")
            raise
    
    # ==================== Configuration Retrieval ====================
    
    def get_configuration(
        self,
        application: str,
        environment: str,
        configuration: str,
        version_label: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get configuration from AppConfig.
        
        This is the runtime retrieval API used by applications
        to fetch configuration.
        
        Args:
            application: Application name or ID
            environment: Environment name or ID
            configuration: Configuration profile name or ID
            version_label: Specific version to retrieve
            client_id: Unique ID for the client
            
        Returns:
            Configuration content and metadata
        """
        if client_id is None:
            client_id = str(uuid.uuid4())
            
        kwargs = {
            'Application': application,
            'Environment': environment,
            'Configuration': configuration,
            'ClientId': client_id,
        }
        if version_label:
            kwargs['VersionLabel'] = version_label
            
        try:
            response = self.client.get_configuration(**kwargs)
            
            content = response['Content'].read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
                
            return {
                'content': content,
                'content_type': response.get('ContentType', 'application/json'),
                'version': response.get('Version', ''),
                'configuration_version': response.get('ConfigurationVersion', ''),
            }
        except ClientError as e:
            logger.error(f"Failed to get configuration: {e}")
            raise
    
    # ==================== Batch Operations ====================
    
    def create_application_with_environment(
        self,
        application_name: str,
        environment_name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create application and environment in one operation.
        
        Args:
            application_name: Application name
            environment_name: Environment name
            description: Description for both resources
            tags: Tags for resources
            
        Returns:
            Dictionary with application and environment details
        """
        app = self.create_application(
            name=application_name,
            description=description,
            tags=tags,
        )
        
        env = self.create_environment(
            application_id=app['Id'],
            name=environment_name,
            description=description,
            tags=tags,
        )
        
        return {
            'application': app,
            'environment': env,
        }
    
    def setup_feature_flag_application(
        self,
        application_name: str,
        environment_name: str,
        initial_flags: Optional[Dict[str, Any]] = None,
        deployment_strategy_id: str = "AppConfig.AllAtOnce",
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Set up a complete feature flag configuration.
        
        Args:
            application_name: Application name
            environment_name: Environment name
            initial_flags: Initial feature flag definitions
            deployment_strategy_id: Deployment strategy to use
            tags: Tags for resources
            
        Returns:
            Complete setup details including application, environment,
            configuration profile, and initial deployment
        """
        initial_flags = initial_flags or {}
        
        app = self.create_application(
            name=application_name,
            description=f"Feature flags for {application_name}",
            tags=tags,
        )
        
        env = self.create_environment(
            application_id=app['Id'],
            name=environment_name,
            description=f"Environments for {application_name}",
            tags=tags,
        )
        
        profile = self.create_configuration_profile(
            application_id=app['Id'],
            name=f"{application_name}-flags",
            config_type=ConfigType.FEATURE_FLAGS,
            description="Feature flag configuration",
        )
        
        flag_content = {
            "flags": initial_flags,
            "flagMetadata": {
                "version": "1.0",
                "lastModified": datetime.utcnow().isoformat(),
            }
        }
        
        version = self.create_hosted_configuration_version(
            application_id=app['Id'],
            configuration_profile_id=profile['Id'],
            content=flag_content,
            content_type="application/json",
        )
        
        deployment = self.start_deployment(
            application_id=app['Id'],
            environment_id=env['Id'],
            configuration_profile_id=profile['Id'],
            deployment_strategy_id=deployment_strategy_id,
            version_label=version['VersionNumber'],
        )
        
        return {
            'application': app,
            'environment': env,
            'configuration_profile': profile,
            'hosted_configuration_version': version,
            'deployment': deployment,
        }
    
    # ==================== Utility Methods ====================
    
    def get_appconfig_arn(
        self,
        resource_type: str,
        application_id: str,
        resource_id: Optional[str] = None,
    ) -> str:
        """
        Build AppConfig resource ARN.
        
        Args:
            resource_type: Type of resource (application, environment, configurationprofile, deploymentstrategy)
            application_id: Application ID
            resource_id: Optional resource ID
            
        Returns:
            ARN string
        """
        if resource_id:
            return f"arn:aws:appconfig:{self.region}::{resource_type}/{application_id}/{resource_id}"
        return f"arn:aws:appconfig:{self.region}::{resource_type}/{application_id}"
    
    def build_deployment_dashboard_url(
        self,
        application_id: str,
        environment_id: str,
    ) -> str:
        """
        Build CloudWatch dashboard URL for AppConfig deployments.
        
        Returns:
            CloudWatch console URL
        """
        return (
            f"https://{self.region}.console.aws.amazon.com/cloudwatch/home"
            f"?#home: dashboards/AppConfig/Deployments"
            f"?~query=SELECT * FROM AppConfig/Deployments "
            f"WHERE application = '{application_id}' "
            f"AND environment = '{environment_id}'"
        )
    
    def export_configuration(
        self,
        application_id: str,
        configuration_profile_id: str,
        version_number: str,
        output_file: Optional[str] = None,
    ) -> str:
        """
        Export hosted configuration content.
        
        Args:
            application_id: Application ID
            configuration_profile_id: Configuration profile ID
            version_number: Version number to export
            output_file: Optional file path to write content
            
        Returns:
            Configuration content
        """
        version = self.get_hosted_configuration_version(
            application_id=application_id,
            configuration_profile_id=configuration_profile_id,
            version_number=version_number,
        )
        
        content = version['Content'].read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
            
        if output_file:
            with open(output_file, 'w') as f:
                f.write(content)
            logger.info(f"Exported configuration to: {output_file}")
            
        return content
