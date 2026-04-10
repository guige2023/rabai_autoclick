"""
AWS Config Integration Module for Workflow System

Implements an AWSConfigIntegration class with:
1. Configuration recorder: Manage configuration recorder
2. Delivery channel: Manage delivery channel
3. Rules: Manage AWS Config rules
4. Conformance packs: Manage conformance packs
5. Aggregators: Manage aggregators
6. Remediations: Configure remediations
7. Organization integration: Organization-level Config
8. Resource compliance: Query resource compliance
9. Timeline: Query configuration timeline
10. CloudWatch integration: Monitoring and CloudTrail

Commit: 'feat(aws-config): add AWS Config with configuration recorder, delivery channel, rules, conformance packs, aggregators, remediations, organization integration, resource compliance, timeline, CloudWatch'
"""

import uuid
import json
import time
import logging
import hashlib
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os
import re

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


class ConfigRuleStatus(Enum):
    """AWS Config rule compliance status."""
    COMPLIANT = "COMPLIANT"
    NON_COMPLIANT = "NON_COMPLIANT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ConfigRecorderState(Enum):
    """Configuration recorder states."""
    RECORDER_RECORDING = "RECORDING"
    RECORDER_STOPPED = "STOPPED"


class DeliveryStatus(Enum):
    """Delivery channel status."""
    DELIVERY_SUCCESS = "SUCCESS"
    DELIVERY_FAILED = "FAILED"
    DELIVERY_NOT_AVAILABLE = "NOT_AVAILABLE"


class ComplianceType(Enum):
    """Resource compliance types."""
    COMPLIANT = "COMPLIANT"
    NON_COMPLIANT = "NON_COMPLIANT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    UNKNOWN = "UNKNOWN"


class AggregatorType(Enum):
    """Aggregator source types."""
    ACCOUNT = "ACCOUNT"
    ORGANIZATION = "ORGANIZATION"


class RemediationStatus(Enum):
    """Remediation execution status."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PENDING = "PENDING"
    SKIPPED = "SKIPPED"


@dataclass
class ConfigurationRecorder:
    """Configuration recorder settings."""
    name: str
    role_arn: str
    resource_types: List[str] = field(default_factory=list)
    recording_group: Dict[str, Any] = field(default_factory=dict)
    recording_mode: Optional[Dict[str, Any]] = None
    is_global: bool = False


@dataclass
class DeliveryChannel:
    """Delivery channel configuration."""
    name: str
    s3_bucket: str
    s3_prefix: str = ""
    sns_topic_arn: str = ""
    config_snapshot_delivery_properties: Dict[str, Any] = field(default_factory=dict)
    file_export_format: str = "JSON"


@dataclass
class ConfigRule:
    """AWS Config rule configuration."""
    name: str
    rule_type: str  # "AWS_MANAGED" or "CUSTOM"
    source: Dict[str, Any]
    description: str = ""
    scope: Dict[str, Any] = field(default_factory=dict)
    input_parameters: Dict[str, Any] = field(default_factory=dict)
    maximum_execution_frequency: str = "TwentyFour_Hours"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConformancePack:
    """Conformance pack configuration."""
    name: str
    template_s3_uri: str = ""
    template_body: str = ""
    delivery_s3_bucket: str = ""
    delivery_s3_key_prefix: str = ""
    input_parameters: List[Dict[str, Any]] = field(default_factory=list)
    conformance_pack_input_parameters: Dict[str, str] = field(default_factory=dict)


@dataclass
class Aggregator:
    """Configuration aggregator settings."""
    name: str
    aggregator_type: AggregatorType
    account_ids: List[str] = field(default_factory=list)
    organization_id: str = ""
    regions: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class RemediationConfiguration:
    """Remediation configuration settings."""
    config_rule_name: str
    target_id: str
    target_type: str
    execution_frequency: str = "TwentyFour_Hours"
    parameters: Dict[str, Any] = field(default_factory=dict)
    retry_attempts: int = 3
    automatic: bool = False


@dataclass
class ComplianceResult:
    """Resource compliance result."""
    resource_id: str
    resource_type: str
    compliance_type: ComplianceType
    rule_name: str
    account_id: str = ""
    region: str = ""
    annotation: str = ""
    evaluation_time: Optional[datetime] = None


@dataclass
class ConfigurationTimeline:
    """Configuration timeline entry."""
    resource_id: str
    resource_type: str
    configuration_item_sha1: str
    configuration_state_id: str
    account_id: str = ""
    region: str = ""
    availability_zone: str = ""
    configuration: Dict[str, Any] = field(default_factory=dict)
    supplementary_configuration: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    creation_date: Optional[datetime] = None
    configuration_item_status: str = "OK"
    resource_creation_time: Optional[datetime] = None


class AWSConfigIntegration:
    """
    AWS Config Integration for Workflow System.
    
    Provides comprehensive AWS Config functionality including:
    - Configuration Recorder: Manage configuration recorder settings
    - Delivery Channel: Manage delivery channel for configuration history
    - Rules: Create and manage AWS Config rules
    - Conformance Packs: Manage conformance packs
    - Aggregators: Configure configuration aggregators
    - Remediations: Configure automatic remediations
    - Organization Integration: Organization-level AWS Config
    - Resource Compliance: Query resource compliance status
    - Timeline: Query configuration timeline for resources
    - CloudWatch Integration: Monitoring and CloudTrail integration
    
    Attributes:
        region_name: AWS region name
        profile_name: AWS profile name (optional)
        endpoint_url: Custom endpoint URL (optional)
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """
        Initialize AWS Config integration.
        
        Args:
            region_name: AWS region for Config operations
            profile_name: AWS credentials profile name
            endpoint_url: Custom Config endpoint URL
        """
        self.region_name = region_name
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self._clients = {}
        self._resources = {}
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize boto3 clients for AWS Config services."""
        try:
            session_kwargs = {"region_name": self.region_name}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            # AWS Config client
            self._clients["config"] = session.client(
                "config",
                endpoint_url=self.endpoint_url
            )
            
            # CloudWatch client for monitoring integration
            self._clients["cloudwatch"] = session.client(
                "cloudwatch",
                endpoint_url=self.endpoint_url
            )
            
            # CloudTrail client
            self._clients["cloudtrail"] = session.client(
                "cloudtrail",
                endpoint_url=self.endpoint_url
            )
            
            # IAM client for role operations
            self._clients["iam"] = session.client(
                "iam",
                endpoint_url=self.endpoint_url
            )
            
            # S3 client for delivery channel
            self._clients["s3"] = session.client(
                "s3",
                endpoint_url=self.endpoint_url
            )
            
            # SNS client for notifications
            self._clients["sns"] = session.client(
                "sns",
                endpoint_url=self.endpoint_url
            )
            
            logger.info(f"Initialized AWS Config clients in region {self.region_name}")
            
        except Exception as e:
            logger.warning(f"Failed to initialize AWS clients: {e}")
    
    @property
    def config_client(self):
        """Get AWS Config client."""
        if "config" not in self._clients:
            self._initialize_clients()
        return self._clients.get("config")
    
    @property
    def cloudwatch_client(self):
        """Get CloudWatch client."""
        if "cloudwatch" not in self._clients:
            self._initialize_clients()
        return self._clients.get("cloudwatch")
    
    @property
    def cloudtrail_client(self):
        """Get CloudTrail client."""
        if "cloudtrail" not in self._clients:
            self._initialize_clients()
        return self._clients.get("cloudtrail")
    
    # =========================================================================
    # Configuration Recorder Methods
    # =========================================================================
    
    def describe_configuration_recorder(self) -> Dict[str, Any]:
        """
        Describe the configuration recorder.
        
        Returns:
            Dictionary with configuration recorder details
        """
        try:
            response = self.config_client.describe_configuration_recorders()
            return {
                "status": "success",
                "data": response.get("ConfigurationRecorders", [])
            }
        except ClientError as e:
            logger.error(f"Error describing configuration recorder: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_configuration_recorder(
        self,
        recorder: ConfigurationRecorder
    ) -> Dict[str, Any]:
        """
        Create or update a configuration recorder.
        
        Args:
            recorder: ConfigurationRecorder object with settings
            
        Returns:
            Operation result
        """
        try:
            recording_group = recorder.recording_group or {}
            
            if not recorder.resource_types:
                # Default recording group for all supported resources
                recording_group = {
                    "allSupported": True,
                    "includeGlobalResourceTypes": recorder.is_global
                }
            else:
                recording_group["resourceTypes"] = recorder.resource_types
            
            configuration_recorder = {
                "name": recorder.name,
                "roleARN": recorder.role_arn,
                "recordingGroup": recording_group
            }
            
            if recorder.recording_mode:
                configuration_recorder["recordingMode"] = recorder.recording_mode
            
            self.config_client.put_configuration_recorder(
                ConfigurationRecorder=configuration_recorder
            )
            
            return {
                "status": "success",
                "message": f"Configuration recorder '{recorder.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting configuration recorder: {e}")
            return {"status": "error", "error": str(e)}
    
    def start_configuration_recorder(self, recorder_name: str) -> Dict[str, Any]:
        """
        Start the configuration recorder.
        
        Args:
            recorder_name: Name of the configuration recorder
            
        Returns:
            Operation result
        """
        try:
            self.config_client.start_configuration_recorder(
                ConfigurationRecorderName=recorder_name
            )
            return {
                "status": "success",
                "message": f"Configuration recorder '{recorder_name}' started"
            }
        except ClientError as e:
            logger.error(f"Error starting configuration recorder: {e}")
            return {"status": "error", "error": str(e)}
    
    def stop_configuration_recorder(self, recorder_name: str) -> Dict[str, Any]:
        """
        Stop the configuration recorder.
        
        Args:
            recorder_name: Name of the configuration recorder
            
        Returns:
            Operation result
        """
        try:
            self.config_client.stop_configuration_recorder(
                ConfigurationRecorderName=recorder_name
            )
            return {
                "status": "success",
                "message": f"Configuration recorder '{recorder_name}' stopped"
            }
        except ClientError as e:
            logger.error(f"Error stopping configuration recorder: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_configuration_recorder(self, recorder_name: str) -> Dict[str, Any]:
        """
        Delete the configuration recorder.
        
        Args:
            recorder_name: Name of the configuration recorder
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_configuration_recorder(
                ConfigurationRecorderName=recorder_name
            )
            return {
                "status": "success",
                "message": f"Configuration recorder '{recorder_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting configuration recorder: {e}")
            return {"status": "error", "error": str(e)}
    
    # =========================================================================
    # Delivery Channel Methods
    # =========================================================================
    
    def describe_delivery_channel(self) -> Dict[str, Any]:
        """
        Describe the delivery channel.
        
        Returns:
            Dictionary with delivery channel details
        """
        try:
            response = self.config_client.describe_delivery_channels()
            return {
                "status": "success",
                "data": response.get("DeliveryChannels", [])
            }
        except ClientError as e:
            logger.error(f"Error describing delivery channel: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_delivery_channel(
        self,
        channel: DeliveryChannel
    ) -> Dict[str, Any]:
        """
        Create or update a delivery channel.
        
        Args:
            channel: DeliveryChannel object with settings
            
        Returns:
            Operation result
        """
        try:
            delivery_channel = {
                "name": channel.name,
                "s3BucketName": channel.s3_bucket,
                "s3KeyPrefix": channel.s3_prefix,
                "fileExport": {
                    "fileFormat": channel.file_export_format
                }
            }
            
            if channel.sns_topic_arn:
                delivery_channel["snsTopicARN"] = channel.sns_topic_arn
            
            if channel.config_snapshot_delivery_properties:
                delivery_channel["configSnapshotDeliveryProperties"] = (
                    channel.config_snapshot_delivery_properties
                )
            
            self.config_client.put_delivery_channel(
                DeliveryChannel=delivery_channel
            )
            
            return {
                "status": "success",
                "message": f"Delivery channel '{channel.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting delivery channel: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_delivery_channel(self, channel_name: str) -> Dict[str, Any]:
        """
        Delete the delivery channel.
        
        Args:
            channel_name: Name of the delivery channel
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_delivery_channel(
                DeliveryChannelName=channel_name
            )
            return {
                "status": "success",
                "message": f"Delivery channel '{channel_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting delivery channel: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_delivery_channel_status(self) -> Dict[str, Any]:
        """
        Get the status of the delivery channel.
        
        Returns:
            Delivery channel status
        """
        try:
            response = self.config_client.describe_delivery_channel_status()
            return {
                "status": "success",
                "data": response.get("DeliveryChannelsStatus", [])
            }
        except ClientError as e:
            logger.error(f"Error describing delivery channel status: {e}")
            return {"status": "error", "error": str(e)}
    
    # =========================================================================
    # Config Rules Methods
    # =========================================================================
    
    def describe_config_rules(
        self,
        rule_names: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Describe AWS Config rules.
        
        Args:
            rule_names: List of specific rule names to describe
            filters: Optional filters for the query
            
        Returns:
            List of Config rules
        """
        try:
            kwargs = {}
            if rule_names:
                kwargs["ConfigRuleNames"] = rule_names
            
            response = self.config_client.describe_config_rules(**kwargs)
            
            rules = response.get("ConfigRules", [])
            
            # Apply filters if provided
            if filters:
                filtered_rules = []
                for rule in rules:
                    match = True
                    for key, value in filters.items():
                        if rule.get(key) != value:
                            match = False
                            break
                    if match:
                        filtered_rules.append(rule)
                rules = filtered_rules
            
            return {
                "status": "success",
                "data": rules
            }
        except ClientError as e:
            logger.error(f"Error describing config rules: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_config_rule(
        self,
        rule: ConfigRule
    ) -> Dict[str, Any]:
        """
        Create or update a Config rule.
        
        Args:
            rule: ConfigRule object with settings
            
        Returns:
            Operation result
        """
        try:
            config_rule = {
                "ConfigRuleName": rule.name,
                "RuleIdentifier": rule.source.get("Owner") + "/" + rule.source.get("SourceIdentifier"),
                "Description": rule.description,
                "Scope": rule.scope,
                "InputParameters": json.dumps(rule.input_parameters),
                "MaximumExecutionFrequency": rule.maximum_execution_frequency
            }
            
            # Remove None values
            config_rule = {k: v for k, v in config_rule.items() if v}
            
            self.config_client.put_config_rule(
                ConfigRule=config_rule
            )
            
            # Apply tags if provided
            if rule.tags:
                self.config_client.tag_config_rule(
                    ResourceArn=self._get_rule_arn(rule.name),
                    Tags=rule.tags
                )
            
            return {
                "status": "success",
                "message": f"Config rule '{rule.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting config rule: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_config_rule(self, rule_name: str) -> Dict[str, Any]:
        """
        Delete a Config rule.
        
        Args:
            rule_name: Name of the Config rule
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_config_rule(
                ConfigRuleName=rule_name
            )
            return {
                "status": "success",
                "message": f"Config rule '{rule_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting config rule: {e}")
            return {"status": "error", "error": str(e)}
    
    def evaluate_config_rule(
        self,
        rule_names: Optional[List[str]] = None,
        resource_types: Optional[List[str]] = None,
        resource_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Manually evaluate Config rules.
        
        Args:
            rule_names: Specific rules to evaluate
            resource_types: Resource types to evaluate
            resource_ids: Specific resource IDs to evaluate
            
        Returns:
            Evaluation result
        """
        try:
            kwargs = {}
            
            if rule_names:
                kwargs["ConfigRuleNames"] = rule_names
            
            evaluation = {
                "ComplianceResourceTypes": resource_types or [],
                "ComplianceResourceIds": resource_ids or []
            }
            
            # Start evaluation
            self.config_client.start_config_rules_evaluation(
                **kwargs
            )
            
            return {
                "status": "success",
                "message": "Config rules evaluation started"
            }
        except ClientError as e:
            logger.error(f"Error evaluating config rules: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_compliance_details_by_config_rule(
        self,
        rule_name: str,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get compliance details for a specific Config rule.
        
        Args:
            rule_name: Name of the Config rule
            limit: Maximum number of results
            
        Returns:
            Compliance details
        """
        try:
            results = []
            paginator = self.config_client.get_paginator(
                "get_compliance_details_by_config_rule"
            )
            
            for page in paginator.paginate(
                ConfigRuleName=rule_name,
                PaginationConfig={"MaxItems": limit}
            ):
                results.extend(page.get("EvaluationResults", []))
            
            return {
                "status": "success",
                "data": results
            }
        except ClientError as e:
            logger.error(f"Error getting compliance details: {e}")
            return {"status": "error", "error": str(e)}
    
    def _get_rule_arn(self, rule_name: str) -> str:
        """Get the ARN for a Config rule."""
        try:
            response = self.config_client.describe_config_rules(
                ConfigRuleNames=[rule_name]
            )
            if response.get("ConfigRules"):
                return response["ConfigRules"][0]["ConfigRuleArn"]
        except ClientError:
            pass
        return f"arn:aws:config:{self.region_name}:*:config-rule/{rule_name}"
    
    # =========================================================================
    # Conformance Packs Methods
    # =========================================================================
    
    def describe_conformance_packs(
        self,
        pack_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Describe conformance packs.
        
        Args:
            pack_names: List of specific conformance pack names
            
        Returns:
            List of conformance packs
        """
        try:
            kwargs = {}
            if pack_names:
                kwargs["ConformancePackNames"] = pack_names
            
            response = self.config_client.describe_conformance_packs(**kwargs)
            
            return {
                "status": "success",
                "data": response.get("ConformancePackDetails", [])
            }
        except ClientError as e:
            logger.error(f"Error describing conformance packs: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_conformance_pack(
        self,
        pack: ConformancePack
    ) -> Dict[str, Any]:
        """
        Create or update a conformance pack.
        
        Args:
            pack: ConformancePack object with settings
            
        Returns:
            Operation result
        """
        try:
            kwargs = {
                "ConformancePackName": pack.name,
            }
            
            if pack.template_s3_uri:
                kwargs["TemplateS3Uri"] = pack.template_s3_uri
            elif pack.template_body:
                kwargs["TemplateBody"] = pack.template_body
            
            if pack.delivery_s3_bucket:
                kwargs["DeliveryS3Bucket"] = pack.delivery_s3_bucket
            
            if pack.delivery_s3_key_prefix:
                kwargs["DeliveryS3KeyPrefix"] = pack.delivery_s3_key_prefix
            
            if pack.conformance_pack_input_parameters:
                kwargs["ConformancePackInputParameters"] = [
                    {"ParameterName": k, "ParameterValue": v}
                    for k, v in pack.conformance_pack_input_parameters.items()
                ]
            
            self.config_client.put_conformance_pack(**kwargs)
            
            return {
                "status": "success",
                "message": f"Conformance pack '{pack.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting conformance pack: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_conformance_pack(self, pack_name: str) -> Dict[str, Any]:
        """
        Delete a conformance pack.
        
        Args:
            pack_name: Name of the conformance pack
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_conformance_pack(
                ConformancePackName=pack_name
            )
            return {
                "status": "success",
                "message": f"Conformance pack '{pack_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting conformance pack: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_conformance_pack_compliance(
        self,
        pack_name: str
    ) -> Dict[str, Any]:
        """
        Get compliance details for a conformance pack.
        
        Args:
            pack_name: Name of the conformance pack
            
        Returns:
            Conformance pack compliance details
        """
        try:
            response = self.config_client.describe_conformance_pack_compliance(
                ConformancePackName=pack_name
            )
            
            return {
                "status": "success",
                "data": response.get("ConformancePackRuleCompliance", [])
            }
        except ClientError as e:
            logger.error(f"Error getting conformance pack compliance: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_conformance_pack_status(
        self,
        pack_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get the status of conformance packs.
        
        Args:
            pack_names: Specific conformance packs to check
            
        Returns:
            Conformance pack status
        """
        try:
            kwargs = {}
            if pack_names:
                kwargs["ConformancePackNames"] = pack_names
            
            response = self.config_client.describe_conformance_pack_status(**kwargs)
            
            return {
                "status": "success",
                "data": response.get("ConformancePackStatusDetails", [])
            }
        except ClientError as e:
            logger.error(f"Error describing conformance pack status: {e}")
            return {"status": "error", "error": str(e)}
    
    # =========================================================================
    # Aggregators Methods
    # =========================================================================
    
    def describe_configuration_aggregators(
        self,
        aggregator_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Describe configuration aggregators.
        
        Args:
            aggregator_names: List of specific aggregator names
            
        Returns:
            List of configuration aggregators
        """
        try:
            kwargs = {}
            if aggregator_names:
                kwargs["ConfigurationAggregatorNames"] = aggregator_names
            
            response = self.config_client.describe_configuration_aggregators(**kwargs)
            
            return {
                "status": "success",
                "data": response.get("ConfigurationAggregators", [])
            }
        except ClientError as e:
            logger.error(f"Error describing aggregators: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_configuration_aggregator(
        self,
        aggregator: Aggregator
    ) -> Dict[str, Any]:
        """
        Create or update a configuration aggregator.
        
        Args:
            aggregator: Aggregator object with settings
            
        Returns:
            Operation result
        """
        try:
            if aggregator.aggregator_type == AggregatorType.ACCOUNT:
                aggregator_definition = {
                    "AccountAggregationSources": [
                        {
                            "AccountIds": aggregator.account_ids,
                            "AllAwsRegions": len(aggregator.regions) == 0
                        }
                    ]
                }
                if aggregator.regions:
                    aggregator_definition["AccountAggregationSources"][0]["AwsRegions"] = (
                        aggregator.regions
                    )
            else:
                aggregator_definition = {
                    "OrganizationAggregationSource": {
                        "RoleArn": self._get_organization_role_arn(),
                        "AllAwsRegions": len(aggregator.regions) == 0
                    }
                }
                if aggregator.organization_id:
                    aggregator_definition["OrganizationAggregationSource"]["OrganizationId"] = (
                        aggregator.organization_id
                    )
                if aggregator.regions:
                    aggregator_definition["OrganizationAggregationSource"]["AwsRegions"] = (
                        aggregator.regions
                    )
            
            self.config_client.put_configuration_aggregator(
                ConfigurationAggregatorName=aggregator.name,
                AccountAggregationSource=aggregator_definition.get("AccountAggregationSources"),
                OrganizationAggregationSource=aggregator_definition.get("OrganizationAggregationSource")
            )
            
            # Apply tags if provided
            if aggregator.tags:
                self.config_client.tag_resource(
                    ResourceArn=self._get_aggregator_arn(aggregator.name),
                    Tags=aggregator.tags
                )
            
            return {
                "status": "success",
                "message": f"Configuration aggregator '{aggregator.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting configuration aggregator: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_configuration_aggregator(
        self,
        aggregator_name: str
    ) -> Dict[str, Any]:
        """
        Delete a configuration aggregator.
        
        Args:
            aggregator_name: Name of the aggregator
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_configuration_aggregator(
                ConfigurationAggregatorName=aggregator_name
            )
            return {
                "status": "success",
                "message": f"Configuration aggregator '{aggregator_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting configuration aggregator: {e}")
            return {"status": "error", "error": str(e)}
    
    def _get_aggregator_arn(self, aggregator_name: str) -> str:
        """Get the ARN for a configuration aggregator."""
        return f"arn:aws:config:{self.region_name}:*:config-aggregator/{aggregator_name}"
    
    def _get_organization_role_arn(self) -> str:
        """Get the IAM role ARN for organization aggregation."""
        return f"arn:aws:iam::{self._get_account_id()}:role/aws-service-role/config.amazonaws.com/AWSConfigRoleForOrganizations"
    
    def _get_account_id(self) -> str:
        """Get the current AWS account ID."""
        try:
            return boto3.Session().client("sts").get_caller_identity()["Account"]
        except:
            return "000000000000"
    
    # =========================================================================
    # Remediation Configuration Methods
    # =========================================================================
    
    def put_remediation_configuration(
        self,
        remediation: RemediationConfiguration
    ) -> Dict[str, Any]:
        """
        Create or update a remediation configuration.
        
        Args:
            remediation: RemediationConfiguration object with settings
            
        Returns:
            Operation result
        """
        try:
            config = {
                "ConfigRuleName": remediation.config_rule_name,
                "TargetType": remediation.target_type,
                "TargetId": remediation.target_id,
                "ExecutionFrequency": remediation.execution_frequency,
                "Parameters": remediation.parameters,
                "RetryAttemptSeconds": remediation.retry_attempts,
                "Automatic": remediation.automatic
            }
            
            self.config_client.put_remediation_configuration(
                RemediationConfiguration=config
            )
            
            return {
                "status": "success",
                "message": f"Remediation configuration for '{remediation.config_rule_name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting remediation configuration: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_remediation_configurations(
        self,
        config_rule_names: List[str]
    ) -> Dict[str, Any]:
        """
        Describe remediation configurations.
        
        Args:
            config_rule_names: List of Config rule names
            
        Returns:
            List of remediation configurations
        """
        try:
            response = self.config_client.describe_remediation_configurations(
                ConfigRuleNames=config_rule_names
            )
            
            return {
                "status": "success",
                "data": response.get("RemediationConfigurations", [])
            }
        except ClientError as e:
            logger.error(f"Error describing remediation configurations: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_remediation_configuration(
        self,
        config_rule_name: str
    ) -> Dict[str, Any]:
        """
        Delete a remediation configuration.
        
        Args:
            config_rule_name: Name of the Config rule
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_remediation_configuration(
                ConfigRuleName=config_rule_name
            )
            return {
                "status": "success",
                "message": f"Remediation configuration for '{config_rule_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting remediation configuration: {e}")
            return {"status": "error", "error": str(e)}
    
    def start_remediation_execution(
        self,
        config_rule_name: str,
        resource_keys: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Start remediation execution for a Config rule.
        
        Args:
            config_rule_name: Name of the Config rule
            resource_keys: Specific resources to remediate
            
        Returns:
            Remediation execution result
        """
        try:
            kwargs = {"ConfigRuleName": config_rule_name}
            if resource_keys:
                kwargs["ResourceKeys"] = resource_keys
            
            self.config_client.start_remediation_execution(**kwargs)
            
            return {
                "status": "success",
                "message": f"Remediation execution started for '{config_rule_name}'"
            }
        except ClientError as e:
            logger.error(f"Error starting remediation execution: {e}")
            return {"status": "error", "error": str(e)}
    
    # =========================================================================
    # Organization Integration Methods
    # =========================================================================
    
    def describe_organization_config_rule(
        self,
        organization_config_rule_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Describe organization Config rules.
        
        Args:
            organization_config_rule_names: List of organization Config rule names
            
        Returns:
            List of organization Config rules
        """
        try:
            kwargs = {}
            if organization_config_rule_names:
                kwargs["OrganizationConfigRuleNames"] = organization_config_rule_names
            
            response = self.config_client.describe_organization_config_rules(**kwargs)
            
            return {
                "status": "success",
                "data": response.get("OrganizationConfigRules", [])
            }
        except ClientError as e:
            logger.error(f"Error describing organization config rules: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_organization_config_rule(
        self,
        rule: ConfigRule,
        organization_id: str
    ) -> Dict[str, Any]:
        """
        Create or update an organization Config rule.
        
        Args:
            rule: ConfigRule object with settings
            organization_id: AWS Organization ID
            
        Returns:
            Operation result
        """
        try:
            config_rule = {
                "OrganizationConfigRuleName": rule.name,
                "RuleIdentifier": {
                    "Owner": rule.source.get("Owner"),
                    "Identifier": rule.source.get("SourceIdentifier")
                },
                "Description": rule.description,
                "Scope": {
                    "OrganizationResourceIdScope": "*",
                    "OrganizationPolicyScope": "ALL"
                },
                "InputParameters": json.dumps(rule.input_parameters),
                "MaximumExecutionFrequency": rule.maximum_execution_frequency
            }
            
            self.config_client.put_organization_config_rule(**config_rule)
            
            return {
                "status": "success",
                "message": f"Organization Config rule '{rule.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting organization config rule: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_organization_config_rule(
        self,
        rule_name: str
    ) -> Dict[str, Any]:
        """
        Delete an organization Config rule.
        
        Args:
            rule_name: Name of the organization Config rule
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_organization_config_rule(
                OrganizationConfigRuleName=rule_name
            )
            return {
                "status": "success",
                "message": f"Organization Config rule '{rule_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting organization config rule: {e}")
            return {"status": "error", "error": str(e)}
    
    def describe_organization_conformance_pack(
        self,
        pack_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Describe organization conformance packs.
        
        Args:
            pack_names: List of organization conformance pack names
            
        Returns:
            List of organization conformance packs
        """
        try:
            kwargs = {}
            if pack_names:
                kwargs["OrganizationConformancePackNames"] = pack_names
            
            response = self.config_client.describe_organization_conformance_packs(**kwargs)
            
            return {
                "status": "success",
                "data": response.get("OrganizationConformancePacks", [])
            }
        except ClientError as e:
            logger.error(f"Error describing organization conformance packs: {e}")
            return {"status": "error", "error": str(e)}
    
    def put_organization_conformance_pack(
        self,
        pack: ConformancePack,
        organization_id: str
    ) -> Dict[str, Any]:
        """
        Create or update an organization conformance pack.
        
        Args:
            pack: ConformancePack object with settings
            organization_id: AWS Organization ID
            
        Returns:
            Operation result
        """
        try:
            kwargs = {
                "OrganizationConformancePackName": pack.name,
                "OrganizationId": organization_id
            }
            
            if pack.template_s3_uri:
                kwargs["TemplateS3Uri"] = pack.template_s3_uri
            elif pack.template_body:
                kwargs["TemplateBody"] = pack.template_body
            
            if pack.delivery_s3_bucket:
                kwargs["DeliveryS3Bucket"] = pack.delivery_s3_bucket
            
            if pack.conformance_pack_input_parameters:
                kwargs["ConformancePackInputParameters"] = [
                    {"ParameterName": k, "ParameterValue": v}
                    for k, v in pack.conformance_pack_input_parameters.items()
                ]
            
            self.config_client.put_organization_conformance_pack(**kwargs)
            
            return {
                "status": "success",
                "message": f"Organization conformance pack '{pack.name}' created/updated"
            }
            
        except ClientError as e:
            logger.error(f"Error putting organization conformance pack: {e}")
            return {"status": "error", "error": str(e)}
    
    def delete_organization_conformance_pack(
        self,
        pack_name: str
    ) -> Dict[str, Any]:
        """
        Delete an organization conformance pack.
        
        Args:
            pack_name: Name of the organization conformance pack
            
        Returns:
            Operation result
        """
        try:
            self.config_client.delete_organization_conformance_pack(
                OrganizationConformancePackName=pack_name
            )
            return {
                "status": "success",
                "message": f"Organization conformance pack '{pack_name}' deleted"
            }
        except ClientError as e:
            logger.error(f"Error deleting organization conformance pack: {e}")
            return {"status": "error", "error": str(e)}
    
    def enable_aws_config(
        self,
        include_global_resources: bool = True
    ) -> Dict[str, Any]:
        """
        Enable AWS Config for the account.
        
        Args:
            include_global_resources: Whether to include global resources
            
        Returns:
            Operation result
        """
        try:
            # Enable the service
            self.config_client.put_configuration_recorder(
                ConfigurationRecorder={
                    "name": "default",
                    "roleARN": self._get_config_role_arn(),
                    "recordingGroup": {
                        "allSupported": True,
                        "includeGlobalResourceTypes": include_global_resources
                    }
                }
            )
            
            self.config_client.start_configuration_recorder(
                ConfigurationRecorderName="default"
            )
            
            return {
                "status": "success",
                "message": "AWS Config enabled successfully"
            }
            
        except ClientError as e:
            logger.error(f"Error enabling AWS Config: {e}")
            return {"status": "error", "error": str(e)}
    
    def _get_config_role_arn(self) -> str:
        """Get or create the IAM role ARN for AWS Config."""
        role_name = "AWSConfigRole"
        try:
            response = self._clients["iam"].get_role(RoleName=role_name)
            return response["Role"]["Arn"]
        except ClientError:
            pass
        
        return f"arn:aws:iam::{self._get_account_id()}:role/{role_name}"
    
    # =========================================================================
    # Resource Compliance Methods
    # =========================================================================
    
    def get_compliance_details_by_resource(
        self,
        resource_type: str,
        resource_id: Optional[str] = None,
        compliance_types: Optional[List[str]] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get compliance details for resources.
        
        Args:
            resource_type: Resource type (e.g., AWS::EC2::Instance)
            resource_id: Specific resource ID
            compliance_types: Filter by compliance types
            limit: Maximum number of results
            
        Returns:
            Resource compliance details
        """
        try:
            kwargs = {
                "ResourceType": resource_type,
                "PaginationConfig": {"MaxItems": limit}
            }
            
            if resource_id:
                kwargs["ResourceId"] = resource_id
            
            if compliance_types:
                kwargs["ComplianceTypes"] = compliance_types
            
            results = []
            paginator = self.config_client.get_paginator(
                "get_compliance_details_by_resource"
            )
            
            for page in paginator.paginate(**kwargs):
                results.extend(page.get("EvaluationResults", []))
            
            return {
                "status": "success",
                "data": results
            }
        except ClientError as e:
            logger.error(f"Error getting compliance details by resource: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_compliance_summary_by_resource_type(
        self,
        resource_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get compliance summary grouped by resource type.
        
        Args:
            resource_types: List of specific resource types
            
        Returns:
            Compliance summary
        """
        try:
            kwargs = {}
            if resource_types:
                kwargs["ResourceTypes"] = resource_types
            
            response = self.config_client.get_compliance_summary_by_resource_type(**kwargs)
            
            summaries = response.get("ComplianceSummariesByResourceType", [])
            
            return {
                "status": "success",
                "data": summaries
            }
        except ClientError as e:
            logger.error(f"Error getting compliance summary: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_discovered_resource_counts(
        self,
        resource_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get counts of discovered resources.
        
        Args:
            resource_types: Filter by specific resource types
            
        Returns:
            Resource counts
        """
        try:
            kwargs = {}
            if resource_types:
                kwargs["ResourceTypes"] = resource_types
            
            response = self.config_client.get_discovered_resource_counts(**kwargs)
            
            return {
                "status": "success",
                "data": {
                    "totalDiscoveredResources": response.get("totalDiscoveredResources"),
                    "resourceCounts": response.get("resourceCounts", [])
                }
            }
        except ClientError as e:
            logger.error(f"Error getting resource counts: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_aggregate_compliance_details_by_config_rule(
        self,
        aggregator_name: str,
        config_rule_name: str,
        account_id: Optional[str] = None,
        region: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get aggregate compliance details for a Config rule.
        
        Args:
            aggregator_name: Name of the configuration aggregator
            config_rule_name: Name of the Config rule
            account_id: Filter by account ID
            region: Filter by region
            limit: Maximum number of results
            
        Returns:
            Aggregate compliance details
        """
        try:
            kwargs = {
                "ConfigurationAggregatorName": aggregator_name,
                "ConfigRuleName": config_rule_name,
                "PaginationConfig": {"MaxItems": limit}
            }
            
            filters = {}
            if account_id:
                filters["AccountId"] = account_id
            if region:
                filters["Region"] = region
            
            if filters:
                kwargs["Filters"] = filters
            
            results = []
            paginator = self.config_client.get_paginator(
                "get_aggregate_compliance_details_by_config_rule"
            )
            
            for page in paginator.paginate(**kwargs):
                results.extend(page.get("EvaluationResults", []))
            
            return {
                "status": "success",
                "data": results
            }
        except ClientError as e:
            logger.error(f"Error getting aggregate compliance details: {e}")
            return {"status": "error", "error": str(e)}
    
    # =========================================================================
    # Configuration Timeline Methods
    # =========================================================================
    
    def get_resource_config_history(
        self,
        resource_type: str,
        resource_id: str,
        chronological_order: str = "Reverse",
        limit: int = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get configuration history for a resource.
        
        Args:
            resource_type: Resource type
            resource_id: Resource ID
            chronological_order: "Reverse" or "Forward"
            limit: Maximum number of results
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Configuration history
        """
        try:
            kwargs = {
                "resourceType": resource_type,
                "resourceId": resource_id,
                "chronologicalOrder": chronological_order,
                "limit": limit
            }
            
            if start_time:
                kwargs["earlierTime"] = start_time
            if end_time:
                kwargs["laterTime"] = end_time
            
            response = self.config_client.list_discovered_resources(**kwargs)
            
            return {
                "status": "success",
                "data": response.get("configurationItems", [])
            }
        except ClientError as e:
            logger.error(f"Error getting resource config history: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_configuration_history(
        self,
        resource_type: str,
        resource_id: str,
        limit: int = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get detailed configuration history for a resource.
        
        Args:
            resource_type: Resource type
            resource_id: Resource ID
            limit: Maximum number of results
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Detailed configuration history
        """
        try:
            kwargs = {
                "resourceType": resource_type,
                "resourceId": resource_id,
                "limit": limit
            }
            
            if start_time:
                kwargs["startTime"] = start_time
            if end_time:
                kwargs["endTime"] = end_time
            
            results = []
            paginator = self.config_client.get_paginator("get_configuration_history")
            
            for page in paginator.paginate(**kwargs):
                results.extend(page.get("configurationItems", []))
            
            return {
                "status": "success",
                "data": results
            }
        except ClientError as e:
            logger.error(f"Error getting configuration history: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_stored_query(self, query_name: str) -> Dict[str, Any]:
        """
        Get a stored query.
        
        Args:
            query_name: Name of the stored query
            
        Returns:
            Stored query details
        """
        try:
            response = self.config_client.get_stored_query(
                QueryName=query_name
            )
            
            return {
                "status": "success",
                "data": response.get("QueryDescription", {})
            }
        except ClientError as e:
            logger.error(f"Error getting stored query: {e}")
            return {"status": "error", "error": str(e)}
    
    def list_aggregate_discovered_resources(
        self,
        aggregator_name: str,
        resource_type: str,
        account_id: Optional[str] = None,
        region: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        List aggregate discovered resources.
        
        Args:
            aggregator_name: Name of the configuration aggregator
            resource_type: Resource type
            account_id: Filter by account ID
            region: Filter by region
            limit: Maximum number of results
            
        Returns:
            Discovered resources
        """
        try:
            kwargs = {
                "ConfigurationAggregatorName": aggregator_name,
                "ResourceType": resource_type,
                "PaginationConfig": {"MaxItems": limit}
            }
            
            filters = []
            if account_id:
                filters.append({"AccountId": account_id})
            if region:
                filters.append({"Region": region})
            
            if filters:
                kwargs["Filters"] = filters
            
            results = []
            paginator = self.config_client.get_paginator(
                "list_aggregate_discovered_resources"
            )
            
            for page in paginator.paginate(**kwargs):
                results.extend(page.get("ResourceIdentifiers", []))
            
            return {
                "status": "success",
                "data": results
            }
        except ClientError as e:
            logger.error(f"Error listing aggregate resources: {e}")
            return {"status": "error", "error": str(e)}
    
    # =========================================================================
    # CloudWatch Integration Methods
    # =========================================================================
    
    def put_cloudwatch_monitoring(
        self,
        config_rule_names: Optional[List[str]] = None,
        compliance_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Configure CloudWatch metrics for Config compliance.
        
        Args:
            config_rule_names: Specific rules to monitor
            compliance_types: Compliance types to track
            
        Returns:
            Operation result
        """
        try:
            # Get compliance summary
            summary = self.get_compliance_summary_by_resource_type()
            
            if summary.get("status") != "success":
                return summary
            
            # Publish metrics to CloudWatch
            for compliance_summary in summary.get("data", []):
                resource_type = compliance_summary.get("ResourceType", "Unknown")
                compliance = compliance_summary.get("Compliance", {})
                
                for compliance_type, count in compliance.items():
                    self.cloudwatch_client.put_metric_data(
                        Namespace="AWS/Config",
                        MetricData=[
                            {
                                "MetricName": compliance_type,
                                "Dimensions": [
                                    {"Name": "ResourceType", "Value": resource_type}
                                ],
                                "Value": count,
                                "Unit": "Count"
                            }
                        ]
                    )
            
            return {
                "status": "success",
                "message": "CloudWatch metrics published for Config compliance"
            }
        except ClientError as e:
            logger.error(f"Error putting CloudWatch monitoring: {e}")
            return {"status": "error", "error": str(e)}
    
    def create_cloudwatch_alarm_for_config(
        self,
        alarm_name: str,
        config_rule_name: str,
        compliance_type: str = "NON_COMPLIANT",
        threshold: int = 1,
        period: int = 300,
        evaluation_periods: int = 1
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for Config rule compliance.
        
        Args:
            alarm_name: Name of the CloudWatch alarm
            config_rule_name: Config rule to monitor
            compliance_type: Compliance type to trigger alarm
            threshold: Threshold for alarm
            period: Evaluation period in seconds
            evaluation_periods: Number of evaluation periods
            
        Returns:
            Operation result
        """
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"Alarm for {config_rule_name} {compliance_type}",
                MetricName="NonCompliantResources",
                Namespace="AWS/Config",
                Statistic="Sum",
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator="GreaterThanOrEqualToThreshold",
                Dimensions=[
                    {"Name": "ConfigRuleName", "Value": config_rule_name},
                    {"Name": "ComplianceType", "Value": compliance_type}
                ]
            )
            
            return {
                "status": "success",
                "message": f"CloudWatch alarm '{alarm_name}' created for '{config_rule_name}'"
            }
        except ClientError as e:
            logger.error(f"Error creating CloudWatch alarm: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_cloudtrail_config_events(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Get CloudTrail events related to AWS Config.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum number of events
            
        Returns:
            CloudTrail events
        """
        try:
            kwargs = {
                "LookupAttributes": [
                    {"AttributeKey": "EventSource", "AttributeValue": "config.amazonaws.com"}
                ],
                "MaxResults": limit
            }
            
            if start_time:
                kwargs["StartTime"] = start_time
            if end_time:
                kwargs["EndTime"] = end_time
            
            results = []
            paginator = self.cloudtrail_client.get_paginator("lookup_events")
            
            for page in paginator.paginate(**kwargs):
                results.extend(page.get("Events", []))
            
            return {
                "status": "success",
                "data": results
            }
        except ClientError as e:
            logger.error(f"Error getting CloudTrail events: {e}")
            return {"status": "error", "error": str(e)}
    
    def enable_cloudtrail_to_config(
        self,
        trail_name: str = "default"
    ) -> Dict[str, Any]:
        """
        Enable CloudTrail to deliver events to AWS Config.
        
        Args:
            trail_name: Name of the CloudTrail trail
            
        Returns:
            Operation result
        """
        try:
            # Ensure CloudTrail is configured to send logs to Config
            self.cloudtrail_client.create_trail(
                Name=trail_name,
                S3BucketName=f"cloudtrail-{self._get_account_id()}",
                IsMultiRegionTrail=True,
                EnableLogFileValidation=True,
                IncludeGlobalServiceEvents=True
            )
            
            self.cloudtrail_client.start_logging(Name=trail_name)
            
            return {
                "status": "success",
                "message": f"CloudTrail '{trail_name}' enabled for Config integration"
            }
        except ClientError as e:
            logger.error(f"Error enabling CloudTrail to Config: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_config_overview(self) -> Dict[str, Any]:
        """
        Get an overview of AWS Config status.
        
        Returns:
            Config overview including recorder, channel, and compliance summary
        """
        try:
            overview = {
                "recorder": self.describe_configuration_recorder(),
                "delivery_channel": self.describe_delivery_channel(),
                "delivery_channel_status": self.describe_delivery_channel_status(),
                "compliance_summary": self.get_compliance_summary_by_resource_type(),
                "discovered_resources": self.get_discovered_resource_counts()
            }
            
            return {
                "status": "success",
                "data": overview
            }
        except ClientError as e:
            logger.error(f"Error getting Config overview: {e}")
            return {"status": "error", "error": str(e)}
    
    def wait_for_configuration(
        self,
        resource_type: str,
        resource_id: str,
        timeout: int = 60,
        poll_interval: int = 5
    ) -> Dict[str, Any]:
        """
        Wait for a resource to appear in AWS Config.
        
        Args:
            resource_type: Resource type
            resource_id: Resource ID
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds
            
        Returns:
            Configuration item if found
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = self.get_resource_config_history(
                resource_type=resource_type,
                resource_id=resource_id,
                limit=1
            )
            
            if response.get("status") == "success" and response.get("data"):
                return {
                    "status": "success",
                    "message": "Resource found",
                    "data": response["data"][0]
                }
            
            time.sleep(poll_interval)
        
        return {
            "status": "timeout",
            "message": f"Resource {resource_id} not found within {timeout} seconds"
        }
