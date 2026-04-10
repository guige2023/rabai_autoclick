"""
AWS Security Hub Integration Module for Workflow System

Implements a SecurityHubIntegration class with:
1. Hub management: Enable/manage Security Hub
2. Standards: Manage security standards
3. Controls: Manage security controls
4. Findings: Get and manage findings
5. Actions: Create and manage custom actions
6. Members: Manage member accounts
7. Admin: Security Hub administrator
8. Insights: Create and manage insights
9. Product integrations: Manage product integrations
10. CloudWatch integration: Monitoring and metrics

Commit: 'feat(aws-securityhub): add AWS Security Hub with hub management, standards, controls, findings, custom actions, members, admin, insights, product integrations, CloudWatch'
"""

import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import hashlib
import base64

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


class SecurityHubState(Enum):
    """Security Hub enabled/disabled states."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    ENABLING = "enabling"
    DISABLING = "disabling"


class FindingSeverity(Enum):
    """Finding severity levels."""
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFORMATIONAL = "Informational"
    NONE = "None"


class FindingStatus(Enum):
    """Finding workflow status."""
    NEW = "NEW"
    NOTIFIED = "NOTIFIED"
    RESOLVED = "RESOLVED"
    ARCHIVED = "ARCHIVED"


class FindingRecordState(Enum):
    """Finding record state."""
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class StandardState(Enum):
    """Security standard state."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class ControlStatus(Enum):
    """Security control status."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class ControlEligibility(Enum):
    """Control eligibility status."""
    ELIGIBLE = "ELIGIBLE"
    INELIGIBLE = "INELIGIBLE"


class MemberAccountStatus(Enum):
    """Member account relationship status."""
    ASSOCIATED = "Associated"
    CREATED = "Created"
    INVITED = "Invited"
    RESIGNED = "Resigned"
    DELETED = "Deleted"
    EMPTY = ""


class AdminStatus(Enum):
    """Security Hub administrator status."""
    ENABLED = "Enabled"
    DISABLED = "Disabled"
    EMPTY = ""


class InsightType(Enum):
    """Insight types."""
    FINDING = "finding"
    RESOURCE = "resource"
    CUSTOM = "custom"


class IntegrationStatus(Enum):
    """Product integration status."""
    CONNECTED = "Connected"
    DISCONNECTED = "Disconnected"
    UNUSED = "Unused"


@dataclass
class SecurityHubConfig:
    """Configuration for Security Hub connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class HubInfo:
    """Information about Security Hub."""
    hub_arn: str
    hub_name: str
    state: SecurityHubState
    enabled_at: datetime
    disabled_at: Optional[datetime] = None
    auto_enable_controls: bool = True
    control_findingGenerator: str = "STANDARD"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class StandardInfo:
    """Information about a security standard."""
    standard_arn: str
    standard_name: str
    standard_version: str
    state: StandardState
    description: Optional[str] = None
    enabled_at: Optional[datetime] = None
    disabled_at: Optional[datetime] = None
    standards_control_count: int = 0


@dataclass
class ControlInfo:
    """Information about a security control."""
    control_arn: str
    control_name: str
    control_category: str
    standard_name: str
    standard_arn: str
    description: str
    remediation_url: Optional[str] = None
    severity_score: Optional[Dict[str, Any]] = None
    related_requirements: List[str] = field(default_factory=list)
    status: ControlStatus = ControlStatus.ENABLED
    eligibility: ControlEligibility = ControlEligibility.ELIGIBLE


@dataclass
class FindingInfo:
    """Information about a security finding."""
    finding_id: str
    product_arn: str
    company_name: str
    product_name: str
    severity: FindingSeverity
    status: FindingStatus
    title: str
    description: str
    resource_type: str
    resource_id: str
    resource_arn: Optional[str] = None
    resource_tags: Dict[str, str] = field(default_factory=dict)
    record_state: FindingRecordState = FindingRecordState.ACTIVE
    workflow_status: FindingStatus = FindingStatus.NEW
    first_observed_at: Optional[datetime] = None
    last_observed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    confidence: Optional[int] = None
    criticality: Optional[int] = None
    related_findings: List[Dict[str, str]] = field(default_factory=list)
    notes: Optional[str] = None
    user_defined_fields: Dict[str, str] = field(default_factory=dict)
    verification_state: str = "UNKNOWN"
    confidence_score: Optional[int] = None
    threat_intel_indicators: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class CustomActionInfo:
    """Information about a custom action."""
    action_name: str
    action_description: str
    action_arn: str
    id: str
    created_at: datetime


@dataclass
class MemberInfo:
    """Information about a member account."""
    account_id: str
    email: str
    relationship_status: MemberAccountStatus
    joined_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    enabled: bool = True


@dataclass
class AdminInfo:
    """Information about Security Hub administrator."""
    account_id: str
    status: AdminStatus
    relationship_created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


@dataclass
class InsightInfo:
    """Information about an insight."""
    insight_arn: str
    name: str
    insight_type: InsightType
    filters: Dict[str, Any]
    grouping_attributes: List[str] = field(default_factory=list)
    created_at: datetime
    updated_at: Optional[datetime] = None
    result_finding_count: int = 0


@dataclass
class ProductIntegrationInfo:
    """Information about a product integration."""
    product_arn: str
    product_name: str
    status: IntegrationStatus
    description: Optional[str] = None
    categories: List[str] = field(default_factory=list)


@dataclass
class CloudWatchConfig:
    """Configuration for CloudWatch metrics."""
    namespace: str = "AWS/SecurityHub"
    metric_name_prefix: str = "SecurityHub"
    enable_finding_metrics: bool = True
    enable_standard_metrics: bool = True
    enable_control_metrics: bool = True


@dataclass
class MetricInfo:
    """Information about a CloudWatch metric."""
    metric_name: str
    value: float
    unit: str
    timestamp: datetime
    dimensions: Dict[str, str] = field(default_factory=dict)


# =============================================================================
# SECURITY HUB INTEGRATION CLASS
# =============================================================================

class SecurityHubIntegration:
    """
    AWS Security Hub Integration.
    
    Provides comprehensive Security Hub management including:
    - Hub management (enable/disable/configure Security Hub)
    - Standards management (enable/disable security standards)
    - Controls management (manage security controls within standards)
    - Findings management (get/update/archive findings)
    - Custom actions (create/manage custom remediation actions)
    - Member account management (manage member accounts)
    - Administrator management (manage Security Hub admin)
    - Insights management (create/manage custom insights)
    - Product integrations (manage third-party product integrations)
    - CloudWatch monitoring (publish Security Hub metrics)
    """
    
    def __init__(self, config: Optional[SecurityHubConfig] = None):
        """
        Initialize the Security Hub integration.
        
        Args:
            config: Security Hub configuration options
        """
        self.config = config or SecurityHubConfig()
        self._client = None
        self._securityhub_client = None
        self._cloudwatch_client = None
        self._lock = threading.RLock()
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AWS clients with proper configuration."""
        with self._lock:
            try:
                session_kwargs = {}
                if self.config.profile_name:
                    session_kwargs['profile_name'] = self.config.profile_name
                
                session = boto3.Session(**session_kwargs)
                
                client_kwargs = {
                    'region_name': self.config.region_name
                }
                
                if self.config.aws_access_key_id and self.config.aws_secret_access_key:
                    client_kwargs['aws_access_key_id'] = self.config.aws_access_key_id
                    client_kwargs['aws_secret_access_key'] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        client_kwargs['aws_session_token'] = self.config.aws_session_token
                
                self._securityhub_client = session.client('securityhub', **client_kwargs)
                self._cloudwatch_client = session.client('cloudwatch', **client_kwargs)
                
            except Exception as e:
                logger.warning(f"Failed to initialize Security Hub clients: {e}")
    
    @property
    def client(self):
        """Get the Security Hub client."""
        return self._securityhub_client
    
    @property
    def is_available(self) -> bool:
        """Check if boto3 is available and clients are initialized."""
        return BOTO3_AVAILABLE and self._securityhub_client is not None
    
    # =========================================================================
    # HUB MANAGEMENT
    # =========================================================================
    
    def enable_security_hub(self, enable_default_standards: bool = True,
                             auto_enable_controls: bool = True) -> bool:
        """
        Enable Security Hub.
        
        Args:
            enable_default_standards: Whether to enable default standards
            auto_enable_controls: Whether to auto-enable controls
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'EnableDefaultStandards': enable_default_standards,
                'AutoEnableControls': auto_enable_controls
            }
            
            self._securityhub_client.enable_security_hub(**kwargs)
            logger.info("Enabled Security Hub")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable Security Hub: {e}")
            raise
    
    def disable_security_hub(self) -> bool:
        """
        Disable Security Hub.
        
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.disable_security_hub()
            logger.info("Disabled Security Hub")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable Security Hub: {e}")
            raise
    
    def get_hub_info(self) -> HubInfo:
        """
        Get information about the Security Hub.
        
        Returns:
            HubInfo object with hub details
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.describe_hub()
            hub = response.get('Hub', {})
            
            return HubInfo(
                hub_arn=hub.get('HubArn', ''),
                hub_name=hub.get('HubName', ''),
                state=SecurityHubState.ENABLED if hub.get('AutoEnableControls') else SecurityHubState.DISABLED,
                enabled_at=hub.get('SubscribedAt', datetime.now()),
                disabled_at=hub.get('DisabledAt'),
                auto_enable_controls=hub.get('AutoEnableControls', True),
                control_findingGenerator=hub.get('ControlFindingGenerator', 'STANDARD'),
                tags=self._get_hub_tags()
            )
            
        except ClientError as e:
            logger.error(f"Failed to get hub info: {e}")
            raise
    
    def _get_hub_tags(self) -> Dict[str, str]:
        """Get tags for the Security Hub."""
        try:
            hub_info = self.get_hub_info()
            response = self._securityhub_client.list_tags_for_resource(
                ResourceArn=hub_info.hub_arn
            )
            return {t['Key']: t['Value'] for t in response.get('Tags', [])}
        except ClientError:
            return {}
    
    def update_hub_configuration(self, auto_enable_controls: Optional[bool] = None,
                                 control_finding_generator: Optional[str] = None) -> bool:
        """
        Update Security Hub configuration.
        
        Args:
            auto_enable_controls: Whether to auto-enable controls
            control_finding_generator: Finding generator (STANDARD or LEGACY)
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            kwargs = {}
            if auto_enable_controls is not None:
                kwargs['AutoEnableControls'] = auto_enable_controls
            if control_finding_generator is not None:
                kwargs['ControlFindingGenerator'] = control_finding_generator
            
            if kwargs:
                self._securityhub_client.update_security_hub_configuration(**kwargs)
            
            logger.info("Updated Security Hub configuration")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update hub configuration: {e}")
            raise
    
    def tag_hub(self, tags: Dict[str, str]) -> bool:
        """
        Tag the Security Hub.
        
        Args:
            tags: Tags to add
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            hub_info = self.get_hub_info()
            self._securityhub_client.tag_resource(
                ResourceArn=hub_info.hub_arn,
                Tags=tags
            )
            logger.info(f"Tagged Security Hub with {len(tags)} tags")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to tag hub: {e}")
            raise
    
    # =========================================================================
    # STANDARDS MANAGEMENT
    # =========================================================================
    
    def get_standards(self) -> List[StandardInfo]:
        """
        Get all available security standards.
        
        Returns:
            List of StandardInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.describe_standards()
            standards = []
            
            for std in response.get('Standards', []):
                standards.append(StandardInfo(
                    standard_arn=std.get('StandardsArn', ''),
                    standard_name=std.get('Name', ''),
                    standard_version=std.get('Version', ''),
                    state=StandardState.ENABLED if std.get('Enabled') else StandardState.DISABLED,
                    description=std.get('Description'),
                    standards_control_count=std.get('StandardsControlCount', 0)
                ))
            
            return standards
            
        except ClientError as e:
            logger.error(f"Failed to get standards: {e}")
            raise
    
    def enable_standard(self, standards_subscription_arn: str) -> bool:
        """
        Enable a security standard.
        
        Args:
            standards_subscription_arn: ARN of the standard to enable
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_enable_standards(
                StandardsSubscriptionRequests=[
                    {'StandardsArn': standards_subscription_arn}
                ]
            )
            logger.info(f"Enabled standard: {standards_subscription_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable standard: {e}")
            raise
    
    def disable_standard(self, standards_subscription_arn: str) -> bool:
        """
        Disable a security standard.
        
        Args:
            standards_subscription_arn: ARN of the standard to disable
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_disable_standards(
                StandardsSubscriptionArns=[standards_subscription_arn]
            )
            logger.info(f"Disabled standard: {standards_subscription_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable standard: {e}")
            raise
    
    def get_enabled_standards(self) -> List[StandardInfo]:
        """
        Get all enabled security standards.
        
        Returns:
            List of enabled StandardInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.describe_standards_controls(
                PaginationConfig={'MaxResults': 100}
            )
            
            standards_controls = defaultdict(list)
            for control in response.get('Controls', []):
                standards_controls[control.get('StandardsArn', '')].append(control)
            
            all_standards = self.get_standards()
            enabled = []
            
            for std in all_standards:
                try:
                    sub_response = self._securityhub_client.describe_standards_subscription(
                        StandardsSubscriptionArn=std.standard_arn.replace(':standards/', ':standards subscription/')
                    )
                    if sub_response.get('StandardsSubscription', {}).get('StandardsStatus') == 'READY':
                        std.state = StandardState.ENABLED
                        std.standards_control_count = len(standards_controls.get(std.standard_arn, []))
                        enabled.append(std)
                except ClientError:
                    pass
            
            return enabled
            
        except ClientError as e:
            logger.error(f"Failed to get enabled standards: {e}")
            raise
    
    # =========================================================================
    # CONTROLS MANAGEMENT
    # =========================================================================
    
    def get_controls(self, standards_subscription_arn: str) -> List[ControlInfo]:
        """
        Get controls for a security standard.
        
        Args:
            standards_subscription_arn: ARN of the standard
            
        Returns:
            List of ControlInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.describe_standards_controls(
                StandardsSubscriptionArn=standards_subscription_arn
            )
            
            controls = []
            for ctrl in response.get('Controls', []):
                controls.append(ControlInfo(
                    control_arn=ctrl.get('ControlArn', ''),
                    control_name=ctrl.get('ControlName', ''),
                    control_category=ctrl.get('ControlCategory', ''),
                    standard_name=ctrl.get('StandardsControlArn', '').split(':standards/')[1].split('/')[0] if ':standards/' in ctrl.get('StandardsControlArn', '') else '',
                    standard_arn=standards_subscription_arn,
                    description=ctrl.get('Description', ''),
                    remediation_url=ctrl.get('RemediationUrl'),
                    severity_score=ctrl.get('Severity'),
                    related_requirements=ctrl.get('RelatedRequirements', []),
                    status=ControlStatus.ENABLED if ctrl.get('ControlStatus') == 'ENABLED' else ControlStatus.DISABLED,
                    eligibility=ControlEligibility.ELIGIBLE if ctrl.get('ControlEligibility') == 'ELIGIBLE' else ControlEligibility.INELIGIBLE
                ))
            
            return controls
            
        except ClientError as e:
            logger.error(f"Failed to get controls: {e}")
            raise
    
    def update_control(self, standards_subscription_arn: str, 
                       control_arn: str, enabled: bool) -> bool:
        """
        Update a security control.
        
        Args:
            standards_subscription_arn: ARN of the standard
            control_arn: ARN of the control
            enabled: Whether to enable or disable the control
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_update_standards_control(
                StandardsSubscriptionArn=standards_subscription_arn,
                ControlArns=[control_arn],
                ControlStatus='ENABLED' if enabled else 'DISABLED'
            )
            logger.info(f"Updated control {control_arn} to {'enabled' if enabled else 'disabled'}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update control: {e}")
            raise
    
    def disable_control(self, standards_subscription_arn: str, 
                        control_arn: str, disabled_reason: Optional[str] = None) -> bool:
        """
        Disable a security control.
        
        Args:
            standards_subscription_arn: ARN of the standard
            control_arn: ARN of the control
            disabled_reason: Reason for disabling
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'StandardsSubscriptionArn': standards_subscription_arn,
                'ControlArns': [control_arn],
                'ControlStatus': 'DISABLED'
            }
            if disabled_reason:
                kwargs['DisabledReason'] = disabled_reason
            
            self._securityhub_client.batch_update_standards_control(**kwargs)
            logger.info(f"Disabled control {control_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable control: {e}")
            raise
    
    def enable_control(self, standards_subscription_arn: str, 
                       control_arn: str) -> bool:
        """
        Enable a security control.
        
        Args:
            standards_subscription_arn: ARN of the standard
            control_arn: ARN of the control
            
        Returns:
            True if successful
        """
        return self.update_control(standards_subscription_arn, control_arn, True)
    
    # =========================================================================
    # FINDINGS MANAGEMENT
    # =========================================================================
    
    def get_findings(self, filters: Optional[Dict[str, Any]] = None,
                     sort_criteria: Optional[List[Dict[str, Any]]] = None,
                     max_results: int = 100) -> List[FindingInfo]:
        """
        Get findings based on filters.
        
        Args:
            filters: Finding filters
            sort_criteria: Sort criteria
            max_results: Maximum number of results
            
        Returns:
            List of FindingInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            kwargs = {
                'Filters': filters or {},
                'SortCriteria': sort_criteria or [{'Field': 'CreatedAt', 'SortOrder': 'desc'}],
                'MaxResults': min(max_results, 100)
            }
            
            findings = []
            paginator = self._securityhub_client.get_paginator('get_findings')
            
            for page in paginator.paginate(**kwargs):
                for finding in page.get('Findings', []):
                    findings.append(self._parse_finding(finding))
            
            return findings
            
        except ClientError as e:
            logger.error(f"Failed to get findings: {e}")
            raise
    
    def _parse_finding(self, finding: Dict[str, Any]) -> FindingInfo:
        """Parse a finding into FindingInfo."""
        severity_str = finding.get('Severity', {}).get('Label', 'INFORMATIONAL')
        try:
            severity = FindingSeverity[severity_str.upper()]
        except KeyError:
            severity = FindingSeverity.INFORMATIONAL
        
        record_state_str = finding.get('RecordState', 'ACTIVE')
        try:
            record_state = FindingRecordState[record_state_str.upper()]
        except KeyError:
            record_state = FindingRecordState.ACTIVE
        
        workflow_str = finding.get('Workflow', {}).get('Status', 'NEW')
        try:
            workflow_status = FindingStatus[workflow_str.upper()]
        except KeyError:
            workflow_status = FindingStatus.NEW
        
        return FindingInfo(
            finding_id=finding.get('Id', ''),
            product_arn=finding.get('ProductArn', ''),
            company_name=finding.get('CompanyName', ''),
            product_name=finding.get('ProductName', ''),
            severity=severity,
            status=workflow_status,
            title=finding.get('Title', ''),
            description=finding.get('Description', ''),
            resource_type=finding.get('Resources', [{}])[0].get('Type', '') if finding.get('Resources') else '',
            resource_id=finding.get('Resources', [{}])[0].get('Id', '') if finding.get('Resources') else '',
            resource_arn=finding.get('Resources', [{}])[0].get('Arn') if finding.get('Resources') else None,
            resource_tags={t['Key']: t['Value'] for t in finding.get('Resources', [{}])[0].get('Tags', {}).items()} if finding.get('Resources') else {},
            record_state=record_state,
            workflow_status=workflow_status,
            first_observed_at=finding.get('FirstObservedAt'),
            last_observed_at=finding.get('LastObservedAt'),
            created_at=finding.get('CreatedAt'),
            updated_at=finding.get('UpdatedAt'),
            confidence=finding.get('Confidence'),
            criticality=finding.get('Criticality'),
            related_findings=[{'ProductArn': f.get('ProductArn', ''), 'Id': f.get('Id', '')} 
                            for f in finding.get('RelatedFindings', [])],
            notes=finding.get('Notes', {}).get('Text') if finding.get('Notes') else None,
            user_defined_fields=finding.get('UserDefinedFields', {}),
            verification_state=finding.get('VerificationState', 'UNKNOWN'),
            confidence_score=finding.get('Confidence', None),
            threat_intel_indicators=finding.get('ThreatIntelIndicators', [])
        )
    
    def get_finding(self, finding_id: str, product_arn: str) -> FindingInfo:
        """
        Get a specific finding.
        
        Args:
            finding_id: Finding ID
            product_arn: Product ARN
            
        Returns:
            FindingInfo object
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.get_findings(
                Filters={'Id': [{'Value': finding_id, 'Comparison': 'EQUALS'}]}
            )
            findings = response.get('Findings', [])
            if findings:
                return self._parse_finding(findings[0])
            raise ValueError(f"Finding not found: {finding_id}")
            
        except ClientError as e:
            logger.error(f"Failed to get finding: {e}")
            raise
    
    def update_findings(self, finding_updates: List[Dict[str, Any]]) -> bool:
        """
        Update findings workflow status.
        
        Args:
            finding_updates: List of finding updates
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_update_findings(
                FindingUpdates=finding_updates
            )
            logger.info(f"Updated {len(finding_updates)} findings")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update findings: {e}")
            raise
    
    def archive_finding(self, finding_id: str, product_arn: str) -> bool:
        """
        Archive a finding.
        
        Args:
            finding_id: Finding ID
            product_arn: Product ARN
            
        Returns:
            True if successful
        """
        return self.update_findings([{
            'Id': finding_id,
            'ProductArn': product_arn,
            'WorkflowStatus': 'RESOLVED'
        }])
    
    def accept_findings(self, finding_ids: List[str], product_arn: str) -> bool:
        """
        Accept findings.
        
        Args:
            finding_ids: List of finding IDs
            product_arn: Product ARN
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_import_findings(
                Findings=[{
                    'Id': fid,
                    'ProductArn': product_arn,
                    'Workflow': {'Status': 'RESOLVED'}
                } for fid in finding_ids]
            )
            logger.info(f"Accepted {len(finding_ids)} findings")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to accept findings: {e}")
            raise
    
    def create_finding(self, finding: Dict[str, Any]) -> bool:
        """
        Create a finding.
        
        Args:
            finding: Finding data
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_import_findings(
                Findings=[finding]
            )
            logger.info(f"Created finding: {finding.get('Title', 'Unknown')}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create finding: {e}")
            raise
    
    # =========================================================================
    # CUSTOM ACTIONS MANAGEMENT
    # =========================================================================
    
    def create_action(self, name: str, description: str) -> CustomActionInfo:
        """
        Create a custom action.
        
        Args:
            name: Action name
            description: Action description
            
        Returns:
            CustomActionInfo object
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.create_action_target(
                Name=name,
                Description=description
            )
            
            action = response.get('ActionTarget', {})
            return CustomActionInfo(
                action_name=action.get('Name', name),
                action_description=description,
                action_arn=action.get('ActionTargetArn', ''),
                id=action.get('Id', ''),
                created_at=datetime.now()
            )
            
        except ClientError as e:
            logger.error(f"Failed to create action: {e}")
            raise
    
    def get_actions(self) -> List[CustomActionInfo]:
        """
        Get all custom actions.
        
        Returns:
            List of CustomActionInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.list_action_targets()
            actions = []
            
            for action in response.get('ActionTargets', []):
                actions.append(CustomActionInfo(
                    action_name=action.get('Name', ''),
                    action_description=action.get('Description', ''),
                    action_arn=action.get('ActionTargetArn', ''),
                    id=action.get('Id', ''),
                    created_at=action.get('CreatedAt', datetime.now())
                ))
            
            return actions
            
        except ClientError as e:
            logger.error(f"Failed to get actions: {e}")
            raise
    
    def delete_action(self, action_arn: str) -> bool:
        """
        Delete a custom action.
        
        Args:
            action_arn: ARN of the action to delete
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.delete_action_target(
                ActionTargetArn=action_arn
            )
            logger.info(f"Deleted action: {action_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete action: {e}")
            raise
    
    def update_action(self, action_arn: str, name: str, description: str) -> bool:
        """
        Update a custom action.
        
        Args:
            action_arn: ARN of the action
            name: New name
            description: New description
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.update_action_target(
                ActionTargetArn=action_arn,
                Name=name,
                Description=description
            )
            logger.info(f"Updated action: {action_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update action: {e}")
            raise
    
    # =========================================================================
    # MEMBER ACCOUNT MANAGEMENT
    # =========================================================================
    
    def get_members(self) -> List[MemberInfo]:
        """
        Get member accounts.
        
        Returns:
            List of MemberInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.list_members()
            members = []
            
            for member in response.get('Members', []):
                status_str = member.get('MemberStatus', '')
                try:
                    status = MemberAccountStatus(status_str.upper()) if status_str else MemberAccountStatus.EMPTY
                except ValueError:
                    status = MemberAccountStatus.EMPTY
                
                members.append(MemberInfo(
                    account_id=member.get('AccountId', ''),
                    email=member.get('Email', ''),
                    relationship_status=status,
                    joined_at=member.get('JoinedAt'),
                    last_updated=member.get('UpdatedAt'),
                    enabled=member.get('Enabled', True)
                ))
            
            return members
            
        except ClientError as e:
            logger.error(f"Failed to get members: {e}")
            raise
    
    def create_member(self, account_id: str, email: str) -> bool:
        """
        Create a member account.
        
        Args:
            account_id: Account ID
            email: Email address
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.create_members(
                AccountDetails=[{
                    'AccountId': account_id,
                    'Email': email
                }]
            )
            logger.info(f"Created member: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create member: {e}")
            raise
    
    def delete_member(self, account_id: str) -> bool:
        """
        Delete a member account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.delete_members(
                AccountIds=[account_id]
            )
            logger.info(f"Deleted member: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete member: {e}")
            raise
    
    def invite_member(self, account_id: str) -> bool:
        """
        Invite a member account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.invite_members(
                AccountIds=[account_id]
            )
            logger.info(f"Invited member: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to invite member: {e}")
            raise
    
    def disassociate_member(self, account_id: str) -> bool:
        """
        Disassociate a member account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.disassociate_members(
                AccountIds=[account_id]
            )
            logger.info(f"Disassociated member: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disassociate member: {e}")
            raise
    
    def enable_member(self, account_id: str) -> bool:
        """
        Enable a member account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.enable_members(
                AccountIds=[account_id]
            )
            logger.info(f"Enabled member: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable member: {e}")
            raise
    
    def disable_member(self, account_id: str) -> bool:
        """
        Disable a member account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.disable_members(
                AccountIds=[account_id]
            )
            logger.info(f"Disabled member: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable member: {e}")
            raise
    
    # =========================================================================
    # ADMINISTRATOR MANAGEMENT
    # =========================================================================
    
    def get_administrator(self) -> Optional[AdminInfo]:
        """
        Get the Security Hub administrator.
        
        Returns:
            AdminInfo object or None
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.list_organization_admin_accounts()
            admins = response.get('AdminAccounts', [])
            
            if admins:
                admin = admins[0]
                status_str = admin.get('Status', '')
                try:
                    status = AdminStatus(status_str.upper()) if status_str else AdminStatus.EMPTY
                except ValueError:
                    status = AdminStatus.EMPTY
                
                return AdminInfo(
                    account_id=admin.get('AccountId', ''),
                    status=status,
                    relationship_created_at=admin.get('CreatedAt'),
                    last_updated=admin.get('UpdatedAt')
                )
            
            return None
            
        except ClientError as e:
            logger.error(f"Failed to get administrator: {e}")
            raise
    
    def enable_organization_admin(self, account_id: str) -> bool:
        """
        Enable an organization admin account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.enable_organization_admin_account(
                AdminAccountId=account_id
            )
            logger.info(f"Enabled organization admin: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable organization admin: {e}")
            raise
    
    def disable_organization_admin(self, account_id: str) -> bool:
        """
        Disable an organization admin account.
        
        Args:
            account_id: Account ID
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.disable_organization_admin_account(
                AdminAccountId=account_id
            )
            logger.info(f"Disabled organization admin: {account_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable organization admin: {e}")
            raise
    
    def get_organization_members(self) -> List[MemberInfo]:
        """
        Get organization member accounts.
        
        Returns:
            List of MemberInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.list_members(
                OnlyAssociated=False
            )
            members = []
            
            for member in response.get('Members', []):
                status_str = member.get('MemberStatus', '')
                try:
                    status = MemberAccountStatus(status_str.upper()) if status_str else MemberAccountStatus.EMPTY
                except ValueError:
                    status = MemberAccountStatus.EMPTY
                
                members.append(MemberInfo(
                    account_id=member.get('AccountId', ''),
                    email=member.get('Email', ''),
                    relationship_status=status,
                    joined_at=member.get('JoinedAt'),
                    last_updated=member.get('UpdatedAt'),
                    enabled=member.get('Enabled', True)
                ))
            
            return members
            
        except ClientError as e:
            logger.error(f"Failed to get organization members: {e}")
            raise
    
    # =========================================================================
    # INSIGHTS MANAGEMENT
    # =========================================================================
    
    def get_insights(self) -> List[InsightInfo]:
        """
        Get all insights.
        
        Returns:
            List of InsightInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.get_insights()
            insights = []
            
            for insight in response.get('Insights', []):
                insight_type_str = 'CUSTOM'
                if 'GroupByAttribute' in insight:
                    if 'ResourceAwsEc2InstanceType' in str(insight.get('Filters', {})):
                        insight_type_str = 'RESOURCE'
                    else:
                        insight_type_str = 'FINDING'
                
                insights.append(InsightInfo(
                    insight_arn=insight.get('InsightArn', ''),
                    name=insight.get('Name', ''),
                    insight_type=InsightType(insight_type_str),
                    filters=insight.get('Filters', {}),
                    grouping_attributes=insight.get('GroupByAttribute', []),
                    created_at=insight.get('CreatedAt', datetime.now()),
                    updated_at=insight.get('UpdatedAt'),
                    result_finding_count=insight.get('ResultFidingCount', 0)
                ))
            
            return insights
            
        except ClientError as e:
            logger.error(f"Failed to get insights: {e}")
            raise
    
    def create_insight(self, name: str, filters: Dict[str, Any],
                       grouping_attributes: List[str]) -> InsightInfo:
        """
        Create an insight.
        
        Args:
            name: Insight name
            filters: Insight filters
            grouping_attributes: Attributes to group by
            
        Returns:
            InsightInfo object
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.create_insight(
                Name=name,
                Filters=filters,
                GroupByAttribute=grouping_attributes
            )
            
            insight = response.get('Insight', {})
            return InsightInfo(
                insight_arn=insight.get('InsightArn', ''),
                name=insight.get('Name', name),
                insight_type=InsightType.CUSTOM,
                filters=insight.get('Filters', {}),
                grouping_attributes=insight.get('GroupByAttribute', []),
                created_at=datetime.now()
            )
            
        except ClientError as e:
            logger.error(f"Failed to create insight: {e}")
            raise
    
    def update_insight(self, insight_arn: str, name: Optional[str] = None,
                       filters: Optional[Dict[str, Any]] = None,
                       grouping_attributes: Optional[List[str]] = None) -> bool:
        """
        Update an insight.
        
        Args:
            insight_arn: ARN of the insight
            name: New name
            filters: New filters
            grouping_attributes: New grouping attributes
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            kwargs = {'InsightArn': insight_arn}
            if name is not None:
                kwargs['Name'] = name
            if filters is not None:
                kwargs['Filters'] = filters
            if grouping_attributes is not None:
                kwargs['GroupByAttribute'] = grouping_attributes
            
            self._securityhub_client.update_insight(**kwargs)
            logger.info(f"Updated insight: {insight_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update insight: {e}")
            raise
    
    def delete_insight(self, insight_arn: str) -> bool:
        """
        Delete an insight.
        
        Args:
            insight_arn: ARN of the insight
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.delete_insight(
                InsightArn=insight_arn
            )
            logger.info(f"Deleted insight: {insight_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete insight: {e}")
            raise
    
    def get_insight_results(self, insight_arn: str) -> List[FindingInfo]:
        """
        Get findings for an insight.
        
        Args:
            insight_arn: ARN of the insight
            
        Returns:
            List of FindingInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.get_insight_results(
                InsightArn=insight_arn
            )
            
            insights = response.get('Insights', [])
            if not insights:
                return []
            
            insight = insights[0]
            return self.get_findings(
                filters=insight.get('Filters', {}),
                max_results=100
            )
            
        except ClientError as e:
            logger.error(f"Failed to get insight results: {e}")
            raise
    
    # =========================================================================
    # PRODUCT INTEGRATIONS MANAGEMENT
    # =========================================================================
    
    def get_integrations(self) -> List[ProductIntegrationInfo]:
        """
        Get product integrations.
        
        Returns:
            List of ProductIntegrationInfo objects
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.list_enabled_products_for_import()
            integrations = []
            
            for product_arn in response.get('ProductSubscriptions', []):
                try:
                    sub_response = self._securityhub_client.get_enabled_product(
                        ProductSubscriptionArn=product_arn
                    )
                    product = sub_response.get('Product', {})
                    status_str = product.get('Status', 'DISCONNECTED')
                    try:
                        status = IntegrationStatus(status_str.upper())
                    except ValueError:
                        status = IntegrationStatus.DISCONNECTED
                    
                    integrations.append(ProductIntegrationInfo(
                        product_arn=product.get('ProductArn', product_arn),
                        product_name=product.get('Name', ''),
                        status=status,
                        description=product.get('Description'),
                        categories=product.get('Categories', [])
                    ))
                except ClientError:
                    integrations.append(ProductIntegrationInfo(
                        product_arn=product_arn,
                        product_name='',
                        status=IntegrationStatus.DISCONNECTED
                    ))
            
            return integrations
            
        except ClientError as e:
            logger.error(f"Failed to get integrations: {e}")
            raise
    
    def enable_integration(self, product_arn: str) -> bool:
        """
        Enable a product integration.
        
        Args:
            product_arn: Product ARN
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.batch_enable_standards(
                StandardsSubscriptionRequests=[]
            )
            self._securityhub_client.enable_import_findings_for_product(
                ProductArn=product_arn
            )
            logger.info(f"Enabled integration: {product_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable integration: {e}")
            raise
    
    def disable_integration(self, product_subscription_arn: str) -> bool:
        """
        Disable a product integration.
        
        Args:
            product_subscription_arn: Product subscription ARN
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            self._securityhub_client.disable_import_findings_for_product(
                ProductSubscriptionArn=product_subscription_arn
            )
            logger.info(f"Disabled integration: {product_subscription_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable integration: {e}")
            raise
    
    def get_available_products(self) -> List[Dict[str, Any]]:
        """
        Get available products for integration.
        
        Returns:
            List of available products
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.list_available_product_arrangements()
            return response.get('ProductArrangements', [])
            
        except ClientError as e:
            logger.error(f"Failed to get available products: {e}")
            raise
    
    # =========================================================================
    # CLOUDWATCH INTEGRATION
    # =========================================================================
    
    def publish_metric(self, metric: MetricInfo, namespace: str = "AWS/SecurityHub") -> bool:
        """
        Publish a CloudWatch metric.
        
        Args:
            metric: Metric information
            namespace: CloudWatch namespace
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("CloudWatch client not available. Install boto3 to enable.")
        
        try:
            metric_data = {
                'MetricName': metric.metric_name,
                'Value': metric.value,
                'Unit': metric.unit,
                'Timestamp': metric.timestamp,
                'Dimensions': [{'Name': k, 'Value': v} for k, v in metric.dimensions.items()]
            }
            
            self._cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=[metric_data]
            )
            logger.debug(f"Published metric: {metric.metric_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to publish metric: {e}")
            raise
    
    def publish_finding_metrics(self, findings: List[FindingInfo]) -> bool:
        """
        Publish finding metrics to CloudWatch.
        
        Args:
            findings: List of findings
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("CloudWatch client not available. Install boto3 to enable.")
        
        try:
            metrics = []
            severity_counts = defaultdict(int)
            
            for finding in findings:
                severity_counts[finding.severity.value] += 1
            
            for severity, count in severity_counts.items():
                metrics.append({
                    'MetricName': f'FindingCountBySeverity_{severity}',
                    'Value': count,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(),
                    'Dimensions': [
                        {'Name': 'Severity', 'Value': severity}
                    ]
                })
            
            metrics.append({
                'MetricName': 'TotalFindings',
                'Value': len(findings),
                'Unit': 'Count',
                'Timestamp': datetime.now()
            })
            
            self._cloudwatch_client.put_metric_data(
                Namespace='AWS/SecurityHub',
                MetricData=metrics
            )
            logger.info(f"Published {len(metrics)} finding metrics")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to publish finding metrics: {e}")
            raise
    
    def publish_standards_metrics(self, standards: List[StandardInfo]) -> bool:
        """
        Publish standards metrics to CloudWatch.
        
        Args:
            standards: List of standards
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("CloudWatch client not available. Install boto3 to enable.")
        
        try:
            metrics = []
            
            enabled_count = sum(1 for s in standards if s.state == StandardState.ENABLED)
            disabled_count = sum(1 for s in standards if s.state == StandardState.DISABLED)
            
            metrics.append({
                'MetricName': 'EnabledStandards',
                'Value': enabled_count,
                'Unit': 'Count',
                'Timestamp': datetime.now()
            })
            
            metrics.append({
                'MetricName': 'DisabledStandards',
                'Value': disabled_count,
                'Unit': 'Count',
                'Timestamp': datetime.now()
            })
            
            self._cloudwatch_client.put_metric_data(
                Namespace='AWS/SecurityHub',
                MetricData=metrics
            )
            logger.info(f"Published {len(metrics)} standards metrics")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to publish standards metrics: {e}")
            raise
    
    def create_dashboard(self, dashboard_name: str) -> bool:
        """
        Create a CloudWatch dashboard for Security Hub.
        
        Args:
            dashboard_name: Name of the dashboard
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("CloudWatch client not available. Install boto3 to enable.")
        
        try:
            dashboard_body = json.dumps({
                'widgets': [
                    {
                        'type': 'metric',
                        'properties': {
                            'metrics': [
                                ['AWS/SecurityHub', 'TotalFindings', {'stat': 'Sum'}]
                            ],
                            'period': 300,
                            'stat': 'Sum',
                            'region': self.config.region_name,
                            'title': 'Total Findings'
                        }
                    },
                    {
                        'type': 'metric',
                        'properties': {
                            'metrics': [
                                ['AWS/SecurityHub', 'FindingCountBySeverity_Critical', {'stat': 'Sum'}],
                                ['.', 'FindingCountBySeverity_High', {'stat': 'Sum'}],
                                ['.', 'FindingCountBySeverity_Medium', {'stat': 'Sum'}],
                                ['.', 'FindingCountBySeverity_Low', {'stat': 'Sum'}]
                            ],
                            'period': 300,
                            'stat': 'Sum',
                            'region': self.config.region_name,
                            'title': 'Findings by Severity'
                        }
                    }
                ]
            })
            
            self._cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=dashboard_body
            )
            logger.info(f"Created CloudWatch dashboard: {dashboard_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def create_alarm(self, alarm_name: str, metric_name: str,
                     threshold: float, comparison_operator: str = 'GreaterThanThreshold',
                     period: int = 300, evaluation_periods: int = 1) -> bool:
        """
        Create a CloudWatch alarm for Security Hub metrics.
        
        Args:
            alarm_name: Name of the alarm
            metric_name: Metric name
            threshold: Threshold value
            comparison_operator: Comparison operator
            period: Period in seconds
            evaluation_periods: Number of evaluation periods
            
        Returns:
            True if successful
        """
        if not self.is_available:
            raise RuntimeError("CloudWatch client not available. Install boto3 to enable.")
        
        try:
            self._cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace='AWS/SecurityHub',
                Statistic='Sum',
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                AlarmActions=[]
            )
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_security_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive security summary.
        
        Returns:
            Dictionary with security summary
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        try:
            response = self._securityhub_client.describe_hub()
            
            findings = self.get_findings(max_results=1000)
            standards = self.get_enabled_standards()
            
            severity_counts = defaultdict(int)
            for finding in findings:
                severity_counts[finding.severity.value] += 1
            
            return {
                'hub_arn': response.get('HubArn', ''),
                'security_score': self._calculate_security_score(findings),
                'total_findings': len(findings),
                'findings_by_severity': dict(severity_counts),
                'enabled_standards': len(standards),
                'total_controls': sum(s.standards_control_count for s in standards),
                'critical_findings': severity_counts.get('Critical', 0),
                'high_findings': severity_counts.get('High', 0)
            }
            
        except ClientError as e:
            logger.error(f"Failed to get security summary: {e}")
            raise
    
    def _calculate_security_score(self, findings: List[FindingInfo]) -> float:
        """Calculate a security score based on findings."""
        if not findings:
            return 100.0
        
        weights = {
            FindingSeverity.CRITICAL: 40,
            FindingSeverity.HIGH: 25,
            FindingSeverity.MEDIUM: 15,
            FindingSeverity.LOW: 5,
            FindingSeverity.INFORMATIONAL: 1,
            FindingSeverity.NONE: 0
        }
        
        total_weight = sum(weights.get(f.severity, 0) for f in findings)
        max_possible = len(findings) * 40
        
        score = max(0, 100 - (total_weight / max_possible * 100)) if max_possible > 0 else 100
        return round(score, 2)
    
    def run_security_audit(self) -> Dict[str, Any]:
        """
        Run a comprehensive security audit.
        
        Returns:
            Dictionary with audit results
        """
        if not self.is_available:
            raise RuntimeError("Security Hub client not available. Install boto3 to enable.")
        
        findings = self.get_findings(max_results=1000)
        standards = self.get_enabled_standards()
        members = self.get_members()
        insights = self.get_insights()
        
        audit_results = {
            'timestamp': datetime.now().isoformat(),
            'hub_info': self.get_hub_info().__dict__,
            'standards': [s.__dict__ for s in standards],
            'total_findings': len(findings),
            'findings_by_severity': defaultdict(int),
            'top_findings': [],
            'member_accounts': len(members),
            'custom_insights': len(insights),
            'recommendations': []
        }
        
        for finding in findings:
            audit_results['findings_by_severity'][finding.severity.value] += 1
        
        top_findings = sorted(findings, 
                             key=lambda f: (list(FindingSeverity).index(f.severity), 
                                          f.created_at or datetime.min),
                             reverse=True)[:10]
        audit_results['top_findings'] = [
            {
                'id': f.finding_id,
                'title': f.title,
                'severity': f.severity.value,
                'resource': f.resource_id
            } for f in top_findings
        ]
        
        if audit_results['findings_by_severity'].get('Critical', 0) > 0:
            audit_results['recommendations'].append(
                "Critical findings detected - immediate action required"
            )
        
        if len(standards) < 3:
            audit_results['recommendations'].append(
                "Consider enabling additional security standards"
            )
        
        return audit_results
