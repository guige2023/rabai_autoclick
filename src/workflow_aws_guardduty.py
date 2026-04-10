"""
AWS GuardDuty Integration Module for Workflow System

Implements a GuardDutyIntegration class with:
1. Detector management: Manage GuardDuty detectors
2. IP sets: Manage IP sets (allowlist/denylist)
3. Threat intel: Manage threat intel sets
4. Findings: Get and manage security findings
5. Archive: Archive and unarchive findings
6. Filters: Create and manage finding filters
7. Member accounts: Manage member accounts
8. Admin: GuardDuty administrator account
9. S3 protection: Enable S3 protection
10. CloudWatch integration: Findings and metrics

Commit: 'feat(aws-guardduty): add AWS GuardDuty with detector management, IP sets, threat intel, findings, archive, filters, member accounts, admin account, S3 protection, CloudWatch'
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


class FindingStatus(Enum):
    """GuardDuty finding status."""
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class DetectorStatus(Enum):
    """GuardDuty detector status."""
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class IPSetStatus(Enum):
    """IP set status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class ThreatIntelSetStatus(Enum):
    """Threat intel set status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


class FindingSeverity(Enum):
    """GuardDuty finding severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class FilterAction(Enum):
    """Filter action types."""
    ARCHIVE = "ARCHIVE"
    NOOP = "NOOP"


@dataclass
class Detector:
    """GuardDuty detector configuration."""
    detector_id: str
    region: str
    status: DetectorStatus
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    finding_publishing_frequency: str = "FIFTEEN_MINUTES"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class IPSet:
    """IP set configuration."""
    ip_set_id: str
    name: str
    format: str = "TXT"  # "TXT", "STIX", "OTX_CSV", "ALIEN_VAULT", "PROOF_POINT", "FIRE_EYE"
    location: str = ""
    status: IPSetStatus = IPSetStatus.INACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ThreatIntelSet:
    """Threat intelligence set configuration."""
    threat_intel_set_id: str
    name: str
    format: str = "TXT"  # "TXT", "STIX", "OTX_CSV", "ALIEN_VAULT", "PROOF_POINT", "FIRE_EYE"
    location: str = ""
    status: ThreatIntelSetStatus = ThreatIntelSetStatus.INACTIVE
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Finding:
    """GuardDuty security finding."""
    finding_id: str
    detector_id: str
    severity: FindingSeverity
    status: FindingStatus
    title: str
    description: str = ""
    account_id: str = ""
    region: str = ""
    resource_type: str = ""
    resource_id: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Filter:
    """Finding filter configuration."""
    name: str
    action: FilterAction
    finding_criteria: Dict[str, Any]
    description: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class MemberAccount:
    """GuardDuty member account."""
    account_id: str
    email: str = ""
    detector_id: str = ""
    relationship_status: str = ""
    invited_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AdminAccount:
    """GuardDuty administrator account."""
    admin_account_id: str
    relationship_status: str = ""
    invited_at: Optional[datetime] = None


class GuardDutyIntegration:
    """
    AWS GuardDuty Integration for Workflow System.
    
    Provides comprehensive GuardDuty functionality including:
    - Detector Management: Manage GuardDuty detectors
    - IP Sets: Manage IP sets (allowlist/denylist)
    - Threat Intel: Manage threat intel sets
    - Findings: Get and manage security findings
    - Archive: Archive and unarchive findings
    - Filters: Create and manage finding filters
    - Member Accounts: Manage member accounts
    - Admin Account: GuardDuty administrator account
    - S3 Protection: Enable S3 protection
    - CloudWatch Integration: Findings and metrics
    
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
        Initialize AWS GuardDuty integration.
        
        Args:
            region_name: AWS region for GuardDuty operations
            profile_name: AWS credentials profile name
            endpoint_url: Custom GuardDuty endpoint URL
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
        """Initialize boto3 clients for AWS GuardDuty services."""
        try:
            session_kwargs = {"region_name": self.region_name}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            # GuardDuty client
            self._clients["guardduty"] = session.client(
                "guardduty",
                endpoint_url=self.endpoint_url
            )
            
            # CloudWatch client for metrics integration
            self._clients["cloudwatch"] = session.client(
                "cloudwatch",
                endpoint_url=self.endpoint_url
            )
            
            # CloudWatch Events client
            self._clients["events"] = session.client(
                "events",
                endpoint_url=self.endpoint_url
            )
            
            # IAM client for role operations
            self._clients["iam"] = session.client(
                "iam",
                endpoint_url=self.endpoint_url
            )
            
            # STS client for account operations
            self._clients["sts"] = session.client(
                "sts",
                endpoint_url=self.endpoint_url
            )
            
            # Organizations client for admin account
            self._clients["organizations"] = session.client(
                "organizations",
                endpoint_url=self.endpoint_url
            )
            
            logger.info(f"Initialized AWS GuardDuty clients in region {self.region_name}")
            
        except Exception as e:
            logger.warning(f"Failed to initialize AWS clients: {e}")
    
    @property
    def guardduty_client(self):
        """Get AWS GuardDuty client."""
        if "guardduty" not in self._clients:
            self._initialize_clients()
        return self._clients.get("guardduty")
    
    @property
    def cloudwatch_client(self):
        """Get CloudWatch client."""
        if "cloudwatch" not in self._clients:
            self._initialize_clients()
        return self._clients.get("cloudwatch")
    
    @property
    def events_client(self):
        """Get CloudWatch Events client."""
        if "events" not in self._clients:
            self._initialize_clients()
        return self._clients.get("events")
    
    # ==================== Detector Management ====================
    
    def create_detector(
        self,
        enable: bool = True,
        finding_publishing_frequency: str = "FIFTEEN_MINUTES",
        tags: Optional[Dict[str, str]] = None
    ) -> Detector:
        """
        Create a GuardDuty detector.
        
        Args:
            enable: Enable GuardDuty immediately
            finding_publishing_frequency: Frequency for publishing findings
            tags: Tags to apply
            
        Returns:
            Detector object
        """
        try:
            kwargs = {
                "enable": enable,
                "findingPublishingFrequency": finding_publishing_frequency
            }
            if tags:
                kwargs["tags"] = tags
            
            response = self.guardduty_client.create_detector(**kwargs)
            detector_id = response["detectorId"]
            
            return Detector(
                detector_id=detector_id,
                region=self.region_name,
                status=DetectorStatus.ENABLED if enable else DetectorStatus.DISABLED,
                finding_publishing_frequency=finding_publishing_frequency,
                tags=tags or {}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create detector: {e}")
            raise
    
    def get_detector(self, detector_id: str) -> Optional[Detector]:
        """
        Get GuardDuty detector details.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            Detector object or None
        """
        try:
            response = self.guardduty_client.get_detector(DetectorId=detector_id)
            
            return Detector(
                detector_id=detector_id,
                region=self.region_name,
                status=DetectorStatus.ENABLED if response.get("status") == "ENABLED" else DetectorStatus.DISABLED,
                finding_publishing_frequency=response.get("findingPublishingFrequency", "FIFTEEN_MINUTES"),
                created_at=self._parse_datetime(response.get("createdAt")),
                updated_at=self._parse_datetime(response.get("updatedAt")),
                tags=response.get("tags", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get detector: {e}")
            return None
    
    def list_detectors(self) -> List[str]:
        """
        List all GuardDuty detector IDs.
        
        Returns:
            List of detector IDs
        """
        try:
            response = self.guardduty_client.list_detectors()
            return response.get("DetectorIds", [])
            
        except ClientError as e:
            logger.error(f"Failed to list detectors: {e}")
            return []
    
    def update_detector(
        self,
        detector_id: str,
        enable: Optional[bool] = None,
        finding_publishing_frequency: Optional[str] = None
    ) -> bool:
        """
        Update GuardDuty detector.
        
        Args:
            detector_id: Detector ID
            enable: Enable or disable GuardDuty
            finding_publishing_frequency: Update publishing frequency
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"DetectorId": detector_id}
            
            if enable is not None:
                kwargs["enable"] = enable
            
            if finding_publishing_frequency:
                kwargs["findingPublishingFrequency"] = finding_publishing_frequency
            
            self.guardduty_client.update_detector(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update detector: {e}")
            return False
    
    def delete_detector(self, detector_id: str) -> bool:
        """
        Delete GuardDuty detector.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.delete_detector(DetectorId=detector_id)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete detector: {e}")
            return False
    
    def get_detector_status(self, detector_id: str) -> Optional[DetectorStatus]:
        """
        Get detector status.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            DetectorStatus or None
        """
        detector = self.get_detector(detector_id)
        return detector.status if detector else None
    
    # ==================== IP Sets ====================
    
    def create_ip_set(
        self,
        detector_id: str,
        name: str,
        format: str = "TXT",
        location: str = "",
        activate: bool = False,
        tags: Optional[Dict[str, str]] = None
    ) -> IPSet:
        """
        Create an IP set.
        
        Args:
            detector_id: Detector ID
            name: IP set name
            format: Format of the IP set
            location: S3 location of the IP set
            activate: Activate the IP set immediately
            tags: Tags to apply
            
        Returns:
            IPSet object
        """
        try:
            kwargs = {
                "DetectorId": detector_id,
                "name": name,
                "format": format,
                "location": location,
                "activate": activate
            }
            if tags:
                kwargs["tags"] = tags
            
            response = self.guardduty_client.create_ip_set(**kwargs)
            ip_set_id = response["ipSetId"]
            
            return IPSet(
                ip_set_id=ip_set_id,
                name=name,
                format=format,
                location=location,
                status=IPSetStatus.ACTIVE if activate else IPSetStatus.INACTIVE,
                tags=tags or {}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create IP set: {e}")
            raise
    
    def get_ip_set(self, detector_id: str, ip_set_id: str) -> Optional[IPSet]:
        """
        Get IP set details.
        
        Args:
            detector_id: Detector ID
            ip_set_id: IP set ID
            
        Returns:
            IPSet object or None
        """
        try:
            response = self.guardduty_client.get_ip_set(
                DetectorId=detector_id,
                IpSetId=ip_set_id
            )
            
            return IPSet(
                ip_set_id=ip_set_id,
                name=response.get("name", ""),
                format=response.get("format", "TXT"),
                location=response.get("location", ""),
                status=IPSetStatus.ACTIVE if response.get("status") == "ACTIVE" else IPSetStatus.INACTIVE,
                tags=response.get("tags", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get IP set: {e}")
            return None
    
    def list_ip_sets(self, detector_id: str) -> List[Dict[str, str]]:
        """
        List IP sets for a detector.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            List of IP set details
        """
        try:
            response = self.guardduty_client.list_ip_sets(DetectorId=detector_id)
            return response.get("ipSets", [])
            
        except ClientError as e:
            logger.error(f"Failed to list IP sets: {e}")
            return []
    
    def update_ip_set(
        self,
        detector_id: str,
        ip_set_id: str,
        location: Optional[str] = None,
        activate: Optional[bool] = None
    ) -> bool:
        """
        Update an IP set.
        
        Args:
            detector_id: Detector ID
            ip_set_id: IP set ID
            location: New S3 location
            activate: Activate or deactivate
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"DetectorId": detector_id, "IpSetId": ip_set_id}
            
            if location:
                kwargs["location"] = location
            if activate is not None:
                kwargs["activate"] = activate
            
            self.guardduty_client.update_ip_set(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update IP set: {e}")
            return False
    
    def delete_ip_set(self, detector_id: str, ip_set_id: str) -> bool:
        """
        Delete an IP set.
        
        Args:
            detector_id: Detector ID
            ip_set_id: IP set ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.delete_ip_set(
                DetectorId=detector_id,
                IpSetId=ip_set_id
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete IP set: {e}")
            return False
    
    def create_allowlist(
        self,
        detector_id: str,
        name: str,
        ips: List[str],
        activate: bool = True
    ) -> IPSet:
        """
        Create an allowlist IP set.
        
        Args:
            detector_id: Detector ID
            name: Allowlist name
            ips: List of allowed IPs
            activate: Activate immediately
            
        Returns:
            IPSet object
        """
        # Note: GuardDuty IP sets are for threat detection, not direct allowlists
        # This creates an IPSet that can be used for filtering
        location = f"s3://guardduty-ip-sets/{name}/allowlist.txt"
        
        return self.create_ip_set(
            detector_id=detector_id,
            name=f"allowlist-{name}",
            format="TXT",
            location=location,
            activate=activate
        )
    
    def create_denylist(
        self,
        detector_id: str,
        name: str,
        ips: List[str],
        activate: bool = True
    ) -> IPSet:
        """
        Create a denylist IP set.
        
        Args:
            detector_id: Detector ID
            name: Denylist name
            ips: List of blocked IPs
            activate: Activate immediately
            
        Returns:
            IPSet object
        """
        location = f"s3://guardduty-ip-sets/{name}/denylist.txt"
        
        return self.create_ip_set(
            detector_id=detector_id,
            name=f"denylist-{name}",
            format="TXT",
            location=location,
            activate=activate
        )
    
    # ==================== Threat Intel Sets ====================
    
    def create_threat_intel_set(
        self,
        detector_id: str,
        name: str,
        format: str = "TXT",
        location: str = "",
        activate: bool = False,
        tags: Optional[Dict[str, str]] = None
    ) -> ThreatIntelSet:
        """
        Create a threat intelligence set.
        
        Args:
            detector_id: Detector ID
            name: Threat intel set name
            format: Format of the set
            location: S3 location of the threat intel set
            activate: Activate immediately
            tags: Tags to apply
            
        Returns:
            ThreatIntelSet object
        """
        try:
            kwargs = {
                "DetectorId": detector_id,
                "name": name,
                "format": format,
                "location": location,
                "activate": activate
            }
            if tags:
                kwargs["tags"] = tags
            
            response = self.guardduty_client.create_threat_intel_set(**kwargs)
            threat_intel_set_id = response["threatIntelSetId"]
            
            return ThreatIntelSet(
                threat_intel_set_id=threat_intel_set_id,
                name=name,
                format=format,
                location=location,
                status=ThreatIntelSetStatus.ACTIVE if activate else ThreatIntelSetStatus.INACTIVE,
                tags=tags or {}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create threat intel set: {e}")
            raise
    
    def get_threat_intel_set(
        self,
        detector_id: str,
        threat_intel_set_id: str
    ) -> Optional[ThreatIntelSet]:
        """
        Get threat intel set details.
        
        Args:
            detector_id: Detector ID
            threat_intel_set_id: Threat intel set ID
            
        Returns:
            ThreatIntelSet object or None
        """
        try:
            response = self.guardduty_client.get_threat_intel_set(
                DetectorId=detector_id,
                ThreatIntelSetId=threat_intel_set_id
            )
            
            return ThreatIntelSet(
                threat_intel_set_id=threat_intel_set_id,
                name=response.get("name", ""),
                format=response.get("format", "TXT"),
                location=response.get("location", ""),
                status=ThreatIntelSetStatus.ACTIVE if response.get("status") == "ACTIVE" else ThreatIntelSetStatus.INACTIVE,
                tags=response.get("tags", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get threat intel set: {e}")
            return None
    
    def list_threat_intel_sets(self, detector_id: str) -> List[Dict[str, str]]:
        """
        List threat intel sets for a detector.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            List of threat intel set details
        """
        try:
            response = self.guardduty_client.list_threat_intel_sets(DetectorId=detector_id)
            return response.get("threatIntelSets", [])
            
        except ClientError as e:
            logger.error(f"Failed to list threat intel sets: {e}")
            return []
    
    def update_threat_intel_set(
        self,
        detector_id: str,
        threat_intel_set_id: str,
        location: Optional[str] = None,
        activate: Optional[bool] = None
    ) -> bool:
        """
        Update a threat intel set.
        
        Args:
            detector_id: Detector ID
            threat_intel_set_id: Threat intel set ID
            location: New S3 location
            activate: Activate or deactivate
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"DetectorId": detector_id, "ThreatIntelSetId": threat_intel_set_id}
            
            if location:
                kwargs["location"] = location
            if activate is not None:
                kwargs["activate"] = activate
            
            self.guardduty_client.update_threat_intel_set(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update threat intel set: {e}")
            return False
    
    def delete_threat_intel_set(self, detector_id: str, threat_intel_set_id: str) -> bool:
        """
        Delete a threat intel set.
        
        Args:
            detector_id: Detector ID
            threat_intel_set_id: Threat intel set ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.delete_threat_intel_set(
                DetectorId=detector_id,
                ThreatIntelSetId=threat_intel_set_id
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete threat intel set: {e}")
            return False
    
    # ==================== Findings ====================
    
    def get_findings(
        self,
        detector_id: str,
        finding_ids: Optional[List[str]] = None,
        filter_criteria: Optional[Dict[str, Any]] = None,
        max_results: int = 50
    ) -> List[Finding]:
        """
        Get GuardDuty findings.
        
        Args:
            detector_id: Detector ID
            finding_ids: Specific finding IDs to retrieve
            filter_criteria: Filter criteria for findings
            max_results: Maximum number of results
            
        Returns:
            List of Finding objects
        """
        try:
            kwargs = {"DetectorId": detector_id, "maxResults": max_results}
            
            if finding_ids:
                kwargs["findingIds"] = finding_ids
            
            if filter_criteria:
                kwargs["filterCriteria"] = filter_criteria
            
            response = self.guardduty_client.get_findings(**kwargs)
            findings = []
            
            for finding_data in response.get("Findings", []):
                severity_str = finding_data.get("Severity", "LOW")
                try:
                    severity = FindingSeverity[severity_str.upper()]
                except KeyError:
                    severity = FindingSeverity.LOW
                
                status_str = finding_data.get("Service", {}).get("Action", {}).get("actionType", "OBSERVED")
                status = FindingStatus.ACTIVE
                
                resource = finding_data.get("Resource", {})
                resource_type = resource.get("resourceType", "")
                resource_id = str(resource.get("resourceId", ""))
                
                findings.append(Finding(
                    finding_id=finding_data.get("id", ""),
                    detector_id=detector_id,
                    severity=severity,
                    status=status,
                    title=finding_data.get("Title", ""),
                    description=finding_data.get("Description", ""),
                    account_id=finding_data.get("AccountId", ""),
                    region=finding_data.get("Region", ""),
                    resource_type=resource_type,
                    resource_id=resource_id,
                    created_at=self._parse_datetime(finding_data.get("CreatedAt")),
                    updated_at=self._parse_datetime(finding_data.get("UpdatedAt")),
                    tags=finding_data.get("Tags", {})
                ))
            
            return findings
            
        except ClientError as e:
            logger.error(f"Failed to get findings: {e}")
            return []
    
    def list_findings(
        self,
        detector_id: str,
        finding_criteria: Optional[Dict[str, Any]] = None,
        max_results: int = 50
    ) -> List[str]:
        """
        List finding IDs.
        
        Args:
            detector_id: Detector ID
            finding_criteria: Criteria to filter findings
            max_results: Maximum number of results
            
        Returns:
            List of finding IDs
        """
        try:
            kwargs = {"DetectorId": detector_id, "maxResults": max_results}
            
            if finding_criteria:
                kwargs["findingCriteria"] = finding_criteria
            
            response = self.guardduty_client.list_findings(**kwargs)
            return response.get("findingIds", [])
            
        except ClientError as e:
            logger.error(f"Failed to list findings: {e}")
            return []
    
    def get_severity_statistics(self, detector_id: str) -> Dict[str, int]:
        """
        Get finding severity statistics.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            Dictionary of severity counts
        """
        try:
            response = self.guardduty_client.get_severity_statistics(DetectorId=detector_id)
            return {
                "low": response.get("lowSeverity", 0),
                "medium": response.get("mediumSeverity", 0),
                "high": response.get("highSeverity", 0),
                "critical": response.get("criticalSeverity", 0)
            }
            
        except ClientError as e:
            logger.error(f"Failed to get severity statistics: {e}")
            return {}
    
    def findings_by_severity(
        self,
        detector_id: str,
        severity: FindingSeverity
    ) -> List[Finding]:
        """
        Get findings filtered by severity.
        
        Args:
            detector_id: Detector ID
            severity: Severity level to filter
            
        Returns:
            List of Finding objects
        """
        finding_ids = self.list_findings(
            detector_id,
            finding_criteria={
                "severity": [{"eq": [severity.value]}]
            }
        )
        
        if not finding_ids:
            return []
        
        return self.get_findings(detector_id, finding_ids=finding_ids)
    
    def find_high_severity_findings(self, detector_id: str) -> List[Finding]:
        """
        Get all high and critical severity findings.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            List of high severity Finding objects
        """
        findings = []
        
        for severity in [FindingSeverity.HIGH, FindingSeverity.CRITICAL]:
            findings.extend(self.findings_by_severity(detector_id, severity))
        
        return findings
    
    # ==================== Archive Findings ====================
    
    def archive_finding(self, detector_id: str, finding_id: str) -> bool:
        """
        Archive a finding.
        
        Args:
            detector_id: Detector ID
            finding_id: Finding ID to archive
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.archive_findings(
                DetectorId=detector_id,
                findingIds=[finding_id]
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to archive finding: {e}")
            return False
    
    def unarchive_finding(self, detector_id: str, finding_id: str) -> bool:
        """
        Unarchive a finding.
        
        Args:
            detector_id: Detector ID
            finding_id: Finding ID to unarchive
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.unarchive_findings(
                DetectorId=detector_id,
                findingIds=[finding_id]
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to unarchive finding: {e}")
            return False
    
    def archive_findings_by_filter(
        self,
        detector_id: str,
        filter_criteria: Dict[str, Any]
    ) -> int:
        """
        Archive multiple findings matching filter criteria.
        
        Args:
            detector_id: Detector ID
            filter_criteria: Criteria to filter findings
            
        Returns:
            Number of findings archived
        """
        try:
            finding_ids = self.list_findings(
                detector_id,
                finding_criteria=filter_criteria
            )
            
            if not finding_ids:
                return 0
            
            # Archive in batches of 50 (GuardDuty limit)
            archived_count = 0
            for i in range(0, len(finding_ids), 50):
                batch = finding_ids[i:i + 50]
                self.guardduty_client.archive_findings(
                    DetectorId=detector_id,
                    findingIds=batch
                )
                archived_count += len(batch)
            
            return archived_count
            
        except ClientError as e:
            logger.error(f"Failed to archive findings: {e}")
            return 0
    
    # ==================== Filters ====================
    
    def create_filter(
        self,
        detector_id: str,
        name: str,
        action: FilterAction,
        finding_criteria: Dict[str, Any],
        description: str = "",
        tags: Optional[Dict[str, str]] = None
    ) -> Filter:
        """
        Create a finding filter.
        
        Args:
            detector_id: Detector ID
            name: Filter name
            action: Filter action (ARCHIVE or NOOP)
            finding_criteria: Criteria for matching findings
            description: Filter description
            tags: Tags to apply
            
        Returns:
            Filter object
        """
        try:
            kwargs = {
                "DetectorId": detector_id,
                "name": name,
                "action": action.value,
                "findingCriteria": finding_criteria
            }
            
            if description:
                kwargs["description"] = description
            if tags:
                kwargs["tags"] = tags
            
            response = self.guardduty_client.create_filter(**kwargs)
            
            return Filter(
                name=name,
                action=action,
                finding_criteria=finding_criteria,
                description=description,
                tags=tags or {}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create filter: {e}")
            raise
    
    def get_filter(self, detector_id: str, filter_name: str) -> Optional[Filter]:
        """
        Get filter details.
        
        Args:
            detector_id: Detector ID
            filter_name: Filter name
            
        Returns:
            Filter object or None
        """
        try:
            response = self.guardduty_client.get_filter(
                DetectorId=detector_id,
                filterName=filter_name
            )
            
            action_str = response.get("action", "NOOP")
            try:
                action = FilterAction[action_str.upper()]
            except KeyError:
                action = FilterAction.NOOP
            
            return Filter(
                name=response.get("name", filter_name),
                action=action,
                finding_criteria=response.get("findingCriteria", {}),
                description=response.get("description", ""),
                tags=response.get("tags", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get filter: {e}")
            return None
    
    def list_filters(self, detector_id: str) -> List[Dict[str, str]]:
        """
        List all filters for a detector.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            List of filter info
        """
        try:
            response = self.guardduty_client.list_filters(DetectorId=detector_id)
            return response.get("filterNames", [])
            
        except ClientError as e:
            logger.error(f"Failed to list filters: {e}")
            return []
    
    def update_filter(
        self,
        detector_id: str,
        filter_name: str,
        action: Optional[FilterAction] = None,
        finding_criteria: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None
    ) -> bool:
        """
        Update a filter.
        
        Args:
            detector_id: Detector ID
            filter_name: Filter name
            action: New action
            finding_criteria: New criteria
            description: New description
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"DetectorId": detector_id, "filterName": filter_name}
            
            if action:
                kwargs["action"] = action.value
            if finding_criteria:
                kwargs["findingCriteria"] = finding_criteria
            if description is not None:
                kwargs["description"] = description
            
            self.guardduty_client.update_filter(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update filter: {e}")
            return False
    
    def delete_filter(self, detector_id: str, filter_name: str) -> bool:
        """
        Delete a filter.
        
        Args:
            detector_id: Detector ID
            filter_name: Filter name
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.delete_filter(
                DetectorId=detector_id,
                filterName=filter_name
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete filter: {e}")
            return False
    
    def create_auto_archive_filter(
        self,
        detector_id: str,
        name: str,
        severity: FindingSeverity,
        description: str = ""
    ) -> Filter:
        """
        Create a filter that auto-archives findings of a specific severity.
        
        Args:
            detector_id: Detector ID
            name: Filter name
            severity: Severity to auto-archive
            description: Filter description
            
        Returns:
            Filter object
        """
        return self.create_filter(
            detector_id=detector_id,
            name=name,
            action=FilterAction.ARCHIVE,
            finding_criteria={"severity": [{"eq": [severity.value]}]},
            description=description
        )
    
    # ==================== Member Accounts ====================
    
    def create_member(
        self,
        detector_id: str,
        account_id: str,
        email: str
    ) -> MemberAccount:
        """
        Create a GuardDuty member account.
        
        Args:
            detector_id: Administrator detector ID
            account_id: Member account ID
            email: Member account email
            
        Returns:
            MemberAccount object
        """
        try:
            response = self.guardduty_client.create_members(
                DetectorId=detector_id,
                AccountDetails=[{
                    "AccountId": account_id,
                    "Email": email
                }]
            )
            
            return MemberAccount(
                account_id=account_id,
                email=email,
                detector_id=detector_id,
                relationship_status="Created"
            )
            
        except ClientError as e:
            logger.error(f"Failed to create member: {e}")
            raise
    
    def get_member(
        self,
        detector_id: str,
        account_id: str
    ) -> Optional[MemberAccount]:
        """
        Get member account details.
        
        Args:
            detector_id: Administrator detector ID
            account_id: Member account ID
            
        Returns:
            MemberAccount object or None
        """
        try:
            response = self.guardduty_client.get_members(
                DetectorId=detector_id,
                AccountIds=[account_id]
            )
            
            members = response.get("Members", [])
            if not members:
                return None
            
            member = members[0]
            return MemberAccount(
                account_id=member.get("AccountId", account_id),
                email=member.get("Email", ""),
                detector_id=detector_id,
                relationship_status=member.get("RelationshipStatus", ""),
                invited_at=self._parse_datetime(member.get("InvitedAt"))
            )
            
        except ClientError as e:
            logger.error(f"Failed to get member: {e}")
            return None
    
    def list_members(self, detector_id: str) -> List[MemberAccount]:
        """
        List all member accounts.
        
        Args:
            detector_id: Administrator detector ID
            
        Returns:
            List of MemberAccount objects
        """
        try:
            response = self.guardduty_client.list_members(DetectorId=detector_id)
            members = []
            
            for member in response.get("Members", []):
                members.append(MemberAccount(
                    account_id=member.get("AccountId", ""),
                    email=member.get("Email", ""),
                    detector_id=detector_id,
                    relationship_status=member.get("RelationshipStatus", ""),
                    invited_at=self._parse_datetime(member.get("InvitedAt"))
                ))
            
            return members
            
        except ClientError as e:
            logger.error(f"Failed to list members: {e}")
            return []
    
    def delete_member(self, detector_id: str, account_id: str) -> bool:
        """
        Delete a member account.
        
        Args:
            detector_id: Administrator detector ID
            account_id: Member account ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.delete_members(
                DetectorId=detector_id,
                AccountIds=[account_id]
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete member: {e}")
            return False
    
    def invite_member(self, detector_id: str, account_id: str) -> bool:
        """
        Invite a member account.
        
        Args:
            detector_id: Administrator detector ID
            account_id: Member account ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.invite_members(
                DetectorId=detector_id,
                AccountIds=[account_id]
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to invite member: {e}")
            return False
    
    def enable_member(self, detector_id: str, account_id: str) -> bool:
        """
        Enable GuardDuty in a member account.
        
        Args:
            detector_id: Administrator detector ID
            account_id: Member account ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.enable_gravity_for_member(
                AdminDetectorId=detector_id,
                MemberAccountId=account_id
            )
            return True
            
        except ClientError:
            # Alternative API
            try:
                self.guardduty_client.start_monitoring(
                    DetectorId=detector_id,
                    AccountIds=[account_id]
                )
                return True
            except ClientError as e:
                logger.error(f"Failed to enable member: {e}")
                return False
    
    # ==================== Admin Account ====================
    
    def enable_administrator(self, admin_account_id: str) -> AdminAccount:
        """
        Enable GuardDuty administrator account.
        
        Args:
            admin_account_id: Admin account ID
            
        Returns:
            AdminAccount object
        """
        try:
            self.guardduty_client.enable_organization_admin_account(
                AdminAccountId=admin_account_id
            )
            
            return AdminAccount(
                admin_account_id=admin_account_id,
                relationship_status="Enabled"
            )
            
        except ClientError as e:
            logger.error(f"Failed to enable administrator: {e}")
            raise
    
    def disable_administrator(self) -> bool:
        """
        Disable GuardDuty administrator account.
        
        Returns:
            True if successful
        """
        try:
            # Requires detector ID - would need to be passed in real implementation
            # This is a simplified version
            logger.warning("Disable administrator requires admin detector ID")
            return False
            
        except ClientError as e:
            logger.error(f"Failed to disable administrator: {e}")
            return False
    
    def get_administrator(self, detector_id: str) -> Optional[AdminAccount]:
        """
        Get GuardDuty administrator account.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            AdminAccount object or None
        """
        try:
            response = self.guardduty_client.get_administrator(
                DetectorId=detector_id
            )
            
            admin = response.get("Administrator", {})
            return AdminAccount(
                admin_account_id=admin.get("AccountId", ""),
                relationship_status=admin.get("RelationshipStatus", ""),
                invited_at=self._parse_datetime(admin.get("InvitedAt"))
            )
            
        except ClientError as e:
            logger.error(f"Failed to get administrator: {e}")
            return None
    
    def list_administrators(self, detector_id: str) -> List[AdminAccount]:
        """
        List all administrator accounts.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            List of AdminAccount objects
        """
        try:
            response = self.guardduty_client.list_administrators(
                DetectorId=detector_id
            )
            
            admins = []
            for admin in response.get("Administrators", []):
                admins.append(AdminAccount(
                    admin_account_id=admin.get("AccountId", ""),
                    relationship_status=admin.get("RelationshipStatus", ""),
                    invited_at=self._parse_datetime(admin.get("InvitedAt"))
                ))
            
            return admins
            
        except ClientError as e:
            logger.error(f"Failed to list administrators: {e}")
            return []
    
    # ==================== S3 Protection ====================
    
    def enable_s3_protection(self, detector_id: str) -> bool:
        """
        Enable S3 protection for GuardDuty.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.update_detector(
                DetectorId=detector_id,
                enable=True,
                findingPublishingFrequency="FIFTEEN_MINUTES"
            )
            
            # Also need to update datasources
            self.guardduty_client.update_datasources(
                DetectorId=detector_id,
                s3Logs={"enable": True}
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable S3 protection: {e}")
            return False
    
    def disable_s3_protection(self, detector_id: str) -> bool:
        """
        Disable S3 protection for GuardDuty.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.update_datasources(
                DetectorId=detector_id,
                s3Logs={"enable": False}
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable S3 protection: {e}")
            return False
    
    def get_s3_protection_status(self, detector_id: str) -> bool:
        """
        Get S3 protection status.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            True if S3 protection is enabled
        """
        try:
            response = self.guardduty_client.get_datasources(
                DetectorId=detector_id
            )
            
            s3_logs = response.get("s3Logs", {})
            return s3_logs.get("enable", False)
            
        except ClientError as e:
            logger.error(f"Failed to get S3 protection status: {e}")
            return False
    
    def update_datasources(
        self,
        detector_id: str,
        s3_logs: Optional[bool] = None,
        kubernetes: Optional[Dict[str, bool]] = None,
        malware_protection: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update GuardDuty data sources.
        
        Args:
            detector_id: Detector ID
            s3_logs: Enable/disable S3 logs
            kubernetes: Kubernetes audit logs config
            malware_protection: Malware protection config
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"DetectorId": detector_id}
            
            if s3_logs is not None:
                kwargs["s3Logs"] = {"enable": s3_logs}
            
            if kubernetes:
                kwargs["kubernetes"] = kubernetes
            
            if malware_protection:
                kwargs["malwareProtection"] = malware_protection
            
            self.guardduty_client.update_datasources(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update datasources: {e}")
            return False
    
    # ==================== CloudWatch Integration ====================
    
    def setup_cloudwatch_integration(
        self,
        detector_id: str,
        sns_topic_arn: Optional[str] = None,
        cloudwatch_event_rule_name: str = "guardduty-finding-events"
    ) -> bool:
        """
        Set up CloudWatch integration for GuardDuty findings.
        
        Args:
            detector_id: Detector ID
            sns_topic_arn: SNS topic ARN for notifications
            cloudwatch_event_rule_name: CloudWatch event rule name
            
        Returns:
            True if successful
        """
        try:
            # Create CloudWatch event rule
            self.events_client.put_rule(
                Name=cloudwatch_event_rule_name,
                EventPattern=json.dumps({
                    "source": ["aws.guardduty"],
                    "detail-type": ["GuardDuty Finding"]
                }),
                State="ENABLED",
                Description="GuardDuty findings CloudWatch event rule"
            )
            
            # Add SNS target if provided
            if sns_topic_arn:
                self.events_client.put_targets(
                    Rule=cloudwatch_event_rule_name,
                    Targets=[{
                        "Id": "GuardDutyTarget",
                        "Arn": sns_topic_arn
                    }]
                )
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to set up CloudWatch integration: {e}")
            return False
    
    def put_guardduty_metric_alarm(
        self,
        alarm_name: str,
        detector_id: str,
        metric_name: str = "GuardDutyFindingCount",
        threshold: int = 1,
        period: int = 3600,
        evaluation_periods: int = 1
    ) -> bool:
        """
        Create a CloudWatch alarm for GuardDuty metrics.
        
        Args:
            alarm_name: CloudWatch alarm name
            detector_id: Detector ID for dimensions
            metric_name: Metric name
            threshold: Alarm threshold
            period: Evaluation period in seconds
            evaluation_periods: Number of evaluation periods
            
        Returns:
            True if successful
        """
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace="AWS/GuardDuty",
                Statistic="Sum",
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator="GreaterThanThreshold",
                Dimensions=[{
                    "Name": "DetectorId",
                    "Value": detector_id
                }]
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to put metric alarm: {e}")
            return False
    
    def get_guardduty_metrics(self, period: int = 3600) -> List[Dict[str, Any]]:
        """
        Get GuardDuty CloudWatch metrics.
        
        Args:
            period: Metric period in seconds
            
        Returns:
            List of metric data
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/GuardDuty",
                MetricName="FindingCount",
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Sum"]
            )
            
            return response.get("Datapoints", [])
            
        except ClientError as e:
            logger.error(f"Failed to get GuardDuty metrics: {e}")
            return []
    
    def enable_finding_publishing(
        self,
        detector_id: str,
        destination_type: str = "S3",
        destination_properties: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Enable finding publishing to S3 or CloudWatch.
        
        Args:
            detector_id: Detector ID
            destination_type: Destination type (S3 or CloudWatch)
            destination_properties: Destination-specific properties
            
        Returns:
            True if successful
        """
        try:
            kwargs = {
                "DetectorId": detector_id,
                "findingPublishingFrequency": "FIFTEEN_MINUTES"
            }
            
            if destination_type == "S3" and destination_properties:
                kwargs["s3DestinationProperties"] = destination_properties
            
            self.guardduty_client.update_detector(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to enable finding publishing: {e}")
            return False
    
    # ==================== Utility Methods ====================
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from AWS response."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
    
    def _generate_finding_id(self) -> str:
        """Generate a unique finding ID."""
        return f"finding-{uuid.uuid4().hex[:16]}"
    
    def tag_resource(
        self,
        resource_arn: str,
        tags: Dict[str, str]
    ) -> bool:
        """
        Tag a GuardDuty resource.
        
        Args:
            resource_arn: Resource ARN
            tags: Tags to apply
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.tag_resource(
                resourceArn=resource_arn,
                tags=tags
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to tag resource: {e}")
            return False
    
    def untag_resource(
        self,
        resource_arn: str,
        tag_keys: List[str]
    ) -> bool:
        """
        Remove tags from a GuardDuty resource.
        
        Args:
            resource_arn: Resource ARN
            tag_keys: Tag keys to remove
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.untag_resource(
                resourceArn=resource_arn,
                tagKeys=tag_keys
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to untag resource: {e}")
            return False
    
    def get_publishing_destination(
        self,
        detector_id: str,
        destination_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get publishing destination details.
        
        Args:
            detector_id: Detector ID
            destination_id: Destination ID
            
        Returns:
            Destination details or None
        """
        try:
            response = self.guardduty_client.get_publishing_destination(
                DetectorId=detector_id,
                DestinationId=destination_id
            )
            
            return {
                "destination_id": destination_id,
                "destination_type": response.get("destinationType"),
                "properties": response.get("properties", {}),
                "status": response.get("status")
            }
            
        except ClientError as e:
            logger.error(f"Failed to get publishing destination: {e}")
            return None
    
    def list_publishing_destinations(self, detector_id: str) -> List[Dict[str, Any]]:
        """
        List publishing destinations.
        
        Args:
            detector_id: Detector ID
            
        Returns:
            List of destinations
        """
        try:
            response = self.guardduty_client.list_publishing_destinations(
                DetectorId=detector_id
            )
            
            destinations = []
            for dest in response.get("Destinations", []):
                destinations.append({
                    "destination_id": dest.get("destinationId"),
                    "destination_type": dest.get("destinationType"),
                    "status": dest.get("status")
                })
            
            return destinations
            
        except ClientError as e:
            logger.error(f"Failed to list publishing destinations: {e}")
            return []
    
    def create_publishing_destination(
        self,
        detector_id: str,
        destination_type: str,
        s3_destination: Optional[Dict[str, Any]] = None,
        firehose_destination: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Create a publishing destination.
        
        Args:
            detector_id: Detector ID
            destination_type: Destination type (S3 or FIREHOSE)
            s3_destination: S3 destination properties
            firehose_destination: Firehose destination properties
            
        Returns:
            Destination ID or None
        """
        try:
            kwargs = {
                "DetectorId": detector_id,
                "destinationType": destination_type
            }
            
            if s3_destination:
                kwargs["s3Destination"] = s3_destination
            elif firehose_destination:
                kwargs["firehoseDestination"] = firehose_destination
            
            response = self.guardduty_client.create_publishing_destination(**kwargs)
            return response.get("destinationId")
            
        except ClientError as e:
            logger.error(f"Failed to create publishing destination: {e}")
            return None
    
    def delete_publishing_destination(
        self,
        detector_id: str,
        destination_id: str
    ) -> bool:
        """
        Delete a publishing destination.
        
        Args:
            detector_id: Detector ID
            destination_id: Destination ID
            
        Returns:
            True if successful
        """
        try:
            self.guardduty_client.delete_publishing_destination(
                DetectorId=detector_id,
                DestinationId=destination_id
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete publishing destination: {e}")
            return False
    
    def close(self):
        """Close all client connections."""
        with self._lock:
            self._clients.clear()
            self._resources.clear()
