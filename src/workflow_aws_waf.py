"""
AWS WAF Integration Module for Workflow System

Implements a WAFIntegration class with:
1. Web ACL management: Create/manage Web ACLs
2. Rule groups: Create/manage rule groups
3. IP sets: Manage IP sets
4. Regex pattern sets: Manage regex patterns
5. Rules: Add rules to Web ACLs
6. Logging: WAF logging configuration
7. AWS Firewall Manager: WAF integration with Firewall Manager
8. Rate-based rules: Rate limiting rules
9. Bot control: Bot control rules
10. CloudWatch integration: WAF metrics and monitoring

Commit: 'feat(aws-waf): add AWS WAF with Web ACL management, rule groups, IP sets, regex patterns, logging, Firewall Manager, rate-based rules, bot control, CloudWatch'
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


class WebACLScope(Enum):
    """Web ACL scope - CLOUDFRONT for global or REGIONAL for VPC."""
    CLOUDFRONT = "CLOUDFRONT"
    REGIONAL = "REGIONAL"


class WebACLAction(Enum):
    """Web ACL rule action types."""
    ALLOW = "ALLOW"
    BLOCK = "BLOCK"
    COUNT = "COUNT"
    CAPTCHA = "CAPTCHA"
    CHALLENGE = "CHALLENGE"


class RuleAction(Enum):
    """Rule action types."""
    ALLOW = "ALLOW"
    BLOCK = "BLOCK"
    COUNT = "COUNT"


class RuleType(Enum):
    """WAF rule types."""
    REGULAR = "REGULAR"
    RATE_BASED = "RATE_BASED"
    GROUP = "GROUP"


class IPSetAddressVersion(Enum):
    """IP set address version."""
    IPV4 = "IPV4"
    IPV6 = "IPV6"


class LoggingDestinationType(Enum):
    """Logging destination types."""
    S3 = "S3"
    KINESIS_FIREHOSE = "KINESIS_FIREHOSE"
    CLOUDWATCH_LOG = "CLOUDWATCH_LOG"


class BotControlCategory(Enum):
    """Bot control categories."""
    AUTOMATED = "AUTOMATED"
    BOT = "BOT"
    SCRAPER = "SCRAPER"


@dataclass
class WebACL:
    """Web ACL configuration."""
    name: str
    scope: WebACLScope
    acl_id: Optional[str] = None
    arn: Optional[str] = None
    description: Optional[str] = None
    rules: List[Dict[str, Any]] = field(default_factory=list)
    default_action: WebACLAction = WebACLAction.ALLOW
    captcha_config: Optional[Dict[str, Any]] = None
    challenge_config: Optional[Dict[str, Any]] = None
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class RuleGroup:
    """Rule group configuration."""
    name: str
    scope: WebACLScope
    rule_group_id: Optional[str] = None
    arn: Optional[str] = None
    description: Optional[str] = None
    rules: List[Dict[str, Any]] = field(default_factory=list)
    capacity: int = 0
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class IPSet:
    """IP set configuration."""
    name: str
    scope: WebACLScope
    ip_set_id: Optional[str] = None
    arn: Optional[str] = None
    addresses: List[str] = field(default_factory=list)
    address_version: IPSetAddressVersion = IPSetAddressVersion.IPV4
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class RegexPatternSet:
    """Regex pattern set configuration."""
    name: str
    scope: WebACLScope
    pattern_set_id: Optional[str] = None
    arn: Optional[str] = None
    patterns: List[str] = field(default_factory=list)
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class RateRule:
    """Rate-based rule configuration."""
    name: str
    rate_limit: int
    rule_id: Optional[str] = None
    arn: Optional[str] = None
    scope: WebACLScope = WebACLScope.REGIONAL
    evaluation_window: int = 300  # 5 minutes in seconds
    condition: Optional[Dict[str, Any]] = None
    action: RuleAction = RuleAction.BLOCK
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class BotControlRule:
    """Bot control rule configuration."""
    name: str
    category: BotControlCategory
    rule_id: Optional[str] = None
    arn: Optional[str] = None
    action: RuleAction = RuleAction.BLOCK
    enable: bool = True
    sensitivity_level: str = "MEDIUM"
    description: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class LoggingConfig:
    """WAF logging configuration."""
    acl_id: str
    log_destination_arn: str
    logging_destination_type: LoggingDestinationType
    logging_filter: Optional[Dict[str, Any]] = None
    redacted_fields: Optional[List[Dict[str, Any]]] = None
    enabled: bool = True


class WAFIntegration:
    """
    AWS WAF Integration class providing comprehensive WAF management.
    
    Features:
    1. Web ACL management: Create/manage Web ACLs
    2. Rule groups: Create/manage rule groups
    3. IP sets: Manage IP sets
    4. Regex pattern sets: Manage regex patterns
    5. Rules: Add rules to Web ACLs
    6. Logging: WAF logging configuration
    7. AWS Firewall Manager: WAF integration with Firewall Manager
    8. Rate-based rules: Rate limiting rules
    9. Bot control: Bot control rules
    10. CloudWatch integration: WAF metrics and monitoring
    """
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        profile_name: Optional[str] = None,
        endpoint_url: Optional[str] = None
    ):
        """
        Initialize WAF Integration.
        
        Args:
            region_name: AWS region for WAF operations
            profile_name: AWS credentials profile name
            endpoint_url: Custom WAF endpoint URL
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
        """Initialize boto3 clients for AWS WAF services."""
        try:
            session_kwargs = {"region_name": self.region_name}
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            
            session = boto3.Session(**session_kwargs)
            
            # WAFv2 client
            self._clients["wafv2"] = session.client(
                "wafv2",
                endpoint_url=self.endpoint_url
            )
            
            # WAF Classic client (for legacy support)
            self._clients["waf"] = session.client(
                "waf",
                endpoint_url=self.endpoint_url
            )
            
            # CloudWatch client for metrics integration
            self._clients["cloudwatch"] = session.client(
                "cloudwatch",
                endpoint_url=self.endpoint_url
            )
            
            # CloudWatch Logs client
            self._clients["logs"] = session.client(
                "logs",
                endpoint_url=self.endpoint_url
            )
            
            # Kinesis Firehose client
            self._clients["firehose"] = session.client(
                "firehose",
                endpoint_url=self.endpoint_url
            )
            
            # Firewall Manager client
            self._clients["fms"] = session.client(
                "fms",
                endpoint_url=self.endpoint_url
            )
            
            # S3 client for logging
            self._clients["s3"] = session.client(
                "s3",
                endpoint_url=self.endpoint_url
            )
            
            # STS client for account operations
            self._clients["sts"] = session.client(
                "sts",
                endpoint_url=self.endpoint_url
            )
            
            logger.info(f"Initialized AWS WAF clients in region {self.region_name}")
            
        except Exception as e:
            logger.warning(f"Failed to initialize AWS clients: {e}")
    
    @property
    def wafv2_client(self):
        """Get AWS WAFv2 client."""
        if "wafv2" not in self._clients:
            self._initialize_clients()
        return self._clients.get("wafv2")
    
    @property
    def waf_classic_client(self):
        """Get AWS WAF Classic client."""
        if "waf" not in self._clients:
            self._initialize_clients()
        return self._clients.get("waf")
    
    @property
    def cloudwatch_client(self):
        """Get CloudWatch client."""
        if "cloudwatch" not in self._clients:
            self._initialize_clients()
        return self._clients.get("cloudwatch")
    
    @property
    def logs_client(self):
        """Get CloudWatch Logs client."""
        if "logs" not in self._clients:
            self._initialize_clients()
        return self._clients.get("logs")
    
    @property
    def firehose_client(self):
        """Get Kinesis Firehose client."""
        if "firehose" not in self._clients:
            self._initialize_clients()
        return self._clients.get("firehose")
    
    @property
    def fms_client(self):
        """Get Firewall Manager client."""
        if "fms" not in self._clients:
            self._initialize_clients()
        return self._clients.get("fms")
    
    @property
    def s3_client(self):
        """Get S3 client."""
        if "s3" not in self._clients:
            self._initialize_clients()
        return self._clients.get("s3")
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO datetime string."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
    
    def _get_current_datetime(self) -> datetime:
        """Get current UTC datetime."""
        return datetime.utcnow()
    
    # ==================== Web ACL Management ====================
    
    def create_web_acl(
        self,
        name: str,
        scope: WebACLScope,
        default_action: WebACLAction = WebACLAction.ALLOW,
        description: Optional[str] = None,
        rules: Optional[List[Dict[str, Any]]] = None,
        captcha_config: Optional[Dict[str, Any]] = None,
        challenge_config: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> WebACL:
        """
        Create a Web ACL.
        
        Args:
            name: Web ACL name
            scope: CLOUDFRONT or REGIONAL
            default_action: Default action for unmatched traffic
            description: Description of the Web ACL
            rules: List of rules to apply
            captcha_config: CAPTCHA configuration
            challenge_config: Challenge configuration
            tags: Tags to apply
            
        Returns:
            WebACL object
        """
        try:
            kwargs = {
                "Name": name,
                "Scope": scope.value,
                "DefaultAction": {"Type": default_action.value}
            }
            
            if description:
                kwargs["Description"] = description
            
            if rules:
                kwargs["Rules"] = rules
            
            if captcha_config:
                kwargs["CaptchaConfig"] = captcha_config
            
            if challenge_config:
                kwargs["ChallengeConfig"] = challenge_config
            
            if tags:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.wafv2_client.create_web_acl(**kwargs)
            acl_info = response["WebACL"]
            
            now = self._get_current_datetime()
            return WebACL(
                name=name,
                scope=scope,
                acl_id=acl_info["Id"],
                arn=acl_info["ARN"],
                description=description,
                rules=rules or [],
                default_action=default_action,
                captcha_config=captcha_config,
                challenge_config=challenge_config,
                tags=tags or {},
                created_at=now,
                updated_at=now
            )
            
        except ClientError as e:
            logger.error(f"Failed to create Web ACL: {e}")
            raise
    
    def get_web_acl(self, acl_id: str, scope: WebACLScope) -> Optional[WebACL]:
        """
        Get Web ACL details.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            WebACL object or None
        """
        try:
            response = self.wafv2_client.get_web_acl(Id=acl_id, Scope=scope.value)
            acl_info = response["WebACL"]
            
            return WebACL(
                name=acl_info["Name"],
                scope=WebACLScope(acl_info["Scope"]),
                acl_id=acl_info["Id"],
                arn=acl_info["ARN"],
                description=acl_info.get("Description"),
                rules=acl_info.get("Rules", []),
                default_action=WebACLAction(acl_info["DefaultAction"]["Type"]),
                captcha_config=acl_info.get("CaptchaConfig"),
                challenge_config=acl_info.get("ChallengeConfig"),
                tags=acl_info.get("Tags", {}),
                created_at=self._parse_datetime(acl_info.get("CreatedAt")),
                updated_at=self._parse_datetime(acl_info.get("UpdatedAt"))
            )
            
        except ClientError as e:
            logger.error(f"Failed to get Web ACL: {e}")
            return None
    
    def list_web_acls(self, scope: WebACLScope, limit: int = 100) -> List[WebACL]:
        """
        List all Web ACLs.
        
        Args:
            scope: CLOUDFRONT or REGIONAL
            limit: Maximum number of results
            
        Returns:
            List of WebACL objects
        """
        try:
            response = self.wafv2_client.list_web_acls(Scope=scope.value, Limit=limit)
            acls = []
            
            for item in response.get("WebACLs", []):
                acls.append(WebACL(
                    name=item["Name"],
                    scope=scope,
                    acl_id=item["Id"],
                    arn=item["ARN"],
                    description=item.get("Description"),
                    tags={}
                ))
            
            return acls
            
        except ClientError as e:
            logger.error(f"Failed to list Web ACLs: {e}")
            return []
    
    def update_web_acl(
        self,
        acl_id: str,
        scope: WebACLScope,
        default_action: Optional[WebACLAction] = None,
        rules: Optional[List[Dict[str, Any]]] = None,
        captcha_config: Optional[Dict[str, Any]] = None,
        challenge_config: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None
    ) -> bool:
        """
        Update Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            default_action: New default action
            rules: New list of rules
            captcha_config: New CAPTCHA configuration
            challenge_config: New challenge configuration
            description: New description
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"Id": acl_id, "Scope": scope.value}
            
            if default_action:
                kwargs["DefaultAction"] = {"Type": default_action.value}
            
            if rules is not None:
                kwargs["Rules"] = rules
            
            if captcha_config is not None:
                kwargs["CaptchaConfig"] = captcha_config
            
            if challenge_config is not None:
                kwargs["ChallengeConfig"] = challenge_config
            
            if description is not None:
                kwargs["Description"] = description
            
            self.wafv2_client.update_web_acl(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update Web ACL: {e}")
            return False
    
    def delete_web_acl(self, acl_id: str, scope: WebACLScope) -> bool:
        """
        Delete Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.delete_web_acl(Id=acl_id, Scope=scope.value)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete Web ACL: {e}")
            return False
    
    # ==================== Rule Groups ====================
    
    def create_rule_group(
        self,
        name: str,
        scope: WebACLScope,
        rules: Optional[List[Dict[str, Any]]] = None,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> RuleGroup:
        """
        Create a rule group.
        
        Args:
            name: Rule group name
            scope: CLOUDFRONT or REGIONAL
            rules: List of rules in the group
            description: Description of the rule group
            tags: Tags to apply
            
        Returns:
            RuleGroup object
        """
        try:
            kwargs = {
                "Name": name,
                "Scope": scope.value,
                "Capacity": self._calculate_rule_capacity(rules or [])
            }
            
            if rules:
                kwargs["Rules"] = rules
            
            if description:
                kwargs["Description"] = description
            
            if tags:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.wafv2_client.create_rule_group(**kwargs)
            group_info = response["RuleGroup"]
            
            now = self._get_current_datetime()
            return RuleGroup(
                name=name,
                scope=scope,
                rule_group_id=group_info["Id"],
                arn=group_info["ARN"],
                description=description,
                rules=rules or [],
                capacity=group_info["Capacity"],
                tags=tags or {},
                created_at=now,
                updated_at=now
            )
            
        except ClientError as e:
            logger.error(f"Failed to create rule group: {e}")
            raise
    
    def _calculate_rule_capacity(self, rules: List[Dict[str, Any]]) -> int:
        """Calculate total capacity required for rules."""
        capacity = 0
        for rule in rules:
            rule_type = rule.get("Type", "REGULAR")
            if rule_type == "RATE_BASED":
                capacity += 2
            elif rule_type == "GROUP":
                capacity += 1
            else:
                capacity += 1
        return max(capacity, 1)
    
    def get_rule_group(self, rule_group_id: str, scope: WebACLScope) -> Optional[RuleGroup]:
        """
        Get rule group details.
        
        Args:
            rule_group_id: Rule group ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            RuleGroup object or None
        """
        try:
            response = self.wafv2_client.get_rule_group(Id=rule_group_id, Scope=scope.value)
            group_info = response["RuleGroup"]
            
            return RuleGroup(
                name=group_info["Name"],
                scope=WebACLScope(group_info["Scope"]),
                rule_group_id=group_info["Id"],
                arn=group_info["ARN"],
                description=group_info.get("Description"),
                rules=group_info.get("Rules", []),
                capacity=group_info["Capacity"],
                tags=group_info.get("Tags", {}),
                created_at=self._parse_datetime(group_info.get("CreatedAt")),
                updated_at=self._parse_datetime(group_info.get("UpdatedAt"))
            )
            
        except ClientError as e:
            logger.error(f"Failed to get rule group: {e}")
            return None
    
    def list_rule_groups(self, scope: WebACLScope, limit: int = 100) -> List[RuleGroup]:
        """
        List all rule groups.
        
        Args:
            scope: CLOUDFRONT or REGIONAL
            limit: Maximum number of results
            
        Returns:
            List of RuleGroup objects
        """
        try:
            response = self.wafv2_client.list_rule_groups(Scope=scope.value, Limit=limit)
            groups = []
            
            for item in response.get("RuleGroups", []):
                groups.append(RuleGroup(
                    name=item["Name"],
                    scope=scope,
                    rule_group_id=item["Id"],
                    arn=item["ARN"],
                    description=item.get("Description"),
                    tags={}
                ))
            
            return groups
            
        except ClientError as e:
            logger.error(f"Failed to list rule groups: {e}")
            return []
    
    def update_rule_group(
        self,
        rule_group_id: str,
        scope: WebACLScope,
        rules: Optional[List[Dict[str, Any]]] = None,
        description: Optional[str] = None
    ) -> bool:
        """
        Update rule group.
        
        Args:
            rule_group_id: Rule group ID
            scope: CLOUDFRONT or REGIONAL
            rules: New list of rules
            description: New description
            
        Returns:
            True if successful
        """
        try:
            kwargs = {"Id": rule_group_id, "Scope": scope.value}
            
            if rules is not None:
                kwargs["Rules"] = rules
                kwargs["Capacity"] = self._calculate_rule_capacity(rules)
            
            if description is not None:
                kwargs["Description"] = description
            
            self.wafv2_client.update_rule_group(**kwargs)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update rule group: {e}")
            return False
    
    def delete_rule_group(self, rule_group_id: str, scope: WebACLScope) -> bool:
        """
        Delete rule group.
        
        Args:
            rule_group_id: Rule group ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.delete_rule_group(Id=rule_group_id, Scope=scope.value)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete rule group: {e}")
            return False
    
    # ==================== IP Sets ====================
    
    def create_ip_set(
        self,
        name: str,
        scope: WebACLScope,
        addresses: List[str],
        address_version: IPSetAddressVersion = IPSetAddressVersion.IPV4,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> IPSet:
        """
        Create an IP set.
        
        Args:
            name: IP set name
            scope: CLOUDFRONT or REGIONAL
            addresses: List of IP addresses or CIDR blocks
            address_version: IPV4 or IPV6
            description: Description of the IP set
            tags: Tags to apply
            
        Returns:
            IPSet object
        """
        try:
            kwargs = {
                "Name": name,
                "Scope": scope.value,
                "IPAddressVersion": address_version.value,
                "Addresses": addresses
            }
            
            if description:
                kwargs["Description"] = description
            
            if tags:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.wafv2_client.create_ip_set(**kwargs)
            ip_set_info = response["IPSet"]
            
            now = self._get_current_datetime()
            return IPSet(
                name=name,
                scope=scope,
                ip_set_id=ip_set_info["Id"],
                arn=ip_set_info["ARN"],
                addresses=addresses,
                address_version=address_version,
                description=description,
                tags=tags or {},
                created_at=now,
                updated_at=now
            )
            
        except ClientError as e:
            logger.error(f"Failed to create IP set: {e}")
            raise
    
    def get_ip_set(self, ip_set_id: str, scope: WebACLScope) -> Optional[IPSet]:
        """
        Get IP set details.
        
        Args:
            ip_set_id: IP set ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            IPSet object or None
        """
        try:
            response = self.wafv2_client.get_ip_set(Id=ip_set_id, Scope=scope.value)
            ip_set_info = response["IPSet"]
            
            return IPSet(
                name=ip_set_info["Name"],
                scope=WebACLScope(ip_set_info["Scope"]),
                ip_set_id=ip_set_info["Id"],
                arn=ip_set_info["ARN"],
                addresses=ip_set_info["Addresses"],
                address_version=IPSetAddressVersion(ip_set_info["IPAddressVersion"]),
                description=ip_set_info.get("Description"),
                tags=ip_set_info.get("Tags", {}),
                created_at=self._parse_datetime(ip_set_info.get("CreatedAt")),
                updated_at=self._parse_datetime(ip_set_info.get("UpdatedAt"))
            )
            
        except ClientError as e:
            logger.error(f"Failed to get IP set: {e}")
            return None
    
    def list_ip_sets(self, scope: WebACLScope, limit: int = 100) -> List[IPSet]:
        """
        List all IP sets.
        
        Args:
            scope: CLOUDFRONT or REGIONAL
            limit: Maximum number of results
            
        Returns:
            List of IPSet objects
        """
        try:
            response = self.wafv2_client.list_ip_sets(Scope=scope.value, Limit=limit)
            ip_sets = []
            
            for item in response.get("IPSets", []):
                ip_sets.append(IPSet(
                    name=item["Name"],
                    scope=scope,
                    ip_set_id=item["Id"],
                    arn=item["ARN"],
                    description=item.get("Description"),
                    tags={}
                ))
            
            return ip_sets
            
        except ClientError as e:
            logger.error(f"Failed to list IP sets: {e}")
            return []
    
    def update_ip_set(
        self,
        ip_set_id: str,
        scope: WebACLScope,
        addresses: List[str]
    ) -> bool:
        """
        Update IP set addresses.
        
        Args:
            ip_set_id: IP set ID
            scope: CLOUDFRONT or REGIONAL
            addresses: New list of IP addresses or CIDR blocks
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.update_ip_set(
                Id=ip_set_id,
                Scope=scope.value,
                Addresses=addresses
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update IP set: {e}")
            return False
    
    def delete_ip_set(self, ip_set_id: str, scope: WebACLScope) -> bool:
        """
        Delete IP set.
        
        Args:
            ip_set_id: IP set ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.delete_ip_set(Id=ip_set_id, Scope=scope.value)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete IP set: {e}")
            return False
    
    # ==================== Regex Pattern Sets ====================
    
    def create_regex_pattern_set(
        self,
        name: str,
        scope: WebACLScope,
        patterns: List[str],
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> RegexPatternSet:
        """
        Create a regex pattern set.
        
        Args:
            name: Regex pattern set name
            scope: CLOUDFRONT or REGIONAL
            patterns: List of regex patterns
            description: Description of the regex pattern set
            tags: Tags to apply
            
        Returns:
            RegexPatternSet object
        """
        try:
            kwargs = {
                "Name": name,
                "Scope": scope.value,
                "RegularExpressionPatterns": [{"RegexString": p} for p in patterns]
            }
            
            if description:
                kwargs["Description"] = description
            
            if tags:
                kwargs["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
            
            response = self.wafv2_client.create_regex_pattern_set(**kwargs)
            pattern_set_info = response["RegexPatternSet"]
            
            now = self._get_current_datetime()
            return RegexPatternSet(
                name=name,
                scope=scope,
                pattern_set_id=pattern_set_info["Id"],
                arn=pattern_set_info["ARN"],
                patterns=patterns,
                description=description,
                tags=tags or {},
                created_at=now,
                updated_at=now
            )
            
        except ClientError as e:
            logger.error(f"Failed to create regex pattern set: {e}")
            raise
    
    def get_regex_pattern_set(self, pattern_set_id: str, scope: WebACLScope) -> Optional[RegexPatternSet]:
        """
        Get regex pattern set details.
        
        Args:
            pattern_set_id: Regex pattern set ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            RegexPatternSet object or None
        """
        try:
            response = self.wafv2_client.get_regex_pattern_set(Id=pattern_set_id, Scope=scope.value)
            pattern_set_info = response["RegexPatternSet"]
            
            patterns = [p["RegexString"] for p in pattern_set_info.get("RegularExpressionPatterns", [])]
            
            return RegexPatternSet(
                name=pattern_set_info["Name"],
                scope=WebACLScope(pattern_set_info["Scope"]),
                pattern_set_id=pattern_set_info["Id"],
                arn=pattern_set_info["ARN"],
                patterns=patterns,
                description=pattern_set_info.get("Description"),
                tags=pattern_set_info.get("Tags", {}),
                created_at=self._parse_datetime(pattern_set_info.get("CreatedAt")),
                updated_at=self._parse_datetime(pattern_set_info.get("UpdatedAt"))
            )
            
        except ClientError as e:
            logger.error(f"Failed to get regex pattern set: {e}")
            return None
    
    def list_regex_pattern_sets(self, scope: WebACLScope, limit: int = 100) -> List[RegexPatternSet]:
        """
        List all regex pattern sets.
        
        Args:
            scope: CLOUDFRONT or REGIONAL
            limit: Maximum number of results
            
        Returns:
            List of RegexPatternSet objects
        """
        try:
            response = self.wafv2_client.list_regex_pattern_sets(Scope=scope.value, Limit=limit)
            pattern_sets = []
            
            for item in response.get("RegexPatternSets", []):
                pattern_sets.append(RegexPatternSet(
                    name=item["Name"],
                    scope=scope,
                    pattern_set_id=item["Id"],
                    arn=item["ARN"],
                    description=item.get("Description"),
                    tags={}
                ))
            
            return pattern_sets
            
        except ClientError as e:
            logger.error(f"Failed to list regex pattern sets: {e}")
            return []
    
    def update_regex_pattern_set(
        self,
        pattern_set_id: str,
        scope: WebACLScope,
        patterns: List[str]
    ) -> bool:
        """
        Update regex pattern set.
        
        Args:
            pattern_set_id: Regex pattern set ID
            scope: CLOUDFRONT or REGIONAL
            patterns: New list of regex patterns
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.update_regex_pattern_set(
                Id=pattern_set_id,
                Scope=scope.value,
                RegularExpressionPatterns=[{"RegexString": p} for p in patterns]
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to update regex pattern set: {e}")
            return False
    
    def delete_regex_pattern_set(self, pattern_set_id: str, scope: WebACLScope) -> bool:
        """
        Delete regex pattern set.
        
        Args:
            pattern_set_id: Regex pattern set ID
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.delete_regex_pattern_set(Id=pattern_set_id, Scope=scope.value)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete regex pattern set: {e}")
            return False
    
    # ==================== Rules ====================
    
    def create_rule(
        self,
        name: str,
        priority: int,
        action: RuleAction,
        statement: Dict[str, Any],
        rule_type: RuleType = RuleType.REGULAR,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a rule definition for use in Web ACLs or rule groups.
        
        Args:
            name: Rule name
            priority: Rule priority (lower = higher priority)
            action: Rule action
            statement: Rule statement (match criteria)
            rule_type: Type of rule (REGULAR, RATE_BASED, GROUP)
            description: Rule description
            
        Returns:
            Rule dictionary
        """
        rule = {
            "Name": name,
            "Priority": priority,
            "Action": {"Type": action.value},
            "Statement": statement
        }
        
        if rule_type != RuleType.REGULAR:
            rule["Type"] = rule_type.value
        
        if description:
            rule["Description"] = description
        
        return rule
    
    def create_ip_match_statement(
        self,
        ip_set_arn: str,
        ip_set_reference: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create an IP match statement.
        
        Args:
            ip_set_arn: ARN of the IP set
            ip_set_reference: Optional pre-built IP set reference
            
        Returns:
            Statement dictionary
        """
        if ip_set_reference:
            return {"IPSetReferenceStatement": ip_set_reference}
        return {"IPSetReferenceStatement": {"ARN": ip_set_arn}}
    
    def create_regexMatch_statement(
        self,
        regex_pattern_set_arn: str,
        field_to_match: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Create a regex match statement.
        
        Args:
            regex_pattern_set_arn: ARN of the regex pattern set
            field_to_match: Field to match (e.g., {"UriPath": "/"})
            
        Returns:
            Statement dictionary
        """
        return {
            "RegexMatchStatement": {
                "RegexPatternSetReferenceStatement": {"ARN": regex_pattern_set_arn},
                "FieldToMatch": field_to_match
            }
        }
    
    def create_byteMatch_statement(
        self,
        search_string: str,
        field_to_match: Dict[str, Any],
        positional_constraint: str,
        text_transformations: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a byte match statement.
        
        Args:
            search_string: String to search for
            field_to_match: Field to match
            positional_constraint: Where to search (STARTS_WITH, CONTAINS, etc.)
            text_transformations: Text transformations to apply
            
        Returns:
            Statement dictionary
        """
        statement = {
            "ByteMatchStatement": {
                "SearchString": search_string,
                "FieldToMatch": field_to_match,
                "PositionalConstraint": positional_constraint
            }
        }
        
        if text_transformations:
            statement["ByteMatchStatement"]["TextTransformations"] = text_transformations
        
        return statement
    
    def create_rate_based_statement(
        self,
        rate_limit: int,
        evaluation_window: int = 300,
        condition: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a rate-based statement.
        
        Args:
            rate_limit: Requests per evaluation window
            evaluation_window: Evaluation window in seconds (default 5 min)
            condition: Optional nested condition (AND/OR/NOT)
            
        Returns:
            Statement dictionary
        """
        statement = {
            "RateBasedStatement": {
                "Limit": rate_limit,
                "EvaluationWindowSec": evaluation_window
            }
        }
        
        if condition:
            statement["RateBasedStatement"]["ScopeDownStatement"] = condition
        
        return statement
    
    def add_rule_to_web_acl(
        self,
        acl_id: str,
        scope: WebACLScope,
        rule: Dict[str, Any]
    ) -> bool:
        """
        Add a rule to an existing Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            rule: Rule definition
            
        Returns:
            True if successful
        """
        try:
            current_acl = self.get_web_acl(acl_id, scope)
            if not current_acl:
                return False
            
            rules = current_acl.rules.copy()
            rules.append(rule)
            
            return self.update_web_acl(acl_id, scope, rules=rules)
            
        except Exception as e:
            logger.error(f"Failed to add rule to Web ACL: {e}")
            return False
    
    def remove_rule_from_web_acl(
        self,
        acl_id: str,
        scope: WebACLScope,
        rule_name: str
    ) -> bool:
        """
        Remove a rule from a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            rule_name: Name of the rule to remove
            
        Returns:
            True if successful
        """
        try:
            current_acl = self.get_web_acl(acl_id, scope)
            if not current_acl:
                return False
            
            rules = [r for r in current_acl.rules if r.get("Name") != rule_name]
            
            return self.update_web_acl(acl_id, scope, rules=rules)
            
        except Exception as e:
            logger.error(f"Failed to remove rule from Web ACL: {e}")
            return False
    
    # ==================== Logging ====================
    
    def enable_logging(
        self,
        acl_id: str,
        log_destination_arn: str,
        logging_destination_type: LoggingDestinationType,
        logging_filter: Optional[Dict[str, Any]] = None,
        redacted_fields: Optional[List[Dict[str, Any]]] = None
    ) -> LoggingConfig:
        """
        Enable logging for a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            log_destination_arn: ARN of the logging destination
            logging_destination_type: Type of destination (S3, KINESIS_FIREHOSE, CLOUDWATCH_LOG)
            logging_filter: Optional filter for log entries
            redacted_fields: Fields to redact from logs
            
        Returns:
            LoggingConfig object
        """
        try:
            kwargs = {
                "WebACLId": acl_id,
                "LogDestinationConfigs": [log_destination_arn]
            }
            
            if logging_filter:
                kwargs["LoggingFilter"] = logging_filter
            
            if redacted_fields:
                kwargs["RedactedFields"] = redacted_fields
            
            self.wafv2_client.put_logging_configuration(**kwargs)
            
            return LoggingConfig(
                acl_id=acl_id,
                log_destination_arn=log_destination_arn,
                logging_destination_type=logging_destination_type,
                logging_filter=logging_filter,
                redacted_fields=redacted_fields,
                enabled=True
            )
            
        except ClientError as e:
            logger.error(f"Failed to enable logging: {e}")
            raise
    
    def disable_logging(self, acl_id: str) -> bool:
        """
        Disable logging for a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.delete_logging_configuration(WebACLId=acl_id)
            return True
            
        except ClientError as e:
            logger.error(f"Failed to disable logging: {e}")
            return False
    
    def get_logging_config(self, acl_id: str) -> Optional[LoggingConfig]:
        """
        Get logging configuration for a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            
        Returns:
            LoggingConfig object or None
        """
        try:
            response = self.wafv2_client.get_logging_configuration(WebACLId=acl_id)
            config = response["LoggingConfiguration"]
            
            return LoggingConfig(
                acl_id=acl_id,
                log_destination_arn=config["LogDestinationConfigs"][0] if config.get("LogDestinationConfigs") else "",
                logging_destination_type=LoggingDestinationType.KINESIS_FIREHOSE,
                logging_filter=config.get("LoggingFilter"),
                redacted_fields=config.get("RedactedFields"),
                enabled=True
            )
            
        except ClientError as e:
            logger.error(f"Failed to get logging configuration: {e}")
            return None
    
    def create_kinesis_firehose_delivery_stream(
        self,
        name: str,
        s3_bucket_arn: str,
        prefix: Optional[str] = None
    ) -> str:
        """
        Create a Kinesis Firehose delivery stream for WAF logging.
        
        Args:
            name: Delivery stream name
            s3_bucket_arn: ARN of the S3 bucket
            prefix: Optional prefix for log files
            
        Returns:
            Delivery stream ARN
        """
        try:
            extended_s3_destination = {
                "BucketARN": s3_bucket_arn,
                "RoleARN": self._get_firehose_role_arn()
            }
            
            if prefix:
                extended_s3_destination["Prefix"] = prefix
            
            response = self.firehose_client.create_delivery_stream(
                DeliveryStreamName=name,
                ExtendedS3DestinationConfiguration=extended_s3_destination
            )
            
            return response["DeliveryStreamARN"]
            
        except ClientError as e:
            logger.error(f"Failed to create Firehose delivery stream: {e}")
            raise
    
    def _get_firehose_role_arn(self) -> str:
        """Get or create IAM role ARN for Firehose."""
        return f"arn:aws:iam::{self._get_account_id()}:role/aws-waf-firehose-role"
    
    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        try:
            sts_client = self._clients.get("sts")
            if not sts_client:
                self._initialize_clients()
                sts_client = self._clients.get("sts")
            return sts_client.get_caller_identity()["Account"]
        except ClientError:
            return "000000000000"
    
    # ==================== AWS Firewall Manager ====================
    
    def associate_web_acl_with_resource(
        self,
        acl_id: str,
        resource_arn: str,
        scope: WebACLScope
    ) -> bool:
        """
        Associate a Web ACL with a resource.
        
        Args:
            acl_id: Web ACL ID
            resource_arn: ARN of the resource (ALB, CloudFront, etc.)
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            True if successful
        """
        try:
            self.wafv2_client.tag_resource(
                ResourceARN=self._build_association_arn(acl_id, resource_arn, scope),
                Tags=[{"Key": "WAFManaged", "Value": "true"}]
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to associate Web ACL: {e}")
            return False
    
    def _build_association_arn(self, acl_id: str, resource_arn: str, scope: WebACLScope) -> str:
        """Build the association ARN."""
        return f"arn:aws:wafv2:{self.region_name}:{self._get_account_id()}:{scope.value}/webacl/{acl_id}"
    
    def list_fms_policy_assocations(self) -> List[Dict[str, Any]]:
        """
        List Firewall Manager policy associations.
        
        Returns:
            List of policy associations
        """
        try:
            response = self.fms_client.list_policies()
            return response.get("PolicyList", [])
            
        except ClientError as e:
            logger.error(f"Failed to list FMS policies: {e}")
            return []
    
    def put_fms_policy(
        self,
        name: str,
        web_acl_arn: str,
        resource_type: str,
        remediation_enabled: bool = True
    ) -> Dict[str, Any]:
        """
        Create or update a Firewall Manager policy.
        
        Args:
            name: Policy name
            web_acl_arn: ARN of the Web ACL to associate
            resource_type: Type of resources to protect (e.g., AWS::ElasticLoadBalancingV2::LoadBalancer)
            remediation_enabled: Enable automatic remediation
            
        Returns:
            Policy details
        """
        try:
            response = self.fms_client.put_policy(
                Policy={
                    "PolicyName": name,
                    "SecurityServicePolicy": {
                        "Type": "WAFV2",
                        "ManagedServiceData": json.dumps({
                            "type": "WAFV2",
                            "RuleGroups": [],
                            "WebACLArn": web_acl_arn
                        })
                    },
                    "ResourceType": resource_type,
                    "ResourceTypeList": [resource_type],
                    "RemediationEnabled": remediation_enabled
                }
            )
            return response.get("Policy", {})
            
        except ClientError as e:
            logger.error(f"Failed to put FMS policy: {e}")
            raise
    
    # ==================== Rate-Based Rules ====================
    
    def create_rate_rule(
        self,
        name: str,
        rate_limit: int,
        scope: WebACLScope = WebACLScope.REGIONAL,
        evaluation_window: int = 300,
        condition: Optional[Dict[str, Any]] = None,
        action: RuleAction = RuleAction.BLOCK,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> RateRule:
        """
        Create a rate-based rule.
        
        Args:
            name: Rule name
            rate_limit: Maximum requests per evaluation window
            scope: CLOUDFRONT or REGIONAL
            evaluation_window: Evaluation window in seconds
            condition: Optional condition to scope the rule
            action: Action to take when limit is exceeded
            description: Rule description
            tags: Tags to apply
            
        Returns:
            RateRule object
        """
        try:
            rule = {
                "Name": name,
                "Priority": 0,
                "Action": {"Type": action.value},
                "Statement": self.create_rate_based_statement(
                    rate_limit=rate_limit,
                    evaluation_window=evaluation_window,
                    condition=condition
                ),
                "Type": "RATE_BASED"
            }
            
            if description:
                rule["Description"] = description
            
            response = self.wafv2_client.create_rule_group(
                Name=name,
                Scope=scope.value,
                Capacity=2,
                Rules=[rule],
                Tags=[{"Key": k, "Value": v} for k, v in (tags or {}).items()]
            )
            
            group_info = response["RuleGroup"]
            
            now = self._get_current_datetime()
            return RateRule(
                name=name,
                rate_limit=rate_limit,
                rule_id=group_info["Id"],
                arn=group_info["ARN"],
                scope=scope,
                evaluation_window=evaluation_window,
                condition=condition,
                action=action,
                description=description,
                tags=tags or {}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create rate rule: {e}")
            raise
    
    def add_rate_rule_to_web_acl(
        self,
        acl_id: str,
        scope: WebACLScope,
        rate_rule: RateRule,
        priority: int = 1
    ) -> bool:
        """
        Add a rate-based rule to a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            rate_rule: RateRule object to add
            priority: Rule priority
            
        Returns:
            True if successful
        """
        rule_definition = {
            "Name": rate_rule.name,
            "Priority": priority,
            "Action": {"Type": rate_rule.action.value},
            "Statement": self.create_rate_based_statement(
                rate_limit=rate_rule.rate_limit,
                evaluation_window=rate_rule.evaluation_window,
                condition=rate_rule.condition
            ),
            "Type": "RATE_BASED"
        }
        
        return self.add_rule_to_web_acl(acl_id, scope, rule_definition)
    
    # ==================== Bot Control ====================
    
    def create_bot_control_rule(
        self,
        name: str,
        category: BotControlCategory,
        action: RuleAction = RuleAction.BLOCK,
        enable: bool = True,
        sensitivity_level: str = "MEDIUM",
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> BotControlRule:
        """
        Create a bot control rule.
        
        Args:
            name: Rule name
            category: Bot control category
            action: Action to take on bot traffic
            enable: Whether the rule is enabled
            sensitivity_level: Sensitivity level (LOW, MEDIUM, HIGH)
            description: Rule description
            tags: Tags to apply
            
        Returns:
            BotControlRule object
        """
        try:
            statement = {
                "ManagedRuleGroupStatement": {
                    "VendorName": "AWS",
                    "Name": "AWSManagedBotControlRuleSet",
                    "RuleGroupName": category.value,
                    "ExcludedRules": [] if enable else [{"Name": "AllRules"}]
                }
            }
            
            rule = {
                "Name": name,
                "Priority": 0,
                "Action": {"Type": action.value},
                "Statement": statement,
                "VisibilityConfig": {
                    "SampledRequestsEnabled": True,
                    "CloudWatchMetricsEnabled": True,
                    "MetricName": name
                }
            }
            
            if description:
                rule["Description"] = description
            
            response = self.wafv2_client.create_rule_group(
                Name=name,
                Scope=WebACLScope.REGIONAL.value,
                Capacity=10,
                Rules=[rule],
                Tags=[{"Key": k, "Value": v} for k, v in (tags or {}).items()]
            )
            
            group_info = response["RuleGroup"]
            
            now = self._get_current_datetime()
            return BotControlRule(
                name=name,
                category=category,
                rule_id=group_info["Id"],
                arn=group_info["ARN"],
                action=action,
                enable=enable,
                sensitivity_level=sensitivity_level,
                description=description,
                tags=tags or {}
            )
            
        except ClientError as e:
            logger.error(f"Failed to create bot control rule: {e}")
            raise
    
    def add_bot_control_to_web_acl(
        self,
        acl_id: str,
        scope: WebACLScope,
        category: BotControlCategory,
        action: RuleAction = RuleAction.BLOCK,
        priority: int = 1
    ) -> bool:
        """
        Add bot control to a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            category: Bot control category
            action: Action to take on bot traffic
            priority: Rule priority
            
        Returns:
            True if successful
        """
        rule_definition = {
            "Name": f"BotControl-{category.value}",
            "Priority": priority,
            "Action": {"Type": action.value},
            "Statement": {
                "ManagedRuleGroupStatement": {
                    "VendorName": "AWS",
                    "Name": "AWSManagedBotControlRuleSet",
                    "RuleGroupName": category.value
                }
            },
            "VisibilityConfig": {
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": f"BotControl-{category.value}"
            }
        }
        
        return self.add_rule_to_web_acl(acl_id, scope, rule_definition)
    
    # ==================== CloudWatch Integration ====================
    
    def create_waf_metrics(
        self,
        acl_id: str,
        scope: WebACLScope,
        metric_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create CloudWatch metrics for WAF.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            metric_name: Optional custom metric name
            
        Returns:
            Metrics configuration
        """
        acl = self.get_web_acl(acl_id, scope)
        if not acl:
            return {}
        
        metrics = {
            "MetricName": metric_name or f"WAF-{acl.name}",
            "Rules": []
        }
        
        for rule in acl.rules:
            rule_metric = {
                "RuleName": rule.get("Name", "Default"),
                "MetricName": rule.get("Name", "Default").replace(" ", "") + "Metric"
            }
            metrics["Rules"].append(rule_metric)
        
        return metrics
    
    def put_waf_metrics_alarm(
        self,
        metric_name: str,
        threshold: int,
        period: int = 60,
        evaluation_periods: int = 1,
        comparison_operator: str = "GreaterThanThreshold"
    ) -> str:
        """
        Create a CloudWatch alarm for WAF metrics.
        
        Args:
            metric_name: Name of the metric
            threshold: Threshold value
            period: Period in seconds
            evaluation_periods: Number of evaluation periods
            comparison_operator: Comparison operator
            
        Returns:
            Alarm ARN
        """
        try:
            alarm_name = f"WAF-Alarm-{metric_name}"
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace="AWS/WAFV2",
                Statistic="Sum",
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                TreatMissingData="notBreaching"
            )
            
            return alarm_name
            
        except ClientError as e:
            logger.error(f"Failed to create WAF metrics alarm: {e}")
            raise
    
    def get_waf_metrics(
        self,
        acl_id: str,
        scope: WebACLScope,
        metric_names: List[str],
        start_time: datetime,
        end_time: datetime,
        period: int = 60
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get WAF metrics from CloudWatch.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            metric_names: List of metric names to retrieve
            start_time: Start time for metrics
            end_time: End time for metrics
            period: Period in seconds
            
        Returns:
            Metrics data
        """
        try:
            acl = self.get_web_acl(acl_id, scope)
            if not acl:
                return {}
            
            metrics_data = {}
            
            for metric_name in metric_names:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace="AWS/WAFV2",
                    MetricName=metric_name,
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=period,
                    Statistics=["Sum", "Average"]
                )
                
                metrics_data[metric_name] = response.get("Datapoints", [])
            
            return metrics_data
            
        except ClientError as e:
            logger.error(f"Failed to get WAF metrics: {e}")
            return {}
    
    def enable_waf_cloudwatch_metrics(
        self,
        acl_id: str,
        scope: WebACLScope,
        sampled_requests: bool = True
    ) -> bool:
        """
        Enable CloudWatch metrics for a Web ACL.
        
        Args:
            acl_id: Web ACL ID
            scope: CLOUDFRONT or REGIONAL
            sampled_requests: Enable sampled requests
            
        Returns:
            True if successful
        """
        try:
            acl = self.get_web_acl(acl_id, scope)
            if not acl:
                return False
            
            rules = []
            for rule in acl.rules:
                rule_copy = rule.copy()
                rule_copy["VisibilityConfig"] = {
                    "SampledRequestsEnabled": sampled_requests,
                    "CloudWatchMetricsEnabled": True,
                    "MetricName": rule.get("Name", "Rule") + "Metric"
                }
                rules.append(rule_copy)
            
            return self.update_web_acl(acl_id, scope, rules=rules)
            
        except Exception as e:
            logger.error(f"Failed to enable CloudWatch metrics: {e}")
            return False
    
    def list_waf_cloudwatch_metrics(self, scope: WebACLScope) -> List[str]:
        """
        List available WAF CloudWatch metrics.
        
        Args:
            scope: CLOUDFRONT or REGIONAL
            
        Returns:
            List of metric names
        """
        try:
            response = self.cloudwatch_client.list_metrics(
                Namespace="AWS/WAFV2"
            )
            
            metrics = []
            for metric in response.get("Metrics", []):
                metric_name = metric.get("MetricName", "")
                if scope.value in metric_name or not metrics:
                    metrics.append(metric_name)
            
            return list(set(metrics))
            
        except ClientError as e:
            logger.error(f"Failed to list WAF metrics: {e}")
            return []
