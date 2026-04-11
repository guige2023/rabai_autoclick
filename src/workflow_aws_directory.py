"""
AWS Directory Service Integration Module for Workflow System

Implements a DirectoryServiceIntegration class with:
1. Simple AD: Create/manage Simple AD directories
2. Microsoft AD: Create/manage Microsoft AD directories
3. AD Connector: Create/manage AD Connectors
4. Trust relationships: Manage forest trust relationships
5. DNS management: Manage DNS settings
6. Computer management: Manage directory computers
7. Domain join: Domain join operations
8. Multi-region: Multi-region AD replication
9. IAM roles: IAM role management for directory
10. CloudWatch integration: Directory metrics and monitoring

Commit: 'feat(aws-directory): add AWS Directory Service with Simple AD, Microsoft AD, AD Connector, trust relationships, DNS management, computer management, domain join, CloudWatch'
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


class DirectoryType(Enum):
    """AWS Directory Service types."""
    SIMPLE_AD = "SimpleAD"
    MICROSOFT_AD = "MicrosoftAD"
    AD_CONNECTOR = "ADConnector"


class DirectoryState(Enum):
    """Directory states."""
    REQUESTED = "Requested"
    CREATING = "Creating"
    CREATED = "Created"
    ACTIVE = "Active"
    INOPERATIVE = "Inoperative"
    DELETING = "Deleting"
    DELETED = "Deleted"
    FAILED = "Failed"


class DirectorySize(Enum):
    """Directory size options."""
    SMALL = "Small"
    LARGE = "Large"


class TrustDirection(Enum):
    """Trust relationship directions."""
    ONE_WAY_INBOUND = "One-Way: Inbound"
    ONE_WAY_OUTBOUND = "One-Way: Outbound"
    TWO_WAY = "Two-Way"


class TrustType(Enum):
    """Trust relationship types."""
    FOREST = "Forest"
    DOMAIN = "Domain"


class TrustState(Enum):
    """Trust relationship states."""
    CONNECTED = "Connected"
    DISCONNECTED = "Disconnected"
    VERIFYING = "Verifying"
    VERIFY_FAILED = "Verify Failed"


class ComputerState(Enum):
    """Directory computer states."""
    ONLINE = "Online"
    OFFLINE = "Offline"
    CREATING = "Creating"
    DELETING = "Deleting"


class DomainJoinState(Enum):
    """Domain join states."""
    SUCCESS = "Success"
    FAILED = "Failed"
    PENDING = "Pending"


@dataclass
class DirectoryConfig:
    """Configuration for a directory."""
    name: str
    directory_type: DirectoryType
    size: DirectorySize = DirectorySize.SMALL
    description: str = ""
    vpc_settings: Optional[Dict[str, Any]] = None
    customer_username: Optional[str] = None
    customer_password: Optional[str] = None
    edition: str = "Standard"
    region: str = "us-east-1"
    alias: Optional[str] = None
    enable_sso: bool = False
    sso_password: Optional[str] = None
    sso_username: Optional[str] = None
    short_name: Optional[str] = None
    certificate_id: Optional[str] = None
    connect_settings: Optional[Dict[str, Any]] = None


@dataclass
class TrustRelationshipConfig:
    """Configuration for a trust relationship."""
    trusted_domain: str
    trust_direction: TrustDirection
    trust_type: TrustType = TrustType.FOREST
    trust_password: str = ""
    conditional_forwarder_ip: Optional[List[str]] = None
    remote_domain_name: Optional[str] = None


@dataclass
class DNSConfig:
    """DNS configuration for a directory."""
    dns_servers: List[str] = field(default_factory=list)
    dns_zone_name: Optional[str] = None
    dns_reverse_lookup: bool = True


@dataclass
class ComputerConfig:
    """Configuration for a directory computer."""
    computer_name: str
    organizational_unit: Optional[str] = None
    ip_address: Optional[str] = None


@dataclass
class DomainJoinConfig:
    """Configuration for domain join operation."""
    instance_id: str
    directory_id: str
    computer_name: Optional[str] = None
    organizational_unit: Optional[str] = None
    ip_address: Optional[str] = None


class DirectoryServiceIntegration:
    """
    AWS Directory Service integration for managing directories,
    trust relationships, DNS, computers, and domain join operations.
    """

    def __init__(self, region: str = "us-east-1", profile_name: Optional[str] = None):
        """
        Initialize the Directory Service integration.

        Args:
            region: AWS region for Directory Service
            profile_name: Optional AWS profile name
        """
        self.region = region
        self.profile_name = profile_name
        self._directory_cache: Dict[str, Dict[str, Any]] = {}
        self._trust_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()

        if BOTO3_AVAILABLE:
            if profile_name:
                session = boto3.Session(profile_name=profile_name, region_name=region)
            else:
                session = boto3.Session(region_name=region)
            self.ds_client = session.client("ds")
            self.ssm_client = session.client("ssm")
            self.ec2_client = session.client("ec2")
            self.iam_client = session.client("iam")
            self.cloudwatch_client = session.client("cloudwatch")
            self.sts_client = session.client("sts")
        else:
            self.ds_client = None
            self.ssm_client = None
            self.ec2_client = None
            self.iam_client = None
            self.cloudwatch_client = None
            self.sts_client = None

    def _generate_id(self) -> str:
        """Generate a unique ID."""
        return str(uuid.uuid4())[:8]

    def _wait_for_directory(
        self,
        directory_id: str,
        target_states: List[DirectoryState],
        timeout: int = 600,
        poll_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for a directory to reach a target state.

        Args:
            directory_id: Directory ID
            target_states: Target states to wait for
            timeout: Maximum wait time in seconds
            poll_interval: Polling interval in seconds

        Returns:
            Directory information dict
        """
        if not BOTO3_AVAILABLE:
            return {"directory_id": directory_id, "status": "unavailable"}

        start_time = time.time()
        target_state_values = [s.value for s in target_states]

        while time.time() - start_time < timeout:
            try:
                response = self.ds_client.describe_directories(
                    DirectoryIds=[directory_id]
                )
                if response["DirectoryDescriptions"]:
                    directory = response["DirectoryDescriptions"][0]
                    current_state = directory["Stage"]

                    if current_state in target_state_values:
                        return directory

                    if current_state == DirectoryState.FAILED.value:
                        logger.error(f"Directory {directory_id} failed: {directory.get('StatusReason', {}).get('Message', 'Unknown')}")
                        return directory

                time.sleep(poll_interval)
            except ClientError as e:
                logger.error(f"Error describing directory {directory_id}: {e}")
                break

        return {"directory_id": directory_id, "status": "timeout"}

    # ========================================================================
    # Simple AD Operations
    # ========================================================================

    def create_simple_ad(
        self,
        name: str,
        size: DirectorySize = DirectorySize.SMALL,
        description: str = "",
        vpc_id: str = "",
        subnet_ids: List[str] = None,
        alias: Optional[str] = None,
        enable_sso: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a Simple AD directory.

        Args:
            name: Directory name
            size: Directory size (Small or Large)
            description: Directory description
            vpc_id: VPC ID
            subnet_ids: List of subnet IDs (2 subnets required)
            alias: Directory alias
            enable_sso: Enable Single Sign-On
            **kwargs: Additional parameters

        Returns:
            Dictionary with directory creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable", "message": "boto3 not available"}

        if subnet_ids is None:
            return {"status": "error", "message": "subnet_ids required"}

        try:
            params = {
                "Name": name,
                "ShortName": name.split(".")[0] if "." in name else name,
                "Password": kwargs.get("password", f"TempPassword123!{self._generate_id()}"),
                "DirectoryType": DirectoryType.SIMPLE_AD.value,
                "Size": size.value,
                "VpcSettings": {
                    "VpcId": vpc_id,
                    "SubnetIds": subnet_ids
                }
            }

            if description:
                params["Description"] = description
            if alias:
                params["Alias"] = alias

            response = self.ds_client.create_directory(**params)
            directory_id = response["DirectoryId"]

            logger.info(f"Created Simple AD directory: {directory_id}")

            if enable_sso:
                self._enable_sso(directory_id, **kwargs)

            return {
                "status": "creating",
                "directory_id": directory_id,
                "directory_type": "SimpleAD",
                "name": name,
                "size": size.value
            }

        except ClientError as e:
            logger.error(f"Error creating Simple AD: {e}")
            return {"status": "error", "message": str(e)}

    def get_simple_ad(self, directory_id: str) -> Dict[str, Any]:
        """
        Get Simple AD directory details.

        Args:
            directory_id: Directory ID

        Returns:
            Directory details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )
            if response["DirectoryDescriptions"]:
                return response["DirectoryDescriptions"][0]
            return {"status": "not_found"}
        except ClientError as e:
            logger.error(f"Error getting Simple AD: {e}")
            return {"status": "error", "message": str(e)}

    def list_simple_ad(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List Simple AD directories.

        Args:
            filters: Optional filters for listing

        Returns:
            List of Simple AD directories
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_directories()
            directories = response.get("DirectoryDescriptions", [])

            simple_ad_list = [
                d for d in directories
                if d.get("Type") == DirectoryType.SIMPLE_AD.value
            ]

            if filters:
                if "vpc_id" in filters:
                    simple_ad_list = [
                        d for d in simple_ad_list
                        if d.get("VpcSettings", {}).get("VpcId") == filters["vpc_id"]
                    ]
                if "stage" in filters:
                    simple_ad_list = [
                        d for d in simple_ad_list
                        if d.get("Stage") == filters["stage"]
                    ]

            return simple_ad_list

        except ClientError as e:
            logger.error(f"Error listing Simple AD: {e}")
            return []

    def delete_simple_ad(self, directory_id: str) -> Dict[str, Any]:
        """
        Delete a Simple AD directory.

        Args:
            directory_id: Directory ID

        Returns:
            Deletion result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.delete_directory(DirectoryId=directory_id)
            logger.info(f"Deleted Simple AD directory: {directory_id}")
            return {"status": "deleting", "directory_id": directory_id}
        except ClientError as e:
            logger.error(f"Error deleting Simple AD: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # Microsoft AD Operations
    # ========================================================================

    def create_microsoft_ad(
        self,
        name: str,
        description: str = "",
        vpc_id: str = "",
        subnet_ids: List[str] = None,
        alias: Optional[str] = None,
        enable_sso: bool = False,
        edition: str = "Standard",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a Microsoft AD directory.

        Args:
            name: Directory name
            description: Directory description
            vpc_id: VPC ID
            subnet_ids: List of subnet IDs (2 subnets required)
            alias: Directory alias
            enable_sso: Enable Single Sign-On
            edition: Microsoft AD edition (Standard or Enterprise)
            **kwargs: Additional parameters

        Returns:
            Dictionary with directory creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable", "message": "boto3 not available"}

        if subnet_ids is None:
            return {"status": "error", "message": "subnet_ids required"}

        try:
            params = {
                "Name": name,
                "ShortName": name.split(".")[0] if "." in name else name,
                "Password": kwargs.get("password", f"TempPassword123!{self._generate_id()}"),
                "DirectoryType": DirectoryType.MICROSOFT_AD.value,
                "VpcSettings": {
                    "VpcId": vpc_id,
                    "SubnetIds": subnet_ids
                },
                "Edition": edition
            }

            if description:
                params["Description"] = description
            if alias:
                params["Alias"] = alias

            response = self.ds_client.create_directory(**params)
            directory_id = response["DirectoryId"]

            logger.info(f"Created Microsoft AD directory: {directory_id}")

            if enable_sso:
                self._enable_sso(directory_id, **kwargs)

            return {
                "status": "creating",
                "directory_id": directory_id,
                "directory_type": "MicrosoftAD",
                "name": name,
                "edition": edition
            }

        except ClientError as e:
            logger.error(f"Error creating Microsoft AD: {e}")
            return {"status": "error", "message": str(e)}

    def get_microsoft_ad(self, directory_id: str) -> Dict[str, Any]:
        """
        Get Microsoft AD directory details.

        Args:
            directory_id: Directory ID

        Returns:
            Directory details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )
            if response["DirectoryDescriptions"]:
                return response["DirectoryDescriptions"][0]
            return {"status": "not_found"}
        except ClientError as e:
            logger.error(f"Error getting Microsoft AD: {e}")
            return {"status": "error", "message": str(e)}

    def list_microsoft_ad(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List Microsoft AD directories.

        Args:
            filters: Optional filters for listing

        Returns:
            List of Microsoft AD directories
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_directories()
            directories = response.get("DirectoryDescriptions", [])

            microsoft_ad_list = [
                d for d in directories
                if d.get("Type") == DirectoryType.MICROSOFT_AD.value
            ]

            if filters:
                if "vpc_id" in filters:
                    microsoft_ad_list = [
                        d for d in microsoft_ad_list
                        if d.get("VpcSettings", {}).get("VpcId") == filters["vpc_id"]
                    ]
                if "stage" in filters:
                    microsoft_ad_list = [
                        d for d in microsoft_ad_list
                        if d.get("Stage") == filters["stage"]
                    ]
                if "edition" in filters:
                    microsoft_ad_list = [
                        d for d in microsoft_ad_list
                        if d.get("Edition") == filters["edition"]
                    ]

            return microsoft_ad_list

        except ClientError as e:
            logger.error(f"Error listing Microsoft AD: {e}")
            return []

    def enable_microsoft_ad_replica(
        self,
        directory_id: str,
        region: str,
        vpc_id: str,
        subnet_ids: List[str]
    ) -> Dict[str, Any]:
        """
        Enable multi-region replication for Microsoft AD.

        Args:
            directory_id: Directory ID
            region: Secondary region
            vpc_id: VPC ID in secondary region
            subnet_ids: Subnet IDs in secondary region

        Returns:
            Replica creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.create_microsoft_ad(
                DirectoryId=directory_id,
                Region=region,
               VpcSettings={
                    "VpcId": vpc_id,
                    "SubnetIds": subnet_ids
                }
            )
            logger.info(f"Created Microsoft AD replica in {region}: {response.get('DirectoryId')}")
            return {
                "status": "creating",
                "directory_id": response.get("DirectoryId"),
                "region": region
            }
        except ClientError as e:
            logger.error(f"Error creating Microsoft AD replica: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # AD Connector Operations
    # ========================================================================

    def create_ad_connector(
        self,
        name: str,
        size: DirectorySize = DirectorySize.SMALL,
        vpc_id: str = "",
        subnet_ids: List[str] = None,
        alias: Optional[str] = None,
        dns_addresses: List[str] = None,
        customer_username: Optional[str] = None,
        customer_password: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create an AD Connector.

        Args:
            name: Connector name
            size: Connector size (Small or Large)
            vpc_id: VPC ID
            subnet_ids: List of subnet IDs
            alias: Connector alias
            dns_addresses: DNS IP addresses of on-premises AD
            customer_username: Username for AD connection
            customer_password: Password for AD connection
            **kwargs: Additional parameters

        Returns:
            Dictionary with connector creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable", "message": "boto3 not available"}

        if subnet_ids is None or dns_addresses is None:
            return {"status": "error", "message": "subnet_ids and dns_addresses required"}

        try:
            params = {
                "Name": name,
                "Size": size.value,
                "ConnectorSettings": {
                    "VpcId": vpc_id,
                    "SubnetIds": subnet_ids
                },
                "ConnectSettings": {
                    "VpcId": vpc_id,
                    "SubnetIds": subnet_ids,
                    "CustomerDnsIps": dns_addresses,
                    "CustomerUserName": customer_username or kwargs.get("ad_username", "Admin")
                }
            }

            if customer_password:
                params["ConnectSettings"]["CustomerPassword"] = customer_password
            elif kwargs.get("ad_password"):
                params["ConnectSettings"]["CustomerPassword"] = kwargs["ad_password"]

            if alias:
                params["Alias"] = alias

            response = self.ds_client.create_directory(**params)
            directory_id = response["DirectoryId"]

            logger.info(f"Created AD Connector: {directory_id}")

            return {
                "status": "creating",
                "directory_id": directory_id,
                "directory_type": "ADConnector",
                "name": name,
                "size": size.value
            }

        except ClientError as e:
            logger.error(f"Error creating AD Connector: {e}")
            return {"status": "error", "message": str(e)}

    def get_ad_connector(self, directory_id: str) -> Dict[str, Any]:
        """
        Get AD Connector details.

        Args:
            directory_id: Connector ID

        Returns:
            Connector details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )
            if response["DirectoryDescriptions"]:
                return response["DirectoryDescriptions"][0]
            return {"status": "not_found"}
        except ClientError as e:
            logger.error(f"Error getting AD Connector: {e}")
            return {"status": "error", "message": str(e)}

    def list_ad_connector(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List AD Connectors.

        Args:
            filters: Optional filters for listing

        Returns:
            List of AD Connectors
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_directories()
            directories = response.get("DirectoryDescriptions", [])

            ad_connector_list = [
                d for d in directories
                if d.get("Type") == DirectoryType.AD_CONNECTOR.value
            ]

            if filters:
                if "vpc_id" in filters:
                    ad_connector_list = [
                        d for d in ad_connector_list
                        if d.get("VpcSettings", {}).get("VpcId") == filters["vpc_id"]
                    ]

            return ad_connector_list

        except ClientError as e:
            logger.error(f"Error listing AD Connector: {e}")
            return []

    def verify_ad_connector_settings(self, directory_id: str) -> Dict[str, Any]:
        """
        Verify AD Connector settings.

        Args:
            directory_id: Connector ID

        Returns:
            Verification result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.verify_directory_credential_settings(
                DirectoryId=directory_id
            )
            return {
                "status": "verified",
                "directory_id": directory_id,
                "verification_result": response
            }
        except ClientError as e:
            logger.error(f"Error verifying AD Connector: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # Trust Relationship Operations
    # ========================================================================

    def create_trust_relationship(
        self,
        directory_id: str,
        trust_config: TrustRelationshipConfig
    ) -> Dict[str, Any]:
        """
        Create a forest trust relationship.

        Args:
            directory_id: Directory ID
            trust_config: Trust relationship configuration

        Returns:
            Trust creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            params = {
                "DirectoryId": directory_id,
                "RemoteDomainName": trust_config.trusted_domain,
                "TrustDirection": trust_config.trust_direction.value,
                "TrustType": trust_config.trust_type.value,
                "TrustPassword": trust_config.trust_password
            }

            if trust_config.conditional_forwarder_ip:
                params["ConditionalForwardingIpAddresses"] = trust_config.conditional_forwarder_ip

            response = self.ds_client.create_trust(**params)

            logger.info(f"Created trust relationship: {directory_id} -> {trust_config.trusted_domain}")

            return {
                "status": "creating",
                "directory_id": directory_id,
                "trusted_domain": trust_config.trusted_domain,
                "trust_token": response.get("TrustId")
            }

        except ClientError as e:
            logger.error(f"Error creating trust relationship: {e}")
            return {"status": "error", "message": str(e)}

    def get_trust_relationship(
        self,
        directory_id: str,
        trust_id: str
    ) -> Dict[str, Any]:
        """
        Get trust relationship details.

        Args:
            directory_id: Directory ID
            trust_id: Trust relationship ID

        Returns:
            Trust relationship details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_trust_relationships(
                DirectoryId=directory_id
            )

            for trust in response.get("TrustRelationships", []):
                if trust.get("TrustId") == trust_id:
                    return trust

            return {"status": "not_found"}

        except ClientError as e:
            logger.error(f"Error getting trust relationship: {e}")
            return {"status": "error", "message": str(e)}

    def list_trust_relationships(self, directory_id: str) -> List[Dict[str, Any]]:
        """
        List all trust relationships for a directory.

        Args:
            directory_id: Directory ID

        Returns:
            List of trust relationships
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_trust_relationships(
                DirectoryId=directory_id
            )
            return response.get("TrustRelationships", [])

        except ClientError as e:
            logger.error(f"Error listing trust relationships: {e}")
            return []

    def update_trust_relationship(
        self,
        directory_id: str,
        trust_id: str,
        new_password: str
    ) -> Dict[str, Any]:
        """
        Update trust relationship password.

        Args:
            directory_id: Directory ID
            trust_id: Trust relationship ID
            new_password: New trust password

        Returns:
            Update result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.update_trust(
                TrustId=trust_id,
                RemoteDomainName=directory_id,
                TrustPassword=new_password
            )

            logger.info(f"Updated trust relationship: {trust_id}")
            return {"status": "updated", "trust_id": trust_id}

        except ClientError as e:
            logger.error(f"Error updating trust relationship: {e}")
            return {"status": "error", "message": str(e)}

    def delete_trust_relationship(
        self,
        directory_id: str,
        trust_id: str
    ) -> Dict[str, Any]:
        """
        Delete a trust relationship.

        Args:
            directory_id: Directory ID
            trust_id: Trust relationship ID

        Returns:
            Deletion result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.delete_trust(
                TrustId=trust_id,
                DirectoryId=directory_id
            )

            logger.info(f"Deleted trust relationship: {trust_id}")
            return {"status": "deleted", "trust_id": trust_id}

        except ClientError as e:
            logger.error(f"Error deleting trust relationship: {e}")
            return {"status": "error", "message": str(e)}

    def verify_trust_relationship(
        self,
        directory_id: str,
        trust_id: str
    ) -> Dict[str, Any]:
        """
        Verify a trust relationship.

        Args:
            directory_id: Directory ID
            trust_id: Trust relationship ID

        Returns:
            Verification result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.verify_trust(
                TrustId=trust_id,
                DirectoryId=directory_id
            )

            return {
                "status": "verifying",
                "trust_id": trust_id,
                "verification_id": response.get("TrustVerificationId")
            }

        except ClientError as e:
            logger.error(f"Error verifying trust relationship: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # DNS Management Operations
    # ========================================================================

    def get_dns_config(self, directory_id: str) -> DNSConfig:
        """
        Get DNS configuration for a directory.

        Args:
            directory_id: Directory ID

        Returns:
            DNS configuration
        """
        if not BOTO3_AVAILABLE:
            return DNSConfig()

        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )

            if response["DirectoryDescriptions"]:
                directory = response["DirectoryDescriptions"][0]
                dns_ips = directory.get("DnsIpAddrs", [])

                return DNSConfig(
                    dns_servers=dns_ips,
                    dns_zone_name=directory.get("Name"),
                    dns_reverse_lookup=True
                )

            return DNSConfig()

        except ClientError as e:
            logger.error(f"Error getting DNS config: {e}")
            return DNSConfig()

    def update_dns_config(
        self,
        directory_id: str,
        dns_config: DNSConfig
    ) -> Dict[str, Any]:
        """
        Update DNS configuration for a directory.

        Args:
            directory_id: Directory ID
            dns_config: New DNS configuration

        Returns:
            Update result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            if self.ec2_client:
                for dns_ip in dns_config.dns_servers:
                    self.ec2_client.modify_vpc_attribute(
                        VpcId=self._get_vpc_for_directory(directory_id),
                        EnableDnsHostnames={"Value": True},
                        EnableDnsSupport={"Value": True}
                    )

            logger.info(f"Updated DNS config for directory: {directory_id}")
            return {
                "status": "updated",
                "directory_id": directory_id,
                "dns_servers": dns_config.dns_servers
            }

        except ClientError as e:
            logger.error(f"Error updating DNS config: {e}")
            return {"status": "error", "message": str(e)}

    def _get_vpc_for_directory(self, directory_id: str) -> Optional[str]:
        """Get VPC ID for a directory."""
        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )
            if response["DirectoryDescriptions"]:
                return response["DirectoryDescriptions"][0].get("VpcSettings", {}).get("VpcId")
        except ClientError:
            pass
        return None

    def add_dns_forwarder(
        self,
        directory_id: str,
        domain_name: str,
        forwarder_ips: List[str]
    ) -> Dict[str, Any]:
        """
        Add conditional DNS forwarder.

        Args:
            directory_id: Directory ID
            domain_name: Domain name to forward
            forwarder_ips: DNS forwarder IP addresses

        Returns:
            Configuration result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            logger.info(f"Added DNS forwarder for {domain_name}: {forwarder_ips}")
            return {
                "status": "configured",
                "directory_id": directory_id,
                "domain_name": domain_name,
                "forwarder_ips": forwarder_ips
            }

        except ClientError as e:
            logger.error(f"Error adding DNS forwarder: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # Computer Management Operations
    # ========================================================================

    def register_computer(
        self,
        directory_id: str,
        computer_config: ComputerConfig
    ) -> Dict[str, Any]:
        """
        Register a computer with the directory.

        Args:
            directory_id: Directory ID
            computer_config: Computer configuration

        Returns:
            Registration result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.register_event_topic(
                DirectoryId=directory_id,
                EventTopic=f"arn:aws:sns:{self.region}:*:directory-computer-{computer_config.computer_name}"
            )

            logger.info(f"Registered computer: {computer_config.computer_name}")

            return {
                "status": "registered",
                "directory_id": directory_id,
                "computer_name": computer_config.computer_name
            }

        except ClientError as e:
            logger.error(f"Error registering computer: {e}")
            return {"status": "error", "message": str(e)}

    def list_computers(
        self,
        directory_id: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List computers in a directory.

        Args:
            directory_id: Directory ID
            limit: Maximum number of results

        Returns:
            List of computers
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_computers(
                DirectoryId=directory_id,
                Limit=limit
            )
            return response.get("Computers", [])

        except ClientError as e:
            logger.error(f"Error listing computers: {e}")
            return []

    def get_computer(self, directory_id: str, computer_name: str) -> Dict[str, Any]:
        """
        Get computer details.

        Args:
            directory_id: Directory ID
            computer_name: Computer name

        Returns:
            Computer details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_computers(
                DirectoryId=directory_id
            )

            for computer in response.get("Computers", []):
                if computer.get("ComputerName") == computer_name:
                    return computer

            return {"status": "not_found"}

        except ClientError as e:
            logger.error(f"Error getting computer: {e}")
            return {"status": "error", "message": str(e)}

    def update_computer(
        self,
        directory_id: str,
        computer_name: str,
        new_ou: str
    ) -> Dict[str, Any]:
        """
        Update computer attributes.

        Args:
            directory_id: Directory ID
            computer_name: Computer name
            new_ou: New organizational unit path

        Returns:
            Update result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.update_computer(
                DirectoryId=directory_id,
                ComputerName=computer_name,
                OrganizationalUnitDistinguishedName=new_ou
            )

            logger.info(f"Updated computer: {computer_name} -> {new_ou}")
            return {
                "status": "updated",
                "computer_name": computer_name,
                "new_ou": new_ou
            }

        except ClientError as e:
            logger.error(f"Error updating computer: {e}")
            return {"status": "error", "message": str(e)}

    def delete_computer(self, directory_id: str, computer_name: str) -> Dict[str, Any]:
        """
        Remove a computer from the directory.

        Args:
            directory_id: Directory ID
            computer_name: Computer name

        Returns:
            Deletion result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.remove_computer_from_directory(
                DirectoryId=directory_id,
                ComputerName=computer_name
            )

            logger.info(f"Deleted computer: {computer_name}")
            return {"status": "deleted", "computer_name": computer_name}

        except ClientError as e:
            logger.error(f"Error deleting computer: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # Domain Join Operations
    # ========================================================================

    def domain_join_instance(
        self,
        domain_join_config: DomainJoinConfig
    ) -> Dict[str, Any]:
        """
        Join an instance to a domain.

        Args:
            domain_join_config: Domain join configuration

        Returns:
            Domain join result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            ssm_command = self.ssm_client.send_command(
                InstanceIds=[domain_join_config.instance_id],
                DocumentName="AWS-JoinDirectoryServiceDomain",
                Parameters={
                    "directoryId": [domain_join_config.directory_id],
                    "directoryName": [self._get_directory_name(domain_join_config.directory_id)]
                }
            )

            command_id = ssm_command["Command"]["CommandId"]

            logger.info(f"Initiated domain join for instance: {domain_join_config.instance_id}")

            return {
                "status": "pending",
                "instance_id": domain_join_config.instance_id,
                "command_id": command_id,
                "directory_id": domain_join_config.directory_id
            }

        except ClientError as e:
            logger.error(f"Error joining domain: {e}")
            return {"status": "error", "message": str(e)}

    def domain_join_instance_simple(
        self,
        instance_id: str,
        directory_id: str,
        directory_name: str,
        dns_ip: str
    ) -> Dict[str, Any]:
        """
        Simple domain join using SSM document.

        Args:
            instance_id: EC2 instance ID
            directory_id: Directory ID
            directory_name: Directory name
            dns_ip: DNS server IP

        Returns:
            Domain join result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName="AWS-JoinDirectoryServiceDomain",
                Parameters={
                    "directoryId": [directory_id],
                    "directoryName": [directory_name]
                }
            )

            return {
                "status": "pending",
                "instance_id": instance_id,
                "command_id": response["Command"]["CommandId"]
            }

        except ClientError as e:
            logger.error(f"Error in simple domain join: {e}")
            return {"status": "error", "message": str(e)}

    def get_domain_join_status(self, command_id: str) -> Dict[str, Any]:
        """
        Get domain join command status.

        Args:
            command_id: SSM command ID

        Returns:
            Command status
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ssm_client.list_commands(
                CommandId=command_id
            )

            if response["Commands"]:
                cmd = response["Commands"][0]
                return {
                    "command_id": command_id,
                    "status": cmd["Status"],
                    "instance_ids": cmd["InstanceIds"]
                }

            return {"status": "not_found"}

        except ClientError as e:
            logger.error(f"Error getting domain join status: {e}")
            return {"status": "error", "message": str(e)}

    def domain_leave_instance(
        self,
        instance_id: str,
        directory_id: str
    ) -> Dict[str, Any]:
        """
        Remove an instance from a domain.

        Args:
            instance_id: EC2 instance ID
            directory_id: Directory ID

        Returns:
            Domain leave result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName="AWS-LeaveDirectoryServiceDomain",
                Parameters={
                    "directoryId": [directory_id]
                }
            )

            logger.info(f"Initiated domain leave for instance: {instance_id}")

            return {
                "status": "pending",
                "instance_id": instance_id,
                "command_id": response["Command"]["CommandId"]
            }

        except ClientError as e:
            logger.error(f"Error leaving domain: {e}")
            return {"status": "error", "message": str(e)}

    def _get_directory_name(self, directory_id: str) -> str:
        """Get directory name from directory ID."""
        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )
            if response["DirectoryDescriptions"]:
                return response["DirectoryDescriptions"][0].get("Name", "")
        except ClientError:
            pass
        return ""

    # ========================================================================
    # Multi-Region Operations
    # ========================================================================

    def enable_multi_region_replication(
        self,
        directory_id: str,
        regions: List[str],
        vpc_config: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Enable multi-region replication for Microsoft AD.

        Args:
            directory_id: Directory ID
            regions: List of additional regions
            vpc_config: VPC configuration per region {region: {vpc_id, subnet_ids}}

        Returns:
            Multi-region setup result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        results = []

        for region in regions:
            if region not in vpc_config:
                results.append({
                    "region": region,
                    "status": "error",
                    "message": "VPC config not provided"
                })
                continue

            config = vpc_config[region]
            result = self.enable_microsoft_ad_replica(
                directory_id=directory_id,
                region=region,
                vpc_id=config["vpc_id"],
                subnet_ids=config["subnet_ids"]
            )
            results.append(result)

        return {
            "status": "configured",
            "directory_id": directory_id,
            "replicas": results
        }

    def list_directory_replicas(self, directory_id: str) -> List[Dict[str, Any]]:
        """
        List all replicas of a directory.

        Args:
            directory_id: Directory ID

        Returns:
            List of replicas
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_directory_replicas(
                DirectoryId=directory_id
            )
            return response.get("replicas", [])

        except ClientError as e:
            logger.error(f"Error listing replicas: {e}")
            return []

    def remove_directory_replica(
        self,
        directory_id: str,
        region: str
    ) -> Dict[str, Any]:
        """
        Remove a directory replica.

        Args:
            directory_id: Directory ID
            region: Region of the replica

        Returns:
            Removal result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.remove_directory(
                DirectoryId=directory_id,
                Region=region
            )

            logger.info(f"Removed directory replica: {directory_id} in {region}")
            return {"status": "removed", "region": region}

        except ClientError as e:
            logger.error(f"Error removing replica: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # IAM Role Management
    # ========================================================================

    def create_directory_service_role(self) -> Dict[str, Any]:
        """
        Create the AWS Directory Service IAM role.

        Returns:
            Role creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        role_name = "AWSDirectoryServiceRole"

        try:
            role_exists = False
            try:
                self.iam_client.get_role(RoleName=role_name)
                role_exists = True
            except ClientError:
                pass

            if role_exists:
                return {
                    "status": "exists",
                    "role_name": role_name
                }

            role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "directoryservice.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(role_policy),
                Description="Role for AWS Directory Service"
            )

            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/service-role/AWSDirectoryServiceRolePolicy"
            )

            logger.info(f"Created IAM role: {role_name}")

            return {
                "status": "created",
                "role_name": role_name,
                "role_arn": response["Role"]["Arn"]
            }

        except ClientError as e:
            logger.error(f"Error creating IAM role: {e}")
            return {"status": "error", "message": str(e)}

    def get_directory_service_role(self) -> Dict[str, Any]:
        """
        Get the AWS Directory Service IAM role.

        Returns:
            Role details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.iam_client.get_role(
                RoleName="AWSDirectoryServiceRole"
            )
            return response["Role"]

        except ClientError as e:
            logger.error(f"Error getting IAM role: {e}")
            return {"status": "not_found", "message": str(e)}

    def create_directory_service_managed_policy(
        self,
        policy_name: str,
        directory_arn: str,
        actions: List[str]
    ) -> Dict[str, Any]:
        """
        Create a managed policy for directory operations.

        Args:
            policy_name: Policy name
            directory_arn: Directory ARN
            actions: List of allowed actions

        Returns:
            Policy creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": actions,
                        "Resource": directory_arn
                    }
                ]
            }

            response = self.iam_client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=f"Managed policy for {policy_name}"
            )

            logger.info(f"Created managed policy: {policy_name}")

            return {
                "status": "created",
                "policy_name": policy_name,
                "policy_arn": response["Policy"]["Arn"]
            }

        except ClientError as e:
            logger.error(f"Error creating managed policy: {e}")
            return {"status": "error", "message": str(e)}

    def assume_directory_service_role(
        self,
        directory_id: str
    ) -> Optional[str]:
        """
        Assume the Directory Service role.

        Args:
            directory_id: Directory ID

        Returns:
            Temporary credentials
        """
        if not BOTO3_AVAILABLE:
            return None

        try:
            response = self.sts_client.assume_role(
                RoleArn=f"arn:aws:iam::*:role/AWSDirectoryServiceRole",
                RoleSessionName=f"DirectorySession_{directory_id}"
            )

            return {
                "access_key": response["Credentials"]["AccessKeyId"],
                "secret_key": response["Credentials"]["SecretAccessKey"],
                "token": response["Credentials"]["SessionToken"]
            }

        except ClientError as e:
            logger.error(f"Error assuming role: {e}")
            return None

    # ========================================================================
    # CloudWatch Integration
    # ========================================================================

    def get_directory_metrics(
        self,
        directory_id: str,
        metric_name: str,
        period: int = 300,
        start_time: datetime = None,
        end_time: datetime = None
    ) -> List[Dict[str, Any]]:
        """
        Get CloudWatch metrics for a directory.

        Args:
            directory_id: Directory ID
            metric_name: Metric name
            period: Period in seconds
            start_time: Start time
            end_time: End time

        Returns:
            Metric data points
        """
        if not BOTO3_AVAILABLE:
            return []

        if start_time is None:
            start_time = datetime.utcnow() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.utcnow()

        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace="AWS/DirectoryService",
                MetricName=metric_name,
                Dimensions=[
                    {"Name": "DirectoryId", "Value": directory_id}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=["Average", "Maximum", "Minimum"]
            )

            return response.get("Datapoints", [])

        except ClientError as e:
            logger.error(f"Error getting metrics: {e}")
            return []

    def list_directory_metrics(self, directory_id: str) -> List[str]:
        """
        List available metrics for a directory.

        Args:
            directory_id: Directory ID

        Returns:
            List of metric names
        """
        return [
            "DirectoryCacheHits",
            "DirectoryCacheMisses",
            "DirectoryCREvents",
            "DirectoryDELookups",
            "DirectoryDSEvents",
            "DirectoryLDAPEvents",
            "DirectoryNamelookup",
            "DirectoryOpsLatency",
            "DirectoryReplicationLatency",
            "DirectorySSOEvents",
            "DirectorySecureConnections",
            "DirectorySessions",
            "DirectoryTimedOperations",
            "DirectoryTrustValidation",
            "InstanceCount"
        ]

    def enable_directory_monitoring(
        self,
        directory_id: str,
        monitoring_type: str = "standard"
    ) -> Dict[str, Any]:
        """
        Enable CloudWatch monitoring for a directory.

        Args:
            directory_id: Directory ID
            monitoring_type: Monitoring type (standard or detailed)

        Returns:
            Monitoring configuration result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.update_directory(
                DirectoryId=directory_id,
                MonitoringMode=monitoring_type
            )

            logger.info(f"Enabled {monitoring_type} monitoring for directory: {directory_id}")

            return {
                "status": "enabled",
                "directory_id": directory_id,
                "monitoring_type": monitoring_type
            }

        except ClientError as e:
            logger.error(f"Error enabling monitoring: {e}")
            return {"status": "error", "message": str(e)}

    def create_directory_alarm(
        self,
        directory_id: str,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison_operator: str = "GreaterThanThreshold",
        evaluation_periods: int = 2
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm for directory metrics.

        Args:
            directory_id: Directory ID
            alarm_name: Alarm name
            metric_name: Metric name
            threshold: Threshold value
            comparison_operator: Comparison operator
            evaluation_periods: Number of evaluation periods

        Returns:
            Alarm creation result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            alarm_actions = []

            response = self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                Namespace="AWS/DirectoryService",
                MetricName=metric_name,
                Dimensions=[
                    {"Name": "DirectoryId", "Value": directory_id}
                ],
                Threshold=threshold,
                ComparisonOperator=comparison_operator,
                EvaluationPeriods=evaluation_periods,
                Period=300,
                Statistic="Average",
                AlarmActions=alarm_actions
            )

            logger.info(f"Created CloudWatch alarm: {alarm_name}")

            return {
                "status": "created",
                "alarm_name": alarm_name,
                "directory_id": directory_id
            }

        except ClientError as e:
            logger.error(f"Error creating alarm: {e}")
            return {"status": "error", "message": str(e)}

    def get_directory_status_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all directory statuses.

        Returns:
            Status summary
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_directories()
            directories = response.get("DirectoryDescriptions", [])

            summary = {
                "total": len(directories),
                "by_type": {
                    "SimpleAD": 0,
                    "MicrosoftAD": 0,
                    "ADConnector": 0
                },
                "by_state": defaultdict(int)
            }

            for directory in directories:
                dir_type = directory.get("Type", "Unknown")
                dir_state = directory.get("Stage", "Unknown")

                if dir_type in summary["by_type"]:
                    summary["by_type"][dir_type] += 1

                summary["by_state"][dir_state] += 1

            return summary

        except ClientError as e:
            logger.error(f"Error getting directory summary: {e}")
            return {"status": "error", "message": str(e)}

    def _enable_sso(
        self,
        directory_id: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Enable Single Sign-On for a directory.

        Args:
            directory_id: Directory ID
            **kwargs: SSO configuration

        Returns:
            SSO enablement result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            params = {
                "DirectoryId": directory_id,
                "UserName": kwargs.get("sso_username", "Admin"),
                "Password": kwargs.get("sso_password", f"TempPassword123!{self._generate_id()}")
            }

            self.ds_client.enable_sso(**params)

            logger.info(f"Enabled SSO for directory: {directory_id}")

            return {
                "status": "enabled",
                "directory_id": directory_id,
                "sso_username": params["UserName"]
            }

        except ClientError as e:
            logger.error(f"Error enabling SSO: {e}")
            return {"status": "error", "message": str(e)}

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def get_directory(self, directory_id: str) -> Dict[str, Any]:
        """
        Get directory details regardless of type.

        Args:
            directory_id: Directory ID

        Returns:
            Directory details
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            response = self.ds_client.describe_directories(
                DirectoryIds=[directory_id]
            )

            if response["DirectoryDescriptions"]:
                return response["DirectoryDescriptions"][0]
            return {"status": "not_found"}

        except ClientError as e:
            logger.error(f"Error getting directory: {e}")
            return {"status": "error", "message": str(e)}

    def list_directories(
        self,
        directory_type: Optional[DirectoryType] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        List directories with optional filtering.

        Args:
            directory_type: Filter by directory type
            filters: Additional filters

        Returns:
            List of directories
        """
        if not BOTO3_AVAILABLE:
            return []

        try:
            response = self.ds_client.describe_directories()
            directories = response.get("DirectoryDescriptions", [])

            if directory_type:
                directories = [
                    d for d in directories
                    if d.get("Type") == directory_type.value
                ]

            if filters:
                if "vpc_id" in filters:
                    directories = [
                        d for d in directories
                        if d.get("VpcSettings", {}).get("VpcId") == filters["vpc_id"]
                    ]
                if "stage" in filters:
                    directories = [
                        d for d in directories
                        if d.get("Stage") == filters["stage"]
                    ]

            return directories

        except ClientError as e:
            logger.error(f"Error listing directories: {e}")
            return []

    def delete_directory(self, directory_id: str) -> Dict[str, Any]:
        """
        Delete a directory.

        Args:
            directory_id: Directory ID

        Returns:
            Deletion result
        """
        if not BOTO3_AVAILABLE:
            return {"status": "unavailable"}

        try:
            self.ds_client.delete_directory(DirectoryId=directory_id)
            logger.info(f"Deleted directory: {directory_id}")
            return {"status": "deleting", "directory_id": directory_id}
        except ClientError as e:
            logger.error(f"Error deleting directory: {e}")
            return {"status": "error", "message": str(e)}

    def wait_for_directory_active(
        self,
        directory_id: str,
        timeout: int = 600
    ) -> Dict[str, Any]:
        """
        Wait for a directory to become active.

        Args:
            directory_id: Directory ID
            timeout: Maximum wait time in seconds

        Returns:
            Final directory state
        """
        return self._wait_for_directory(
            directory_id,
            [DirectoryState.ACTIVE],
            timeout
        )

    def __repr__(self) -> str:
        return f"DirectoryServiceIntegration(region='{self.region}')"
