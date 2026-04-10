"""
AWS EKS Kubernetes Integration Module for Workflow System

Implements an EKSIntegration class with:
1. Cluster management: Create/manage EKS clusters
2. Node group management: Manage node groups
3. Fargate profiles: Manage Fargate profiles
4. Kubernetes resources: Deploy Kubernetes resources
5. Add-ons: Manage EKS add-ons
6. Cluster access: Manage cluster access
7. VPC configuration: Configure VPC for clusters
8. IAM roles: Configure IAM roles for clusters
9. CloudWatch integration: Logging and monitoring
10. Helm integration: Deploy Helm charts on EKS

Commit: 'feat(aws-eks): add AWS EKS integration with cluster management, node groups, Fargate profiles, Kubernetes resources, add-ons, cluster access, VPC, IAM, CloudWatch, Helm'
"""

import uuid
import json
import time
import logging
import hashlib
import subprocess
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Union, Type, Awaitable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import copy
import threading
import os

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


class ClusterStatus(Enum):
    """EKS cluster status values."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    FAILED = "FAILED"
    UPDATING = "UPDATING"


class NodeGroupStatus(Enum):
    """EKS node group status values."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    CREATE_FAILED = "CREATE_FAILED"
    DELETE_FAILED = "DELETE_FAILED"


class FargateProfileStatus(Enum):
    """Fargate profile status values."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    CREATE_FAILED = "CREATE_FAILED"
    DELETE_FAILED = "DELETE_FAILED"


class AddonStatus(Enum):
    """EKS add-on status values."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    UPDATING = "UPDATING"
    DELETING = "DELETING"
    CREATE_FAILED = "CREATE_FAILED"
    DELETE_FAILED = "DELETE_FAILED"
    UPDATE_FAILED = "UPDATE_FAILED"


class IpFamily(Enum):
    """IP family for cluster networking."""
    IPV4 = "ipv4"
    IPV6 = "ipv6"


class CapacityType(Enum):
    """Node group capacity types."""
    ON_DEMAND = "OnDemand"
    SPOT = "Spot"


class RemoteAccessPolicy(Enum):
    """Remote access security group policies."""
    Ec2SecurityGroup = "Ec2SecurityGroup"


@dataclass
class VpcConfig:
    """VPC configuration for EKS clusters."""
    subnet_ids: List[str]
    security_group_ids: List[str] = field(default_factory=list)
    cluster_security_group_id: str = ""
    vpc_id: str = ""
    endpoint_public_access: bool = True
    endpoint_private_access: bool = False
    public_access_cidrs: List[str] = field(default_factory=list)


@dataclass
class KubernetesResource:
    """Kubernetes resource specification."""
    kind: str
    api_version: str
    metadata: Dict[str, Any]
    spec: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NodeGroupConfig:
    """Node group configuration."""
    name: str
    instance_types: List[str]
    min_size: int = 1
    max_size: int = 3
    desired_size: int = 2
    capacity_type: CapacityType = CapacityType.ON_DEMAND
    disk_size: int = 20
    subnet_ids: List[str] = field(default_factory=list)
    remote_access: Dict[str, Any] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    taints: List[Dict[str, str]] = field(default_factory=list)
    scaling_config: Dict[str, int] = field(default_factory=dict)


@dataclass
class FargateProfileConfig:
    """Fargate profile configuration."""
    profile_name: str
    pod_execution_role_arn: str
    subnet_ids: List[str] = field(default_factory=list)
    selectors: List[Dict[str, Any]] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AddonConfig:
    """EKS add-on configuration."""
    addon_name: str
    addon_version: str = ""
    service_account_role_arn: str = ""
    preserve: bool = True
    configuration_values: str = ""


@dataclass
class ClusterInfo:
    """EKS cluster information."""
    arn: str
    name: str
    status: ClusterStatus = ClusterStatus.ACTIVE
    version: str = "1.28"
    platform_version: str = ""
    vpc_config: Dict[str, Any] = field(default_factory=dict)
    role_arn: str = ""
    created_at: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)
    logging: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NodeGroupInfo:
    """EKS node group information."""
    nodegroup_name: str
    cluster_name: str
    status: NodeGroupStatus = NodeGroupStatus.ACTIVE
    instance_types: List[str] = field(default_factory=list)
    node_info: Dict[str, Any] = field(default_factory=dict)
    scaling_config: Dict[str, int] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class HelmChart:
    """Helm chart configuration."""
    chart_name: str
    release_name: str
    namespace: str = "default"
    values: Dict[str, Any] = field(default_factory=dict)
    version: str = ""
    timeout: int = 300


class EKSIntegration:
    """
    AWS EKS Kubernetes Orchestration Integration.
    
    Provides comprehensive EKS cluster management with support for:
    - Cluster lifecycle management
    - Node group management (managed and self-managed nodes)
    - Fargate profiles for serverless containers
    - Kubernetes resource deployment
    - EKS add-ons management
    - Cluster access management
    - VPC configuration
    - IAM role configuration
    - CloudWatch logging and monitoring
    - Helm chart deployment
    """
    
    def __init__(
        self,
        region: str = "us-east-1",
        profile: Optional[str] = None,
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ):
        """
        Initialize EKS integration.
        
        Args:
            region: AWS region for EKS operations
            profile: AWS profile name for credentials
            cluster_name: Default cluster name for operations
            kubeconfig_path: Path to kubeconfig file
        """
        self.region = region
        self.profile = profile
        self.cluster_name = cluster_name or "default"
        self.kubeconfig_path = kubeconfig_path or os.path.expanduser("~/.kube/config")
        
        self.eks_client = None
        self.ec2_client = None
        self.iam_client = None
        self.cloudwatch_client = None
        self.cloudformation_client = None
        self._kubeconfig_context = None
        
        if BOTO3_AVAILABLE:
            self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AWS clients."""
        try:
            session_kwargs = {"region_name": self.region}
            if self.profile:
                session_kwargs["profile_name"] = self.profile
            
            session = boto3.Session(**session_kwargs)
            
            self.eks_client = session.client("eks")
            self.ec2_client = session.client("ec2")
            self.iam_client = session.client("iam")
            self.cloudwatch_client = session.client("cloudwatch")
            
            try:
                self.cloudformation_client = session.client("cloudformation")
            except Exception:
                logger.warning("CloudFormation client not available")
                
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise
    
    def ensure_aws_credentials(func):
        """Decorator to ensure AWS credentials are available."""
        def wrapper(self, *args, **kwargs):
            if not BOTO3_AVAILABLE:
                raise ImportError("boto3 is required for AWS EKS operations")
            if not self.eks_client:
                self._initialize_clients()
            return func(self, *args, **kwargs)
        return wrapper

    # =========================================================================
    # IAM Role Management
    # =========================================================================
    
    @ensure_aws_credentials
    def create_iam_role_for_eks_cluster(self, role_name: str) -> str:
        """
        Create IAM role for EKS cluster.
        
        Args:
            role_name: Name of the IAM role
            
        Returns:
            IAM role ARN
        """
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "eks.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        try:
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=f"IAM role for EKS cluster {role_name}"
            )
            role_arn = response["Role"]["Arn"]
            
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
            )
            
            logger.info(f"Created IAM role: {role_name}")
            return role_arn
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                response = self.iam_client.get_role(RoleName=role_name)
                return response["Role"]["Arn"]
            raise
    
    @ensure_aws_credentials
    def create_iam_role_for_node_group(self, role_name: str) -> str:
        """
        Create IAM role for EKS node group.
        
        Args:
            role_name: Name of the IAM role
            
        Returns:
            IAM role ARN
        """
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        try:
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=f"IAM role for EKS node group {role_name}"
            )
            role_arn = response["Role"]["Arn"]
            
            policies = [
                "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
                "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
                "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
            ]
            
            for policy in policies:
                self.iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy
                )
            
            logger.info(f"Created IAM role for node group: {role_name}")
            return role_arn
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                response = self.iam_client.get_role(RoleName=role_name)
                return response["Role"]["Arn"]
            raise

    # =========================================================================
    # VPC Configuration
    # =========================================================================
    
    @ensure_aws_credentials
    def create_vpc_for_eks(self, vpc_name: str, cidr_block: str = "10.0.0.0/16") -> Dict[str, Any]:
        """
        Create VPC for EKS cluster.
        
        Args:
            vpc_name: Name of the VPC
            cidr_block: CIDR block for the VPC
            
        Returns:
            VPC configuration dictionary
        """
        try:
            response = self.ec2_client.create_vpc(
                CidrBlock=cidr_block,
                TagSpecifications=[
                    {"ResourceType": "vpc", "Tags": [{"Key": "Name", "Value": vpc_name}]}
                ]
            )
            vpc_id = response["Vpc"]["VpcId"]
            
            self.ec2_client.modify_vpc_attribute(
                VpcId=vpc_id,
                EnableDnsHostnames={"Value": True}
            )
            self.ec2_client.modify_vpc_attribute(
                VpcId=vpc_id,
                EnableDnsSupport={"Value": True}
            )
            
            igw_response = self.ec2_client.create_internet_gateway(
                TagSpecifications=[
                    {"ResourceType": "internet-gateway", "Tags": [{"Key": "Name", "Value": f"{vpc_name}-igw"}]}
                ]
            )
            igw_id = igw_response["InternetGateway"]["InternetGatewayId"]
            
            self.ec2_client.attach_internet_gateway(
                VpcId=vpc_id,
                InternetGatewayId=igw_id
            )
            
            route_table_response = self.ec2_client.describe_route_tables(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            )
            route_table_id = route_table_response["RouteTables"][0]["RouteTableId"]
            
            self.ec2_client.create_route(
                RouteTableId=route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                GatewayId=igw_id
            )
            
            subnet_configs = [
                {"name": f"{vpc_name}-public-1", "cidr": "10.0.1.0/24", "az": f"{self.region}a", "public": True},
                {"name": f"{vpc_name}-public-2", "cidr": "10.0.2.0/24", "az": f"{self.region}b", "public": True},
                {"name": f"{vpc_name}-private-1", "cidr": "10.0.3.0/24", "az": f"{self.region}a", "public": False},
                {"name": f"{vpc_name}-private-2", "cidr": "10.0.4.0/24", "az": f"{self.region}b", "public": False},
            ]
            
            subnet_ids = []
            for config in subnet_configs:
                subnet = self.ec2_client.create_subnet(
                    VpcId=vpc_id,
                    CidrBlock=config["cidr"],
                    AvailabilityZone=config["az"],
                    TagSpecifications=[
                        {"ResourceType": "subnet", "Tags": [{"Key": "Name", "Value": config["name"]}]}
                    ]
                )
                subnet_id = subnet["Subnet"]["SubnetId"]
                subnet_ids.append(subnet_id)
                
                if config["public"]:
                    self.ec2_client.modify_subnet_attribute(
                        SubnetId=subnet_id,
                        MapPublicIpOnLaunch={"Value": True}
                    )
            
            security_group = self.ec2_client.create_security_group(
                GroupName=f"{vpc_name}-eks-sg",
                Description=f"Security group for EKS cluster {vpc_name}",
                VpcId=vpc_id,
                TagSpecifications=[
                    {"ResourceType": "security-group", "Tags": [{"Key": "Name", "Value": f"{vpc_name}-eks-sg"}]}
                ]
            )
            sg_id = security_group["GroupId"]
            
            self.ec2_client.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
                ]
            )
            
            logger.info(f"Created VPC: {vpc_id} with {len(subnet_ids)} subnets")
            
            return {
                "vpc_id": vpc_id,
                "subnet_ids": subnet_ids,
                "security_group_id": sg_id,
                "internet_gateway_id": igw_id,
                "route_table_id": route_table_id
            }
            
        except ClientError as e:
            logger.error(f"Failed to create VPC: {e}")
            raise
    
    @ensure_aws_credentials
    def describe_vpc(self, vpc_id: str) -> Dict[str, Any]:
        """
        Describe VPC details.
        
        Args:
            vpc_id: VPC ID
            
        Returns:
            VPC information
        """
        try:
            response = self.ec2_client.describe_vpcs(VpcIds=[vpc_id])
            return response["Vpcs"][0] if response["Vpcs"] else {}
        except ClientError as e:
            logger.error(f"Failed to describe VPC: {e}")
            raise

    # =========================================================================
    # Cluster Management
    # =========================================================================
    
    @ensure_aws_credentials
    def create_cluster(
        self,
        cluster_name: str,
        role_arn: str,
        vpc_config: Dict[str, Any],
        version: str = "1.28",
        encryption_config: Optional[List[Dict[str, Any]]] = None,
        logging_config: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> ClusterInfo:
        """
        Create EKS cluster.
        
        Args:
            cluster_name: Name of the cluster
            role_arn: IAM role ARN for cluster
            vpc_config: VPC configuration
            version: Kubernetes version
            encryption_config: Encryption configuration
            logging_config: Cluster logging configuration
            tags: Resource tags
            
        Returns:
            ClusterInfo object
        """
        try:
            params = {
                "name": cluster_name,
                "roleArn": role_arn,
                "version": version,
                "resourcesVpcConfig": {
                    "subnetIds": vpc_config["subnet_ids"],
                    "securityGroupIds": vpc_config.get("security_group_ids", []),
                    "endpointPublicAccess": vpc_config.get("endpoint_public_access", True),
                    "endpointPrivateAccess": vpc_config.get("endpoint_private_access", False)
                }
            }
            
            if encryption_config:
                params["encryptionConfig"] = encryption_config
            
            if logging_config:
                params["logging"] = logging_config
            elif logging_config is None:
                params["logging"] = {
                    "clusterLogging": [
                        {
                            "types": ["api", "audit", "authenticator", "controllerManager", "scheduler"],
                            "enabled": True
                        }
                    ]
                }
            
            if tags:
                params["tags"] = tags
            
            response = self.eks_client.create_cluster(**params)
            cluster = response["cluster"]
            
            logger.info(f"Creating EKS cluster: {cluster_name}")
            
            return ClusterInfo(
                arn=cluster["arn"],
                name=cluster["name"],
                status=ClusterStatus.CREATING,
                version=cluster["version"],
                platform_version=cluster.get("platformVersion", ""),
                vpc_config=cluster.get("resourcesVpcConfig", {}),
                role_arn=cluster["roleArn"],
                created_at=cluster.get("createdAt"),
                tags=cluster.get("tags", {}),
                logging=cluster.get("logging", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to create cluster: {e}")
            raise
    
    @ensure_aws_credentials
    def get_cluster(self, cluster_name: Optional[str] = None) -> ClusterInfo:
        """
        Get EKS cluster information.
        
        Args:
            cluster_name: Name of the cluster (uses default if not specified)
            
        Returns:
            ClusterInfo object
        """
        name = cluster_name or self.cluster_name
        
        try:
            response = self.eks_client.describe_cluster(name=name)
            cluster = response["cluster"]
            
            status_map = {
                "CREATING": ClusterStatus.CREATING,
                "ACTIVE": ClusterStatus.ACTIVE,
                "DELETING": ClusterStatus.DELETING,
                "FAILED": ClusterStatus.FAILED,
                "UPDATING": ClusterStatus.UPDATING
            }
            
            return ClusterInfo(
                arn=cluster["arn"],
                name=cluster["name"],
                status=status_map.get(cluster["status"], ClusterStatus.ACTIVE),
                version=cluster["version"],
                platform_version=cluster.get("platformVersion", ""),
                vpc_config=cluster.get("resourcesVpcConfig", {}),
                role_arn=cluster["roleArn"],
                created_at=cluster.get("createdAt"),
                tags=cluster.get("tags", {}),
                logging=cluster.get("logging", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get cluster: {e}")
            raise
    
    @ensure_aws_credentials
    def list_clusters(self) -> List[str]:
        """
        List all EKS clusters.
        
        Returns:
            List of cluster names
        """
        try:
            response = self.eks_client.list_clusters()
            return response.get("clusters", [])
        except ClientError as e:
            logger.error(f"Failed to list clusters: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_cluster(self, cluster_name: Optional[str] = None) -> bool:
        """
        Delete EKS cluster.
        
        Args:
            cluster_name: Name of the cluster (uses default if not specified)
            
        Returns:
            True if deletion initiated successfully
        """
        name = cluster_name or self.cluster_name
        
        try:
            self.eks_client.delete_cluster(name=name)
            logger.info(f"Deleting EKS cluster: {name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete cluster: {e}")
            raise
    
    @ensure_aws_credentials
    def update_cluster_version(
        self,
        cluster_name: str,
        version: str
    ) -> Dict[str, Any]:
        """
        Update cluster Kubernetes version.
        
        Args:
            cluster_name: Name of the cluster
            version: Target Kubernetes version
            
        Returns:
            Update result
        """
        try:
            response = self.eks_client.update_cluster_version(
                name=cluster_name,
                version=version
            )
            logger.info(f"Updating cluster {cluster_name} to version {version}")
            return response["update"]
        except ClientError as e:
            logger.error(f"Failed to update cluster version: {e}")
            raise
    
    @ensure_aws_credentials
    def wait_for_cluster_active(
        self,
        cluster_name: Optional[str] = None,
        timeout: int = 1800
    ) -> bool:
        """
        Wait for cluster to become active.
        
        Args:
            cluster_name: Name of the cluster
            timeout: Maximum wait time in seconds
            
        Returns:
            True if cluster is active
        """
        name = cluster_name or self.cluster_name
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            cluster = self.get_cluster(name)
            if cluster.status == ClusterStatus.ACTIVE:
                return True
            if cluster.status == ClusterStatus.FAILED:
                raise TimeoutError(f"Cluster {name} failed")
            
            logger.info(f"Waiting for cluster {name} to be active...")
            time.sleep(30)
        
        raise TimeoutError(f"Timeout waiting for cluster {name} to be active")
    
    # =========================================================================
    # Node Group Management
    # =========================================================================
    
    @ensure_aws_credentials
    def create_node_group(
        self,
        cluster_name: str,
        nodegroup_name: str,
        node_role_arn: str,
        subnet_ids: List[str],
        instance_types: List[str],
        min_size: int = 1,
        max_size: int = 3,
        desired_size: int = 2,
        capacity_type: str = "OnDemand",
        disk_size: int = 20,
        labels: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        taints: Optional[List[Dict[str, str]]] = None
    ) -> NodeGroupInfo:
        """
        Create EKS node group.
        
        Args:
            cluster_name: Name of the cluster
            nodegroup_name: Name of the node group
            node_role_arn: IAM role ARN for nodes
            subnet_ids: Subnet IDs for nodes
            instance_types: EC2 instance types
            min_size: Minimum number of nodes
            max_size: Maximum number of nodes
            desired_size: Desired number of nodes
            capacity_type: OnDemand or Spot
            disk_size: Root disk size in GB
            labels: Node labels
            tags: Resource tags
            taints: Node taints
            
        Returns:
            NodeGroupInfo object
        """
        try:
            params = {
                "clusterName": cluster_name,
                "nodegroupName": nodegroup_name,
                "nodeRole": node_role_arn,
                "subnets": subnet_ids,
                "instanceTypes": instance_types,
                "scalingConfig": {
                    "minSize": min_size,
                    "maxSize": max_size,
                    "desiredSize": desired_size
                },
                "diskSize": disk_size,
                "capacityType": capacity_type
            }
            
            if labels:
                params["labels"] = labels
            
            if taints:
                params["taints"] = taints
            
            if tags:
                params["tags"] = tags
            
            response = self.eks_client.create_nodegroup(**params)
            nodegroup = response["nodegroup"]
            
            logger.info(f"Creating node group: {nodegroup_name} in cluster {cluster_name}")
            
            return NodeGroupInfo(
                nodegroup_name=nodegroup["nodegroupName"],
                cluster_name=nodegroup["clusterName"],
                status=NodeGroupStatus.CREATING,
                instance_types=nodegroup.get("instanceTypes", []),
                node_info=nodegroup,
                scaling_config=nodegroup.get("scalingConfig", {}),
                labels=nodegroup.get("labels", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to create node group: {e}")
            raise
    
    @ensure_aws_credentials
    def get_node_group(
        self,
        cluster_name: str,
        nodegroup_name: str
    ) -> NodeGroupInfo:
        """
        Get node group information.
        
        Args:
            cluster_name: Name of the cluster
            nodegroup_name: Name of the node group
            
        Returns:
            NodeGroupInfo object
        """
        try:
            response = self.eks_client.describe_nodegroup(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name
            )
            nodegroup = response["nodegroup"]
            
            status_map = {
                "CREATING": NodeGroupStatus.CREATING,
                "ACTIVE": NodeGroupStatus.ACTIVE,
                "UPDATING": NodeGroupStatus.UPDATING,
                "DELETING": NodeGroupStatus.DELETING,
                "CREATE_FAILED": NodeGroupStatus.CREATE_FAILED,
                "DELETE_FAILED": NodeGroupStatus.DELETE_FAILED
            }
            
            return NodeGroupInfo(
                nodegroup_name=nodegroup["nodegroupName"],
                cluster_name=nodegroup["clusterName"],
                status=status_map.get(nodegroup["status"], NodeGroupStatus.ACTIVE),
                instance_types=nodegroup.get("instanceTypes", []),
                node_info=nodegroup,
                scaling_config=nodegroup.get("scalingConfig", {}),
                labels=nodegroup.get("labels", {})
            )
            
        except ClientError as e:
            logger.error(f"Failed to get node group: {e}")
            raise
    
    @ensure_aws_credentials
    def list_node_groups(self, cluster_name: Optional[str] = None) -> List[str]:
        """
        List node groups in cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of node group names
        """
        name = cluster_name or self.cluster_name
        
        try:
            response = self.eks_client.list_nodegroups(clusterName=name)
            return response.get("nodegroups", [])
        except ClientError as e:
            logger.error(f"Failed to list node groups: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_node_group(
        self,
        cluster_name: str,
        nodegroup_name: str
    ) -> bool:
        """
        Delete node group.
        
        Args:
            cluster_name: Name of the cluster
            nodegroup_name: Name of the node group
            
        Returns:
            True if deletion initiated
        """
        try:
            self.eks_client.delete_nodegroup(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name
            )
            logger.info(f"Deleting node group: {nodegroup_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete node group: {e}")
            raise
    
    @ensure_aws_credentials
    def update_node_group_scaling(
        self,
        cluster_name: str,
        nodegroup_name: str,
        min_size: int,
        max_size: int,
        desired_size: int
    ) -> Dict[str, Any]:
        """
        Update node group scaling configuration.
        
        Args:
            cluster_name: Name of the cluster
            nodegroup_name: Name of the node group
            min_size: Minimum number of nodes
            max_size: Maximum number of nodes
            desired_size: Desired number of nodes
            
        Returns:
            Update result
        """
        try:
            response = self.eks_client.update_nodegroup_scaling(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name,
                scalingConfig={
                    "minSize": min_size,
                    "maxSize": max_size,
                    "desiredSize": desired_size
                }
            )
            logger.info(f"Updating node group {nodegroup_name} scaling")
            return response["nodegroup"]
        except ClientError as e:
            logger.error(f"Failed to update node group scaling: {e}")
            raise
    
    @ensure_aws_credentials
    def wait_for_node_group_active(
        self,
        cluster_name: str,
        nodegroup_name: str,
        timeout: int = 1800
    ) -> bool:
        """
        Wait for node group to become active.
        
        Args:
            cluster_name: Name of the cluster
            nodegroup_name: Name of the node group
            timeout: Maximum wait time in seconds
            
        Returns:
            True if node group is active
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            nodegroup = self.get_node_group(cluster_name, nodegroup_name)
            if nodegroup.status == NodeGroupStatus.ACTIVE:
                return True
            if nodegroup.status in [NodeGroupStatus.CREATE_FAILED, NodeGroupStatus.DELETE_FAILED]:
                raise TimeoutError(f"Node group {nodegroup_name} failed")
            
            logger.info(f"Waiting for node group {nodegroup_name} to be active...")
            time.sleep(30)
        
        raise TimeoutError(f"Timeout waiting for node group {nodegroup_name}")

    # =========================================================================
    # Fargate Profiles
    # =========================================================================
    
    @ensure_aws_credentials
    def create_fargate_profile(
        self,
        cluster_name: str,
        profile_name: str,
        pod_execution_role_arn: str,
        subnet_ids: List[str],
        selectors: List[Dict[str, Any]],
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create Fargate profile.
        
        Args:
            cluster_name: Name of the cluster
            profile_name: Name of the Fargate profile
            pod_execution_role_arn: IAM role ARN for Fargate pods
            subnet_ids: Subnet IDs for Fargate pods
            selectors: Pod selectors for Fargate
            tags: Resource tags
            
        Returns:
            Fargate profile information
        """
        try:
            params = {
                "clusterName": cluster_name,
                "fargateProfileName": profile_name,
                "podExecutionRoleArn": pod_execution_role_arn,
                "subnets": subnet_ids,
                "selectors": selectors
            }
            
            if tags:
                params["tags"] = tags
            
            response = self.eks_client.create_fargate_profile(**params)
            fargate_profile = response["fargateProfile"]
            
            logger.info(f"Creating Fargate profile: {profile_name}")
            
            return fargate_profile
            
        except ClientError as e:
            logger.error(f"Failed to create Fargate profile: {e}")
            raise
    
    @ensure_aws_credentials
    def get_fargate_profile(
        self,
        cluster_name: str,
        profile_name: str
    ) -> Dict[str, Any]:
        """
        Get Fargate profile information.
        
        Args:
            cluster_name: Name of the cluster
            profile_name: Name of the Fargate profile
            
        Returns:
            Fargate profile information
        """
        try:
            response = self.eks_client.describe_fargate_profile(
                clusterName=cluster_name,
                fargateProfileName=profile_name
            )
            return response["fargateProfile"]
        except ClientError as e:
            logger.error(f"Failed to get Fargate profile: {e}")
            raise
    
    @ensure_aws_credentials
    def list_fargate_profiles(self, cluster_name: Optional[str] = None) -> List[str]:
        """
        List Fargate profiles in cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of Fargate profile names
        """
        name = cluster_name or self.cluster_name
        
        try:
            response = self.eks_client.list_fargate_profiles(clusterName=name)
            return response.get("fargateProfileNames", [])
        except ClientError as e:
            logger.error(f"Failed to list Fargate profiles: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_fargate_profile(
        self,
        cluster_name: str,
        profile_name: str
    ) -> bool:
        """
        Delete Fargate profile.
        
        Args:
            cluster_name: Name of the cluster
            profile_name: Name of the Fargate profile
            
        Returns:
            True if deletion initiated
        """
        try:
            self.eks_client.delete_fargate_profile(
                clusterName=cluster_name,
                fargateProfileName=profile_name
            )
            logger.info(f"Deleting Fargate profile: {profile_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete Fargate profile: {e}")
            raise

    # =========================================================================
    # EKS Add-ons
    # =========================================================================
    
    @ensure_aws_credentials
    def create_addon(
        self,
        cluster_name: str,
        addon_name: str,
        addon_version: str = "",
        service_account_role_arn: str = "",
        preserve: bool = True,
        configuration_values: str = ""
    ) -> Dict[str, Any]:
        """
        Create EKS add-on.
        
        Args:
            cluster_name: Name of the cluster
            addon_name: Name of the add-on
            addon_version: Version of the add-on
            service_account_role_arn: IAM role for add-on
            preserve: Preserve add-on on deletion
            configuration_values: Configuration values
            
        Returns:
            Add-on information
        """
        try:
            params = {
                "clusterName": cluster_name,
                "addonName": addon_name,
                "preserveOnDelete": preserve
            }
            
            if addon_version:
                params["addonVersion"] = addon_version
            
            if service_account_role_arn:
                params["serviceAccountRoleArn"] = service_account_role_arn
            
            if configuration_values:
                params["configurationValues"] = configuration_values
            
            response = self.eks_client.createAddon(**params)
            addon = response["addon"]
            
            logger.info(f"Creating add-on: {addon_name}")
            
            return addon
            
        except ClientError as e:
            logger.error(f"Failed to create add-on: {e}")
            raise
    
    @ensure_aws_credentials
    def get_addon(
        self,
        cluster_name: str,
        addon_name: str
    ) -> Dict[str, Any]:
        """
        Get add-on information.
        
        Args:
            cluster_name: Name of the cluster
            addon_name: Name of the add-on
            
        Returns:
            Add-on information
        """
        try:
            response = self.eks_client.describeAddon(
                clusterName=cluster_name,
                addonName=addon_name
            )
            return response["addon"]
        except ClientError as e:
            logger.error(f"Failed to get add-on: {e}")
            raise
    
    @ensure_aws_credentials
    def list_addons(self, cluster_name: Optional[str] = None) -> List[str]:
        """
        List add-ons in cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of add-on names
        """
        name = cluster_name or self.cluster_name
        
        try:
            response = self.eks_client.listAddons(clusterName=name)
            return response.get("addons", [])
        except ClientError as e:
            logger.error(f"Failed to list add-ons: {e}")
            raise
    
    @ensure_aws_credentials
    def update_addon(
        self,
        cluster_name: str,
        addon_name: str,
        addon_version: str = "",
        service_account_role_arn: str = "",
        preserve: bool = True,
        configuration_values: str = ""
    ) -> Dict[str, Any]:
        """
        Update EKS add-on.
        
        Args:
            cluster_name: Name of the cluster
            addon_name: Name of the add-on
            addon_version: Version of the add-on
            service_account_role_arn: IAM role for add-on
            preserve: Preserve add-on on deletion
            configuration_values: Configuration values
            
        Returns:
            Update result
        """
        try:
            params = {
                "clusterName": cluster_name,
                "addonName": addon_name,
                "preserveOnDelete": preserve
            }
            
            if addon_version:
                params["addonVersion"] = addon_version
            
            if service_account_role_arn:
                params["serviceAccountRoleArn"] = service_account_role_arn
            
            if configuration_values:
                params["configurationValues"] = configuration_values
            
            response = self.eks_client.updateAddon(**params)
            addon = response["addon"]
            
            logger.info(f"Updating add-on: {addon_name}")
            
            return addon
            
        except ClientError as e:
            logger.error(f"Failed to update add-on: {e}")
            raise
    
    @ensure_aws_credentials
    def delete_addon(
        self,
        cluster_name: str,
        addon_name: str
    ) -> bool:
        """
        Delete EKS add-on.
        
        Args:
            cluster_name: Name of the cluster
            addon_name: Name of the add-on
            
        Returns:
            True if deletion initiated
        """
        try:
            self.eks_client.deleteAddon(
                clusterName=cluster_name,
                addonName=addon_name
            )
            logger.info(f"Deleting add-on: {addon_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete add-on: {e}")
            raise

    # =========================================================================
    # Cluster Access
    # =========================================================================
    
    @ensure_aws_credentials
    def update_cluster_config(
        self,
        cluster_name: str,
        endpoint_public_access: bool = True,
        endpoint_private_access: bool = False,
        public_access_cidrs: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Update cluster endpoint access configuration.
        
        Args:
            cluster_name: Name of the cluster
            endpoint_public_access: Enable public endpoint access
            endpoint_private_access: Enable private endpoint access
            public_access_cidrs: Allowed CIDR blocks for public access
            
        Returns:
            Updated VPC config
        """
        try:
            params = {
                "name": cluster_name,
                "resourcesVpcConfig": {
                    "endpointPublicAccess": endpoint_public_access,
                    "endpointPrivateAccess": endpoint_private_access
                }
            }
            
            if public_access_cidrs:
                params["resourcesVpcConfig"]["publicAccessCidrs"] = public_access_cidrs
            
            response = self.eks_client.update_cluster_config(**params)
            logger.info(f"Updating cluster config: {cluster_name}")
            return response["cluster"]["resourcesVpcConfig"]
            
        except ClientError as e:
            logger.error(f"Failed to update cluster config: {e}")
            raise
    
    @ensure_aws_credentials
    def associate_cluster_access(
        self,
        cluster_name: str,
        access_entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Associate access entries with cluster.
        
        Args:
            cluster_name: Name of the cluster
            access_entries: List of access entries to associate
            
        Returns:
            List of associated access entries
        """
        results = []
        for entry in access_entries:
            try:
                if "principalArn" in entry:
                    response = self.eks_client.associate_access_entry(
                        clusterName=cluster_name,
                        principalArn=entry["principalArn"],
                        kubernetes_groups=entry.get("kubernetesGroups", []),
                        access_scope=entry.get("accessScope", {})
                    )
                    results.append(response["accessEntry"])
                elif "username" in entry:
                    self.eks_client.associate_identity_provider_config(
                        clusterName=cluster_name,
                        identityProviderConfig=entry
                    )
                    results.append(entry)
            except ClientError as e:
                logger.error(f"Failed to associate access entry: {e}")
                raise
        
        logger.info(f"Associated {len(results)} access entries with cluster {cluster_name}")
        return results
    
    @ensure_aws_credentials
    def describe_cluster_access(self, cluster_name: str) -> List[Dict[str, Any]]:
        """
        Describe cluster access entries.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of access entries
        """
        try:
            response = self.eks_client.list_access_entries(clusterName=cluster_name)
            access_entries = []
            
            for principal_arn in response.get("accessEntries", []):
                entry = self.eks_client.describe_access_entry(
                    clusterName=cluster_name,
                    principalArn=principal_arn
                )
                access_entries.append(entry["accessEntry"])
            
            return access_entries
            
        except ClientError as e:
            logger.error(f"Failed to describe cluster access: {e}")
            raise
    
    @ensure_aws_credentials
    def revoke_cluster_access(
        self,
        cluster_name: str,
        principal_arn: str
    ) -> bool:
        """
        Revoke cluster access for a principal.
        
        Args:
            cluster_name: Name of the cluster
            principal_arn: Principal ARN to revoke
            
        Returns:
            True if access revoked
        """
        try:
            self.eks_client.revoke_access_entry(
                clusterName=cluster_name,
                principalArn=principal_arn
            )
            logger.info(f"Revoked access for {principal_arn}")
            return True
        except ClientError as e:
            logger.error(f"Failed to revoke access: {e}")
            raise

    # =========================================================================
    # Kubernetes Resources
    # =========================================================================
    
    def _get_kubeconfig_for_cluster(self, cluster_name: str) -> str:
        """
        Get kubeconfig content for cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            kubeconfig content as string
        """
        try:
            response = self.eks_client.describe_cluster(name=cluster_name)
            cluster = response["cluster"]
            
            endpoint = cluster["endpoint"]
            ca_data = cluster["certificateAuthority"]["data"]
            arn = cluster["arn"]
            
            config = {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {
                        "cluster": {
                            "server": endpoint,
                            "certificate-authority-data": ca_data
                        },
                        "name": cluster_name
                    }
                ],
                "contexts": [
                    {
                        "context": {
                            "cluster": cluster_name,
                            "user": cluster_name
                        },
                        "name": cluster_name
                    }
                ],
                "current-context": cluster_name,
                "users": [
                    {
                        "name": cluster_name,
                        "user": {}
                    }
                ]
            }
            
            return yaml.dump(config)
            
        except ClientError as e:
            logger.error(f"Failed to get kubeconfig: {e}")
            raise
    
    def _write_kubeconfig(self, cluster_name: str, path: Optional[str] = None) -> str:
        """
        Write kubeconfig file for cluster.
        
        Args:
            cluster_name: Name of the cluster
            path: Optional path to write kubeconfig
            
        Returns:
            Path to kubeconfig file
        """
        kubeconfig_content = self._get_kubeconfig_for_cluster(cluster_name)
        config_path = path or self.kubeconfig_path
        
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, "w") as f:
            f.write(kubeconfig_content)
        
        logger.info(f"Wrote kubeconfig to {config_path}")
        return config_path
    
    def apply_kubernetes_manifest(
        self,
        manifest: Union[Dict, List],
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Apply Kubernetes manifest.
        
        Args:
            manifest: Kubernetes manifest (dict or list of dicts)
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            List of applied resource results
        """
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        manifests = manifest if isinstance(manifest, list) else [manifest]
        
        manifest_file = f"/tmp/manifest_{uuid.uuid4().hex[:8]}.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump_all(manifests, f)
        
        try:
            result = subprocess.run(
                ["kubectl", "apply", "-f", manifest_file, "--kubeconfig", config_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            results = []
            for m in manifests:
                results.append({
                    "kind": m.get("kind"),
                    "name": m.get("metadata", {}).get("name"),
                    "namespace": m.get("metadata", {}).get("namespace", "default"),
                    "status": "applied"
                })
            
            logger.info(f"Applied {len(results)} Kubernetes resources")
            return results
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to apply manifest: {e.stderr}")
            raise
        finally:
            if os.path.exists(manifest_file):
                os.remove(manifest_file)
    
    def delete_kubernetes_resource(
        self,
        resource_type: str,
        resource_name: str,
        namespace: str = "default",
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> bool:
        """
        Delete Kubernetes resource.
        
        Args:
            resource_type: Resource type (deployment, service, etc.)
            resource_name: Resource name
            namespace: Namespace
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            True if deleted successfully
        """
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        try:
            subprocess.run(
                ["kubectl", "delete", resource_type, resource_name, "-n", namespace, 
                 "--kubeconfig", config_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"Deleted {resource_type}/{resource_name}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to delete resource: {e.stderr}")
            raise
    
    def get_kubernetes_resources(
        self,
        resource_type: str,
        namespace: str = "default",
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get Kubernetes resources.
        
        Args:
            resource_type: Resource type
            namespace: Namespace
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            List of resources
        """
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        try:
            result = subprocess.run(
                ["kubectl", "get", resource_type, "-n", namespace, "-o", "json",
                 "--kubeconfig", config_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            resources = json.loads(result.stdout)
            return resources.get("items", [])
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get resources: {e.stderr}")
            raise
    
    def scale_deployment(
        self,
        deployment_name: str,
        replicas: int,
        namespace: str = "default",
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> bool:
        """
        Scale Kubernetes deployment.
        
        Args:
            deployment_name: Deployment name
            replicas: Number of replicas
            namespace: Namespace
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            True if scaled successfully
        """
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        try:
            subprocess.run(
                ["kubectl", "scale", "deployment", deployment_name, 
                 f"--replicas={replicas}", "-n", namespace,
                 "--kubeconfig", config_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"Scaled deployment {deployment_name} to {replicas} replicas")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to scale deployment: {e.stderr}")
            raise

    # =========================================================================
    # CloudWatch Integration
    # =========================================================================
    
    @ensure_aws_credentials
    def setup_cluster_logging(
        self,
        cluster_name: str,
        log_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Setup cluster logging.
        
        Args:
            cluster_name: Name of the cluster
            log_types: List of log types to enable
            
        Returns:
            Logging configuration
        """
        if log_types is None:
            log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
        
        try:
            logging_config = {
                "clusterLogging": [
                    {
                        "types": log_types,
                        "enabled": True
                    }
                ]
            }
            
            response = self.eks_client.put_cluster_logging(
                name=cluster_name,
                logging=logging_config
            )
            
            logger.info(f"Setup cluster logging for {cluster_name}")
            return response["logging"]
            
        except ClientError as e:
            logger.error(f"Failed to setup cluster logging: {e}")
            raise
    
    @ensure_aws_credentials
    def get_cluster_logging(self, cluster_name: str) -> Dict[str, Any]:
        """
        Get cluster logging configuration.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            Logging configuration
        """
        try:
            response = self.eks_client.describe_cluster_logging(
                name=cluster_name
            )
            return response["logging"]
        except ClientError as e:
            logger.error(f"Failed to get cluster logging: {e}")
            raise
    
    @ensure_aws_credentials
    def put_metric_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        namespace: str,
        dimensions: List[Dict[str, str]],
        threshold: float,
        period: int = 300,
        evaluation_periods: int = 2
    ) -> Dict[str, Any]:
        """
        Create CloudWatch metric alarm.
        
        Args:
            alarm_name: Alarm name
            metric_name: Metric name
            namespace: CloudWatch namespace
            dimensions: Metric dimensions
            threshold: Alarm threshold
            period: Evaluation period in seconds
            evaluation_periods: Number of evaluation periods
            
        Returns:
            Alarm configuration
        """
        try:
            response = self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                MetricName=metric_name,
                Namespace=namespace,
                Dimensions=dimensions,
                Threshold=threshold,
                Period=period,
                EvaluationPeriods=evaluation_periods,
                ComparisonOperator="GreaterThanThreshold",
                Statistic="Average"
            )
            
            logger.info(f"Created CloudWatch alarm: {alarm_name}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create alarm: {e}")
            raise
    
    @ensure_aws_credentials
    def get_container_insights_metrics(
        self,
        cluster_name: str,
        metric_name: str,
        period: int = 300
    ) -> Dict[str, Any]:
        """
        Get Container Insights metrics.
        
        Args:
            cluster_name: Name of the cluster
            metric_name: Metric name
            period: Metric period
            
        Returns:
            Metric data
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)
            
            response = self.cloudwatch_client.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": "container_metrics",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "ContainerInsights",
                                "MetricName": metric_name,
                                "Dimensions": [
                                    {"Name": "ClusterName", "Value": cluster_name}
                                ]
                            },
                            "Period": period,
                            "Stat": "Average"
                        }
                    }
                ],
                StartTime=start_time,
                EndTime=end_time
            )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to get Container Insights metrics: {e}")
            raise

    # =========================================================================
    # Helm Integration
    # =========================================================================
    
    def _ensure_helm_installed(self) -> bool:
        """
        Check if Helm is installed.
        
        Returns:
            True if Helm is available
        """
        try:
            result = subprocess.run(
                ["helm", "version"],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def install_helm_chart(
        self,
        chart: HelmChart,
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Install Helm chart on EKS cluster.
        
        Args:
            chart: HelmChart configuration
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            Helm install result
        """
        if not self._ensure_helm_installed():
            raise ImportError("Helm is not installed")
        
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        values_file = f"/tmp/values_{uuid.uuid4().hex[:8]}.yaml"
        with open(values_file, "w") as f:
            yaml.dump(chart.values, f)
        
        cmd = [
            "helm", "install", chart.release_name,
            chart.chart_name,
            "-n", chart.namespace,
            "--kubeconfig", config_path,
            "-f", values_file
        ]
        
        if chart.timeout:
            cmd.extend(["--timeout", f"{chart.timeout}s"])
        
        if chart.version:
            cmd.extend(["--version", chart.version])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Helm install failed: {result.stderr}")
            
            logger.info(f"Installed Helm chart {chart.release_name}")
            
            return {
                "name": chart.release_name,
                "namespace": chart.namespace,
                "status": "deployed",
                "output": result.stdout
            }
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install Helm chart: {e.stderr}")
            raise
        finally:
            if os.path.exists(values_file):
                os.remove(values_file)
    
    def upgrade_helm_chart(
        self,
        chart: HelmChart,
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Upgrade Helm chart on EKS cluster.
        
        Args:
            chart: HelmChart configuration
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            Helm upgrade result
        """
        if not self._ensure_helm_installed():
            raise ImportError("Helm is not installed")
        
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        values_file = f"/tmp/values_{uuid.uuid4().hex[:8]}.yaml"
        with open(values_file, "w") as f:
            yaml.dump(chart.values, f)
        
        cmd = [
            "helm", "upgrade", chart.release_name,
            chart.chart_name,
            "-n", chart.namespace,
            "--kubeconfig", config_path,
            "-f", values_file
        ]
        
        if chart.timeout:
            cmd.extend(["--timeout", f"{chart.timeout}s"])
        
        if chart.version:
            cmd.extend(["--version", chart.version])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Helm upgrade failed: {result.stderr}")
            
            logger.info(f"Upgraded Helm chart {chart.release_name}")
            
            return {
                "name": chart.release_name,
                "namespace": chart.namespace,
                "status": "upgraded",
                "output": result.stdout
            }
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to upgrade Helm chart: {e.stderr}")
            raise
        finally:
            if os.path.exists(values_file):
                os.remove(values_file)
    
    def uninstall_helm_release(
        self,
        release_name: str,
        namespace: str = "default",
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> bool:
        """
        Uninstall Helm release.
        
        Args:
            release_name: Release name
            namespace: Namespace
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            True if uninstalled
        """
        if not self._ensure_helm_installed():
            raise ImportError("Helm is not installed")
        
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        try:
            result = subprocess.run(
                ["helm", "uninstall", release_name, "-n", namespace, "--kubeconfig", config_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Helm uninstall failed: {result.stderr}")
            
            logger.info(f"Uninstalled Helm release {release_name}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to uninstall Helm release: {e.stderr}")
            raise
    
    def list_helm_releases(
        self,
        namespace: str = "",
        cluster_name: Optional[str] = None,
        kubeconfig_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List Helm releases.
        
        Args:
            namespace: Namespace (empty for all namespaces)
            cluster_name: Name of the cluster
            kubeconfig_path: Path to kubeconfig file
            
        Returns:
            List of Helm releases
        """
        if not self._ensure_helm_installed():
            raise ImportError("Helm is not installed")
        
        name = cluster_name or self.cluster_name
        config_path = kubeconfig_path or self._write_kubeconfig(name)
        
        cmd = ["helm", "list", "--kubeconfig", config_path, "-o", "json"]
        
        if namespace:
            cmd.extend(["-n", namespace])
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Helm list failed: {result.stderr}")
            
            return json.loads(result.stdout)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to list Helm releases: {e.stderr}")
            raise
    
    def add_helm_repo(
        self,
        repo_name: str,
        repo_url: str
    ) -> bool:
        """
        Add Helm repository.
        
        Args:
            repo_name: Repository name
            repo_url: Repository URL
            
        Returns:
            True if added successfully
        """
        if not self._ensure_helm_installed():
            raise ImportError("Helm is not installed")
        
        try:
            subprocess.run(
                ["helm", "repo", "add", repo_name, repo_url],
                capture_output=True,
                text=True,
                check=True
            )
            
            subprocess.run(
                ["helm", "repo", "update"],
                capture_output=True,
                text=True
            )
            
            logger.info(f"Added Helm repo: {repo_name}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to add Helm repo: {e.stderr}")
            raise
    
    def search_helm_charts(
        self,
        search_term: str,
        repo: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Search Helm charts.
        
        Args:
            search_term: Search term
            repo: Repository to search in
            
        Returns:
            List of matching charts
        """
        if not self._ensure_helm_installed():
            raise ImportError("Helm is not installed")
        
        cmd = ["helm", "search", "repo", search_term, "-o", "json"]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Helm search failed: {result.stderr}")
            
            return json.loads(result.stdout)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to search Helm charts: {e.stderr}")
            raise

    # =========================================================================
    # Convenience Methods
    # =========================================================================
    
    def create_cluster_with_defaults(
        self,
        cluster_name: str,
        vpc_name: Optional[str] = None,
        cidr_block: str = "10.0.0.0/16",
        version: str = "1.28"
    ) -> ClusterInfo:
        """
        Create EKS cluster with default configuration.
        
        Args:
            cluster_name: Name of the cluster
            vpc_name: VPC name (uses cluster name if not specified)
            cidr_block: VPC CIDR block
            version: Kubernetes version
            
        Returns:
            ClusterInfo object
        """
        vpc_name = vpc_name or f"{cluster_name}-vpc"
        role_name = f"{cluster_name}-cluster-role"
        
        role_arn = self.create_iam_role_for_eks_cluster(role_name)
        vpc_config = self.create_vpc_for_eks(vpc_name, cidr_block)
        
        cluster = self.create_cluster(
            cluster_name=cluster_name,
            role_arn=role_arn,
            vpc_config=vpc_config,
            version=version
        )
        
        self.wait_for_cluster_active(cluster_name)
        
        return self.get_cluster(cluster_name)
    
    def create_node_group_with_defaults(
        self,
        cluster_name: str,
        nodegroup_name: str,
        instance_type: str = "t3.medium"
    ) -> NodeGroupInfo:
        """
        Create node group with default configuration.
        
        Args:
            cluster_name: Name of the cluster
            nodegroup_name: Name of the node group
            instance_type: EC2 instance type
            
        Returns:
            NodeGroupInfo object
        """
        role_name = f"{cluster_name}-node-role"
        role_arn = self.create_iam_role_for_node_group(role_name)
        
        cluster = self.get_cluster(cluster_name)
        subnet_ids = cluster.vpc_config.get("subnetIds", [])
        
        nodegroup = self.create_node_group(
            cluster_name=cluster_name,
            nodegroup_name=nodegroup_name,
            node_role_arn=role_arn,
            subnet_ids=subnet_ids,
            instance_types=[instance_type]
        )
        
        self.wait_for_node_group_active(cluster_name, nodegroup_name)
        
        return self.get_node_group(cluster_name, nodegroup_name)
