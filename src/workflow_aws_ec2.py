"""
AWS EC2 Instance Management Integration Module for Workflow System

Implements an EC2Integration class with:
1. Instance management: Create/manage EC2 instances
2. AMI management: Manage AMIs
3. Security groups: Manage security groups
4. Key pairs: Manage key pairs
5. VPC management: VPC and subnet management
6. EBS volumes: EBS volume management
7. Auto scaling: Auto Scaling groups
8. Load balancers: ELB management
9. CloudWatch integration: Monitoring and metrics
10. Instance scheduling: Start/stop scheduling

Commit: 'feat(aws-ec2): add AWS EC2 integration with instance management, AMI, security groups, key pairs, VPC, EBS, auto scaling, load balancers, CloudWatch, scheduling'
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


class InstanceState(Enum):
    """EC2 instance states."""
    PENDING = "pending"
    RUNNING = "running"
    SHUTTING_DOWN = "shutting-down"
    TERMINATED = "terminated"
    STOPPING = "stopping"
    STOPPED = "stopped"


class InstanceType(Enum):
    """Common EC2 instance types."""
    T2_MICRO = "t2.micro"
    T2_SMALL = "t2.small"
    T2_MEDIUM = "t2.medium"
    T2_LARGE = "t2.large"
    T3_MICRO = "t3.micro"
    T3_SMALL = "t3.small"
    T3_MEDIUM = "t3.medium"
    T3_LARGE = "t3.large"
    M5_LARGE = "m5.large"
    M5_XLARGE = "m5.xlarge"
    M5_2XLARGE = "m5.2xlarge"
    C5_LARGE = "c5.large"
    C5_XLARGE = "c5.xlarge"
    R5_LARGE = "r5.large"


class VolumeType(Enum):
    """EBS volume types."""
    GP2 = "gp2"
    GP3 = "gp3"
    IO1 = "io1"
    IO2 = "io2"
    ST1 = "st1"
    SC1 = "sc1"


class LoadBalancerType(Enum):
    """ELB types."""
    APPLICATION = "application"
    NETWORK = "network"
    GATEWAY = "gateway"
    CLASSIC = "classic"


@dataclass
class EC2Config:
    """Configuration for EC2 connection."""
    region_name: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class EC2Instance:
    """Represents an EC2 instance."""
    instance_id: str
    instance_type: str
    state: InstanceState
    image_id: str
    private_ip: Optional[str] = None
    public_ip: Optional[str] = None
    private_dns_name: Optional[str] = None
    public_dns_name: Optional[str] = None
    vpc_id: Optional[str] = None
    subnet_id: Optional[str] = None
    security_groups: List[str] = field(default_factory=list)
    key_name: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    ami_id: Optional[str] = None
    root_device_name: Optional[str] = None
    block_device_mappings: List[Dict] = field(default_factory=list)
    launch_time: Optional[datetime] = None
    region: Optional[str] = None


@dataclass
class AMIInfo:
    """Represents an AMI."""
    image_id: str
    name: str
    description: Optional[str] = None
    state: str = "available"
    owner_id: Optional[str] = None
    owner_alias: Optional[str] = None
    platform: Optional[str] = None
    architecture: str = "x86_64"
    virtualization_type: str = "hvm"
    root_device_type: str = "ebs"
    root_device_name: Optional[str] = None
    size: Optional[int] = None
    snapshot_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    creation_date: Optional[str] = None


@dataclass
class SecurityGroupInfo:
    """Represents a security group."""
    group_id: str
    group_name: str
    description: str
    vpc_id: str
    owner_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    rules: List[Dict] = field(default_factory=list)


@dataclass
class KeyPairInfo:
    """Represents a key pair."""
    key_name: str
    key_fingerprint: Optional[str] = None
    key_material: Optional[str] = None
    key_type: str = "rsa"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class VPCInfo:
    """Represents a VPC."""
    vpc_id: str
    cidr_block: str
    state: str = "available"
    is_default: bool = False
    owner_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SubnetInfo:
    """Represents a subnet."""
    subnet_id: str
    vpc_id: str
    cidr_block: str
    availability_zone: Optional[str] = None
    state: str = "available"
    map_public_ip_on_launch: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class VolumeInfo:
    """Represents an EBS volume."""
    volume_id: str
    size: int
    volume_type: VolumeType
    state: str = "available"
    snapshot_id: Optional[str] = None
    availability_zone: Optional[str] = None
    encrypted: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AutoScalingGroupInfo:
    """Represents an Auto Scaling group."""
    auto_scaling_group_name: str
    min_size: int
    max_size: int
    desired_capacity: int
    vpc_id: str
    availability_zones: List[str] = field(default_factory=list)
    load_balancers: List[str] = field(default_factory=list)
    target_group_arns: List[str] = field(default_factory=list)
    health_check_type: str = "EC2"
    health_check_period: int = 300
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class LoadBalancerInfo:
    """Represents a load balancer."""
    load_balancer_name: str
    load_balancer_arn: str
    type: LoadBalancerType
    dns_name: str
    vpc_id: str
    state: str = "active"
    scheme: str = "internet-facing"
    availability_zones: List[str] = field(default_factory=list)
    subnets: List[str] = field(default_factory=list)
    security_groups: List[str] = field(default_factory=list)


@dataclass
class CloudWatchMetric:
    """CloudWatch metric data."""
    namespace: str
    metric_name: str
    value: float
    unit: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    dimensions: Dict[str, str] = field(default_factory=dict)


@dataclass
class ScheduleRule:
    """Instance scheduling rule."""
    rule_name: str
    instance_ids: List[str]
    action: str  # "start" or "stop"
    schedule: str  # cron expression
    enabled: bool = True
    target_id: Optional[str] = None
    rule_arn: Optional[str] = None


class EC2Integration:
    """
    AWS EC2 Integration class providing comprehensive EC2 management.

    Features:
    - Instance management (create, start, stop, terminate, describe)
    - AMI management (create, register, deregister, copy)
    - Security groups (create, configure, manage rules)
    - Key pairs (create, import, delete)
    - VPC management (create VPCs, subnets, manage routing)
    - EBS volumes (create, attach, detach, snapshot)
    - Auto Scaling groups (create, update, delete)
    - Load balancers (Application, Network, Classic, Gateway)
    - CloudWatch integration (metrics, alarms, logs)
    - Instance scheduling (start/stop based on cron)
    """

    def __init__(self, config: Optional[EC2Config] = None):
        """
        Initialize EC2 integration.

        Args:
            config: EC2 configuration. Uses default if not provided.
        """
        self.config = config or EC2Config()
        self._client = None
        self._ec2_resource = None
        self._autoscaling_client = None
        self._elbv2_client = None
        self._elb_client = None
        self._cloudwatch_client = None
        self._events_client = None
        self._iam_client = None
        self._lock = threading.RLock()
        self._scheduled_rules: Dict[str, ScheduleRule] = {}

    @property
    def client(self):
        """Get or create EC2 client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    kwargs = {
                        "region_name": self.config.region_name,
                    }
                    if self.config.endpoint_url:
                        kwargs["endpoint_url"] = self.config.endpoint_url
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name

                    self._client = boto3.client("ec2", **kwargs)
        return self._client

    @property
    def resource(self):
        """Get or create EC2 resource."""
        if self._ec2_resource is None:
            with self._lock:
                if self._ec2_resource is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    if self.config.profile_name:
                        kwargs["profile_name"] = self.config.profile_name

                    self._ec2_resource = boto3.resource("ec2", **kwargs)
        return self._ec2_resource

    @property
    def autoscaling_client(self):
        """Get or create Auto Scaling client."""
        if self._autoscaling_client is None:
            with self._lock:
                if self._autoscaling_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    self._autoscaling_client = boto3.client("autoscaling", **kwargs)
        return self._autoscaling_client

    @property
    def elbv2_client(self):
        """Get or create ELBv2 client."""
        if self._elbv2_client is None:
            with self._lock:
                if self._elbv2_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    self._elbv2_client = boto3.client("elbv2", **kwargs)
        return self._elbv2_client

    @property
    def elb_client(self):
        """Get or create ELB client (Classic)."""
        if self._elb_client is None:
            with self._lock:
                if self._elb_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    self._elb_client = boto3.client("elb", **kwargs)
        return self._elb_client

    @property
    def cloudwatch_client(self):
        """Get or create CloudWatch client."""
        if self._cloudwatch_client is None:
            with self._lock:
                if self._cloudwatch_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    self._cloudwatch_client = boto3.client("cloudwatch", **kwargs)
        return self._cloudwatch_client

    @property
    def events_client(self):
        """Get or create CloudWatch Events client."""
        if self._events_client is None:
            with self._lock:
                if self._events_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    self._events_client = boto3.client("events", **kwargs)
        return self._events_client

    @property
    def iam_client(self):
        """Get or create IAM client."""
        if self._iam_client is None:
            with self._lock:
                if self._iam_client is None:
                    kwargs = {"region_name": self.config.region_name}
                    if self.config.aws_access_key_id:
                        kwargs["aws_access_key_id"] = self.config.aws_access_key_id
                    if self.config.aws_secret_access_key:
                        kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
                    if self.config.aws_session_token:
                        kwargs["aws_session_token"] = self.config.aws_session_token
                    self._iam_client = boto3.client("iam", **kwargs)
        return self._iam_client

    # ==================== Instance Management ====================

    def create_instance(
        self,
        image_id: str,
        instance_type: str = "t2.micro",
        min_count: int = 1,
        max_count: int = 1,
        key_name: Optional[str] = None,
        security_groups: Optional[List[str]] = None,
        security_group_ids: Optional[List[str]] = None,
        subnet_id: Optional[str] = None,
        user_data: Optional[str] = None,
        monitoring_enabled: bool = False,
        iam_instance_profile: Optional[str] = None,
        ebs_optimized: bool = False,
        tags: Optional[Dict[str, str]] = None,
        block_device_mappings: Optional[List[Dict]] = None,
        network_interfaces: Optional[List[Dict]] = None,
        private_ip_address: Optional[str] = None,
        associate_public_ip: bool = True,
    ) -> List[EC2Instance]:
        """
        Create EC2 instances.

        Args:
            image_id: AMI ID to launch
            instance_type: Instance type (e.g., t2.micro)
            min_count: Minimum number of instances
            max_count: Maximum number of instances
            key_name: Key pair name
            security_groups: Security group names (for default VPC)
            security_group_ids: Security group IDs
            subnet_id: Subnet ID
            user_data: User data script
            monitoring_enabled: Enable detailed monitoring
            iam_instance_profile: IAM instance profile name/ARN
            ebs_optimized: Enable EBS optimization
            tags: Instance tags
            block_device_mappings: Block device mappings
            network_interfaces: Network interfaces
            private_ip_address: Private IP address
            associate_public_ip: Associate public IP (if in VPC)

        Returns:
            List of EC2Instance objects
        """
        kwargs = {
            "ImageId": image_id,
            "InstanceType": instance_type,
            "MinCount": min_count,
            "MaxCount": max_count,
            "Monitoring": {"Enabled": monitoring_enabled},
        }

        if key_name:
            kwargs["KeyName"] = key_name

        if security_groups:
            kwargs["SecurityGroups"] = security_groups

        if security_group_ids:
            kwargs["SecurityGroupIds"] = security_group_ids

        if subnet_id:
            kwargs["SubnetId"] = subnet_id

        if user_data:
            kwargs["UserData"] = base64.b64encode(user_data.encode()).decode()

        if iam_instance_profile:
            kwargs["IamInstanceProfile"] = {"Name": iam_instance_profile}

        if ebs_optimized:
            kwargs["EbsOptimized"] = ebs_optimized

        if tags:
            kwargs["TagSpecifications"] = [{
                "ResourceType": "instance",
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]
            }]

        if block_device_mappings:
            kwargs["BlockDeviceMappings"] = block_device_mappings

        if network_interfaces:
            kwargs["NetworkInterfaces"] = network_interfaces

        if private_ip_address:
            kwargs["PrivateIpAddress"] = private_ip_address

        if associate_public_ip and subnet_id and not network_interfaces:
            kwargs["AssociatePublicIpAddress"] = True

        response = self.client.run_instances(**kwargs)
        instances = []

        for inst in response.get("Instances", []):
            instances.append(self._parse_instance(inst))

        logger.info(f"Created {len(instances)} EC2 instance(s)")
        return instances

    def _parse_instance(self, inst: Dict) -> EC2Instance:
        """Parse instance dict to EC2Instance object."""
        state = InstanceState.RUNNING
        if inst.get("State"):
            state = InstanceState(inst["State"]["Name"])

        security_groups = []
        if inst.get("SecurityGroups"):
            security_groups = [sg["GroupId"] for sg in inst["SecurityGroups"]]

        tags = {}
        if inst.get("Tags"):
            tags = {t["Key"]: t["Value"] for t in inst["Tags"]}

        return EC2Instance(
            instance_id=inst["InstanceId"],
            instance_type=inst.get("InstanceType", "unknown"),
            state=state,
            image_id=inst.get("ImageId", ""),
            private_ip=inst.get("PrivateIpAddress"),
            public_ip=inst.get("PublicIpAddress"),
            private_dns_name=inst.get("PrivateDnsName"),
            public_dns_name=inst.get("PublicDnsName"),
            vpc_id=inst.get("VpcId"),
            subnet_id=inst.get("SubnetId"),
            security_groups=security_groups,
            key_name=inst.get("KeyName"),
            tags=tags,
            ami_id=inst.get("ImageId"),
            root_device_name=inst.get("RootDeviceName"),
            block_device_mappings=inst.get("BlockDeviceMappings", []),
            launch_time=inst.get("LaunchTime"),
        )

    def get_instance(self, instance_id: str) -> EC2Instance:
        """Get instance details."""
        response = self.client.describe_instances(InstanceIds=[instance_id])
        for reservation in response.get("Reservations", []):
            for inst in reservation.get("Instances", []):
                return self._parse_instance(inst)
        raise ValueError(f"Instance {instance_id} not found")

    def list_instances(
        self,
        instance_ids: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
        max_results: Optional[int] = None,
    ) -> List[EC2Instance]:
        """
        List EC2 instances.

        Args:
            instance_ids: Specific instance IDs
            filters: Filters for listing
            max_results: Maximum number of results

        Returns:
            List of EC2Instance objects
        """
        kwargs = {}
        if instance_ids:
            kwargs["InstanceIds"] = instance_ids
        if filters:
            kwargs["Filters"] = filters
        if max_results:
            kwargs["MaxResults"] = max_results

        response = self.client.describe_instances(**kwargs)
        instances = []

        for reservation in response.get("Reservations", []):
            for inst in reservation.get("Instances", []):
                instances.append(self._parse_instance(inst))

        return instances

    def start_instances(self, instance_ids: List[str]) -> List[EC2Instance]:
        """Start instances."""
        response = self.client.start_instances(InstanceIds=instance_ids)
        instances = []
        for inst in response.get("StartingInstances", []):
            instances.append(self._parse_instance(inst["CurrentState"]))
        logger.info(f"Starting {len(instances)} instance(s)")
        return instances

    def stop_instances(
        self,
        instance_ids: List[str],
        hibernate: bool = False,
        force: bool = False,
    ) -> List[EC2Instance]:
        """Stop instances."""
        kwargs = {"InstanceIds": instance_ids}
        if hibernate:
            kwargs["Hibernate"] = True
        if force:
            kwargs["Force"] = True

        response = self.client.stop_instances(**kwargs)
        instances = []
        for inst in response.get("StoppingInstances", []):
            instances.append(self._parse_instance(inst["CurrentState"]))
        logger.info(f"Stopping {len(instances)} instance(s)")
        return instances

    def terminate_instances(self, instance_ids: List[str]) -> List[EC2Instance]:
        """Terminate instances."""
        response = self.client.terminate_instances(InstanceIds=instance_ids)
        instances = []
        for inst in response.get("TerminatingInstances", []):
            instances.append(self._parse_instance(inst["CurrentState"]))
        logger.info(f"Terminating {len(instances)} instance(s)")
        return instances

    def reboot_instances(self, instance_ids: List[str]) -> bool:
        """Reboot instances."""
        self.client.reboot_instances(InstanceIds=instance_ids)
        logger.info(f"Rebooted {len(instance_ids)} instance(s)")
        return True

    def modify_instance(
        self,
        instance_id: str,
        instance_type: Optional[str] = None,
        user_data: Optional[str] = None,
        attributes: Optional[Dict] = None,
    ) -> bool:
        """Modify instance attributes."""
        if instance_type:
            self.client.modify_instance_attribute(
                InstanceId=instance_id,
                InstanceType={"Value": instance_type}
            )
        if attributes:
            for attr, value in attributes.items():
                self.client.modify_instance_attribute(
                    InstanceId=instance_id,
                    **{attr: value}
                )
        return True

    def wait_for_instance_state(
        self,
        instance_id: str,
        target_state: InstanceState,
        timeout: int = 300,
        poll_interval: int = 5,
    ) -> bool:
        """Wait for instance to reach a target state."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            inst = self.get_instance(instance_id)
            if inst.state == target_state:
                return True
            time.sleep(poll_interval)
        return False

    # ==================== AMI Management ====================

    def create_ami(
        self,
        instance_id: str,
        name: str,
        description: str = "",
        no_reboot: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ) -> AMIInfo:
        """
        Create AMI from instance.

        Args:
            instance_id: Source instance ID
            name: AMI name
            description: AMI description
            no_reboot: Don't reboot instance before creating image
            tags: Tags for the AMI

        Returns:
            AMIInfo object
        """
        kwargs = {
            "InstanceId": instance_id,
            "Name": name,
            "Description": description,
            "NoReboot": no_reboot,
        }

        if tags:
            kwargs["TagSpecifications"] = [{
                "ResourceType": "image",
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]
            }]

        response = self.client.create_image(**kwargs)

        ami = AMIInfo(
            image_id=response["ImageId"],
            name=name,
            description=description,
            state="pending",
            tags=tags or {},
        )

        logger.info(f"Creating AMI {ami.image_id} from instance {instance_id}")
        return ami

    def get_ami(self, ami_id: str) -> AMIInfo:
        """Get AMI details."""
        response = self.client.describe_images(ImageIds=[ami_id])
        images = response.get("Images", [])
        if not images:
            raise ValueError(f"AMI {ami_id} not found")

        img = images[0]
        tags = {t["Key"]: t["Value"] for t in img.get("Tags", [])}

        return AMIInfo(
            image_id=img["ImageId"],
            name=img["Name"],
            description=img.get("Description", ""),
            state=img["State"],
            owner_id=img.get("OwnerId"),
            owner_alias=img.get("ImageOwnerAlias"),
            platform=img.get("Platform"),
            architecture=img.get("Architecture", "x86_64"),
            virtualization_type=img.get("VirtualizationType", "hvm"),
            root_device_type=img.get("RootDeviceType", "ebs"),
            root_device_name=img.get("RootDeviceName"),
            tags=tags,
            creation_date=img.get("CreationDate"),
        )

    def list_amis(
        self,
        owner_ids: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
        executable_users: Optional[List[str]] = None,
    ) -> List[AMIInfo]:
        """
        List AMIs.

        Args:
            owner_ids: AMI owners
            filters: Filters
            executable_users: Executable users

        Returns:
            List of AMIInfo objects
        """
        kwargs = {}
        if owner_ids:
            kwargs["Owners"] = owner_ids
        if filters:
            kwargs["Filters"] = filters
        if executable_users:
            kwargs["ExecutableUsers"] = executable_users

        response = self.client.describe_images(**kwargs)
        amis = []

        for img in response.get("Images", []):
            tags = {t["Key"]: t["Value"] for t in img.get("Tags", [])}
            amis.append(AMIInfo(
                image_id=img["ImageId"],
                name=img["Name"],
                description=img.get("Description", ""),
                state=img["State"],
                owner_id=img.get("OwnerId"),
                owner_alias=img.get("ImageOwnerAlias"),
                platform=img.get("Platform"),
                architecture=img.get("Architecture", "x86_64"),
                virtualization_type=img.get("VirtualizationType", "hvm"),
                root_device_type=img.get("RootDeviceType", "ebs"),
                root_device_name=img.get("RootDeviceName"),
                tags=tags,
                creation_date=img.get("CreationDate"),
            ))

        return amis

    def deregister_ami(self, ami_id: str, deprecate: bool = True) -> bool:
        """Deregister an AMI."""
        self.client.deregister_image(ImageId=ami_id)
        if deprecate:
            self.client.create_tags(
                Resources=[ami_id],
                Tags=[{"Key": "DeprecationTime", "Value": datetime.utcnow().isoformat()}]
            )
        logger.info(f"Deregistered AMI: {ami_id}")
        return True

    def copy_ami(
        self,
        source_ami_id: str,
        name: str,
        description: str = "",
        source_region: Optional[str] = None,
        encrypted: bool = False,
        kms_key_id: Optional[str] = None,
    ) -> AMIInfo:
        """Copy an AMI to the current region."""
        kwargs = {
            "SourceImageId": source_ami_id,
            "Name": name,
            "Description": description,
        }
        if source_region:
            kwargs["SourceRegion"] = source_region
        if encrypted:
            kwargs["Encrypted"] = True
        if kms_key_id:
            kwargs["KmsKeyId"] = kms_key_id

        response = self.client.copy_image(**kwargs)

        return AMIInfo(
            image_id=response["ImageId"],
            name=name,
            description=description,
            state="pending",
            tags={},
        )

    def wait_for_ami_available(self, ami_id: str, timeout: int = 600) -> AMIInfo:
        """Wait for AMI to become available."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            ami = self.get_ami(ami_id)
            if ami.state == "available":
                return ami
            time.sleep(30)
        raise TimeoutError(f"AMI {ami_id} did not become available within {timeout}s")

    # ==================== Security Groups ====================

    def create_security_group(
        self,
        group_name: str,
        description: str,
        vpc_id: str,
        tags: Optional[Dict[str, str]] = None,
    ) -> SecurityGroupInfo:
        """
        Create a security group.

        Args:
            group_name: Security group name
            description: Description
            vpc_id: VPC ID
            tags: Tags

        Returns:
            SecurityGroupInfo object
        """
        kwargs = {
            "GroupName": group_name,
            "Description": description,
            "VpcId": vpc_id,
        }

        response = self.client.create_security_group(**kwargs)
        group_id = response["GroupId"]

        if tags:
            self.client.create_tags(
                Resources=[group_id],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created security group: {group_id}")
        return self.get_security_group(group_id)

    def get_security_group(self, group_id: str) -> SecurityGroupInfo:
        """Get security group details."""
        response = self.client.describe_security_groups(GroupIds=[group_id])
        groups = response.get("SecurityGroups", [])
        if not groups:
            raise ValueError(f"Security group {group_id} not found")

        sg = groups[0]
        tags = {t["Key"]: t["Value"] for t in sg.get("Tags", [])}

        return SecurityGroupInfo(
            group_id=sg["GroupId"],
            group_name=sg["GroupName"],
            description=sg["Description"],
            vpc_id=sg["VpcId"],
            owner_id=sg.get("OwnerId"),
            tags=tags,
            rules=self._parse_security_group_rules(sg),
        )

    def _parse_security_group_rules(self, sg: Dict) -> List[Dict]:
        """Parse security group rules."""
        rules = []
        for perm in sg.get("IpPermissions", []):
            rule = {
                "protocol": perm.get("IpProtocol", "-1"),
                "from_port": perm.get("FromPort"),
                "to_port": perm.get("ToPort"),
                "ranges": perm.get("IpRanges", []),
                "prefix_list_ids": perm.get("PrefixListIds", []),
                "user_id_group_pairs": perm.get("UserIdGroupPairs", []),
            }
            rules.append(rule)
        return rules

    def list_security_groups(
        self,
        filters: Optional[List[Dict]] = None,
        group_ids: Optional[List[str]] = None,
    ) -> List[SecurityGroupInfo]:
        """List security groups."""
        kwargs = {}
        if filters:
            kwargs["Filters"] = filters
        if group_ids:
            kwargs["GroupIds"] = group_ids

        response = self.client.describe_security_groups(**kwargs)
        groups = []

        for sg in response.get("SecurityGroups", []):
            tags = {t["Key"]: t["Value"] for t in sg.get("Tags", [])}
            groups.append(SecurityGroupInfo(
                group_id=sg["GroupId"],
                group_name=sg["GroupName"],
                description=sg["Description"],
                vpc_id=sg["VpcId"],
                owner_id=sg.get("OwnerId"),
                tags=tags,
                rules=self._parse_security_group_rules(sg),
            ))

        return groups

    def delete_security_group(self, group_id: str) -> bool:
        """Delete a security group."""
        self.client.delete_security_group(GroupId=group_id)
        logger.info(f"Deleted security group: {group_id}")
        return True

    def authorize_security_group_ingress(
        self,
        group_id: str,
        ip_permissions: List[Dict],
    ) -> bool:
        """Add ingress rules to security group."""
        self.client.authorize_security_group_ingress(
            GroupId=group_id,
            IpPermissions=ip_permissions,
        )
        logger.info(f"Added ingress rules to security group: {group_id}")
        return True

    def revoke_security_group_ingress(
        self,
        group_id: str,
        ip_permissions: List[Dict],
    ) -> bool:
        """Remove ingress rules from security group."""
        self.client.revoke_security_group_ingress(
            GroupId=group_id,
            IpPermissions=ip_permissions,
        )
        logger.info(f"Removed ingress rules from security group: {group_id}")
        return True

    def authorize_security_group_egress(
        self,
        group_id: str,
        ip_permissions: List[Dict],
    ) -> bool:
        """Add egress rules to security group."""
        self.client.authorize_security_group_egress(
            GroupId=group_id,
            IpPermissions=ip_permissions,
        )
        logger.info(f"Added egress rules to security group: {group_id}")
        return True

    def revoke_security_group_egress(
        self,
        group_id: str,
        ip_permissions: List[Dict],
    ) -> bool:
        """Remove egress rules from security group."""
        self.client.revoke_security_group_egress(
            GroupId=group_id,
            IpPermissions=ip_permissions,
        )
        logger.info(f"Removed egress rules from security group: {group_id}")
        return True

    # ==================== Key Pairs ====================

    def create_key_pair(
        self,
        key_name: str,
        key_type: str = "rsa",
        tags: Optional[Dict[str, str]] = None,
    ) -> KeyPairInfo:
        """
        Create a key pair.

        Args:
            key_name: Key pair name
            key_type: Key type (rsa or ed25519)
            tags: Tags

        Returns:
            KeyPairInfo object (includes private key material)
        """
        kwargs = {"KeyName": key_name, "KeyType": key_type}

        response = self.client.create_key_pair(**kwargs)

        if tags:
            self.client.create_tags(
                Resources=[response["KeyPairId"]],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created key pair: {key_name}")
        return KeyPairInfo(
            key_name=response["KeyName"],
            key_fingerprint=response.get("KeyFingerprint"),
            key_material=response.get("KeyMaterial"),
            key_type=key_type,
            tags=tags or {},
        )

    def import_key_pair(
        self,
        key_name: str,
        public_key_material: str,
        tags: Optional[Dict[str, str]] = None,
    ) -> KeyPairInfo:
        """
        Import an existing key pair.

        Args:
            key_name: Key pair name
            public_key_material: Public key in OpenSSH format (base64 encoded)
            tags: Tags

        Returns:
            KeyPairInfo object
        """
        kwargs = {
            "KeyName": key_name,
            "PublicKeyMaterial": public_key_material,
        }

        response = self.client.import_key_pair(**kwargs)

        if tags:
            self.client.create_tags(
                Resources=[response["KeyPairId"]],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Imported key pair: {key_name}")
        return KeyPairInfo(
            key_name=response["KeyName"],
            key_fingerprint=response.get("KeyFingerprint"),
            key_type="rsa",
            tags=tags or {},
        )

    def get_key_pair(self, key_name: str) -> KeyPairInfo:
        """Get key pair details."""
        response = self.client.describe_key_pairs(KeyNames=[key_name])
        pairs = response.get("KeyPairs", [])
        if not pairs:
            raise ValueError(f"Key pair {key_name} not found")

        kp = pairs[0]
        return KeyPairInfo(
            key_name=kp["KeyName"],
            key_fingerprint=kp.get("KeyFingerprint"),
            key_type=kp.get("KeyType", "rsa"),
        )

    def list_key_pairs(self) -> List[KeyPairInfo]:
        """List all key pairs."""
        response = self.client.describe_key_pairs()
        pairs = []

        for kp in response.get("KeyPairs", []):
            pairs.append(KeyPairInfo(
                key_name=kp["KeyName"],
                key_fingerprint=kp.get("KeyFingerprint"),
                key_type=kp.get("KeyType", "rsa"),
            ))

        return pairs

    def delete_key_pair(self, key_name: str) -> bool:
        """Delete a key pair."""
        self.client.delete_key_pair(KeyName=key_name)
        logger.info(f"Deleted key pair: {key_name}")
        return True

    # ==================== VPC Management ====================

    def create_vpc(
        self,
        cidr_block: str,
        instance_tenancy: str = "default",
        tags: Optional[Dict[str, str]] = None,
    ) -> VPCInfo:
        """
        Create a VPC.

        Args:
            cidr_block: CIDR block (e.g., 10.0.0.0/16)
            instance_tenancy: Instance tenancy (default, dedicated, host)
            tags: Tags

        Returns:
            VPCInfo object
        """
        kwargs = {
            "CidrBlock": cidr_block,
            "InstanceTenancy": instance_tenancy,
        }

        response = self.client.create_vpc(**kwargs)
        vpc_id = response["Vpc"]["VpcId"]

        if tags:
            self.client.create_tags(
                Resources=[vpc_id],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created VPC: {vpc_id}")
        return self.get_vpc(vpc_id)

    def get_vpc(self, vpc_id: str) -> VPCInfo:
        """Get VPC details."""
        response = self.client.describe_vpcs(VpcIds=[vpc_id])
        vpcs = response.get("Vpcs", [])
        if not vpcs:
            raise ValueError(f"VPC {vpc_id} not found")

        vpc = vpcs[0]
        tags = {t["Key"]: t["Value"] for t in vpc.get("Tags", [])}

        return VPCInfo(
            vpc_id=vpc["VpcId"],
            cidr_block=vpc["CidrBlock"],
            state=vpc["State"],
            is_default=vpc.get("IsDefault", False),
            owner_id=vpc.get("OwnerId"),
            tags=tags,
        )

    def list_vpcs(self, filters: Optional[List[Dict]] = None) -> List[VPCInfo]:
        """List VPCs."""
        kwargs = {}
        if filters:
            kwargs["Filters"] = filters

        response = self.client.describe_vpcs(**kwargs)
        vpcs = []

        for vpc in response.get("Vpcs", []):
            tags = {t["Key"]: t["Value"] for t in vpc.get("Tags", [])}
            vpcs.append(VPCInfo(
                vpc_id=vpc["VpcId"],
                cidr_block=vpc["CidrBlock"],
                state=vpc["State"],
                is_default=vpc.get("IsDefault", False),
                owner_id=vpc.get("OwnerId"),
                tags=tags,
            ))

        return vpcs

    def delete_vpc(self, vpc_id: str) -> bool:
        """Delete a VPC."""
        self.client.delete_vpc(VpcId=vpc_id)
        logger.info(f"Deleted VPC: {vpc_id}")
        return True

    def create_subnet(
        self,
        vpc_id: str,
        cidr_block: str,
        availability_zone: Optional[str] = None,
        map_public_ip_on_launch: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ) -> SubnetInfo:
        """
        Create a subnet.

        Args:
            vpc_id: VPC ID
            cidr_block: CIDR block
            availability_zone: AZ (optional)
            map_public_ip_on_launch: Auto-assign public IP
            tags: Tags

        Returns:
            SubnetInfo object
        """
        kwargs = {
            "VpcId": vpc_id,
            "CidrBlock": cidr_block,
        }

        if availability_zone:
            kwargs["AvailabilityZone"] = availability_zone

        response = self.client.create_subnet(**kwargs)
        subnet_id = response["Subnet"]["SubnetId"]

        self.client.modify_subnet_attribute(
            SubnetId=subnet_id,
            MapPublicIpOnLaunch={"Value": map_public_ip_on_launch}
        )

        if tags:
            self.client.create_tags(
                Resources=[subnet_id],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created subnet: {subnet_id}")
        return self.get_subnet(subnet_id)

    def get_subnet(self, subnet_id: str) -> SubnetInfo:
        """Get subnet details."""
        response = self.client.describe_subnets(SubnetIds=[subnet_id])
        subnets = response.get("Subnets", [])
        if not subnets:
            raise ValueError(f"Subnet {subnet_id} not found")

        subnet = subnets[0]
        tags = {t["Key"]: t["Value"] for t in subnet.get("Tags", [])}

        return SubnetInfo(
            subnet_id=subnet["SubnetId"],
            vpc_id=subnet["VpcId"],
            cidr_block=subnet["CidrBlock"],
            availability_zone=subnet.get("AvailabilityZone"),
            state=subnet["State"],
            map_public_ip_on_launch=subnet.get("MapPublicIpOnLaunch", False),
            tags=tags,
        )

    def list_subnets(
        self,
        filters: Optional[List[Dict]] = None,
        vpc_id: Optional[str] = None,
    ) -> List[SubnetInfo]:
        """List subnets."""
        kwargs = {}
        if filters:
            kwargs["Filters"] = filters
        if vpc_id:
            kwargs["Filters"] = kwargs.get("Filters", []) + [{"Name": "vpc-id", "Values": [vpc_id]}]

        response = self.client.describe_subnets(**kwargs)
        subnets = []

        for subnet in response.get("Subnets", []):
            tags = {t["Key"]: t["Value"] for t in subnet.get("Tags", [])}
            subnets.append(SubnetInfo(
                subnet_id=subnet["SubnetId"],
                vpc_id=subnet["VpcId"],
                cidr_block=subnet["CidrBlock"],
                availability_zone=subnet.get("AvailabilityZone"),
                state=subnet["State"],
                map_public_ip_on_launch=subnet.get("MapPublicIpOnLaunch", False),
                tags=tags,
            ))

        return subnets

    def delete_subnet(self, subnet_id: str) -> bool:
        """Delete a subnet."""
        self.client.delete_subnet(SubnetId=subnet_id)
        logger.info(f"Deleted subnet: {subnet_id}")
        return True

    def create_internet_gateway(self, tags: Optional[Dict[str, str]] = None) -> str:
        """Create an internet gateway."""
        response = self.client.create_internet_gateway()
        igw_id = response["InternetGateway"]["InternetGatewayId"]

        if tags:
            self.client.create_tags(
                Resources=[igw_id],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created internet gateway: {igw_id}")
        return igw_id

    def attach_internet_gateway(self, igw_id: str, vpc_id: str) -> bool:
        """Attach internet gateway to VPC."""
        self.client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        logger.info(f"Attached IGW {igw_id} to VPC {vpc_id}")
        return True

    def detach_internet_gateway(self, igw_id: str, vpc_id: str) -> bool:
        """Detach internet gateway from VPC."""
        self.client.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        logger.info(f"Detached IGW {igw_id} from VPC {vpc_id}")
        return True

    def create_route_table(self, vpc_id: str) -> str:
        """Create a route table."""
        response = self.client.create_route_table(VpcId=vpc_id)
        rtb_id = response["RouteTable"]["RouteTableId"]
        logger.info(f"Created route table: {rtb_id}")
        return rtb_id

    def create_route(
        self,
        route_table_id: str,
        destination_cidr_block: str,
        gateway_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        network_interface_id: Optional[str] = None,
        vpc_peering_connection_id: Optional[str] = None,
    ) -> bool:
        """Create a route."""
        kwargs = {
            "RouteTableId": route_table_id,
            "DestinationCidrBlock": destination_cidr_block,
        }

        if gateway_id:
            kwargs["GatewayId"] = gateway_id
        if instance_id:
            kwargs["InstanceId"] = instance_id
        if network_interface_id:
            kwargs["NetworkInterfaceId"] = network_interface_id
        if vpc_peering_connection_id:
            kwargs["VpcPeeringConnectionId"] = vpc_peering_connection_id

        self.client.create_route(**kwargs)
        logger.info(f"Created route in {route_table_id}: {destination_cidr_block}")
        return True

    # ==================== EBS Volumes ====================

    def create_volume(
        self,
        size: int,
        availability_zone: str,
        volume_type: VolumeType = VolumeType.GP3,
        snapshot_id: Optional[str] = None,
        encrypted: bool = False,
        kms_key_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> VolumeInfo:
        """
        Create an EBS volume.

        Args:
            size: Size in GiB
            availability_zone: AZ
            volume_type: Volume type
            snapshot_id: Source snapshot
            encrypted: Enable encryption
            kms_key_id: KMS key ID
            tags: Tags

        Returns:
            VolumeInfo object
        """
        kwargs = {
            "Size": size,
            "AvailabilityZone": availability_zone,
            "VolumeType": volume_type.value,
        }

        if snapshot_id:
            kwargs["SnapshotId"] = snapshot_id
        if encrypted:
            kwargs["Encrypted"] = True
        if kms_key_id:
            kwargs["KmsKeyId"] = kms_key_id

        response = self.client.create_volume(**kwargs)
        volume_id = response["VolumeId"]

        if tags:
            self.client.create_tags(
                Resources=[volume_id],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created volume: {volume_id}")
        return self.get_volume(volume_id)

    def get_volume(self, volume_id: str) -> VolumeInfo:
        """Get volume details."""
        response = self.client.describe_volumes(VolumeIds=[volume_id])
        volumes = response.get("Volumes", [])
        if not volumes:
            raise ValueError(f"Volume {volume_id} not found")

        vol = volumes[0]
        tags = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}

        return VolumeInfo(
            volume_id=vol["VolumeId"],
            size=vol["Size"],
            volume_type=VolumeType(vol["VolumeType"]),
            state=vol["State"],
            snapshot_id=vol.get("SnapshotId"),
            availability_zone=vol["AvailabilityZone"],
            encrypted=vol.get("Encrypted", False),
            tags=tags,
        )

    def list_volumes(
        self,
        filters: Optional[List[Dict]] = None,
        volume_ids: Optional[List[str]] = None,
    ) -> List[VolumeInfo]:
        """List volumes."""
        kwargs = {}
        if filters:
            kwargs["Filters"] = filters
        if volume_ids:
            kwargs["VolumeIds"] = volume_ids

        response = self.client.describe_volumes(**kwargs)
        volumes = []

        for vol in response.get("Volumes", []):
            tags = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}
            volumes.append(VolumeInfo(
                volume_id=vol["VolumeId"],
                size=vol["Size"],
                volume_type=VolumeType(vol["VolumeType"]),
                state=vol["State"],
                snapshot_id=vol.get("SnapshotId"),
                availability_zone=vol["AvailabilityZone"],
                encrypted=vol.get("Encrypted", False),
                tags=tags,
            ))

        return volumes

    def delete_volume(self, volume_id: str) -> bool:
        """Delete a volume."""
        self.client.delete_volume(VolumeId=volume_id)
        logger.info(f"Deleted volume: {volume_id}")
        return True

    def attach_volume(
        self,
        volume_id: str,
        instance_id: str,
        device: str,
    ) -> bool:
        """Attach volume to instance."""
        self.client.attach_volume(
            VolumeId=volume_id,
            InstanceId=instance_id,
            Device=device,
        )
        logger.info(f"Attached volume {volume_id} to instance {instance_id} as {device}")
        return True

    def detach_volume(self, volume_id: str, force: bool = False) -> bool:
        """Detach volume from instance."""
        kwargs = {"VolumeId": volume_id}
        if force:
            kwargs["Force"] = True

        self.client.detach_volume(**kwargs)
        logger.info(f"Detached volume: {volume_id}")
        return True

    def create_snapshot(
        self,
        volume_id: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Create snapshot from volume.

        Args:
            volume_id: Source volume ID
            description: Description
            tags: Tags

        Returns:
            Snapshot ID
        """
        kwargs = {
            "VolumeId": volume_id,
            "Description": description,
        }

        response = self.client.create_snapshot(**kwargs)
        snapshot_id = response["SnapshotId"]

        if tags:
            self.client.create_tags(
                Resources=[snapshot_id],
                Tags=[{"Key": k, "Value": v} for k, v in tags.items()]
            )

        logger.info(f"Created snapshot: {snapshot_id}")
        return snapshot_id

    def list_snapshots(
        self,
        owner_ids: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
    ) -> List[Dict]:
        """List snapshots."""
        kwargs = {}
        if owner_ids:
            kwargs["OwnerIds"] = owner_ids
        if filters:
            kwargs["Filters"] = filters

        response = self.client.describe_snapshots(**kwargs)
        return response.get("Snapshots", [])

    def modify_volume(
        self,
        volume_id: str,
        size: Optional[int] = None,
        volume_type: Optional[VolumeType] = None,
        iops: Optional[int] = None,
        throughput: Optional[int] = None,
    ) -> bool:
        """Modify volume attributes."""
        kwargs = {"VolumeId": volume_id}

        modifications = []
        if size:
            modifications.append({"VolumeSize": size})
        if volume_type:
            modifications.append({"VolumeType": volume_type.value})
        if iops:
            modifications.append({"Iops": iops})
        if throughput:
            modifications.append({"Throughput": throughput})

        if modifications:
            for mod in modifications:
                kwargs.update(mod)
                self.client.modify_volume(**kwargs)

        logger.info(f"Modified volume: {volume_id}")
        return True

    # ==================== Auto Scaling ====================

    def create_auto_scaling_group(
        self,
        name: str,
        min_size: int,
        max_size: int,
        vpc_id: str,
        availability_zones: List[str],
        desired_capacity: int = None,
        launch_template_id: Optional[str] = None,
        launch_template_version: Optional[str] = None,
        instance_id: Optional[str] = None,
        load_balancer_names: Optional[List[str]] = None,
        target_group_arns: Optional[List[str]] = None,
        health_check_type: str = "EC2",
        health_check_period: int = 300,
        tags: Optional[List[Dict]] = None,
        lifecycle_hook_specs: Optional[List[Dict]] = None,
    ) -> AutoScalingGroupInfo:
        """
        Create an Auto Scaling group.

        Args:
            name: ASG name
            min_size: Minimum size
            max_size: Maximum size
            vpc_id: VPC ID
            availability_zones: AZs
            desired_capacity: Desired capacity
            launch_template_id: Launch template ID
            launch_template_version: Launch template version
            instance_id: Instance ID to use
            load_balancer_names: Classic ELB names
            target_group_arns: Target group ARNs
            health_check_type: Health check type (EC2 or ELB)
            health_check_period: Health check period
            tags: Tags
            lifecycle_hook_specs: Lifecycle hooks

        Returns:
            AutoScalingGroupInfo object
        """
        kwargs = {
            "AutoScalingGroupName": name,
            "MinSize": min_size,
            "MaxSize": max_size,
            "VPCZoneIdentifier": ",".join(availability_zones),  # Subnet IDs
        }

        if desired_capacity is not None:
            kwargs["DesiredCapacity"] = desired_capacity

        if launch_template_id:
            kwargs["LaunchTemplate"] = {
                "LaunchTemplateId": launch_template_id,
            }
            if launch_template_version:
                kwargs["LaunchTemplate"]["Version"] = launch_template_version

        if instance_id:
            kwargs["InstanceId"] = instance_id

        if load_balancer_names:
            kwargs["LoadBalancerNames"] = load_balancer_names

        if target_group_arns:
            kwargs["TargetGroupARNs"] = target_group_arns

        kwargs["HealthCheckType"] = health_check_type
        kwargs["HealthCheckGracePeriod"] = health_check_period

        if tags:
            kwargs["Tags"] = tags

        self.autoscaling_client.create_auto_scaling_group(**kwargs)

        if lifecycle_hook_specs:
            for hook in lifecycle_hook_specs:
                self.autoscaling_client.put_lifecycle_hook(
                    AutoScalingGroupName=name,
                    **hook
                )

        logger.info(f"Created Auto Scaling group: {name}")
        return self.get_auto_scaling_group(name)

    def get_auto_scaling_group(self, name: str) -> AutoScalingGroupInfo:
        """Get Auto Scaling group details."""
        response = self.autoscaling_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )
        groups = response.get("AutoScalingGroups", [])
        if not groups:
            raise ValueError(f"Auto Scaling group {name} not found")

        asg = groups[0]
        return AutoScalingGroupInfo(
            auto_scaling_group_name=asg["AutoScalingGroupName"],
            min_size=asg["MinSize"],
            max_size=asg["MaxSize"],
            desired_capacity=asg["DesiredCapacity"],
            vpc_id=asg.get("VPCZoneIdentifier", ""),
            availability_zones=asg.get("AvailabilityZones", []),
            load_balancers=asg.get("LoadBalancerNames", []),
            target_group_arns=asg.get("TargetGroupARNs", []),
            health_check_type=asg.get("HealthCheckType", "EC2"),
            health_check_period=asg.get("HealthCheckGracePeriod", 300),
            tags={t["Key"]: t["Value"] for t in asg.get("Tags", [])},
        )

    def list_auto_scaling_groups(
        self,
        names: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
    ) -> List[AutoScalingGroupInfo]:
        """List Auto Scaling groups."""
        kwargs = {}
        if names:
            kwargs["AutoScalingGroupNames"] = names

        response = self.autoscaling_client.describe_auto_scaling_groups(**kwargs)
        groups = []

        for asg in response.get("AutoScalingGroups", []):
            groups.append(AutoScalingGroupInfo(
                auto_scaling_group_name=asg["AutoScalingGroupName"],
                min_size=asg["MinSize"],
                max_size=asg["MaxSize"],
                desired_capacity=asg["DesiredCapacity"],
                vpc_id=asg.get("VPCZoneIdentifier", ""),
                availability_zones=asg.get("AvailabilityZones", []),
                load_balancers=asg.get("LoadBalancerNames", []),
                target_group_arns=asg.get("TargetGroupARNs", []),
                health_check_type=asg.get("HealthCheckType", "EC2"),
                health_check_period=asg.get("HealthCheckGracePeriod", 300),
                tags={t["Key"]: t["Value"] for t in asg.get("Tags", [])},
            ))

        return groups

    def update_auto_scaling_group(
        self,
        name: str,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        desired_capacity: Optional[int] = None,
    ) -> bool:
        """Update Auto Scaling group."""
        kwargs = {"AutoScalingGroupName": name}
        if min_size is not None:
            kwargs["MinSize"] = min_size
        if max_size is not None:
            kwargs["MaxSize"] = max_size
        if desired_capacity is not None:
            kwargs["DesiredCapacity"] = desired_capacity

        self.autoscaling_client.update_auto_scaling_group(**kwargs)
        logger.info(f"Updated Auto Scaling group: {name}")
        return True

    def delete_auto_scaling_group(self, name: str, force_delete: bool = False) -> bool:
        """Delete Auto Scaling group."""
        if force_delete:
            self.autoscaling_client.delete_auto_scaling_group(
                AutoScalingGroupName=name,
                ForceDelete=True
            )
        else:
            self.autoscaling_client.delete_auto_scaling_group(
                AutoScalingGroupName=name
            )
        logger.info(f"Deleted Auto Scaling group: {name}")
        return True

    def set_desired_capacity(self, name: str, desired_capacity: int) -> bool:
        """Set desired capacity."""
        self.autoscaling_client.set_desired_capacity(
            AutoScalingGroupName=name,
            DesiredCapacity=desired_capacity,
        )
        return True

    def attach_instances_to_asg(
        self,
        asg_name: str,
        instance_ids: List[str],
    ) -> bool:
        """Attach instances to Auto Scaling group."""
        self.autoscaling_client.attach_instances(
            AutoScalingGroupName=asg_name,
            InstanceIds=instance_ids,
        )
        logger.info(f"Attached {len(instance_ids)} instances to ASG {asg_name}")
        return True

    def detach_instances_from_asg(
        self,
        asg_name: str,
        instance_ids: List[str],
        decrement_desired_capacity: bool = True,
    ) -> bool:
        """Detach instances from Auto Scaling group."""
        self.autoscaling_client.detach_instances(
            AutoScalingGroupName=asg_name,
            InstanceIds=instance_ids,
            ShouldDecrementDesiredCapacity=decrement_desired_capacity,
        )
        logger.info(f"Detached {len(instance_ids)} instances from ASG {asg_name}")
        return True

    # ==================== Load Balancers ====================

    def create_load_balancer(
        self,
        name: str,
        type: LoadBalancerType,
        subnets: List[str],
        security_groups: Optional[List[str]] = None,
        scheme: str = "internet-facing",
        tags: Optional[List[Dict]] = None,
        load_balancer_attributes: Optional[Dict] = None,
    ) -> LoadBalancerInfo:
        """
        Create a load balancer.

        Args:
            name: Load balancer name
            type: Load balancer type
            subnets: Subnet IDs
            security_groups: Security group IDs
            scheme: Scheme (internet-facing or internal)
            tags: Tags
            load_balancer_attributes: Additional attributes

        Returns:
            LoadBalancerInfo object
        """
        kwargs = {
            "Name": name,
            "Scheme": scheme,
            "Subnets": subnets,
        }

        if security_groups:
            kwargs["SecurityGroups"] = security_groups

        if tags:
            kwargs["Tags"] = tags

        if type == LoadBalancerType.APPLICATION:
            kwargs["Type"] = "application"
            response = self.elbv2_client.create_load_balancer(**kwargs)
        elif type == LoadBalancerType.NETWORK:
            kwargs["Type"] = "network"
            response = self.elbv2_client.create_load_balancer(**kwargs)
        elif type == LoadBalancerType.GATEWAY:
            kwargs["Type"] = "gateway"
            response = self.elbv2_client.create_load_balancer(**kwargs)
        else:
            kwargs["LoadBalancerNames"] = [name]
            if security_groups:
                kwargs["SecurityGroups"] = security_groups
            response = self.elb_client.create_load_balancer(**kwargs)

        lbs = response.get("LoadBalancers", [])
        if not lbs:
            lbs = response.get("LoadBalancerDescriptions", [])

        lb = lbs[0]
        lb_info = LoadBalancerInfo(
            load_balancer_name=lb.get("LoadBalancerName", name),
            load_balancer_arn=lb.get("LoadBalancerArn", ""),
            type=type,
            dns_name=lb.get("DNSName", ""),
            vpc_id=lb.get("VPCId", ""),
            scheme=lb.get("Scheme", scheme),
            availability_zones=lb.get("AvailabilityZones", []),
            subnets=lb.get("Subnets", []),
            security_groups=lb.get("SecurityGroups", []),
        )

        logger.info(f"Created load balancer: {name}")
        return lb_info

    def get_load_balancer(self, name: str) -> LoadBalancerInfo:
        """Get load balancer details."""
        response = self.elbv2_client.describe_load_balancers(Names=[name])
        lbs = response.get("LoadBalancers", [])
        if not lbs:
            raise ValueError(f"Load balancer {name} not found")

        lb = lbs[0]
        return LoadBalancerInfo(
            load_balancer_name=lb["LoadBalancerName"],
            load_balancer_arn=lb["LoadBalancerArn"],
            type=LoadBalancerType(lb["Type"]),
            dns_name=lb["DNSName"],
            vpc_id=lb["VpcId"],
            scheme=lb.get("Scheme", "internet-facing"),
            availability_zones=lb.get("AvailabilityZones", []),
            subnets=lb.get("AvailabilityZones", []),
            security_groups=lb.get("SecurityGroups", []),
        )

    def list_load_balancers(self, names: Optional[List[str]] = None) -> List[LoadBalancerInfo]:
        """List load balancers."""
        response = self.elbv2_client.describe_load_balancers(Names=names or [])
        lbs = []

        for lb in response.get("LoadBalancers", []):
            lbs.append(LoadBalancerInfo(
                load_balancer_name=lb["LoadBalancerName"],
                load_balancer_arn=lb["LoadBalancerArn"],
                type=LoadBalancerType(lb["Type"]),
                dns_name=lb["DNSName"],
                vpc_id=lb["VpcId"],
                scheme=lb.get("Scheme", "internet-facing"),
                availability_zones=lb.get("AvailabilityZones", []),
                subnets=lb.get("AvailabilityZones", []),
                security_groups=lb.get("SecurityGroups", []),
            ))

        return lbs

    def delete_load_balancer(self, name: str) -> bool:
        """Delete load balancer."""
        self.elbv2_client.delete_load_balancer(LoadBalancerName=name)
        logger.info(f"Deleted load balancer: {name}")
        return True

    def create_target_group(
        self,
        name: str,
        port: int,
        protocol: str,
        vpc_id: str,
        health_check_interval_seconds: int = 30,
        health_check_path: Optional[str] = None,
        health_check_port: Optional[str] = None,
        healthy_threshold_count: int = 5,
        unhealthy_threshold_count: int = 2,
        timeout_seconds: int = 5,
        matcher: Optional[str] = None,
    ) -> str:
        """
        Create a target group.

        Args:
            name: Target group name
            port: Port
            protocol: Protocol (HTTP, HTTPS, TCP, etc.)
            vpc_id: VPC ID
            health_check_interval_seconds: Health check interval
            health_check_path: Health check path (for HTTP)
            health_check_port: Health check port
            healthy_threshold_count: Healthy threshold
            unhealthy_threshold_count: Unhealthy threshold
            timeout_seconds: Timeout seconds
            matcher: HTTP matcher (for HTTP health check)

        Returns:
            Target group ARN
        """
        kwargs = {
            "Name": name,
            "Port": port,
            "Protocol": protocol,
            "VpcId": vpc_id,
            "HealthCheckIntervalSeconds": health_check_interval_seconds,
            "HealthyThresholdCount": healthy_threshold_count,
            "UnhealthyThresholdCount": unhealthy_threshold_count,
            "HealthCheckTimeoutSeconds": timeout_seconds,
        }

        if health_check_path:
            kwargs["HealthCheckPath"] = health_check_path
        if health_check_port:
            kwargs["HealthCheckPort"] = health_check_port
        if matcher:
            kwargs["Matcher"] = {"HttpCode": matcher}

        response = self.elbv2_client.create_target_group(**kwargs)
        return response["TargetGroups"][0]["TargetGroupArn"]

    def register_targets(
        self,
        target_group_arn: str,
        target_ids: List[str],
        port: Optional[int] = None,
    ) -> bool:
        """Register targets with target group."""
        targets = [{"Id": tid} for tid in target_ids]
        if port:
            for t in targets:
                t["Port"] = port

        self.elbv2_client.register_targets(TargetGroupArn=target_group_arn, Targets=targets)
        logger.info(f"Registered {len(target_ids)} targets to target group")
        return True

    def create_listener(
        self,
        load_balancer_arn: str,
        protocol: str,
        port: int,
        target_group_arn: Optional[str] = None,
        certificates: Optional[List[Dict]] = None,
        ssl_policy: Optional[str] = None,
        default_actions: Optional[List[Dict]] = None,
    ) -> str:
        """
        Create a listener.

        Args:
            load_balancer_arn: Load balancer ARN
            protocol: Protocol
            port: Port
            target_group_arn: Default target group ARN
            certificates: Certificates
            ssl_policy: SSL policy
            default_actions: Default actions

        Returns:
            Listener ARN
        """
        kwargs = {
            "LoadBalancerArn": load_balancer_arn,
            "Protocol": protocol,
            "Port": port,
        }

        if target_group_arn:
            kwargs["DefaultActions"] = [{"Type": "forward", "TargetGroupArn": target_group_arn}]

        if certificates:
            kwargs["Certificates"] = certificates

        if ssl_policy:
            kwargs["SslPolicy"] = ssl_policy

        response = self.elbv2_client.create_listener(**kwargs)
        return response["Listeners"][0]["ListenerArn"]

    # ==================== CloudWatch Integration ====================

    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int,
        statistics: List[str],
        dimensions: Optional[List[Dict]] = None,
        unit: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get CloudWatch metric statistics.

        Args:
            namespace: Namespace (e.g., AWS/EC2)
            metric_name: Metric name
            start_time: Start time
            end_time: End time
            period: Period in seconds
            statistics: Statistics (Sum, Average, Maximum, Minimum, SampleCount)
            dimensions: Dimensions
            unit: Unit

        Returns:
            List of data points
        """
        kwargs = {
            "Namespace": namespace,
            "MetricName": metric_name,
            "StartTime": start_time,
            "EndTime": end_time,
            "Period": period,
            "Statistics": statistics,
        }

        if dimensions:
            kwargs["Dimensions"] = dimensions
        if unit:
            kwargs["Unit"] = unit

        response = self.cloudwatch_client.get_metric_statistics(**kwargs)
        return response.get("Datapoints", [])

    def put_metric_data(
        self,
        namespace: str,
        metrics: List[Dict],
    ) -> bool:
        """
        Put metric data.

        Args:
            namespace: Namespace
            metrics: List of metric data

        Returns:
            True if successful
        """
        self.cloudwatch_client.put_metric_data(
            Namespace=namespace,
            MetricData=metrics,
        )
        return True

    def list_metrics(
        self,
        namespace: Optional[str] = None,
        metric_name: Optional[str] = None,
        dimensions: Optional[List[Dict]] = None,
    ) -> List[Dict]:
        """List CloudWatch metrics."""
        kwargs = {}
        if namespace:
            kwargs["Namespace"] = namespace
        if metric_name:
            kwargs["MetricName"] = metric_name
        if dimensions:
            kwargs["Dimensions"] = dimensions

        response = self.cloudwatch_client.list_metrics(**kwargs)
        return response.get("Metrics", [])

    def put_metric_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        namespace: str,
        period: int,
        evaluation_periods: int,
        threshold: float,
        comparison_operator: str,
        statistics: str = "Average",
        alarm_actions: Optional[List[str]] = None,
        ok_actions: Optional[List[str]] = None,
        dimensions: Optional[List[Dict]] = None,
        description: str = "",
    ) -> bool:
        """
        Create or update a metric alarm.

        Args:
            alarm_name: Alarm name
            metric_name: Metric name
            namespace: Namespace
            period: Period in seconds
            evaluation_periods: Evaluation periods
            threshold: Threshold value
            comparison_operator: Comparison operator
            statistics: Statistic (Average, Sum, Maximum, Minimum, SampleCount)
            alarm_actions: Actions to execute when alarm triggers
            ok_actions: Actions when alarm resolves
            dimensions: Dimensions
            description: Alarm description

        Returns:
            True if successful
        """
        kwargs = {
            "AlarmName": alarm_name,
            "MetricName": metric_name,
            "Namespace": namespace,
            "Period": period,
            "EvaluationPeriods": evaluation_periods,
            "Threshold": threshold,
            "ComparisonOperator": comparison_operator,
            "Statistic": statistics,
        }

        if alarm_actions:
            kwargs["AlarmActions"] = alarm_actions
        if ok_actions:
            kwargs["OKActions"] = ok_actions
        if dimensions:
            kwargs["Dimensions"] = dimensions
        if description:
            kwargs["AlarmDescription"] = description

        self.cloudwatch_client.put_metric_alarm(**kwargs)
        logger.info(f"Created metric alarm: {alarm_name}")
        return True

    def get_alarm_state(self, alarm_name: str) -> str:
        """Get alarm state."""
        response = self.cloudwatch_client.describe_alarms(
            AlarmNames=[alarm_name]
        )
        alarms = response.get("MetricAlarms", [])
        if not alarms:
            raise ValueError(f"Alarm {alarm_name} not found")
        return alarms[0]["StateValue"]

    def get_instance_metrics(
        self,
        instance_id: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        period: int = 300,
    ) -> List[Dict]:
        """Get EC2 instance metrics."""
        return self.get_metric_statistics(
            namespace="AWS/EC2",
            metric_name=metric_name,
            start_time=start_time,
            end_time=end_time,
            period=period,
            statistics=["Average", "Maximum", "Minimum"],
            dimensions=[{"Name": "InstanceId", "Value": instance_id}],
        )

    def get_ec2_metrics(self) -> List[str]:
        """Get available EC2 CloudWatch metrics."""
        return [
            "CPUUtilization",
            "NetworkIn",
            "NetworkOut",
            "NetworkPacketsIn",
            "NetworkPacketsOut",
            "DiskReadBytes",
            "DiskWriteBytes",
            "DiskReadOps",
            "DiskWriteOps",
            "CPUCreditBalance",
            "CPUCreditUsage",
            "StatusCheckFailed",
            "StatusCheckFailed_Instance",
            "StatusCheckFailed_System",
        ]

    # ==================== Instance Scheduling ====================

    def create_schedule_rule(
        self,
        name: str,
        schedule: str,
        instance_ids: List[str],
        action: str,
        description: str = "",
        enabled: bool = True,
    ) -> ScheduleRule:
        """
        Create an instance scheduling rule using CloudWatch Events.

        Args:
            name: Rule name
            schedule: Cron expression (e.g., "0 9 * * MON-FRI")
            instance_ids: List of instance IDs to target
            action: Action ("start" or "stop")
            description: Rule description
            enabled: Whether rule is enabled

        Returns:
            ScheduleRule object
        """
        target_id = f"ec2-scheduler-{name}"

        # Create IAM role for CloudWatch Events if needed
        role_arn = self._get_or_create_scheduler_role(name)

        # Create the rule
        kwargs = {
            "Name": name,
            "ScheduleExpression": f"cron({schedule})",
            "Description": description,
            "State": "ENABLED" if enabled else "DISABLED",
            "Targets": [{
                "Id": target_id,
                "Arn": f"arn:aws:ec2:{self.config.region_name}:*:instance/*",
                "RoleArn": role_arn,
            }],
        }

        response = self.events_client.put_rule(**kwargs)
        rule_arn = response["RuleArn"]

        rule = ScheduleRule(
            rule_name=name,
            instance_ids=instance_ids,
            action=action,
            schedule=schedule,
            enabled=enabled,
            target_id=target_id,
            rule_arn=rule_arn,
        )

        self._scheduled_rules[name] = rule
        logger.info(f"Created schedule rule: {name}")
        return rule

    def _get_or_create_scheduler_role(self, name: str) -> str:
        """Get or create IAM role for scheduler."""
        role_name = f"ec2-scheduler-role-{name}"

        try:
            response = self.iam_client.get_role(RoleName=role_name)
            return response["Role"]["Arn"]
        except ClientError:
            pass

        # Create role with trust policy
        assume_role_policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "events.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }]
        })

        self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy,
        )

        # Attach policy
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "ec2:StartInstances",
                    "ec2:StopInstances",
                    "ec2:DescribeInstances",
                ],
                "Resource": "*",
            }]
        })

        self.iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=f"{name}-policy",
            PolicyDocument=policy,
        )

        response = self.iam_client.get_role(RoleName=role_name)
        return response["Role"]["Arn"]

    def get_schedule_rule(self, name: str) -> ScheduleRule:
        """Get scheduling rule details."""
        if name in self._scheduled_rules:
            return self._scheduled_rules[name]

        response = self.events_client.describe_rules(Name=name)
        rules = response.get("Rules", [])
        if not rules:
            raise ValueError(f"Schedule rule {name} not found")

        rule = rules[0]
        return ScheduleRule(
            rule_name=rule["Name"],
            instance_ids=[],  # Would need to query targets
            action="unknown",
            schedule=rule["ScheduleExpression"].replace("cron(", "").replace(")", ""),
            enabled=rule["State"] == "ENABLED",
            rule_arn=rule["Arn"],
        )

    def list_schedule_rules(self) -> List[ScheduleRule]:
        """List all scheduling rules."""
        response = self.events_client.list_rules()
        rules = []

        for rule in response.get("Rules", []):
            if rule["Name"].startswith("ec2-scheduler-"):
                rules.append(ScheduleRule(
                    rule_name=rule["Name"],
                    instance_ids=[],
                    action="unknown",
                    schedule=rule["ScheduleExpression"].replace("cron(", "").replace(")", ""),
                    enabled=rule["State"] == "ENABLED",
                    rule_arn=rule["Arn"],
                ))

        return rules

    def delete_schedule_rule(self, name: str) -> bool:
        """Delete a scheduling rule."""
        self.events_client.delete_rule(Name=name)

        if name in self._scheduled_rules:
            del self._scheduled_rules[name]

        logger.info(f"Deleted schedule rule: {name}")
        return True

    def enable_schedule_rule(self, name: str) -> bool:
        """Enable a scheduling rule."""
        self.events_client.enable_rule(Name=name)
        if name in self._scheduled_rules:
            self._scheduled_rules[name].enabled = True
        return True

    def disable_schedule_rule(self, name: str) -> bool:
        """Disable a scheduling rule."""
        self.events_client.disable_rule(Name=name)
        if name in self._scheduled_rules:
            self._scheduled_rules[name].enabled = False
        return True

    def execute_scheduled_action(
        self,
        instance_ids: List[str],
        action: str,
    ) -> Dict[str, bool]:
        """
        Execute a scheduled action on instances.

        Args:
            instance_ids: List of instance IDs
            action: "start" or "stop"

        Returns:
            Dictionary of instance_id -> success
        """
        results = {}

        if action == "start":
            response = self.start_instances(instance_ids)
            for inst in response:
                results[inst.instance_id] = True
        elif action == "stop":
            response = self.stop_instances(instance_ids)
            for inst in response:
                results[inst.instance_id] = True
        else:
            raise ValueError(f"Invalid action: {action}")

        return results

    def create_start_stop_schedule(
        self,
        name: str,
        instance_ids: List[str],
        start_schedule: str,
        stop_schedule: str,
        description: str = "",
    ) -> tuple:
        """
        Create a start/stop schedule for instances.

        Args:
            name: Base name for schedules
            instance_ids: List of instance IDs
            start_schedule: Cron for start (e.g., "0 8 * * MON-FRI")
            stop_schedule: Cron for stop (e.g., "0 18 * * MON-FRI")
            description: Description

        Returns:
            Tuple of (start_rule, stop_rule)
        """
        start_rule = self.create_schedule_rule(
            name=f"{name}-start",
            schedule=start_schedule,
            instance_ids=instance_ids,
            action="start",
            description=f"{description} - Start instances",
        )

        stop_rule = self.create_schedule_rule(
            name=f"{name}-stop",
            schedule=stop_schedule,
            instance_ids=instance_ids,
            action="stop",
            description=f"{description} - Stop instances",
        )

        return start_rule, stop_rule
