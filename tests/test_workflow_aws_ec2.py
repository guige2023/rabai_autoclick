"""
Tests for workflow_aws_ec2 module
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

# Create mock boto3 module before importing workflow_aws_ec2
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

# Now we can import the module
from src.workflow_aws_ec2 import (
    EC2Integration,
    InstanceState,
    InstanceType,
    VolumeType,
    LoadBalancerType,
    EC2Config,
    EC2Instance,
    AMIInfo,
    SecurityGroupInfo,
    KeyPairInfo,
    VPCInfo,
    SubnetInfo,
    VolumeInfo,
    AutoScalingGroupInfo,
    LoadBalancerInfo,
    CloudWatchMetric,
    ScheduleRule,
)


class TestInstanceState(unittest.TestCase):
    """Test InstanceState enum"""

    def test_instance_state_values(self):
        self.assertEqual(InstanceState.PENDING.value, "pending")
        self.assertEqual(InstanceState.RUNNING.value, "running")
        self.assertEqual(InstanceState.SHUTTING_DOWN.value, "shutting-down")
        self.assertEqual(InstanceState.TERMINATED.value, "terminated")
        self.assertEqual(InstanceState.STOPPING.value, "stopping")
        self.assertEqual(InstanceState.STOPPED.value, "stopped")


class TestInstanceType(unittest.TestCase):
    """Test InstanceType enum"""

    def test_instance_type_values(self):
        self.assertEqual(InstanceType.T2_MICRO.value, "t2.micro")
        self.assertEqual(InstanceType.T3_LARGE.value, "t3.large")
        self.assertEqual(InstanceType.M5_XLARGE.value, "m5.xlarge")
        self.assertEqual(InstanceType.C5_LARGE.value, "c5.large")


class TestVolumeType(unittest.TestCase):
    """Test VolumeType enum"""

    def test_volume_type_values(self):
        self.assertEqual(VolumeType.GP2.value, "gp2")
        self.assertEqual(VolumeType.GP3.value, "gp3")
        self.assertEqual(VolumeType.IO1.value, "io1")
        self.assertEqual(VolumeType.IO2.value, "io2")
        self.assertEqual(VolumeType.ST1.value, "st1")


class TestLoadBalancerType(unittest.TestCase):
    """Test LoadBalancerType enum"""

    def test_load_balancer_type_values(self):
        self.assertEqual(LoadBalancerType.APPLICATION.value, "application")
        self.assertEqual(LoadBalancerType.NETWORK.value, "network")
        self.assertEqual(LoadBalancerType.GATEWAY.value, "gateway")
        self.assertEqual(LoadBalancerType.CLASSIC.value, "classic")


class TestEC2Config(unittest.TestCase):
    """Test EC2Config dataclass"""

    def test_ec2_config_defaults(self):
        config = EC2Config()
        self.assertEqual(config.region_name, "us-east-1")
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertTrue(config.verify_ssl)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)


class TestEC2Instance(unittest.TestCase):
    """Test EC2Instance dataclass"""

    def test_ec2_instance_creation(self):
        instance = EC2Instance(
            instance_id="i-1234567890abcdef0",
            instance_type="t2.micro",
            state=InstanceState.RUNNING,
            image_id="ami-12345678"
        )
        self.assertEqual(instance.instance_id, "i-1234567890abcdef0")
        self.assertEqual(instance.instance_type, "t2.micro")
        self.assertEqual(instance.state, InstanceState.RUNNING)
        self.assertEqual(instance.image_id, "ami-12345678")

    def test_ec2_instance_with_tags(self):
        instance = EC2Instance(
            instance_id="i-1234567890abcdef0",
            instance_type="t2.micro",
            state=InstanceState.RUNNING,
            image_id="ami-12345678",
            tags={"Name": "test-instance", "Environment": "dev"}
        )
        self.assertEqual(instance.tags["Name"], "test-instance")


class TestAMIInfo(unittest.TestCase):
    """Test AMIInfo dataclass"""

    def test_ami_info_creation(self):
        ami = AMIInfo(
            image_id="ami-12345678",
            name="test-ami",
            description="Test AMI"
        )
        self.assertEqual(ami.image_id, "ami-12345678")
        self.assertEqual(ami.name, "test-ami")
        self.assertEqual(ami.state, "available")


class TestSecurityGroupInfo(unittest.TestCase):
    """Test SecurityGroupInfo dataclass"""

    def test_security_group_info_creation(self):
        sg = SecurityGroupInfo(
            group_id="sg-12345678",
            group_name="test-sg",
            description="Test security group",
            vpc_id="vpc-12345678"
        )
        self.assertEqual(sg.group_id, "sg-12345678")
        self.assertEqual(sg.group_name, "test-sg")
        self.assertEqual(sg.vpc_id, "vpc-12345678")


class TestKeyPairInfo(unittest.TestCase):
    """Test KeyPairInfo dataclass"""

    def test_key_pair_info_creation(self):
        kp = KeyPairInfo(
            key_name="test-key",
            key_fingerprint="xx:xx:xx:xx"
        )
        self.assertEqual(kp.key_name, "test-key")
        self.assertEqual(kp.key_type, "rsa")


class TestVPCInfo(unittest.TestCase):
    """Test VPCInfo dataclass"""

    def test_vpc_info_creation(self):
        vpc = VPCInfo(
            vpc_id="vpc-12345678",
            cidr_block="10.0.0.0/16"
        )
        self.assertEqual(vpc.vpc_id, "vpc-12345678")
        self.assertEqual(vpc.cidr_block, "10.0.0.0/16")
        self.assertEqual(vpc.state, "available")
        self.assertFalse(vpc.is_default)


class TestSubnetInfo(unittest.TestCase):
    """Test SubnetInfo dataclass"""

    def test_subnet_info_creation(self):
        subnet = SubnetInfo(
            subnet_id="subnet-12345678",
            vpc_id="vpc-12345678",
            cidr_block="10.0.1.0/24",
            availability_zone="us-east-1a"
        )
        self.assertEqual(subnet.subnet_id, "subnet-12345678")
        self.assertEqual(subnet.vpc_id, "vpc-12345678")
        self.assertEqual(subnet.availability_zone, "us-east-1a")


class TestVolumeInfo(unittest.TestCase):
    """Test VolumeInfo dataclass"""

    def test_volume_info_creation(self):
        volume = VolumeInfo(
            volume_id="vol-12345678",
            size=100,
            volume_type=VolumeType.GP3
        )
        self.assertEqual(volume.volume_id, "vol-12345678")
        self.assertEqual(volume.size, 100)
        self.assertEqual(volume.volume_type, VolumeType.GP3)


class TestAutoScalingGroupInfo(unittest.TestCase):
    """Test AutoScalingGroupInfo dataclass"""

    def test_autoscaling_group_info_creation(self):
        asg = AutoScalingGroupInfo(
            auto_scaling_group_name="test-asg",
            min_size=1,
            max_size=5,
            desired_capacity=2,
            vpc_id="vpc-12345678",
            availability_zones=["us-east-1a", "us-east-1b"]
        )
        self.assertEqual(asg.auto_scaling_group_name, "test-asg")
        self.assertEqual(asg.min_size, 1)
        self.assertEqual(asg.max_size, 5)


class TestLoadBalancerInfo(unittest.TestCase):
    """Test LoadBalancerInfo dataclass"""

    def test_load_balancer_info_creation(self):
        lb = LoadBalancerInfo(
            load_balancer_name="test-lb",
            load_balancer_arn="arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/test-lb",
            type=LoadBalancerType.APPLICATION,
            dns_name="test-lb-123456789.us-east-1.elb.amazonaws.com",
            vpc_id="vpc-12345678"
        )
        self.assertEqual(lb.load_balancer_name, "test-lb")
        self.assertEqual(lb.type, LoadBalancerType.APPLICATION)


class TestCloudWatchMetric(unittest.TestCase):
    """Test CloudWatchMetric dataclass"""

    def test_cloudwatch_metric_creation(self):
        metric = CloudWatchMetric(
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            value=75.5,
            unit="Percent"
        )
        self.assertEqual(metric.namespace, "AWS/EC2")
        self.assertEqual(metric.value, 75.5)


class TestScheduleRule(unittest.TestCase):
    """Test ScheduleRule dataclass"""

    def test_schedule_rule_creation(self):
        rule = ScheduleRule(
            rule_name="test-rule",
            instance_ids=["i-12345678"],
            action="start",
            schedule="cron(0 9 * * ? *)"
        )
        self.assertEqual(rule.rule_name, "test-rule")
        self.assertEqual(rule.action, "start")
        self.assertTrue(rule.enabled)


class TestEC2Integration(unittest.TestCase):
    """Test EC2Integration class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ec2_client = MagicMock()
        self.mock_ec2_resource = MagicMock()
        self.mock_autoscaling_client = MagicMock()
        self.mock_elbv2_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        self.mock_events_client = MagicMock()
        self.mock_iam_client = MagicMock()
        
        # Create integration instance with mocked clients
        self.integration = EC2Integration()
        self.integration._client = self.mock_ec2_client
        self.integration._ec2_resource = self.mock_ec2_resource
        self.integration._autoscaling_client = self.mock_autoscaling_client
        self.integration._elbv2_client = self.mock_elbv2_client
        self.integration._cloudwatch_client = self.mock_cloudwatch_client
        self.integration._events_client = self.mock_events_client
        self.integration._iam_client = self.mock_iam_client

    def test_create_instance(self):
        """Test creating EC2 instances"""
        mock_response = {
            "Instances": [
                {
                    "InstanceId": "i-1234567890abcdef0",
                    "InstanceType": "t2.micro",
                    "State": {"Name": "running"},
                    "ImageId": "ami-12345678",
                    "PrivateIpAddress": "10.0.1.100",
                    "PublicIpAddress": "54.123.45.67",
                    "VpcId": "vpc-12345678",
                    "SubnetId": "subnet-12345678",
                    "SecurityGroups": [{"GroupId": "sg-12345678"}],
                    "KeyName": "test-key",
                    "Tags": [{"Key": "Name", "Value": "test-instance"}],
                    "LaunchTime": datetime(2024, 1, 1, 0, 0, 0)
                }
            ]
        }
        self.mock_ec2_client.run_instances.return_value = mock_response
        
        instances = self.integration.create_instance(
            image_id="ami-12345678",
            instance_type="t2.micro",
            key_name="test-key",
            subnet_id="subnet-12345678",
            tags={"Name": "test-instance"}
        )
        
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0].instance_id, "i-1234567890abcdef0")
        self.assertEqual(instances[0].instance_type, "t2.micro")

    def test_get_instance(self):
        """Test getting instance details"""
        mock_response = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-1234567890abcdef0",
                            "InstanceType": "t2.micro",
                            "State": {"Name": "running"},
                            "ImageId": "ami-12345678",
                            "PrivateIpAddress": "10.0.1.100",
                            "VpcId": "vpc-12345678",
                            "SubnetId": "subnet-12345678",
                            "SecurityGroups": [],
                            "Tags": [],
                            "LaunchTime": datetime(2024, 1, 1, 0, 0, 0)
                        }
                    ]
                }
            ]
        }
        self.mock_ec2_client.describe_instances.return_value = mock_response
        
        instance = self.integration.get_instance("i-1234567890abcdef0")
        
        self.assertEqual(instance.instance_id, "i-1234567890abcdef0")
        self.assertEqual(instance.state, InstanceState.RUNNING)

    def test_list_instances(self):
        """Test listing instances"""
        mock_response = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": "i-1234567890abcdef0",
                            "InstanceType": "t2.micro",
                            "State": {"Name": "running"},
                            "ImageId": "ami-12345678",
                            "SecurityGroups": [],
                            "Tags": [],
                            "LaunchTime": datetime(2024, 1, 1, 0, 0, 0)
                        },
                        {
                            "InstanceId": "i-abcdef01234567890",
                            "InstanceType": "t3.large",
                            "State": {"Name": "stopped"},
                            "ImageId": "ami-87654321",
                            "SecurityGroups": [],
                            "Tags": [],
                            "LaunchTime": datetime(2024, 1, 1, 0, 0, 0)
                        }
                    ]
                }
            ]
        }
        self.mock_ec2_client.describe_instances.return_value = mock_response
        
        instances = self.integration.list_instances()
        
        self.assertEqual(len(instances), 2)

    def test_start_instances(self):
        """Test starting instances"""
        mock_response = {
            "StartingInstances": [
                {
                    "InstanceId": "i-1234567890abcdef0",
                    "CurrentState": {"Name": "running"},
                    "PreviousState": {"Name": "stopped"}
                }
            ]
        }
        self.mock_ec2_client.start_instances.return_value = mock_response
        
        with patch.object(self.integration, '_parse_instance') as mock_parse:
            mock_parse.return_value = EC2Instance(
                instance_id="i-1234567890abcdef0",
                instance_type="t2.micro",
                state=InstanceState.RUNNING,
                image_id="ami-12345678"
            )
            instances = self.integration.start_instances(["i-1234567890abcdef0"])
        
        self.assertEqual(len(instances), 1)

    def test_stop_instances(self):
        """Test stopping instances"""
        mock_response = {
            "StoppingInstances": [
                {
                    "InstanceId": "i-1234567890abcdef0",
                    "CurrentState": {"Name": "stopped"},
                    "PreviousState": {"Name": "running"}
                }
            ]
        }
        self.mock_ec2_client.stop_instances.return_value = mock_response
        
        with patch.object(self.integration, '_parse_instance') as mock_parse:
            mock_parse.return_value = EC2Instance(
                instance_id="i-1234567890abcdef0",
                instance_type="t2.micro",
                state=InstanceState.STOPPED,
                image_id="ami-12345678"
            )
            instances = self.integration.stop_instances(["i-1234567890abcdef0"])
        
        self.assertEqual(len(instances), 1)

    def test_terminate_instances(self):
        """Test terminating instances"""
        mock_response = {
            "TerminatingInstances": [
                {
                    "InstanceId": "i-1234567890abcdef0",
                    "CurrentState": {"Name": "terminated"},
                    "PreviousState": {"Name": "running"}
                }
            ]
        }
        self.mock_ec2_client.terminate_instances.return_value = mock_response
        
        with patch.object(self.integration, '_parse_instance') as mock_parse:
            mock_parse.return_value = EC2Instance(
                instance_id="i-1234567890abcdef0",
                instance_type="t2.micro",
                state=InstanceState.TERMINATED,
                image_id="ami-12345678"
            )
            instances = self.integration.terminate_instances(["i-1234567890abcdef0"])
        
        self.assertEqual(len(instances), 1)

    def test_reboot_instances(self):
        """Test rebooting instances"""
        self.mock_ec2_client.reboot_instances.return_value = {}
        
        result = self.integration.reboot_instances(["i-1234567890abcdef0"])
        
        self.assertTrue(result)
        self.mock_ec2_client.reboot_instances.assert_called_once()

    def test_create_ami(self):
        """Test creating AMI from instance"""
        mock_response = {
            "ImageId": "ami-new12345678"
        }
        self.mock_ec2_client.create_image.return_value = mock_response
        
        ami = self.integration.create_ami(
            instance_id="i-1234567890abcdef0",
            name="test-ami",
            description="Test AMI"
        )
        
        self.assertEqual(ami.image_id, "ami-new12345678")
        self.assertEqual(ami.name, "test-ami")

    def test_get_ami(self):
        """Test getting AMI details"""
        mock_response = {
            "Images": [
                {
                    "ImageId": "ami-12345678",
                    "Name": "test-ami",
                    "Description": "Test AMI",
                    "State": "available",
                    "OwnerId": "123456789012",
                    "Architecture": "x86_64",
                    "VirtualizationType": "hvm",
                    "RootDeviceType": "ebs",
                    "Tags": [{"Key": "Name", "Value": "test"}]
                }
            ]
        }
        self.mock_ec2_client.describe_images.return_value = mock_response
        
        ami = self.integration.get_ami("ami-12345678")
        
        self.assertEqual(ami.image_id, "ami-12345678")
        self.assertEqual(ami.name, "test-ami")
        self.assertEqual(ami.state, "available")

    def test_list_amis(self):
        """Test listing AMIs"""
        mock_response = {
            "Images": [
                {
                    "ImageId": "ami-12345678",
                    "Name": "ami-1",
                    "State": "available",
                    "Architecture": "x86_64",
                    "Tags": []
                },
                {
                    "ImageId": "ami-87654321",
                    "Name": "ami-2",
                    "State": "available",
                    "Architecture": "x86_64",
                    "Tags": []
                }
            ]
        }
        self.mock_ec2_client.describe_images.return_value = mock_response
        
        amis = self.integration.list_amis(owner_ids=["123456789012"])
        
        self.assertEqual(len(amis), 2)

    def test_create_security_group(self):
        """Test creating security group"""
        mock_create_response = {"GroupId": "sg-new12345678"}
        mock_describe_response = {
            "SecurityGroups": [{
                "GroupId": "sg-new12345678",
                "GroupName": "test-sg",
                "Description": "Test security group",
                "VpcId": "vpc-12345678",
                "Tags": []
            }]
        }
        self.mock_ec2_client.create_security_group.return_value = mock_create_response
        self.mock_ec2_client.describe_security_groups.return_value = mock_describe_response
        
        sg = self.integration.create_security_group(
            group_name="test-sg",
            description="Test security group",
            vpc_id="vpc-12345678"
        )
        
        self.assertEqual(sg.group_id, "sg-new12345678")
        self.assertEqual(sg.group_name, "test-sg")

    def test_authorize_security_group_ingress(self):
        """Test authorizing security group ingress"""
        self.mock_ec2_client.authorize_security_group_ingress.return_value = {}
        
        ip_permissions = [{
            "IpProtocol": "tcp",
            "FromPort": 80,
            "ToPort": 80,
            "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
        }]
        result = self.integration.authorize_security_group_ingress(
            group_id="sg-12345678",
            ip_permissions=ip_permissions
        )
        
        self.assertTrue(result)

    def test_create_key_pair(self):
        """Test creating key pair"""
        mock_response = {
            "KeyName": "test-key",
            "KeyFingerprint": "xx:xx:xx:xx:xx:xx",
            "KeyMaterial": "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        }
        self.mock_ec2_client.create_key_pair.return_value = mock_response
        
        kp = self.integration.create_key_pair("test-key")
        
        self.assertEqual(kp.key_name, "test-key")
        self.assertIsNotNone(kp.key_material)

    def test_delete_key_pair(self):
        """Test deleting key pair"""
        self.mock_ec2_client.delete_key_pair.return_value = {}
        
        result = self.integration.delete_key_pair("test-key")
        
        self.assertTrue(result)

    def test_import_key_pair(self):
        """Test importing key pair"""
        mock_response = {
            "KeyName": "imported-key",
            "KeyFingerprint": "xx:xx:xx:xx"
        }
        self.mock_ec2_client.import_key_pair.return_value = mock_response
        
        result = self.integration.import_key_pair(
            key_name="imported-key",
            public_key_material=b"ssh-rsa AAAA..."
        )
        
        self.assertTrue(result)

    def test_create_vpc(self):
        """Test creating VPC"""
        vpc_id = "vpc-new12345678"
        mock_create_response = {
            "Vpc": {
                "VpcId": vpc_id,
                "CidrBlock": "10.0.0.0/16",
                "State": "available",
                "IsDefault": False,
                "OwnerId": "123456789012",
                "Tags": []
            }
        }
        mock_describe_response = {
            "Vpcs": [{
                "VpcId": vpc_id,
                "CidrBlock": "10.0.0.0/16",
                "State": "available",
                "IsDefault": False,
                "OwnerId": "123456789012",
                "Tags": []
            }]
        }
        self.mock_ec2_client.create_vpc.return_value = mock_create_response
        self.mock_ec2_client.describe_vpcs.return_value = mock_describe_response
        
        vpc = self.integration.create_vpc(cidr_block="10.0.0.0/16")
        
        self.assertEqual(vpc.vpc_id, vpc_id)
        self.assertEqual(vpc.cidr_block, "10.0.0.0/16")

    def test_create_subnet(self):
        """Test creating subnet"""
        subnet_id = "subnet-new12345678"
        mock_create_response = {
            "Subnet": {
                "SubnetId": subnet_id,
                "VpcId": "vpc-12345678",
                "CidrBlock": "10.0.1.0/24",
                "AvailabilityZone": "us-east-1a",
                "State": "available",
                "MapPublicIpOnLaunch": True,
                "Tags": []
            }
        }
        mock_describe_response = {
            "Subnets": [{
                "SubnetId": subnet_id,
                "VpcId": "vpc-12345678",
                "CidrBlock": "10.0.1.0/24",
                "AvailabilityZone": "us-east-1a",
                "State": "available",
                "MapPublicIpOnLaunch": True,
                "Tags": []
            }]
        }
        self.mock_ec2_client.create_subnet.return_value = mock_create_response
        self.mock_ec2_client.describe_subnets.return_value = mock_describe_response
        
        subnet = self.integration.create_subnet(
            vpc_id="vpc-12345678",
            cidr_block="10.0.1.0/24",
            availability_zone="us-east-1a"
        )
        
        self.assertEqual(subnet.subnet_id, subnet_id)

    def test_create_volume(self):
        """Test creating EBS volume"""
        mock_create_response = {
            "VolumeId": "vol-new12345678",
            "Size": 100,
            "VolumeType": "gp3",
            "State": "available",
            "AvailabilityZone": "us-east-1a",
            "Encrypted": False,
            "SnapshotId": None,
            "Tags": []
        }
        mock_describe_response = {
            "Volumes": [mock_create_response]
        }
        self.mock_ec2_client.create_volume.return_value = mock_create_response
        self.mock_ec2_client.describe_volumes.return_value = mock_describe_response
        
        volume = self.integration.create_volume(
            size=100,
            availability_zone="us-east-1a",
            volume_type=VolumeType.GP3
        )
        
        self.assertEqual(volume.volume_id, "vol-new12345678")
        self.assertEqual(volume.size, 100)

    def test_attach_volume(self):
        """Test attaching volume to instance"""
        self.mock_ec2_client.attach_volume.return_value = {
            "VolumeId": "vol-12345678",
            "InstanceId": "i-1234567890abcdef0",
            "State": "attaching"
        }
        
        result = self.integration.attach_volume(
            volume_id="vol-12345678",
            instance_id="i-1234567890abcdef0",
            device="/dev/sdf"
        )
        
        self.assertTrue(result)

    def test_detach_volume(self):
        """Test detaching volume"""
        self.mock_ec2_client.detach_volume.return_value = {
            "VolumeId": "vol-12345678",
            "State": "detaching"
        }
        
        result = self.integration.detach_volume(volume_id="vol-12345678")
        
        self.assertTrue(result)

    def test_create_snapshot(self):
        """Test creating snapshot"""
        mock_response = {
            "SnapshotId": "snap-new12345678",
            "VolumeId": "vol-12345678",
            "State": "pending",
            "VolumeSize": 100
        }
        self.mock_ec2_client.create_snapshot.return_value = mock_response
        
        snapshot_id = self.integration.create_snapshot(volume_id="vol-12345678")
        
        self.assertEqual(snapshot_id, "snap-new12345678")


class TestEC2IntegrationAutoScaling(unittest.TestCase):
    """Test EC2Integration Auto Scaling functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ec2_client = MagicMock()
        self.mock_autoscaling_client = MagicMock()
        
        self.integration = EC2Integration()
        self.integration._client = self.mock_ec2_client
        self.integration._autoscaling_client = self.mock_autoscaling_client

    def test_create_auto_scaling_group(self):
        """Test creating Auto Scaling group"""
        self.mock_autoscaling_client.create_auto_scaling_group.return_value = {}
        
        result = self.integration.create_auto_scaling_group(
            name="test-asg",
            min_size=1,
            max_size=5,
            vpc_id="vpc-12345678",
            availability_zones=["us-east-1a", "us-east-1b"],
            desired_capacity=2
        )
        
        self.assertTrue(result)

    def test_update_auto_scaling_group(self):
        """Test updating Auto Scaling group"""
        self.mock_autoscaling_client.update_auto_scaling_group.return_value = {}
        
        result = self.integration.update_auto_scaling_group(
            name="test-asg",
            desired_capacity=3
        )
        
        self.assertTrue(result)

    def test_delete_auto_scaling_group(self):
        """Test deleting Auto Scaling group"""
        self.mock_autoscaling_client.delete_auto_scaling_group.return_value = {}
        
        result = self.integration.delete_auto_scaling_group("test-asg")
        
        self.assertTrue(result)

    def test_attach_instances_to_asg(self):
        """Test attaching instances to ASG"""
        self.mock_autoscaling_client.attach_instances.return_value = {}
        
        result = self.integration.attach_instances_to_asg(
            asg_name="test-asg",
            instance_ids=["i-1234567890abcdef0"]
        )
        
        self.assertTrue(result)

    def test_detach_instances_from_asg(self):
        """Test detaching instances from ASG"""
        self.mock_autoscaling_client.detach_instances.return_value = {}
        
        result = self.integration.detach_instances_from_asg(
            asg_name="test-asg",
            instance_ids=["i-1234567890abcdef0"]
        )
        
        self.assertTrue(result)


class TestEC2IntegrationLoadBalancer(unittest.TestCase):
    """Test EC2Integration Load Balancer functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ec2_client = MagicMock()
        self.mock_elbv2_client = MagicMock()
        
        self.integration = EC2Integration()
        self.integration._client = self.mock_ec2_client
        self.integration._elbv2_client = self.mock_elbv2_client

    def test_create_load_balancer(self):
        """Test creating Load Balancer"""
        mock_response = {
            "LoadBalancers": [
                {
                    "LoadBalancerName": "test-lb",
                    "LoadBalancerArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test-lb/abc123",
                    "DNSName": "test-lb-123456789.us-east-1.elb.amazonaws.com",
                    "State": {"Code": "active"},
                    "Type": "application"
                }
            ]
        }
        self.mock_elbv2_client.create_load_balancer.return_value = mock_response
        
        lb = self.integration.create_load_balancer(
            name="test-lb",
            type=LoadBalancerType.APPLICATION,
            subnets=["subnet-12345678", "subnet-87654321"]
        )
        
        self.assertEqual(lb.load_balancer_name, "test-lb")
        self.assertEqual(lb.dns_name, "test-lb-123456789.us-east-1.elb.amazonaws.com")

    def test_create_target_group(self):
        """Test creating target group"""
        mock_response = {
            "TargetGroups": [
                {
                    "TargetGroupName": "test-tg",
                    "TargetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/test-tg/abc123",
                    "Protocol": "HTTP",
                    "Port": 80,
                    "HealthCheckProtocol": "HTTP",
                    "HealthCheckPort": "80"
                }
            ]
        }
        self.mock_elbv2_client.create_target_group.return_value = mock_response
        
        tg_arn = self.integration.create_target_group(
            name="test-tg",
            port=80,
            protocol="HTTP",
            vpc_id="vpc-12345678"
        )
        
        self.assertEqual(tg_arn, "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/test-tg/abc123")


class TestEC2IntegrationCloudWatch(unittest.TestCase):
    """Test EC2Integration CloudWatch functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ec2_client = MagicMock()
        self.mock_cloudwatch_client = MagicMock()
        
        self.integration = EC2Integration()
        self.integration._client = self.mock_ec2_client
        self.integration._cloudwatch_client = self.mock_cloudwatch_client

    def test_put_metric_data(self):
        """Test putting metric data"""
        self.mock_cloudwatch_client.put_metric_data.return_value = {}
        
        metrics = [{
            "MetricName": "CPUUtilization",
            "Value": 75.5,
            "Unit": "Percent"
        }]
        
        result = self.integration.put_metric_data(
            namespace="AWS/EC2",
            metrics=metrics
        )
        
        self.assertTrue(result)

    def test_get_metric_statistics(self):
        """Test getting metric statistics"""
        mock_response = {
            "Label": "CPUUtilization",
            "Datapoints": [
                {"Timestamp": datetime(2024, 1, 1), "Average": 50.0, "Unit": "Percent"},
                {"Timestamp": datetime(2024, 1, 2), "Average": 60.0, "Unit": "Percent"}
            ]
        }
        self.mock_cloudwatch_client.get_metric_statistics.return_value = mock_response
        
        result = self.integration.get_metric_statistics(
            namespace="AWS/EC2",
            metric_name="CPUUtilization",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
            period=3600,
            statistics=["Average"]
        )
        
        self.assertEqual(len(result), 2)


class TestEC2IntegrationScheduling(unittest.TestCase):
    """Test EC2Integration scheduling functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_ec2_client = MagicMock()
        self.mock_events_client = MagicMock()
        self.mock_iam_client = MagicMock()
        
        self.integration = EC2Integration()
        self.integration._client = self.mock_ec2_client
        self.integration._events_client = self.mock_events_client
        self.integration._iam_client = self.mock_iam_client

    def test_create_schedule_rule(self):
        """Test creating schedule rule"""
        mock_rule_response = {
            "RuleArn": "arn:aws:events:us-east-1:123456789012:rule/test-rule"
        }
        mock_target_response = {}
        mock_role_response = {"Role": {"Arn": "arn:aws:iam::123456789012:role/test-role"}}
        self.mock_events_client.put_rule.return_value = mock_rule_response
        self.mock_events_client.put_targets.return_value = mock_target_response
        self.mock_iam_client.get_role.return_value = mock_role_response
        self.mock_iam_client.create_role.return_value = mock_role_response
        
        rule = self.integration.create_schedule_rule(
            name="test-rule",
            schedule="0 9 * * ? *",
            instance_ids=["i-1234567890abcdef0"],
            action="start"
        )
        
        self.assertEqual(rule.rule_name, "test-rule")
        self.assertEqual(rule.action, "start")

    def test_create_start_stop_schedule(self):
        """Test creating start/stop schedule"""
        mock_rule_response = {
            "RuleArn": "arn:aws:events:us-east-1:123456789012:rule/test-rule"
        }
        mock_target_response = {}
        mock_role_response = {"Role": {"Arn": "arn:aws:iam::123456789012:role/test-role"}}
        self.mock_events_client.put_rule.return_value = mock_rule_response
        self.mock_events_client.put_targets.return_value = mock_target_response
        self.mock_iam_client.get_role.return_value = mock_role_response
        self.mock_iam_client.create_role.return_value = mock_role_response
        
        start_rule, stop_rule = self.integration.create_start_stop_schedule(
            name="test-schedule",
            instance_ids=["i-1234567890abcdef0"],
            start_schedule="0 8 * * ? *",
            stop_schedule="0 18 * * ? *"
        )
        
        self.assertEqual(start_rule.action, "start")
        self.assertEqual(stop_rule.action, "stop")


if __name__ == "__main__":
    unittest.main()
